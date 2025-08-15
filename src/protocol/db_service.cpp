#include "db_service.h"
#include "data_stream_manager.h"
#include "runtime_state.h"
#include "exec_node.h"
#include <arrow/type.h>
#include <arrow/acero/options.h>
#include <arrow/stl_iterator.h>
#include "dual_scan_node.h"
#include "arrow_io_excutor.h"
#include "network_socket.h"
#include "exchange_sender_node.h"
#include "arrow/util/byte_size.h"
#include "joiner.h"

namespace baikaldb {
DEFINE_int64(fragment_timeout_s, 600, "fragment timeout 10 min");
DECLARE_int64(print_time_us);
bvar::LatencyRecorder mpp_fragment_latency {"mpp_fragment_latency"};

#define DB_WARNING_TRANSMIT_DATA(_fmt_, args...) \
    do {\
        DB_WARNING("[log_id:%lu][frag_ins_id:%lu][node_id:%d][remote:%s][region_id:%ld][%s]: " _fmt_, \
            request->log_id(), request->receiver_fragment_instance_id(), \
            request->receiver_node_id(), remote_side.c_str(), request->region_id(), \
            pb::ExchangeState_Name(request->exchange_state()).c_str(), ##args); \
    } while (0);


void DbService::transmit_data(google::protobuf::RpcController* controller,
                              const pb::TransmitDataParam* request,
                              pb::DbResponse* response,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    std::string remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    response->set_errcode(pb::SUCCESS);
    TimeCost cost;
    int64_t attchment_cost = 0;
    int64_t deserize_cost = 0;
    int64_t add_record_batch_cost = 0;
    std::shared_ptr<DataStreamReceiver> receiver = DataStreamManager::get_instance()->get_receiver(
                request->log_id(), request->receiver_fragment_instance_id(), request->receiver_node_id());
    if (receiver == nullptr) {
        {
            std::string key = std::to_string(request->log_id()) 
                                + "_" + std::to_string(request->receiver_fragment_instance_id()) 
                                + "_" + std::to_string(request->receiver_node_id());
            BAIDU_SCOPED_LOCK(_finished_exchange_receiver_keys_lock);
            if (_finished_exchange_receiver_keys.count(key) > 0) {
                // backup request可能上游fragment已经执行结束了
                DB_WARNING_TRANSMIT_DATA("Receiver is already finished success");
                return;
            }
        }
        if (request->exchange_state() != pb::ES_EXEC_FAIL) {
            DB_WARNING_TRANSMIT_DATA("Receiver is nullptr");
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Receiver is nullptr");
        }
        return;
    }
    if (request->exchange_state() == pb::ES_EXEC_FAIL) {
        if (!receiver->is_failed()) {
            DB_WARNING_TRANSMIT_DATA("Exchange state is ES_EXEC_FAIL");
            receiver->set_failed();
        }
        return;
    }
    if (receiver->is_done()) {
        return;
    }
    if (receiver->is_failed() || receiver->is_cancelled()) {
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("Receiver is failed or cancelled");
        return;
    }
    if (request->exchange_state() == pb::ES_VERSION_OLD) {
        DB_WARNING_TRANSMIT_DATA("Exchange state is ES_VERSION_OLD, region_id: %ld, request: %s",
            request->region_id(), request->ShortDebugString().c_str());
        if (receiver->handle_version_old(*request) != 0) {
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Fail to handle_version_old");
        }
        return;
    }
    // local pass through, batch是null
    std::shared_ptr<arrow::RecordBatch> batch;
    int64_t row_size = 0;
    int64_t attachment_size = 0;
    if (!request->is_local_pass_through()) {
        // 从attachment中解析数据
        std::shared_ptr<std::string> attachment_str(new (std::nothrow) std::string());
        if (attachment_str == nullptr) {
            DB_WARNING_TRANSMIT_DATA("Failed to allocate memory for attachment_str");
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Failed to allocate memory for attachment_str");
            return;
        }
        // 从非连续内存的IOBuf拷贝到连续内存，用于装载到arrow::Buffer
        cntl->request_attachment().copy_to(attachment_str.get());
        attchment_cost = cost.get_time();
        cost.reset();

        // 解析列存格式
        std::shared_ptr<std::string> schema_str(new (std::nothrow) std::string());
        if (schema_str == nullptr) {
            DB_WARNING_TRANSMIT_DATA("Failed to allocate memory for schema_str");
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Failed to allocate memory for schema_str");
            return;
        }
        *schema_str = request->vectorized_schema();
        std::shared_ptr<arrow::Schema> schema;
        std::shared_ptr<arrow::Buffer> schema_buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(schema_str->data()), static_cast<int64_t>(schema_str->size()));
        arrow::io::BufferReader schema_reader(schema_buffer);
        auto schema_ret = arrow::ipc::ReadSchema(&schema_reader, nullptr);
        if (schema_ret.ok()) {
            schema = *schema_ret;
        } else {
            DB_WARNING_TRANSMIT_DATA("Fail to ReadSchema, err: %s", schema_ret.status().ToString().c_str());
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Fail to ReadSchema");
            return;
        }
        // 解析列存数据
        std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(attachment_str->data()), static_cast<int64_t>(attachment_str->size()));
        arrow::io::BufferReader buf_reader(buffer);
        arrow::ipc::IpcReadOptions options = arrow::ipc::IpcReadOptions::Defaults();
        options.use_threads = false;
        auto batch_ret = arrow::ipc::ReadRecordBatch(schema, nullptr, options, &buf_reader);
        if (batch_ret.ok()) {
            batch = *batch_ret;
        } else {
            DB_WARNING_TRANSMIT_DATA("Fail to ReadRecordBatch, error: %s", batch_ret.status().ToString().c_str());
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Fail to ReadRecordBatch");
            return;
        }

        deserize_cost = cost.get_time();
        cost.reset();
        int ret = receiver->add_record_batch(*request, batch, schema_str, attachment_str);
        if (ret != 0) {
            DB_WARNING_TRANSMIT_DATA("Fail to add_record_batch");
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Fail to add_record_batch");
            return;
        }
        attachment_size = attachment_str->size();
        row_size = batch->num_rows();
    } else {
        // 直接从本地获取record batch, 只会是db fragment -> db fragment
        int ret = receiver->add_record_batch(*request, nullptr, nullptr, nullptr);
        if (ret != 0) {
            DB_WARNING_TRANSMIT_DATA("Fail to add_record_batch");
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg("Fail to add_record_batch");
            return;
        }
    }
    RuntimeState* receiver_state = receiver->runtime_state();
    if (receiver_state == nullptr) {
        DB_WARNING_TRANSMIT_DATA("receiver runtime state is nullptr");
        response->set_errcode(pb::EXEC_FAIL);
        response->set_errmsg("Receiver runtime state is nullptr");
        return;
    }
    if (request->has_query_stat()) {
        receiver_state->inc_num_affected_rows(request->query_stat().num_affected_rows());
        receiver_state->inc_num_returned_rows(request->query_stat().num_returned_rows());
        receiver_state->inc_num_scan_rows(request->query_stat().num_scan_rows());
        receiver_state->inc_num_filter_rows(request->query_stat().num_filter_rows());
        receiver_state->region_count += request->query_stat().region_count();
        receiver_state->inc_db_handle_rows(request->query_stat().db_handle_rows());
        receiver_state->inc_db_handle_bytes(request->query_stat().db_handle_bytes());
    }
    if (batch != nullptr && request->has_region_id()) {
        receiver_state->inc_db_handle_rows(batch->num_rows());
        receiver_state->inc_db_handle_bytes(arrow::util::TotalBufferSize(*batch));
    }
    add_record_batch_cost = cost.get_time();
    if (attchment_cost + deserize_cost + add_record_batch_cost >= FLAGS_print_time_us) {
        DB_WARNING_TRANSMIT_DATA(
            "add record batch success FROM sender_fragment_instance_id: %ld, local_pass_through: %d, row: %ld, "
            "attchment_cost: %ld, deserize_cost: %ld, add_record_batch_cost: %ld, attachment_size: %lu",
            request->sender_fragment_instance_id(), request->is_local_pass_through(), row_size, 
            attchment_cost, deserize_cost, add_record_batch_cost, attachment_size);
    }
    return;
}

int DbService::init_after_listen() {
    _query_cancel_bth.run([this]() {query_cancel_thread();});
    return 0;
}

void DbService::handle_mpp_dag_fragment(google::protobuf::RpcController* controller,
                              const pb::DAGFragmentRequest* request,
                              pb::DbResponse* response,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    switch (request->op()) {
        case pb::OP_FRAGMENT_START: 
            handle_fragment_start(cntl, request, response, done_guard.release());
            break;
        case pb::OP_FRAGMENT_STOP: 
            handle_fragment_stop(cntl, request, response, done_guard.release());
            break;
    }
}

void DbService::handle_fragment_start(google::protobuf::RpcController* controller,
                                const pb::DAGFragmentRequest* request,
                                pb::DbResponse* response,
                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    std::string remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
    response->set_errcode(pb::SUCCESS);
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    std::unordered_map<uint64_t, pb::FragmentInfo> fragment_need_to_build;
    std::vector<std::shared_ptr<FragmentInfo> > fragment_need_to_exec;

    std::shared_ptr<UserInfo> userinfo = SchemaFactory::get_instance()->get_user_info(request->username());
    if (userinfo == nullptr) {
        DB_FATAL("mpp execute fail: get user info fail, log_id: %lu, username: %s, remote_side: %s", 
            log_id, request->username().c_str(), remote_side.c_str());
        return;
    }
    if (0 != get_all_fragment_instance_id(controller, request, fragment_need_to_build)) {
        DB_FATAL("mpp execute fail: get all fragment instance id fail, log_id: %lu, remote_side: %s", 
            log_id, remote_side.c_str());
        return;
    }
    for (auto& [fragment_instance_id, pb_fragment] : fragment_need_to_build) {
        if (0 != fragment_internal_open(controller, 
                                    request, 
                                    response, 
                                    fragment_instance_id, 
                                    pb_fragment, 
                                    fragment_need_to_exec,
                                    userinfo)) {
            DB_FATAL("mpp execute fail: fragment internal build fail, log_id: %lu, fragment_id: %d, fragment_instance_id: %lu, remote_side: %s", 
                    log_id, pb_fragment.fragment_id(), fragment_instance_id, remote_side.c_str());
            return;
        }
    }
    fragment_internal_exec(fragment_need_to_exec, false);
    return;
}

int DbService::get_all_fragment_instance_id(google::protobuf::RpcController* controller,
                                    const pb::DAGFragmentRequest* request,
                                    std::unordered_map<uint64_t, pb::FragmentInfo>& fragment_need_to_build) {
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    TimeCost cost;
    std::vector<uint64_t> fragment_instance_id_list;
    for (int i = 0; i < request->fragments_size(); i++) {
        const pb::FragmentInfo& pb_fragment = request->fragments(i);
        const pb::Plan& plan = pb_fragment.plan();
        for (int j = 0; j < plan.nodes_size(); j++) {
            const pb::PlanNode& node = plan.nodes(j);
            if (node.node_type() != pb::EXCHANGE_SENDER_NODE) {
                continue;
            }
            // set exchange_sender fragment_instance_id
            for (int k = 0; k < node.derive_node().exchange_sender_node().fragment_addresses_size(); k++) {
                if (node.derive_node().exchange_sender_node().fragment_addresses(k).address() 
                        != SchemaFactory::get_instance()->get_address()) {
                    continue;
                }
                uint64_t fragment_instance_id = node.derive_node().exchange_sender_node().fragment_addresses(k).fragment_instance_id();
                fragment_need_to_build[fragment_instance_id].CopyFrom(pb_fragment);
            }
        }
    }
    int64_t cost_time = cost.get_time();
    if (cost_time > FLAGS_print_time_us) {
        DB_WARNING("log_id: %lu, get all fragment instance id and build plan cost time: %ld, fragments size: %ld",
                 log_id, cost_time, fragment_need_to_build.size());
    }
    return 0;
}

int DbService::handle_fragment_scan_nodes(RuntimeState* state, std::vector<ExecNode*>& scan_nodes) {
    for (auto& node : scan_nodes) {
        // 发到副db的scannode有两种情况
        //  1. index join的非驱动表, 需要进行runtime filter后查询, 这里会多进行一次indexselector和planrouter
        //  2. index join非驱动表子树里的no index join的非驱动表, 需要在这里先进行indexselector和planrouter, 
        //     acero一执行就会直接访问
        auto scan_node = static_cast<RocksdbScanNode*>(node);
        // 不clear select_index_common 会core 
        scan_node->mutable_pb_node()->mutable_derive_node()->mutable_scan_node()->clear_use_indexes();

        // 关联scan node和对应的select manager node
        auto sm_node = static_cast<SelectManagerNode*>(scan_node->get_parent_node(pb::SELECT_MANAGER_NODE));
        if (sm_node == nullptr) {
            DB_FATAL("mpp execute fail: fragment get select manager node fail, log_id: %lu, tuple_id: %d", state->log_id(), scan_node->tuple_id());
            return -1;
        }
        static_cast<RocksdbScanNode*>(scan_node)->set_related_manager_node(sm_node);

        // index selector and plan router
        DB_DEBUG("scannode tuple_id: %d, need planrouter", scan_node->tuple_id());
        bool index_has_null = false;
        if (0 != Joiner::do_plan_router(state, {scan_node}, index_has_null, false)) {
            DB_FATAL("mpp execute fail: fragment plan router fail, log_id: %lu, tuple_id: %d", state->log_id(), scan_node->tuple_id());
            return -1;
        }
        if (index_has_null) {
            sm_node->set_return_empty();
        }
    }
    return 0;
}

int DbService::fragment_internal_open(google::protobuf::RpcController* controller,
                                        const pb::DAGFragmentRequest* request,
                                        pb::DbResponse* response,
                                        uint64_t fragment_instance_id,
                                        const pb::FragmentInfo& pb_fragment,
                                        std::vector<std::shared_ptr<FragmentInfo> >& fragment_need_to_exec,
                                        std::shared_ptr<UserInfo>& userinfo) {
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    std::string err_msg = "";
    std::shared_ptr<FragmentInfo> fragment_info = std::make_shared<FragmentInfo>();
    ExecNode* root = nullptr;
    SmartState state_ptr = std::make_shared<RuntimeState>();
    RuntimeState& state = *state_ptr;
    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side = remote_side_tmp.c_str();
    state.set_remote_side(remote_side);
    int ret = 0;

    ON_SCOPE_EXIT(([this, &log_id, &response, &root, &state, &ret, &err_msg, fragment_instance_id]() {
        if (ret < 0) {
            handle_fragment_fatal(log_id, root, state, true);
            response->set_errcode(pb::EXEC_FAIL);
            response->set_errmsg(err_msg);
            DB_FATAL("mpp execute fail: log_id: %lu, fragment_instance_id: %lu, error: %s", log_id, fragment_instance_id, err_msg.c_str());
        }  
    }));
    ret = state.init(pb_fragment.runtime_state());
    if (ret < 0) {
        err_msg = "RuntimeState init fail";
        return -1;
    }
    state.client_conn()->user_info = userinfo;
    ret = ExecNode::create_tree(pb_fragment.plan(), &root, CreateExecOptions());
    if (ret < 0) {
        err_msg = "create plan fail";
        return -1;
    }
    if (root == nullptr) {
        err_msg = "plan empty fail";
        return -1;
    }
    // 设置所有的exchange node的fragment_instance_id
    std::vector<ExecNode*> exchange_receiver_nodes;
    std::vector<ExecNode*> exchange_sender_nodes;
    root->get_node_pass_subquery(pb::EXCHANGE_RECEIVER_NODE, exchange_receiver_nodes);
    root->get_node_pass_subquery(pb::EXCHANGE_SENDER_NODE, exchange_sender_nodes);
    for (auto& receiver : exchange_receiver_nodes) {
        static_cast<ExchangeReceiverNode*>(receiver)->set_fragment_instance_id(fragment_instance_id);
    }
    for (auto& sender : exchange_sender_nodes) {
        static_cast<ExchangeSenderNode*>(sender)->set_fragment_instance_id(fragment_instance_id);
    }

    // 处理所有的scannode
    std::vector<ExecNode*> scan_nodes;
    root->get_node(pb::SCAN_NODE, scan_nodes);
    if (0 != handle_fragment_scan_nodes(&state, scan_nodes)) {
        err_msg = "handle fragment scan nodes fail";
        return -1;
    }   
    std::vector<ExecNode*> dual_scan_nodes;
    root->get_all_dual_scan_node(dual_scan_nodes);
    for (auto node : dual_scan_nodes) {
        DualScanNode* dual_scan_node = static_cast<DualScanNode*>(node);
        auto sub_query_plan = dual_scan_node->sub_query_node();
        auto sub_query_runtime_state = dual_scan_node->sub_query_runtime_state();
        if (sub_query_plan == nullptr) {
            err_msg = "sub_query_plan is nullptr";
            return -1;
        }
        if (sub_query_runtime_state == nullptr) {
            err_msg = "sub_query_runtime_state is nullptr";
            return -1;
        }
        sub_query_runtime_state->client_conn()->user_info = userinfo;
        std::vector<ExecNode*> derived_scan_nodes;
        sub_query_plan->get_node(pb::SCAN_NODE, derived_scan_nodes);
        if (0 != handle_fragment_scan_nodes(sub_query_runtime_state, derived_scan_nodes)) {
            err_msg = "handle fragment scan nodes in dual scannode fail";
            return -1;
        }
    }

    ret = root->open(&state);
    if (ret < 0) {
        err_msg = "open plan fail";
        return -1;
    }
    ret = root->build_arrow_declaration(&state);
    if (ret != 0) {
        err_msg = "build arrow declaration fail";
        return -1;
    }
    fragment_info->fragment_instance_id = fragment_instance_id;
    fragment_info->smart_state = state_ptr;
    fragment_info->runtime_state = state_ptr.get();
    fragment_info->fragment_id = pb_fragment.fragment_id();
    fragment_info->log_id = log_id;
    fragment_info->root = root;
    fragment_info->set_open_cost();
    fragment_need_to_exec.push_back(fragment_info);
    return 0;
}

void DbService::fragment_internal_exec(std::vector<std::shared_ptr<FragmentInfo> >& fragment_need_to_exec,
                                       bool is_main_db) {
    // 主db,非主db执行逻辑统一在这里
    if (fragment_need_to_exec.size() == 0) {
        return;
    }
    uint64_t log_id = fragment_need_to_exec[0]->log_id;
    int64_t cur_time = butil::gettimeofday_us();
    std::shared_ptr<ExecQueryFragments> batch_fragments = std::make_shared<ExecQueryFragments>(cur_time, is_main_db, fragment_need_to_exec);
    {
        BAIDU_SCOPED_LOCK(_query_start_time_map_lock);
        _query_start_time_map.insert(std::make_pair(log_id, std::make_pair(cur_time, fragment_need_to_exec.size())));
    }
    for (std::shared_ptr<FragmentInfo> fragment_ptr : fragment_need_to_exec) {
        RuntimeState* state = fragment_ptr->runtime_state;
        uint64_t log_id = fragment_ptr->log_id;
        // mpp fragmnet一定是异步的方式执行
        std::shared_ptr<FragmentExecutor> executor = std::make_shared<FragmentExecutor>();
        fragment_ptr->executor = executor;
        executor->future = arrow::acero::DeclarationToTableAsync(arrow::acero::Declaration::Sequence(std::move(state->acero_declarations)), true);
        executor->future.AddCallback([this, log_id, fragment_ptr, cur_time, &mpp_fragment_latency, batch_fragments] (arrow::Result<std::shared_ptr<arrow::Table>> result_) {
            bool destory_all = false;
            ON_SCOPE_EXIT(([this, &fragment_ptr, &result_, &destory_all, batch_fragments]() {
                auto executor = fragment_ptr->executor;
                executor->result = std::move(result_);
                {
                    std::lock_guard<bthread::Mutex> lock(executor->mu);
                    executor->done = true;
                    executor->cond.notify_one();
                }
                if (destory_all) {
                    batch_fragments->close(); // wait and close/destory all
                }
            }));

            RuntimeState& state = *(fragment_ptr->runtime_state);
            ExecNode* root = fragment_ptr->root;
            fragment_ptr->set_exec_cost();
            {   
                BAIDU_SCOPED_LOCK(_query_start_time_map_lock);
                auto iter = _query_start_time_map.find(log_id);
                if (iter != _query_start_time_map.end()) {
                    if (_query_start_time_map[log_id].second <= 1) {
                        _query_start_time_map.erase(log_id);
                    } else {
                        _query_start_time_map[log_id].second--;
                    }
                }
                batch_fragments->doing_cnt--;
                if (batch_fragments->doing_cnt == 0) {
                    destory_all = true;
                }
            }
            if (!result_.ok()) {
                handle_fragment_fatal(log_id, root, state, false);
                DB_FATAL("mpp execute fail: fragment run fail, log_id: %lu, fragment_id: %d, frag_ins_id: %lu, remote_side: %s, open: %lu, exec: %lu, status: %s", 
                        fragment_ptr->log_id,
                        fragment_ptr->fragment_id, 
                        fragment_ptr->fragment_instance_id, 
                        state.remote_side().c_str(),
                        fragment_ptr->open_cost,
                        fragment_ptr->exec_cost,
                        result_.status().ToString().c_str());
                return;
            }
            {
                std::vector<ExecNode*> exchange_receiver_nodes;
                root->get_node_pass_subquery(pb::EXCHANGE_RECEIVER_NODE, exchange_receiver_nodes);
                BAIDU_SCOPED_LOCK(_finished_exchange_receiver_keys_lock);
                for (auto& er : exchange_receiver_nodes) {
                    _finished_exchange_receiver_keys[static_cast<ExchangeReceiverNode*>(er)->receiver_key()] = cur_time;
                }
            }
            std::string exec_type = "parallel";
            if (state.sign_exec_type == SIGN_EXEC_ARROW_FORCE_NO_INDEX_JOIN) {
                exec_type += "|noindexjoin";
            }
            mpp_fragment_latency << fragment_ptr->exec_cost;
            DB_NOTICE("mpp fragment run success: log_id: %lu, fragment_id: %d, frag_ins_id: %lu, exec_type: %s, remote_side: %s, "
                    "open: %lu, exec: %lu, region_cnt: %d, return_row: %ld",
                    fragment_ptr->log_id,
                    fragment_ptr->fragment_id, 
                    fragment_ptr->fragment_instance_id,
                    exec_type.c_str(),
                    state.remote_side().c_str(),
                    fragment_ptr->open_cost,
                    fragment_ptr->exec_cost,
                    state.region_count,
                    state.num_scan_rows());
        });
    }
    return;
}

void DbService::handle_fragment_stop(google::protobuf::RpcController* controller,
                                const pb::DAGFragmentRequest* request,
                                pb::DbResponse* response,
                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }
    response->set_errcode(pb::SUCCESS);
    DataStreamManager::get_instance()->cancel_receivers(log_id);
    {   
        BAIDU_SCOPED_LOCK(_query_start_time_map_lock);
        _query_start_time_map.erase(log_id);
    }
    return;
}

void DbService::handle_fragment_fatal(int64_t log_id, 
                                    ExecNode* root, 
                                    RuntimeState& state,
                                    bool need_destory) {
    DataStreamManager::get_instance()->cancel_receivers(log_id);
    if (root != nullptr 
            && root->node_type() == pb::EXCHANGE_SENDER_NODE) {
        // 执行失败向上游发送fail快速失败
        static_cast<ExchangeSenderNode*>(root)->send_exec_fail();
    }
    if (need_destory) {
        root->close(&state);
        ExecNode::destroy_tree(root);
    }
    {   
        BAIDU_SCOPED_LOCK(_query_start_time_map_lock);
        _query_start_time_map.erase(log_id);
    }
}

void DbService::query_cancel_thread() {
    while (!_shutdown) {
        int64_t cur_time = butil::gettimeofday_us();
        std::vector<uint64_t> to_cancel;
        {
            BAIDU_SCOPED_LOCK(_query_start_time_map_lock);
            for(auto iter = _query_start_time_map.begin(); iter != _query_start_time_map.end();) {
                uint64_t log_id = iter->first;
                int64_t start_time = iter->second.first;
                if (cur_time - start_time > FLAGS_fragment_timeout_s * 1000 * 1000LL) {
                    to_cancel.push_back(log_id);
                    iter = _query_start_time_map.erase(iter);
                } else {
                    ++iter;
                }
            }
        }
        {
            BAIDU_SCOPED_LOCK(_finished_exchange_receiver_keys_lock);
            for (auto iter = _finished_exchange_receiver_keys.begin(); iter != _finished_exchange_receiver_keys.end(); ) {
                if (cur_time - iter->second > FLAGS_fragment_timeout_s * 1000 * 1000LL) {
                    iter = _finished_exchange_receiver_keys.erase(iter);
                } else {
                    ++iter;
                }
            }
        }
        for (auto& log_id : to_cancel) {
            DB_WARNING("mpp fragment exec timeout log_id: %lu", log_id);
            DataStreamManager::get_instance()->cancel_receivers(log_id);
        }
        bthread_usleep_fast_shutdown(10 * 1000 * 1000, _shutdown);
    }
}

} // namespace baikaldb