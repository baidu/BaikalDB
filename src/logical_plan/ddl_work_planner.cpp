#include "ddl_work_planner.h"
#include "lock_secondary_node.h"
#include <unordered_set>
#include "exec_node.h"
#include "plan_router.h"
#include "separate.h"
#include "rocksdb_scan_node.h"
#include "index_ddl_manager_node.h"

namespace baikaldb {

template<typename Type>
std::unique_ptr<Type> create_generic_manager_node(pb::PlanNodeType node_type) {
    pb::PlanNode pb_manager_node;
    pb_manager_node.set_node_type(node_type);
    pb_manager_node.set_limit(-1);

    std::unique_ptr<Type> manager_node(new (std::nothrow) Type);
    if (manager_node == nullptr) {
        DB_WARNING("create manager_node failed");
        return nullptr;
    }
    manager_node->init(pb_manager_node);
    return manager_node;
}

int create_single_txn(std::unique_ptr<IndexDDLManagerNode> dml_root,
        std::unique_ptr<SingleTxnManagerNode>& txn_manager_node) {
    // create baikaldb commit node
    pb::PlanNode pb_plan_node;
    pb_plan_node.set_node_type(pb::SIGNEL_TXN_MANAGER_NODE);
    pb_plan_node.set_num_children(5);
    pb_plan_node.set_limit(-1);
    txn_manager_node.reset(new (std::nothrow) SingleTxnManagerNode);
    if (txn_manager_node == nullptr) {
        DB_WARNING("create store_txn_node failed");
        return -1;
    }
    txn_manager_node->init(pb_plan_node);

    // create store begin node
    std::unique_ptr<TransactionNode> store_begin_node(Separate().create_txn_node(pb::TXN_BEGIN_STORE));
    if (store_begin_node == nullptr) {
        DB_WARNING("create store_begin_node failed");
        return -1;
    }
    store_begin_node->mutable_pb_node()->set_num_children(0);
    store_begin_node->set_txn_timeout(40 * 1000);      // 整个事务超时时间40s
    store_begin_node->set_txn_lock_timeout(5 * 1000);  // 等锁时间5s
    // create store prepare node
    std::unique_ptr<TransactionNode> store_prepare_node(Separate().create_txn_node(pb::TXN_PREPARE));
    if (store_prepare_node == nullptr) {
        DB_WARNING("create store_prepare_node failed");
        return -1;
    }
    store_prepare_node->mutable_pb_node()->set_num_children(0);
    // create store commit node
    std::unique_ptr<TransactionNode> store_commit_node(Separate().create_txn_node(pb::TXN_COMMIT_STORE));
    if (store_commit_node == nullptr) {
        DB_WARNING("create store_commit_node failed");
        return -1;
    }

    store_commit_node->mutable_pb_node()->set_num_children(0);
    // create store rollback node
    std::unique_ptr<TransactionNode> store_rollback_node(Separate().create_txn_node(pb::TXN_ROLLBACK_STORE));
    if (store_rollback_node == nullptr) {
        DB_WARNING("create store_rollback_node failed");
        return -1;
    }
    store_rollback_node->mutable_pb_node()->set_num_children(0);

    txn_manager_node->add_child(store_begin_node.release());
    txn_manager_node->add_child(dml_root.release());
    txn_manager_node->add_child(store_prepare_node.release());
    txn_manager_node->add_child(store_commit_node.release());
    txn_manager_node->add_child(store_rollback_node.release());
    return 0;
}

int DDLWorkPlanner::create_index_ddl_plan() {
    auto table_ptr = _factory->get_table_info_ptr(_table_id);
    auto table_name = table_ptr->name;
    auto pk_index_ptr = _factory->get_index_info_ptr(_table_id);
    auto index_ptr = _factory->get_index_info_ptr(_index_id);
    if (pk_index_ptr == nullptr || index_ptr == nullptr) {
        DB_FATAL("index or pk index is nullptr.");
        return -1;
    }

    if (_is_global_index && !_factory->is_region_info_exist(index_ptr->id)) {
        DB_FATAL("create global index error.");
        return -1;
    }
    std::unordered_set<int64_t> field_id_set;
    auto create_scan_tuple = [&field_id_set, this, &table_name](IndexInfo* index_ptr) {
        for (const auto& field_info : index_ptr->fields) {
            if (field_id_set.count(field_info.id) == 0) {
                field_id_set.insert(field_info.id);
                DB_NOTICE("insert index_id_%ld field_id_%d", index_ptr->id, field_info.id);
                get_scan_ref_slot(table_name, field_info.table_id, field_info.id, field_info.type);
            }
        }
    };

    create_scan_tuple(pk_index_ptr.get());
    create_scan_tuple(index_ptr.get());
    create_scan_tuple_descs();
    return 0;
}

int DDLWorkPlanner::create_column_ddl_plan() {
    for (auto& tuple_desc : _work.column_ddl_info().tuples()) {
        _scan_tuples.emplace_back(tuple_desc);
        _ctx->add_tuple(tuple_desc);
    }
    return 0;
}


int DDLWorkPlanner::plan() {
    if (_is_column_ddl) {
        create_column_ddl_plan();
    } else {
        if (create_index_ddl_plan() != 0) {
            return -1;
        }
    }
    
    create_scan_nodes();

    // 分配 RuntimeState
    RuntimeState& state = *_ctx->get_runtime_state();
    state.init(_ctx, _ctx->client_conn->send_buf);
    state.set_client_conn(_ctx->client_conn);

    // 设置 limit
    _ctx->plan.mutable_nodes(0)->set_limit(_limit);
    _ctx->plan.mutable_nodes(0)->set_num_children(0);
    return 0;
}

int DDLWorkPlanner::create_txn_dml_node(std::unique_ptr<SingleTxnManagerNode>& txn_node, std::unique_ptr<ScanNode> scan_node) {
    auto manager_node = create_generic_manager_node<IndexDDLManagerNode>(pb::INDEX_DDL_MANAGER_NODE);
    manager_node->set_table_id(_table_id);
    manager_node->set_index_id(_index_id);
    manager_node->set_task_id(_task_id);
    std::map<int64_t, pb::RegionInfo> region_infos =
            static_cast<RocksdbScanNode*>(scan_node.get())->region_infos();
        
    DB_DEBUG("region_info size : %zu", region_infos.size());
    manager_node->set_region_infos(region_infos);
    manager_node->add_child(scan_node.release());
    if (_is_global_index) {
        auto secondary_node_ptr = new (std::nothrow) LockSecondaryNode;
        if (secondary_node_ptr == nullptr) {
            DB_WARNING("create manager_node failed");
            return -1;
        }
        manager_node->set_is_global_index(true);
        pb::PlanNode plan_node;
        plan_node.set_node_type(pb::LOCK_SECONDARY_NODE);
        plan_node.set_num_children(0);
        plan_node.set_limit(_limit);
        plan_node.mutable_derive_node()->mutable_lock_secondary_node()->set_lock_type(
            _is_uniq ? pb::LOCK_GLOBAL_DDL : pb::LOCK_NO_GLOBAL_DDL);
        plan_node.mutable_derive_node()->mutable_lock_secondary_node()->set_global_index_id(_index_id);
        plan_node.mutable_derive_node()->mutable_lock_secondary_node()->set_table_id(
                _table_id);
        plan_node.mutable_derive_node()->mutable_lock_secondary_node()->set_lock_secondary_type(pb::LST_GLOBAL_DDL);
        secondary_node_ptr->init(plan_node);
        manager_node->add_child(secondary_node_ptr);
    }
    if (create_single_txn(std::move(manager_node), txn_node) != 0) {
        DB_WARNING("create signele txn error.");            
        return -1;
    }
    return 0;
}

std::unique_ptr<ScanNode> DDLWorkPlanner::create_scan_node() {
    //plan 已经add_nodes。
    int ret = 0;
    std::unique_ptr<ScanNode> scan_node(ScanNode::create_scan_node(_ctx->plan.nodes(0))); 
    if (scan_node == nullptr) {
        return scan_node;
    }
    scan_node->init(_ctx->plan.nodes(0));
    _ctx->client_conn->txn_id = 0;
    _ctx->client_conn->on_begin();
    _ctx->open_binlog = false;
    _ctx->client_conn->open_binlog = false;

    pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->
        mutable_derive_node()->mutable_scan_node();       

    pb_scan_node->set_lock(pb::LOCK_GET);
    _pos_index.Clear();
    _pos_index.set_index_id(_table_id);
    auto range_index = _pos_index.add_ranges();
    if (_start_key != "") {
        range_index->set_left_key(_start_key);
        range_index->set_left_full(_ddl_pk_key_is_full);
        range_index->set_left_field_cnt(_field_num);
        range_index->set_left_open(false);
    }
    // 暂时用不上
    // if (_end_key != "") {
    //     range_index->set_right_pb_record(_end_key);
    //     range_index->set_right_field_cnt(_field_num);
    //     range_index->set_right_open(true);
    // }
    if (!_is_global_index) {
        if (_is_column_ddl) {
            pb_scan_node->set_ddl_work_type(pb::DDL_COLUMN);
            pb_scan_node->mutable_column_ddl_info()->CopyFrom(_work.column_ddl_info());
        } else {
            pb_scan_node->set_ddl_work_type(pb::DDL_LOCAL_INDEX);
            pb_scan_node->set_ddl_index_id(_index_id);
        }
    } else {
        pb_scan_node->set_ddl_work_type(pb::DDL_GLOBAL_INDEX);
    }
    
    google::protobuf::RepeatedPtrField<pb::RegionInfo> old_region_infos;
    auto old_region_info = old_region_infos.Add();
    old_region_info->set_table_id(_table_id);
    old_region_info->set_partition_id(_partition_id);
    old_region_info->set_start_key(_router_start_key);
    old_region_info->set_end_key(_router_end_key);
    scan_node->set_old_region_infos(std::move(old_region_infos));
    scan_node->set_router_policy(RouterPolicy::RP_REGION);
    // 更新 路由 index信息
    scan_node->serialize_index_and_set_router_index(_pos_index, &_pos_index, true);

    // scan 路由
    ret = PlanRouter().scan_node_analyze(static_cast<RocksdbScanNode*>(scan_node.get()), _ctx, false, {});
    if (ret < 0) {
        DB_FATAL("router plan error.");
        return nullptr;
    }
    return scan_node;
}

int DDLWorkPlanner::execute() {
    bool first_flag = true;
    RuntimeState& state = *_ctx->get_runtime_state();
    auto client_conn = state.client_conn();
    if (!_is_global_index) {
        state.set_single_txn_need_separate_execute(true);
    }
    int retry_times = 0;
    const int MAX_RETRY_TIMES = 20;
    while (true) {
        int ret = 0;

        if (!first_flag) {
            //计算是否需继续请求。
            if (!_is_global_index && (state.ddl_scan_size == 0 || _start_key == state.ddl_max_pk_key)) {
                DB_NOTICE("task_%s scan end, break", _task_id.c_str());
                break;
            } else if (_is_global_index && state.ddl_scan_size < _limit) {
                DB_NOTICE("task_%s num < limit , break", _task_id.c_str());
                break;
            } else {
                DB_DEBUG("update start key.");
                uint64_t log_id = butil::fast_rand();
                state.set_log_id(log_id);
                retry_times = 0;
                _ddl_pk_key_is_full = state.ddl_pk_key_is_full;
                _start_key = state.ddl_max_pk_key;
                _router_start_key = state.ddl_max_router_key;
            }
        } else {
            first_flag = false;
        }

        bool success_flag = true;
        uint64_t log_id = state.log_id();
        do {
            success_flag = true;
            std::unique_ptr<ScanNode> scan_node = create_scan_node();
            if (scan_node == nullptr) {
                DB_FATAL("task_%s logid %lu create scan node error.", _task_id.c_str(), log_id);
                success_flag = false;
                retry_times++;
                continue;
            }
            if (!_is_column_ddl) {
                auto index_info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(_index_id);
                if (index_info_ptr == nullptr ||
                    (index_info_ptr->state != pb::IS_WRITE_LOCAL && index_info_ptr->state != pb::IS_WRITE_ONLY)) {
                    //说明任务已经完成，或者任务失败，该索引正在被删除。
                    DB_FATAL("index info ptr is nullptr or index state is not pb::IS_WRITE_LOCAL/pb::IS_WRITE_ONLY");
                    _work.set_status(pb::DdlWorkFail);
                    return -1;
                }
            }
            state.txn_id = client_conn->txn_id;
            std::unique_ptr<SingleTxnManagerNode> txn_manager_node;
            ret = create_txn_dml_node(txn_manager_node, std::move(scan_node));
            if (ret != 0) {
                DB_FATAL("task_%s logid %lu create txn node error.", _task_id.c_str(), log_id);
                success_flag = false;
                retry_times++;
                continue;
            }
            ret = txn_manager_node->open(_ctx->get_runtime_state().get());
            if (ret == -1) {
                if (state.ddl_error_code == ER_DUP_ENTRY) {
                    DB_FATAL("task_%s logid %lu txn manager node open error: %d ER_DUP_ENTRY.", 
                        _task_id.c_str(), log_id, state.ddl_error_code);
                    _work.set_status(pb::DdlWorkDupUniq);
                    return -1;
                } else {
                    DB_FATAL("task_%s logid %lu txn manager node open error: %d.", 
                        _task_id.c_str(), log_id, state.ddl_error_code);
                }
                success_flag = false;
            }
            if (!success_flag) {
                retry_times++;
                uint64_t log_id = butil::fast_rand();
                state.set_log_id(log_id);
                if (retry_times < MAX_RETRY_TIMES) {
                    bthread_usleep(50 * 1000 * 1000LL);
                }
            }

        } while (retry_times < MAX_RETRY_TIMES && !success_flag);

        if (!success_flag) {
            DB_FATAL("task_%s logid %lu failed retry_times %d error: %d.", 
                        _task_id.c_str(), log_id, retry_times, state.ddl_error_code);
            _work.set_status(pb::DdlWorkFail);
            return -1;
        }
    }
    std::string first_record_str;
    std::string last_record_str;
    if (state.first_record_ptr != nullptr) {
        first_record_str = *state.first_record_ptr;
    }
    if (state.last_record_ptr != nullptr) {
        last_record_str = *state.last_record_ptr;
    }
    DB_NOTICE("task_%s summary: first %s last %s", _task_id.c_str(), first_record_str.c_str(), last_record_str.c_str());
    _work.set_status(pb::DdlWorkDone);
    return 0;
}
} // namespace  aikaldbame
