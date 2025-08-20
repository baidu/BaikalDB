// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "limit_node.h"
#include "runtime_state.h"
#include "query_context.h"
#include "arrow_exec_node_manager.h"
#include "exchange_receiver_node.h"
#include "vectorize_helpper.h"
#include "data_stream_manager.h"

namespace baikaldb {

class DataStreamReceiver::SenderQueue {
public:
    SenderQueue() {}
    ~SenderQueue() {}
    int get_record_batch(std::shared_ptr<arrow::RecordBatch>& batch);
    int add_record_batch(std::shared_ptr<arrow::RecordBatch> record_batch, 
                         std::shared_ptr<std::string> schema_str,
                         std::shared_ptr<std::string> attachment_str);
    int add_record_batch(std::queue<std::shared_ptr<arrow::RecordBatch>>& batchs,
                         int64_t total_rows_for_check);
    void set_done() {
        std::unique_lock<bthread::Mutex> lck(_mtx);
        _is_done = true;
        _cv.notify_all();
    }
    void set_cancelled() {
        std::unique_lock<bthread::Mutex> lck(_mtx);
        _is_cancelled = true;
        _cv.notify_all();
    }
    void set_failed() {
        std::unique_lock<bthread::Mutex> lck(_mtx);
        _is_failed = true;
        _cv.notify_all();
    }
    void join() {
        std::unique_lock<bthread::Mutex> lck(_mtx);
        while (!_is_done && !_is_cancelled && !_is_failed) {
            _cv.wait(lck);
        }
    }

private:
    bthread::Mutex _mtx;
    bthread::ConditionVariable _cv;
    std::queue<std::shared_ptr<arrow::RecordBatch>> _batches;
    // record_batch的schema内存指向schema_str，需要保留schema_str
    std::vector<std::shared_ptr<std::string>> _schema_str_vec;
    // record_batch的数据内存指向attachment_str，需要保留attachment_str
    std::vector<std::shared_ptr<std::string>> _attachment_str_vec;
    bool _is_done = false;
    bool _is_cancelled = false;
    bool _is_failed = false;
};

int DataStreamReceiver::SenderQueue::get_record_batch(std::shared_ptr<arrow::RecordBatch>& batch) {
    std::unique_lock<bthread::Mutex> lck(_mtx);
    while (!_is_done && !_is_cancelled && !_is_failed && _batches.empty()) {
        _cv.wait(lck);
    }
    if (_is_cancelled) {
        DB_WARNING("SenderQueue is cancelled");
        return -1;
    }
    if (_is_failed) {
        DB_WARNING("SenderQueue is failed");
        return -1;
    }
    if (_batches.empty()) {
        return 0;
    }
    batch = _batches.front();
    _batches.pop();
    return 0;
}

int DataStreamReceiver::SenderQueue::add_record_batch(
        std::shared_ptr<arrow::RecordBatch> batch, 
        std::shared_ptr<std::string> schema_str,
        std::shared_ptr<std::string> attachment_str) {
    std::unique_lock<bthread::Mutex> lck(_mtx);
    _batches.push(batch);
    _schema_str_vec.emplace_back(schema_str);
    _attachment_str_vec.emplace_back(attachment_str);
    _cv.notify_one();
    return 0;
}

int DataStreamReceiver::SenderQueue::add_record_batch(
        std::queue<std::shared_ptr<arrow::RecordBatch>>& batchs,
        int64_t total_rows_for_check) {
    int64_t total_rows = 0;
    std::unique_lock<bthread::Mutex> lck(_mtx);
    while (!batchs.empty()) {
        auto& batch = batchs.front();
        if (batch != nullptr) {
            total_rows += batch->num_rows();
            _batches.push(batch);
        }
        batchs.pop();
    }
    // 可能上一批还没唤醒出来
    // if (total_rows != total_rows_for_check) {
    //     DB_FATAL("local pass through chunk rows check fail: total_rows: %ld != total_rows_for_check: %ld", 
    //             total_rows, total_rows_for_check);
    //     return -1;
    // }
    _cv.notify_one();
    return 0;
}

int DataStreamReceiver::init(RuntimeState* state, ExchangeReceiverNode* node) {
    if (node == nullptr) {
        DB_WARNING("exchange_receiver_node is nullptr");
        return -1;
    }
    _state = state;
    _exchange_receiver_node = node;
    // 构建本次需要接收的下游集合
    pb::ExchangeReceiverNode pb_node = 
        _exchange_receiver_node->pb_node().derive_node().exchange_receiver_node();
    // 下游db es集合，通过fragment_instance_id区分
    for (const auto& sender_destination : *(node->get_sender_destinations())) {
        _should_do_set.insert(DB_PREFIX + std::to_string(sender_destination.fragment_instance_id()));
    }
    // 下游store region集合，通过region_id区分
    if (node->get_relate_select_manager_node() != nullptr) {
        // 主db
        for (const auto& [region_id, region] : node->get_relate_select_manager_node()->region_infos()) {
            _region_info_map[region_id] = region;
            _should_do_set.insert(STORE_PREFIX + std::to_string(region_id));
        }
    } else {
        // 非主db
        for (const auto& region : pb_node.regions()) {
            _region_info_map[region.region_id()] = region;
            _should_do_set.insert(STORE_PREFIX + std::to_string(region.region_id()));
        }
    }
    if (_should_do_set.empty()) {
        DB_FATAL("should_do_set is empty, node_id: %d", _node_id);
        return -1;
    }
    _sender_queue_map[0] = std::make_shared<SenderQueue>();
    if (_sender_queue_map[0] == nullptr) {
        DB_WARNING("sender_queue is nullptr, node_id: %d", _node_id);
        return -1;
    }
    return 0;
}

int DataStreamReceiver::handle_version_old(const pb::TransmitDataParam& param) {
    // es所在region
    pb::RegionInfo region_info;
    const int64_t region_id = param.region_id();
    {
        BAIDU_SCOPED_LOCK(_mtx);
        if (_region_info_map.find(region_id) == _region_info_map.end()) {
            DB_WARNING("Fail to find region_id: %ld", region_id);
            return -1;
        }
        region_info = _region_info_map[region_id];
    }

    // 获取需要新增或者删除的region_info
    ::google::protobuf::RepeatedPtrField<pb::RegionInfo> del_region_infos;
    ::google::protobuf::RepeatedPtrField<pb::RegionInfo> add_region_infos;
    if (param.region_infos().size() >= 2) {
        if (!param.is_merge()) {
            // 分裂场景
            for (const auto& r : param.region_infos()) {
                DB_WARNING("version region: %s", r.ShortDebugString().c_str());
                if (end_key_compare(r.end_key(), region_info.end_key()) > 0) {
                    DB_WARNING("region:%ld r.end_key:%s > info.end_key:%s",
                                r.region_id(),
                                str_to_hex(r.end_key()).c_str(),
                                str_to_hex(region_info.end_key()).c_str());
                    // 其他region会处理该region，这里不需要删除；删除和添加顺序可能导致数据丢失。
                    // *del_region_infos.Add() = r;
                    continue;
                }
                *add_region_infos.Add() = r;
            }
        } else {
            // 合并场景
            for (auto r : param.region_infos()) {
                if (r.region_id() == region_id) {
                    DB_WARNING("merge can`t add this region:%s", r.ShortDebugString().c_str());
                    *del_region_infos.Add() = r;
                    continue;
                }
                DB_WARNING("version region:%s", r.ShortDebugString().c_str());
                *add_region_infos.Add() = r;
            }
        }
    } else if (param.region_infos().size() == 1) {
        const pb::RegionInfo& r = param.region_infos(0);
        if (r.region_id() != region_id) {
            DB_WARNING("not the same region:%s", r.ShortDebugString().c_str());
            return -1;
        }
        if (!(r.start_key() <= region_info.start_key() && end_key_compare(r.end_key(), region_info.end_key()) >= 0)) {
            DB_FATAL("store region not overlap local region, region_id:%ld", region_id);
            return -1;
        }
        *add_region_infos.Add() = r;
    } else {
        DB_WARNING("region_infos size is 0");
        return -1;
    }
    BAIDU_SCOPED_LOCK(_mtx);
    for (const auto& region_info : del_region_infos) {
        if (_region_info_map.find(region_info.region_id()) == _region_info_map.end()) {
            continue;
        }
        // 已经处理过的region，不进行删除
        const std::string process_key = STORE_PREFIX + std::to_string(region_info.region_id());
        if (_done_set.find(process_key) != _done_set.end()) {
            continue;
        }
        _region_info_map.erase(region_info.region_id());
        _should_do_set.erase(process_key);
    }
    // 添加新split的region、添加merge后的region、更新version变化的region
    for (const auto& region_info : add_region_infos) {
        if (_region_info_map.find(region_info.region_id()) != _region_info_map.end() &&
                _region_info_map[region_info.region_id()].version() >= region_info.version()) {
            continue;
        }
        _region_info_map[region_info.region_id()] = region_info;
        _should_do_set.insert(STORE_PREFIX + std::to_string(region_info.region_id()));
    }
    return 0;
}

int DataStreamReceiver::add_record_batch(const pb::TransmitDataParam& param, 
                                         std::shared_ptr<arrow::RecordBatch> record_batch, 
                                         std::shared_ptr<std::string> schema_str,
                                         std::shared_ptr<std::string> attachment_str) {
    // param中存在region_id，表示是store region数据，否则为db数据
    std::string process_key = get_process_key(param);
    if (process_key.empty()) {
        DB_WARNING_ER("get process key failed");
        return -1;
    }
    BAIDU_SCOPED_LOCK(_mtx);
    // 重复数据以第一次为准，merge场景可能重复发送同一个region的数据
    if (_done_set.find(process_key) != _done_set.end()) {
        DB_WARNING_ER("process_key: %s is already done", process_key.c_str());
        if (param.exchange_state() == pb::ExchangeState::ES_EOF) {
            // 检查是否整个任务都完成
            if (_should_do_set == _done_set) {
                _is_done = true;
                _sender_queue_map[0]->set_done();
            }
        }
        return 0;
    }
    if (_record_batch_seq_map.find(process_key) != _record_batch_seq_map.end()) {
        if (_record_batch_seq_map[process_key] >= param.record_batch_seq()) {
            // db ES由于网络问题重试场景，可能重复发送同一个RecordBatch
            DB_WARNING_ER("process_key: %s, record_batch seq[%ld] is already processed", 
                        process_key.c_str(), _record_batch_seq_map[process_key]);
            return 0;
        } else if (_record_batch_seq_map[process_key] + 1 != param.record_batch_seq()) {
            // 异常数据，返回错误
            DB_FATAL_ER("process_key: %s, record_batch not continue, [%ld] vs [%ld]", 
                      process_key.c_str(), _record_batch_seq_map[process_key], param.record_batch_seq());
            return -1;
        }
    }
    // 判断region version是否一致
    if (param.has_region_id()) {
        if (_region_info_map.find(param.region_id()) == _region_info_map.end()) {
            DB_WARNING_ER("region_id: %ld is not in region_info_map", param.region_id());
            return -1;
        }
        if (_region_info_map[param.region_id()].version() != param.region_version()) {
            DB_WARNING_ER("region_id: %ld version is not equal, %ld vs %ld", 
                        param.region_id(), _region_info_map[param.region_id()].version(), param.region_version());
            return -1;
        }
    }
    // 异常数据，返回错误
    if (_should_do_set.find(process_key) == _should_do_set.end()) {
        DB_FATAL_ER("should not do this process_key: %s", process_key.c_str());
        return -1;
    }
    if (param.is_local_pass_through()) {
        auto iter = _local_pass_through_record_batchs.find(param.sender_fragment_instance_id());
        if (iter == _local_pass_through_record_batchs.end()) {
            DB_FATAL_ER("local pass through record batch not found, sender_fragment_instance_id: %ld", 
                        param.sender_fragment_instance_id());
            return -1;
        }
        if ( 0 != _sender_queue_map[0]->add_record_batch(iter->second, param.record_batch_rows())) {
            DB_FATAL_ER("add local pass through record batch failed, sender_fragment_instance_id: %ld", 
                        param.sender_fragment_instance_id());
            _is_failed = true;
            _sender_queue_map[0]->set_failed();
            return -1;
        }
    } else if (record_batch != nullptr && record_batch->num_rows() != 0) {
        _sender_queue_map[0]->add_record_batch(record_batch, schema_str, attachment_str);
    }
    _record_batch_seq_map[process_key] = param.record_batch_seq();
    if (param.exchange_state() == pb::ExchangeState::ES_EOF) {
        // 设置当前ES状态
        _done_set.insert(process_key);
        // 检查是否整个任务都完成
        if (_should_do_set == _done_set) {
            _is_done = true;
            _sender_queue_map[0]->set_done();
        }
    }
    return 0;
}

int DataStreamReceiver::add_local_pass_through_chunk(uint64_t sender_fragment_instance_id, 
                        const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batchs) {
    BAIDU_SCOPED_LOCK(_mtx);
    _local_pass_through_record_batchs[sender_fragment_instance_id];
    if (record_batchs.empty()) {
        return 0;
    }
    for (auto& record_batch : record_batchs) {
        if (record_batch != nullptr) {
            DB_DEBUG("local pass through record batch num rows: %ld, _fragment_instance_id: %lu, sender_fragment_instance_id: %lu", 
                record_batch->num_rows(), _fragment_instance_id, sender_fragment_instance_id);  
            _local_pass_through_record_batchs[sender_fragment_instance_id].push(record_batch);
        }
    } 
    return 0;
}

int DataStreamReceiver::get_record_batch(std::shared_ptr<arrow::RecordBatch>& record_batch) {
    if (_sender_queue_map[0]->get_record_batch(record_batch) != 0) {
        DB_WARNING("Fail to get_record_batch");
        return -1;
    }
    return 0;
}

void DataStreamReceiver::set_cancelled() {
    BAIDU_SCOPED_LOCK(_mtx);
    _is_cancelled = true;
    _sender_queue_map[0]->set_cancelled();
}

void DataStreamReceiver::set_failed() {
    BAIDU_SCOPED_LOCK(_mtx);
    _is_failed = true;
    _sender_queue_map[0]->set_failed();
}

void DataStreamReceiver::close() {
    for (auto& [_, send_queue] : _sender_queue_map) {
        if (send_queue != nullptr) {
            send_queue->join();
        }
    }
    DataStreamManager::get_instance()->remove_receiver(_log_id, _fragment_instance_id, _node_id);
}

int ExchangeReceiverNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    // 主db此时fragment_id和fragment_instance_id都为0, 构建计划的时候会设置
    // 非主db此时fragment_instance_id为0, open之前需要单独设置
    const pb::ExchangeReceiverNode& er_node = node.derive_node().exchange_receiver_node();
    _log_id = er_node.log_id();
    _fragment_id = er_node.fragment_id();
    _sender_fragment_id = er_node.sender_fragment_id();
    _node_id = er_node.node_id();
    //_fragment_instance_id = er_node.fragment_instance_id();
    _partition_property.type = er_node.partition_property().type();
    for (auto& des : er_node.sender_destinations()) {
        _sender_destinations.emplace_back(des);
    }
    // 解析schema
    const auto& pb_schema = er_node.schema();
    if (pb_schema.size() > 0) {
        std::shared_ptr<arrow::Buffer> schema_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(pb_schema.data()),
                                                                                   static_cast<int64_t>(pb_schema.size()));
        arrow::io::BufferReader schema_reader(schema_buffer);
        auto schema_ret = arrow::ipc::ReadSchema(&schema_reader, nullptr);
        if (schema_ret.ok()) {
            _arrow_schema = *schema_ret;
        } else {
            DB_WARNING("parser from schema error [%s]. ", schema_ret.status().ToString().c_str());
            return -1;
        }
    }
    // sorter
    for (auto& expr : er_node.slot_order_exprs()) {
        ExprNode* slot_order_expr = nullptr;
        ret = ExprNode::create_tree(expr, &slot_order_expr);
        if (ret < 0) {
            DB_FATAL("create slot order expr fail");
            ExprNode::destroy_tree(slot_order_expr);
            return ret;
        }
        _slot_order_exprs.push_back(slot_order_expr);
    }
    for (bool is_asc : er_node.is_asc()) {
        _is_asc.push_back(is_asc);
    }
    for (bool is_null_first : er_node.is_null_first()) {
        _is_null_first.push_back(is_null_first);
    }
    return 0;
}

int ExchangeReceiverNode::open(RuntimeState* state) {
    int ret = ExecNode::open(state);
    if (ret < 0) {
        DB_FATAL_ER("ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    _data_stream_receiver = 
        DataStreamManager::get_instance()->create_receiver(_log_id,
                                                           _fragment_instance_id,
                                                           _node_id,
                                                           state,
                                                           this);
    if (_data_stream_receiver == nullptr) {
        DB_FATAL_ER("Fail to create data stream receiver");
        return -1;
    }
    _data_stream_receiver->set_fragment_id(_fragment_id);

    // 主db的schema和region在构建物理计划的时候生成, pb里没有
    // 非主db的schema和region在init(pb)的时候生成
    if (_exchange_sender_node != nullptr && _exchange_sender_node->children_size() > 0) {
        auto data_schema = _exchange_sender_node->children(0)->data_schema();
        _arrow_schema = VectorizeHelpper::get_arrow_schema(data_schema);
    }
    if (_arrow_schema == nullptr) {
        DB_FATAL_ER("transfer receiver arrow schema fail");
        return -1;
    }
    set_node_exec_type(pb::EXEC_ARROW_ACERO);
    return 0;
}

int ExchangeReceiverNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    // 应该不需要往下get_next, 直接转向量化执行
    // int ret = 0;
    // ret = _children[0]->get_next(state, batch, eos);
    // if (ret < 0) {
    //     DB_WARNING("_children get_next fail");
    //     return ret;
    // }
    set_node_exec_type(pb::EXEC_ARROW_ACERO);
    *eos = true;
    return 0;
}
 
void ExchangeReceiverNode::close(RuntimeState* state) {
    ExecNode::close(state);
    if (_data_stream_receiver != nullptr) {
        _data_stream_receiver->close();
    }
    if (_relate_select_manager_node != nullptr) {
        _relate_select_manager_node->close(state);
    }
    _sender_destinations.clear();
    if (_pb_node.derive_node().exchange_receiver_node().slot_order_exprs_size() > 0) {
        for (auto& expr : _slot_order_exprs) {
            ExprNode::destroy_tree(expr);
        }
    }
    if (_arrow_io_executor != nullptr) {
        _arrow_io_executor.reset();
    }
}

void ExchangeReceiverNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto er_node = pb_node->mutable_derive_node()->mutable_exchange_receiver_node();
    er_node->set_log_id(_log_id);
    er_node->set_fragment_id(_fragment_id);
    er_node->set_sender_fragment_id(_sender_fragment_id);
    er_node->set_fragment_instance_id(_fragment_instance_id);
    er_node->set_node_id(_node_id);
    er_node->mutable_partition_property()->set_type(_partition_property.type);
    for (auto& sender_desc : _sender_destinations) {
        auto pb_des = er_node->add_sender_destinations();
        *pb_des = sender_desc;
    }
    
    if (_relate_select_manager_node != nullptr) {
        auto regions = _relate_select_manager_node->region_infos();
        if (regions.empty()) {
            DB_FATAL_ER("relate select manager node region is empty");
            return;
        }
        for (auto& region : regions) {
            // TODO 如果region多, 改成[region_id, version]? 确认实际执行用了region info哪些信息
            er_node->add_regions()->CopyFrom(region.second);
        }
    }
    // 序列化schema
    if (_exchange_sender_node == nullptr || _exchange_sender_node->children(0) == nullptr) {
        DB_FATAL_ER("exchange sender node is null");
        return;
    }
    auto data_schema = _exchange_sender_node->children(0)->data_schema();
    _arrow_schema = VectorizeHelpper::get_arrow_schema(data_schema);
    if (_arrow_schema == nullptr) {
        DB_FATAL_ER("transfer receiver arrow schema fail");
        return;
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = arrow::ipc::SerializeSchema(*_arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL_ER("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return;
    }
    er_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());

    // sorter
    for (ExprNode* expr : _slot_order_exprs) {
        ExprNode::create_pb_expr(er_node->add_slot_order_exprs(), expr);
    }
    for (bool asc : _is_asc) {
        er_node->add_is_asc(asc);
    }
    for (bool null_first : _is_null_first) {
        er_node->add_is_null_first(null_first);
    }
    return;
}

int ExchangeReceiverNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    int ret = 0;
    // add SourceNode
    std::shared_ptr<ExchangeReceiverVectorizedReader> reader = std::make_shared<ExchangeReceiverVectorizedReader>();
    ret = reader->init(state, this);
    if (ret != 0) {
        return -1;
    }
    std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [reader] () {
        arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(reader);
        return batch_it;
    };
    auto executor = BthreadArrowExecutor::Make(1);
    _arrow_io_executor = *executor;
    arrow::acero::Declaration dec{"record_batch_source",
        arrow::acero::RecordBatchSourceNodeOptions{reader->schema(), std::move(iter_maker), _arrow_io_executor.get()}};
    LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, reader->schema(), nullptr);
    state->append_acero_declaration(dec);

    if (!_slot_order_exprs.empty()) {
        std::vector<arrow::compute::SortKey> sort_keys;
        sort_keys.reserve(_slot_order_exprs.size());
        for (int i = 0; i < _slot_order_exprs.size(); ++i) {
            int ret = _slot_order_exprs[i]->transfer_to_arrow_expression();
            if (ret != 0 || _slot_order_exprs[i]->arrow_expr().field_ref() == nullptr) {
                DB_FATAL_STATE(state, "get sort field ref fail, maybe is not slot ref");
                return -1;
            }
            auto field_ref = _slot_order_exprs[i]->arrow_expr().field_ref();
            sort_keys.emplace_back(*field_ref, _is_asc[i] ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending);
        }
        arrow::compute::Ordering ordering{sort_keys, 
                                      _is_asc[0] ? arrow::compute::NullPlacement::AtStart : arrow::compute::NullPlacement::AtEnd};
        if (_limit < 0) {
            arrow::acero::Declaration dec{"order_by", arrow::acero::OrderByNodeOptions{ordering}};
            LOCAL_TRACE_ARROW_PLAN(dec);
            state->append_acero_declaration(dec);
        } else {
            arrow::acero::Declaration dec{"topk", TopKNodeOptions{ordering, _limit}};
            LOCAL_TRACE_ARROW_PLAN(dec);
            state->append_acero_declaration(dec);
        }
    }
    return 0;
}

int ExchangeReceiverVectorizedReader::init(RuntimeState* state, ExchangeReceiverNode* exchange_receiver_node) {
    _state = state;
    if (_state == nullptr) {
        DB_WARNING("_state is nullptr");
        return -1;
    }
    _exchange_receiver_node = exchange_receiver_node;
    if (_exchange_receiver_node == nullptr) {
        DB_WARNING("_exchange_receiver_node is nullptr");
        return -1;
    }
    // 获取schema
    _arrow_schema = _exchange_receiver_node->get_arrow_schema();
    if (_arrow_schema == nullptr) {
        DB_WARNING("arrow schema is null");
        return -1;
    }
    _fetcher_store_node = _exchange_receiver_node->get_relate_select_manager_node();
    return 0;
}

arrow::Status ExchangeReceiverVectorizedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    out->reset();
    const int64_t limit = _exchange_receiver_node->get_limit();
    std::shared_ptr<DataStreamReceiver> data_stream_receiver = _exchange_receiver_node->get_data_stream_receiver();
    if (data_stream_receiver == nullptr) {
        DB_FATAL_STATE(_state, "Fail to get data stream receiver");
        return arrow::Status::IOError("data_stream_receiver is nullptr");
    }
    if (_fetcher_store_node != nullptr) {
        if (_fetcher_store_node->open(_state) < 0) {
            DB_FATAL_STATE(_state, "open select manager node fail");
            return arrow::Status::IOError("fetcher store fail");
        }
        _fetcher_store_node = nullptr;
    }
    if (_state->is_cancelled()) {
        DB_WARNING_STATE(_state, "cancelled");
        return arrow::Status::OK();
    }
    if (limit > 0 && _processed_row_cnt >= limit) {
        return arrow::Status::OK();
    }
    if (_record_batch == nullptr) {
        std::shared_ptr<arrow::RecordBatch> record_batch;
        int ret = data_stream_receiver->get_record_batch(record_batch);
        if (ret < 0) {
            DB_FATAL_STATE(_state, "Fail to get_record_batch");
            return arrow::Status::IOError("Fail to get_record_batch");
        }
        // record_batch为空，表示eof
        if (record_batch == nullptr) {
            return arrow::Status::OK();
        }
        // er的schema是构建计划时生成，es的schema是acero执行时生成，两者可能不一致，需要进行转化
        // 比如，在acero执行过程中，agg节点会改变列顺序
        if (_arrow_schema->Equals(record_batch->schema())) {
            _record_batch = record_batch;
        } else {
            if (VectorizeHelpper::change_arrow_record_batch_schema(
                    _arrow_schema, record_batch, &_record_batch, true) != 0) {
                return arrow::Status::IOError("change arrow record batch schema fail");
            }
        }
    }
    if (_record_batch != nullptr) {
        int64_t slice_cnt = FLAGS_chunk_size;
        if (limit > 0 && _processed_row_cnt + slice_cnt >= limit) {
            slice_cnt = limit - _processed_row_cnt;
        }
        *out = _record_batch->Slice(_row_idx_in_record_batch, slice_cnt);
        _processed_row_cnt += (*out)->num_rows();
        _row_idx_in_record_batch += (*out)->num_rows();
        if (_record_batch->num_rows() <= _row_idx_in_record_batch) {
            _record_batch.reset();
            _row_idx_in_record_batch = 0;
        }
    }
    return arrow::Status::OK();
}

} // naemspace baikaldb
