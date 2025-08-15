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

#pragma once
#include "exec_node.h"
#include "proto/db.interface.pb.h"
#include "arrow_io_excutor.h"
#include "sort_node.h"

namespace baikaldb {

#define DB_WARNING_ER(_fmt_, args...) \
    do {\
        DB_WARNING("[log_id:%lu][frag_id:%d][frag_ins_id:%lu][node_id:%d]: " _fmt_, \
                _log_id, _fragment_id, _fragment_instance_id, _node_id, ##args); \
    } while (0);

#define DB_FATAL_ER(_fmt_, args...) \
    do {\
        DB_FATAL("[log_id:%lu][frag_id:%d][frag_ins_id:%lu][node_id:%d], " _fmt_, \
                _log_id, _fragment_id, _fragment_instance_id, _node_id, ##args); \
    } while (0);


class DataStreamManager;
class ExchangeReceiverNode;
class DataStreamReceiver {
public:
    DataStreamReceiver(const uint64_t log_id, const uint64_t fragment_instance_id, const int node_id)
        : _log_id(log_id), _fragment_instance_id(fragment_instance_id), _node_id(node_id) {}
    ~DataStreamReceiver() {}
    int init(RuntimeState* state, ExchangeReceiverNode* node);
    int handle_version_old(const pb::TransmitDataParam& param);
    int add_record_batch(const pb::TransmitDataParam& param, 
                         std::shared_ptr<arrow::RecordBatch> record_batch, 
                         std::shared_ptr<std::string> schema_str,
                         std::shared_ptr<std::string> attachment_str);
    int add_local_pass_through_chunk(uint64_t sender_fragment_instance_id, 
                         const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batch);
    int get_record_batch(std::shared_ptr<arrow::RecordBatch>& record_batch);
    void set_cancelled();
    void set_failed();
    void close();
    bool is_done() {
        return _is_done.load(); 
    }
    bool is_cancelled() {
        return _is_cancelled.load(); 
    }
    bool is_failed() {
        return _is_failed.load(); 
    }
    void set_fragment_id(int32_t fragment_id) {
        _fragment_id = fragment_id; 
    }
    RuntimeState* runtime_state() {
        return _state;
    }

public:
    class SenderQueue;

    std::string to_string() {
        BAIDU_SCOPED_LOCK(_mtx);
        std::string str = std::to_string(_log_id) + 
                                "|" + std::to_string(_fragment_instance_id) + 
                                "|" + std::to_string(_fragment_id) +
                                "|" + std::to_string(_node_id);
        str += ": _should_do_set(";
        for (const auto& item : _should_do_set) {
            str += item + ",";
        }
        str += "), ";
        str += "_done_set:(";
        for (const auto& item : _done_set) {
            str += item + ",";
        }
        // str += "), ";
        // str += "_region_info_map:(";
        // for (const auto& [region_id, region_info] : _region_info_map) {
        //     str += std::to_string(region_id) + "-(" + region_info.ShortDebugString() + "),";
        // }
        str += ")";
        return str;
    }

    inline std::string get_process_key(const pb::TransmitDataParam& param) {
        std::string process_key;
        if (param.has_region_id()) {
            // 数据来自store region
            process_key = STORE_PREFIX + std::to_string(param.region_id());
        } else {
            // 数据来自db
            process_key = DB_PREFIX + std::to_string(param.sender_fragment_instance_id());
        }
        return process_key;
    }

    uint64_t _log_id = 0;
    uint64_t _fragment_instance_id = 0;
    int32_t _fragment_id = 0;
    int32_t _node_id = -1;

    RuntimeState* _state = nullptr;
    ExchangeReceiverNode* _exchange_receiver_node = nullptr;

    bthread::Mutex _mtx;
    // <region_id, RegionInfo>，需要接收region的信息
    std::unordered_map<int64_t, pb::RegionInfo> _region_info_map;
    // 后续支持merge sort时，不同ES发送的数据需放不同的SenderQueue
    // <sender_fragment_instance_id, SenderQueue>
    std::unordered_map<int64_t, std::shared_ptr<SenderQueue>> _sender_queue_map;
    // 需要接收的下游ES集合
    std::unordered_set<std::string> _should_do_set;
    // 已经完成接收的下游ES集合
    std::unordered_set<std::string> _done_set;
    // 已经处理的record_batch序号
    std::unordered_map<std::string, int64_t> _record_batch_seq_map;
    // 是否处理完成
    std::atomic<bool> _is_done = false;
    // 是否已经取消
    std::atomic<bool> _is_cancelled = false;
    // 是否已经失败
    std::atomic<bool> _is_failed = false;

    // 本地传递的record_batch
    // <fragment_instance_id, record_batch>
    std::unordered_map<int64_t, std::queue<std::shared_ptr<arrow::RecordBatch>>> _local_pass_through_record_batchs;

    const std::string DB_PREFIX = "db_";
    const std::string STORE_PREFIX = "store_";
};

class ExchangeReceiverNode : public ExecNode {
public:
    ExchangeReceiverNode() {}
    virtual ~ExchangeReceiverNode() {
        if (_relate_select_manager_node != nullptr) {
            ExecNode::destroy_tree(_relate_select_manager_node);
        }
    }

    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    virtual int build_arrow_declaration(RuntimeState* state);

    virtual bool can_use_arrow_vector(RuntimeState* state) {
        for (auto& c : _children) {
            if (!c->can_use_arrow_vector(state)) {
                return false;
            }
        }
        return true;
    }

    int init_sort_info(SortNode* sort_node) {
        _slot_order_exprs = sort_node->slot_order_exprs();
        _is_asc = sort_node->is_asc();
        _is_null_first = sort_node->is_null_first();
        return 0;
    }

    std::shared_ptr<DataStreamReceiver> get_data_stream_receiver() const {
        return _data_stream_receiver; 
    }

    int get_fragment_id() {
        return _fragment_id;
    }

    void set_sender_fragment_id(int32_t sender_fragment_id) {
        _sender_fragment_id = sender_fragment_id;
    }

    void set_fragment_instance_id(uint64_t fragment_instance_id) {
        _fragment_instance_id = fragment_instance_id;
    }

    void set_node_id(int32_t node_id) {
        _node_id = node_id;
    }

    ExecNode* get_exchange_sender() {
        return _exchange_sender_node;
    }

    ExecNode* get_relate_select_manager_node() {
        return _relate_select_manager_node;
    }
    
    std::shared_ptr<arrow::Schema> get_arrow_schema() {
        return _arrow_schema;
    }

    std::vector<pb::ExchangeDestination>* get_sender_destinations() {
        return &_sender_destinations;
    }

    // 已下只有主db调用
    void set_fragment_id(int32_t fragment_id) {
        _fragment_id = fragment_id;
        _fragment_instance_id = (uint64_t)fragment_id << 32 | 0;
    }

    void set_exchange_sender(ExecNode* exchange_sender) {
        _exchange_sender_node = exchange_sender;
    }

    void set_destination(const std::vector<pb::ExchangeDestination>* es_addresses) {
        if (es_addresses == nullptr) {
            return;
        }
        _sender_destinations = *es_addresses;
    }

    void set_relate_select_manager(ExecNode* relate_select_manager) {
        _relate_select_manager_node = relate_select_manager;
    }

    std::string receiver_key() const {
        return std::to_string(_log_id) + "_" + std::to_string(_fragment_instance_id) + "_" + std::to_string(_node_id);
    }

private:
    uint64_t _log_id = 0; 
    int32_t _fragment_id = 0;
    int32_t _sender_fragment_id = 0;
    uint64_t _fragment_instance_id = 0;      // 该节点所在fragment instance
    int32_t _node_id = 0;                    // exchange node id
    std::vector<pb::ExchangeDestination> _sender_destinations;
    std::shared_ptr<arrow::Schema> _arrow_schema = nullptr;
    std::shared_ptr<DataStreamReceiver> _data_stream_receiver = nullptr;
    std::shared_ptr<BthreadArrowExecutor> _arrow_io_executor;

    // sorter
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;

    // 已下只有主db赋值
    ExecNode* _exchange_sender_node = nullptr; 
    ExecNode* _relate_select_manager_node = nullptr;
};

class ExchangeReceiverVectorizedReader : public arrow::RecordBatchReader {
public:
    ExchangeReceiverVectorizedReader() {}
    virtual ~ExchangeReceiverVectorizedReader() {}
    std::shared_ptr<arrow::Schema> schema() const override { 
        return _arrow_schema;
    }
    int init(RuntimeState* state, ExchangeReceiverNode* exchange_receiver_node);
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

private:
    RuntimeState* _state = nullptr;
    ExchangeReceiverNode* _exchange_receiver_node;
    std::shared_ptr<arrow::Schema> _arrow_schema;
    std::shared_ptr<arrow::RecordBatch> _record_batch;
    int64_t _row_idx_in_record_batch = 0;
    int64_t _processed_row_cnt = 0;
    ExecNode* _fetcher_store_node = nullptr;
};

} // naemspace baikaldb
