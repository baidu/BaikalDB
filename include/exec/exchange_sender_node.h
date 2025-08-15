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

#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/light_array_internal.h>

#define DB_WARNING_ES(_fmt_, args...) \
    do {\
        DB_WARNING("[log_id:%lu][frag_id:%d][frag_ins_id:%lu][node_id:%d][region_id:%ld]: " _fmt_, \
                _log_id, _fragment_id, _fragment_instance_id, _node_id, _region_id, ##args); \
    } while (0);

#define DB_FATAL_ES(_fmt_, args...) \
    do {\
        DB_FATAL("[log_id:%lu][frag_id:%d][frag_ins_id:%lu][node_id:%d][region_id:%ld]," _fmt_, \
                _log_id, _fragment_id, _fragment_instance_id, _node_id, _region_id, ##args); \
    } while (0);

#define DB_WARNING_CHANNEL(_fmt_, args...) \
    do {\
        DB_WARNING("[log_id:%lu][frag_ins_id:%ld][region_id:%ld][receiver:%s, frag_ins_id:%lu, node_id:%d]: " _fmt_, \
                _log_id, _sender_fragment_instance_id, _region_id, _receiver_destination.address().c_str(), \
                _receiver_destination.fragment_instance_id(),  \
                _receiver_destination.node_id(), ##args); \
    } while (0);

#define DB_FATAL_CHANNEL(_fmt_, args...) \
    do {\
        DB_FATAL("[log_id:%lu][frag_ins_id:%ld][region_id:%ld][receiver:%s, frag_ins_id:%lu, node_id:%d]: " _fmt_, \
                _log_id, _sender_fragment_instance_id, _region_id, _receiver_destination.address().c_str(), \
                _receiver_destination.fragment_instance_id(),  \
                _receiver_destination.node_id(), ##args); \
    } while (0);


namespace baikaldb {

class ExchangeSenderNode : public ExecNode {
public:
    ExchangeSenderNode() {}
    virtual ~ExchangeSenderNode() {}

    class OnRPCDone : public google::protobuf::Closure {
    public:
        OnRPCDone() {}
        virtual ~OnRPCDone() {}
        virtual void Run() {}
        brpc::Controller cntl;
        pb::DbResponse response;
        std::shared_ptr<arrow::Buffer> data_buffer; // 用于保证在本次rpc中，data buffer的内存不被释放
    };

    class StockRecordBatchKeeper {
    public:
        int add_record_batch(RuntimeState* state, 
                              const std::shared_ptr<arrow::RecordBatch>& record_batch, 
                              bool is_db_fragment,
                              std::vector<std::shared_ptr<arrow::RecordBatch>>& need_send_record_batches, 
                              const pb::ExchangeState exchange_state,
                              bool& need_send_rpc);
    private:
        std::mutex _mtx;
        std::vector<std::shared_ptr<arrow::RecordBatch>> _record_batches; // 保存存量的record batch
        uint64_t _remained_rows = 0;
        uint64_t _remained_bytes = 0;
    };

    class Channel {
    public:
        Channel(const int64_t log_id, 
                const uint64_t sender_fragment_instance_id, 
                const pb::ExchangeDestination& receiver_destination,
                const bool is_from_db,
                const int64_t region_id,
                const bool is_local_pass_through) 
                : _log_id(log_id)
                , _sender_fragment_instance_id(sender_fragment_instance_id)
                , _receiver_destination(receiver_destination)
                , _is_db_fragment(is_from_db)
                , _region_id(region_id)
                , _is_local_pass_through(is_local_pass_through) {}
        ~Channel() {}
        int init();
        int send_record_batch(RuntimeState* state, 
                              const std::shared_ptr<arrow::RecordBatch>& record_batch, 
                              const pb::ExchangeState exchange_state,
                              bool is_send_query_stat);
        int send_record_batch_data(RuntimeState* state, 
                              const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches, 
                              const std::shared_ptr<arrow::Buffer> schema_buffer,
                              const std::shared_ptr<arrow::Buffer> data_buffer,
                              const pb::ExchangeState exchange_state,
                              bool is_send_query_stat,
                              int64_t send_rows);
        int send_record_batch_with_local_pass_through(RuntimeState* state, 
                              const std::shared_ptr<arrow::RecordBatch>& record_batch, 
                              const pb::ExchangeState exchange_state,
                              bool is_send_query_stat);
        int send_version_old(const bool is_merge, const int64_t region_id, 
                             const ::google::protobuf::RepeatedPtrField<pb::RegionInfo>& region_infos);
        int send_exec_fail();
        int wait_last_rpc();
        void close();
    private:
        uint64_t _log_id = 0;
        uint64_t _sender_fragment_instance_id = 0;
        int64_t _region_id = 0;
        pb::ExchangeDestination _receiver_destination;
        bool _is_db_fragment = false;

        int64_t _record_batch_seq = 0;

        std::mutex _rpc_mtx;
        brpc::Channel _channel;
        std::shared_ptr<OnRPCDone> _closure;
        brpc::CallId _call_id;

        // 接收方是自己, 那么发送的数据无需combine/压缩/解压, 直接内存传递
        bool _is_local_pass_through = false;

        StockRecordBatchKeeper _record_batch_keepper;
    };

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

    int init_schema(std::shared_ptr<arrow::Schema> arrow_schema);
    int send_record_batch(RuntimeState* state, 
            std::shared_ptr<arrow::RecordBatch> record_batch, const pb::ExchangeState exchange_state);
    int send_curr_record_batch(RuntimeState* state, std::shared_ptr<arrow::RecordBatch> record_batch);
    int send_eof_record_batch(RuntimeState* state, std::shared_ptr<arrow::RecordBatch> record_batch);
    int send_exec_fail();

    std::shared_ptr<Channel> get_channel(const int bucket) {
        if (bucket >= _channels.size()) {
            return nullptr;
        }
        return _channels[bucket];
    }

    void set_log_id(uint64_t log_id) {
        _log_id = log_id;
    }

    void set_fragment_id(int32_t fragment_id) {
        _fragment_id = fragment_id;
        _fragment_instance_id = (uint64_t)_fragment_id << 32 | 0;
    }

    void set_receiver_fragment_id(int32_t receiver_fragment_id) {
        _receiver_fragment_id = receiver_fragment_id;
    }

    void set_fragment_instance_id(uint64_t fragment_instance_id) {
        _fragment_instance_id = fragment_instance_id;
    }

    void set_node_id(int32_t node_id) {
        _node_id = node_id;
    }

    ExecNode* get_exchange_receiver_node() {
        return _exchange_receiver_node;
    }

    void set_exchange_receiver(ExecNode* receiver) {
        _exchange_receiver_node = receiver;
    }
    
    void add_address(const std::string& address);

    std::vector<pb::ExchangeDestination>* get_fragment_address() {
        return &_addresses;
    }

    void set_destination(const std::vector<pb::ExchangeDestination>* er_addresses) {
        if (er_addresses == nullptr) {
            return;
        }
        for (auto& des : *er_addresses) {
            pb::ExchangeDestination rec_des; 
            rec_des.set_fragment_instance_id(des.fragment_instance_id());
            rec_des.set_address(des.address());
            rec_des.set_node_id(_node_id);
            _receiver_destinations.push_back(std::move(rec_des));
        }
    }

    void add_destination(const std::string& address);

    int64_t get_repartition_cost() {
        return _repartition_cost_us.load();
    }

    int64_t get_repartition_rows() {
        return _repartition_rows.load();
    }
private:
    int init_repartition_param();

    // @brief 将batch按照_hash_indices和_hash_bucket_num进行分区，并返回分区后的batch；
    //        select * from tbl1 join tbl2 on tbl1.id = tbl2.id; 如果tbl1.id和tbl2.id的类型不一致，
    //        需要先都转换成字符串类型，再计算hash值   
    // @param [in]  batch                 : 待分区batch
    //        [out] hash_batch_map        : 分区后的batch
    arrow::Status repartition(std::shared_ptr<arrow::RecordBatch> batch,
                              std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>>& hash_batch_map);

    uint64_t _log_id = 0; 
    int32_t _fragment_id = 0;
    int32_t _receiver_fragment_id = 0;
    uint64_t _fragment_instance_id = 0;
    int32_t _node_id = 0;
    int64_t _region_id = 0;
    std::vector<pb::ExchangeDestination> _receiver_destinations;
    std::shared_ptr<arrow::Schema> _arrow_schema = nullptr;
    
    // 已下主db才存在
    ExecNode* _exchange_receiver_node = nullptr;
    std::vector<pb::ExchangeDestination> _addresses; // 本fragment所在的实例

    bool _is_db_fragment = false;
    std::vector<std::shared_ptr<Channel>> _channels;

    // repartition使用
    int32_t _hash_bucket_num = -1;
    std::vector<int> _hash_indices; // 分区列名
    std::vector<bool> _need_cast_indices; // 分区列是否需要投影到字符串类型
    std::vector<arrow::compute::KeyColumnMetadata> _metadata_vec; // 分区列的arrow元信息

    // 统计信息
    std::atomic<int64_t> _repartition_cost_us {0};
    std::atomic<int64_t> _repartition_rows {0};

    StockRecordBatchKeeper _broadcast_record_batch_keepper; // for broadcast type
};

} // naemspace baikaldb
