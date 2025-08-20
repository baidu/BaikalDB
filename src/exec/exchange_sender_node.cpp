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

#include "exchange_sender_node.h"
#include "data_stream_manager.h"
#include "exchange_receiver_node.h"
#include "runtime_state.h"
#include "query_context.h"
#include "arrow_exec_node_manager.h"
#include "db.interface.pb.h"
#include "vectorize_helpper.h"
#include "arrow/util/compression.h"
#include "arrow/util/byte_size.h"
#include "vectorize_helpper.h"
#include <arrow/compute/key_hash_internal.h>

namespace baikaldb {

DEFINE_int32(exchange_max_retry, 1, "exchange max retry, default:1");
DEFINE_int32(exchange_connect_timeout, 3000, "exchange connect timeout, default:3000ms");
DEFINE_int32(exchange_request_timeout, 30000, "exchange request timeout, default:30000ms");
DEFINE_int32(compression_type, 6, "record batch compression type, UNCOMPRESSED: 0, ZSTD: 4, LZ4_FRAME: 6");
DEFINE_int32(exchange_send_max_row_count_per_rpc, 100000, "exchange send max row count per rpc, default: 10w");
DEFINE_int64(exchange_send_max_bytes_per_rpc, 262144, "exchange send max bytes per rpc, default: 256KB");
DECLARE_int64(print_time_us);

std::unordered_map<int, double> COMPRESSION_RATIO = {
    {0, 1.0}, {4, 2.884}, {6, 2.101}
};

int ExchangeSenderNode::Channel::init() {
    brpc::ChannelOptions option;
    option.max_retry = FLAGS_exchange_max_retry;
    option.connect_timeout_ms = FLAGS_exchange_connect_timeout;
    option.timeout_ms = FLAGS_exchange_request_timeout;
    if (_channel.Init(_receiver_destination.address().c_str(), &option) != 0) {
        DB_WARNING("Fail to init channel");
        return -1;
    }
    return 0;
}

// AceroExchangeSenderNode可能并发调用该接口，需要并发保护
int ExchangeSenderNode::Channel::send_record_batch(RuntimeState* state, 
                                                   const std::shared_ptr<arrow::RecordBatch>& record_batch, 
                                                   const pb::ExchangeState exchange_state,
                                                   bool is_send_query_stat) {
    if (record_batch == nullptr) {
        DB_WARNING_CHANNEL("record_batch is nullptr");
        return -1;
    }
    if (state == nullptr) {
        DB_WARNING_CHANNEL("state is nullptr");
        return -1;
    }
    // 攒一批数据发送
    std::vector<std::shared_ptr<arrow::RecordBatch>> need_send_record_batches;
    bool need_send_rpc = false;
    if (0 != _record_batch_keepper.add_record_batch(state, 
                        record_batch,    // 新到的record batch
                        _is_db_fragment,
                        need_send_record_batches, 
                        exchange_state,
                        need_send_rpc)) {
        DB_WARNING_CHANNEL("get need send record batches fail");
        return -1;
    }
    if (!need_send_rpc) {
        return 0;
    }
    TimeCost time_cost;
    int64_t combine_cost = 0;
    int64_t serialize_cost = 0;
    int64_t send_cost = 0;
    int64_t attachment_size = 0;
    int64_t send_rows = 0;

    std::shared_ptr<arrow::RecordBatch> concatenate_record_batch;
    std::shared_ptr<arrow::Buffer> schema_buffer;
    std::shared_ptr<arrow::Buffer> data_buffer;
    if (!_is_local_pass_through) { 
        // 合并
        if (0 != VectorizeHelpper::concatenate_record_batches(
                        record_batch->schema(), 
                        need_send_record_batches, 
                        concatenate_record_batch)) {
            DB_FATAL_CHANNEL("Fail to concatenate record batches");
            return -1;
        }
        combine_cost = time_cost.get_time();
        time_cost.reset();
        send_rows = concatenate_record_batch->num_rows();
        // 序列化
        if (0 != VectorizeHelpper::serialize_record_batch(
                        concatenate_record_batch, 
                        schema_buffer, 
                        data_buffer,
                        static_cast<arrow::Compression::type>(FLAGS_compression_type))) {
            DB_FATAL_CHANNEL("Fail to serialize record batches");
            return -1;
        }
        serialize_cost = time_cost.get_time();
        time_cost.reset();
        attachment_size = data_buffer->size();
    } else {
        for (auto& batch : need_send_record_batches) {
            send_rows += batch->num_rows();
        }
    }
    // 发送
    if (0 != send_record_batch_data(state, 
                                need_send_record_batches, 
                                schema_buffer, 
                                data_buffer, 
                                exchange_state, 
                                is_send_query_stat, 
                                send_rows)) {
        DB_WARNING_CHANNEL("Fail to send record batches");
        return -1;
    }
    send_cost = time_cost.get_time();
    if (combine_cost + serialize_cost + send_cost > FLAGS_print_time_us) {
        DB_WARNING_CHANNEL("combine: %ld, serialize: %ld, send: %ld, rows: %ld, attachment_size: %ld, local_pass_through: %d",
                            combine_cost, serialize_cost, send_cost, send_rows, attachment_size, _is_local_pass_through);
    }
    return 0;
}

int ExchangeSenderNode::StockRecordBatchKeeper::add_record_batch(RuntimeState* state, 
                              const std::shared_ptr<arrow::RecordBatch>& record_batch, 
                              bool is_db_fragment,
                              std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches, 
                              const pb::ExchangeState exchange_state,
                              bool& need_send_rpc) {
    need_send_rpc = false;
    if (exchange_state == pb::ExchangeState::ES_EOF) {
        // EOF时，把剩余数据一起发送
        std::lock_guard<std::mutex> lock(_mtx);
        _record_batches.swap(record_batches);
        need_send_rpc = true;
    } else {
        // 非EOF时，攒一批数据发送
        std::lock_guard<std::mutex> lock(_mtx);
        _record_batches.emplace_back(record_batch);
        if (is_db_fragment) {
            // db的ES数据分批发送
            _remained_rows += record_batch->num_rows();
            double compression_ratio = COMPRESSION_RATIO[FLAGS_compression_type];
            if (compression_ratio <= 0) {
                compression_ratio = 1.0;
            }
            // 粗略的估计压缩后的大小
            _remained_bytes += arrow::util::TotalBufferSize(*record_batch) / compression_ratio;
            if (_remained_rows >= FLAGS_exchange_send_max_row_count_per_rpc) {
                _record_batches.swap(record_batches);
            } else if (_remained_bytes >= FLAGS_exchange_send_max_bytes_per_rpc) {
                _record_batches.swap(record_batches);
            }
        } else {
            // store的ES整个region数据最后一起发送
        }
        if (record_batches.empty()) {
            return 0;
        }
        _remained_bytes = 0;
        _remained_rows = 0;
        need_send_rpc = true;
    }
    return 0;
}

// 真正发送数据
int ExchangeSenderNode::Channel::send_record_batch_data(RuntimeState* state, 
                        const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
                        const std::shared_ptr<arrow::Buffer> schema_buffer,
                        const std::shared_ptr<arrow::Buffer> data_buffer,
                        const pb::ExchangeState exchange_state,
                        bool is_send_query_stat,
                        int64_t send_rows) {
    // 构造request
    pb::TransmitDataParam request;
    request.set_exchange_state(exchange_state);
    request.set_log_id(_log_id);
    request.set_sender_fragment_instance_id(_sender_fragment_instance_id);
    request.set_receiver_fragment_instance_id(_receiver_destination.fragment_instance_id());
    request.set_receiver_node_id(_receiver_destination.node_id());
    request.set_is_local_pass_through(_is_local_pass_through);
    if (!_is_db_fragment) {
        request.set_region_id(state->region_id());
        request.set_region_version(state->region_version());
    }
    butil::IOBuf data;
    // 处理chunk数据
    if (!_is_local_pass_through) { 
        if (data_buffer == nullptr || schema_buffer == nullptr) {
            DB_WARNING_CHANNEL("data_buffer or schema_buffer is nullptr");
            return -1;
        }
        // schema
        request.set_vectorized_schema(schema_buffer->data(), schema_buffer->size());
        // 零拷贝，需要维护data_ret的生命周期
        data.append_user_data(const_cast<void*>(reinterpret_cast<const void*>(data_buffer->data())), data_buffer->size(), [] (void*) {});
    } else {
        std::shared_ptr<DataStreamReceiver> receiver = DataStreamManager::get_instance()->get_receiver(
                _log_id, _receiver_destination.fragment_instance_id(), _receiver_destination.node_id());
        if (receiver == nullptr) {
            DB_FATAL_CHANNEL("Fail to add local pass through chunk with null DataStreamReceiver");
            return -1;
        }
        if (0 != receiver->add_local_pass_through_chunk(_sender_fragment_instance_id, record_batches)) {
            DB_WARNING_CHANNEL("Fail to add local pass through chunk");
            return -1;
        }
        // for row number double check
        request.set_record_batch_rows(send_rows);
    }
    // TODO - Limit场景下，可能在下游ES发送EOF之前，ER所在的fragment已经处理完毕，此时ER所在fragment的ES已经向上游发送EOF，下游ES的统计信息来不及传递。
    // 查询统计信息添加
    if (is_send_query_stat && _is_db_fragment) {
        request.mutable_query_stat()->set_num_affected_rows(state->num_affected_rows());
        request.mutable_query_stat()->set_num_returned_rows(state->num_returned_rows());
        request.mutable_query_stat()->set_num_scan_rows(state->num_scan_rows());
        request.mutable_query_stat()->set_num_filter_rows(state->num_filter_rows());
        request.mutable_query_stat()->set_region_count(state->region_count);
        request.mutable_query_stat()->set_db_handle_rows(state->db_handle_rows());
        request.mutable_query_stat()->set_db_handle_bytes(state->db_handle_bytes());
        // 置零，避免其他地方重复统计
        state->set_num_affected_rows(0);
        state->set_num_returned_rows(0);
        state->set_num_scan_rows(0);
        state->set_num_filter_rows(0);
        state->region_count = 0;
        state->set_db_handle_rows(0);
        state->set_db_handle_bytes(0);
    }

    // 实际发送数据
    // 并发保护
    std::lock_guard<std::mutex> lock(_rpc_mtx);
    // 需要等待上一个RPC返回，下一个RPC才可以开始
    // TODO - 改成一个Channel支持同时发送多个RPC，注意接收端_record_batch_seq乱序问题
    if (_closure == nullptr) {
        _closure = std::make_shared<OnRPCDone>();
    } else {
        if (wait_last_rpc() != 0) {
            DB_WARNING_CHANNEL("last rpc fail");
            return -1;
        }
        _closure->cntl.Reset();
        _closure->response.Clear();
    }
    // keep生命周期到本chunk对应的rpc结束, 非local pass through
    if (data_buffer != nullptr) {
        _closure->data_buffer = data_buffer;
        _closure->cntl.request_attachment().append(data);
    }
    request.set_record_batch_seq(_record_batch_seq++);
    _call_id = _closure->cntl.call_id();
    pb::DbService_Stub(&_channel).transmit_data(&_closure->cntl, &request, &_closure->response, _closure.get());
    return 0;
}

int ExchangeSenderNode::Channel::send_version_old(
        const bool is_merge, const int64_t region_id, const ::google::protobuf::RepeatedPtrField<pb::RegionInfo>& region_infos) {
    pb::TransmitDataParam request;
    request.set_exchange_state(pb::ExchangeState::ES_VERSION_OLD);
    request.set_log_id(_log_id);
    request.set_sender_fragment_instance_id(_sender_fragment_instance_id);
    request.set_receiver_fragment_instance_id(_receiver_destination.fragment_instance_id());
    request.set_receiver_node_id(_receiver_destination.node_id());
    request.set_region_id(region_id);
    request.set_is_merge(is_merge);
    request.mutable_region_infos()->CopyFrom(region_infos);
    request.set_is_local_pass_through(false);

    std::lock_guard<std::mutex> lock(_rpc_mtx);
    if (_closure == nullptr) {
        _closure = std::make_shared<OnRPCDone>();
    } else {
        if (wait_last_rpc() != 0) {
            DB_WARNING_CHANNEL("last rpc fail");
            return -1;
        }
        _closure->cntl.Reset();
        _closure->response.Clear();
    }
    _call_id = _closure->cntl.call_id();
    pb::DbService_Stub(&_channel).transmit_data(&_closure->cntl, &request, &_closure->response, _closure.get());
    return 0;
}

int ExchangeSenderNode::Channel::send_exec_fail() {
    pb::TransmitDataParam request;
    request.set_exchange_state(pb::ExchangeState::ES_EXEC_FAIL);
    request.set_log_id(_log_id);
    request.set_sender_fragment_instance_id(_sender_fragment_instance_id);
    request.set_receiver_fragment_instance_id(_receiver_destination.fragment_instance_id());
    request.set_receiver_node_id(_receiver_destination.node_id());
    request.set_is_local_pass_through(_is_local_pass_through);

    std::lock_guard<std::mutex> lock(_rpc_mtx);
    if (_closure == nullptr) {
        _closure = std::make_shared<OnRPCDone>();
    } else {
        if (wait_last_rpc() != 0) {
            DB_WARNING_CHANNEL("last rpc fail");
            return -1;
        }
        _closure->cntl.Reset();
        _closure->response.Clear();
    }
    _call_id = _closure->cntl.call_id();
    pb::DbService_Stub(&_channel).transmit_data(&_closure->cntl, &request, &_closure->response, _closure.get());
    return 0;
}

int ExchangeSenderNode::Channel::wait_last_rpc() {
    if (_closure == nullptr) {
        return 0;
    }
    TimeCost time_cost;
    brpc::Join(_call_id);
    if (_closure->cntl.Failed()) {
        DB_FATAL_CHANNEL("Fail to send_record_batch, %s", _closure->cntl.ErrorText().c_str());
        return -1;
    }
    if (_closure->response.errcode() != pb::SUCCESS) {
        DB_FATAL_CHANNEL("Fail to send_record_batch, status:%s", _closure->response.errmsg().c_str());
        return -1;
    }
    if (time_cost.get_time() > FLAGS_print_time_us) {
        DB_WARNING_CHANNEL("wait last rpc cost: %ld", time_cost.get_time());
    }
    return 0;
}

void ExchangeSenderNode::Channel::close() {
    brpc::Join(_call_id);
}

int ExchangeSenderNode::init(const pb::PlanNode& node) {
    int ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    // 主db此时fragment_id和fragment_instance_id都为0, 构建计划的时候会设置
    // 非主db此时fragment_instance_id为0, open之前需要单独设置
    const pb::ExchangeSenderNode& es_node = node.derive_node().exchange_sender_node();
    _log_id = es_node.log_id();
    _fragment_id = es_node.fragment_id();
    _receiver_fragment_id = es_node.receiver_fragment_id();
    _node_id = es_node.node_id();
    _fragment_instance_id = es_node.fragment_instance_id();
    _partition_property.type = es_node.partition_property().type();
    std::shared_ptr<HashPartitionColumns> hash_cols = std::make_shared<HashPartitionColumns>();
    if (es_node.partition_property().hash_cols_size() > 0) {
        for (auto& arrow_col : es_node.partition_property().hash_cols()) {
            hash_cols->add_column(arrow_col);
        }
        if (es_node.partition_property().need_project_hash_cols_size()
             != es_node.partition_property().need_project_hash_exprs_size()) {
            DB_FATAL_ES("need_project_hash_cols size must be equal need_project_hash_exprs size");
            return -1;
        }
        for (auto i = 0; i < es_node.partition_property().need_project_hash_cols_size(); ++i) {
            std::string col_name = es_node.partition_property().need_project_hash_cols(i);
            ExprNode* expr = nullptr;
            ret = ExprNode::create_tree(es_node.partition_property().need_project_hash_exprs(i), &expr);
            if (ret < 0) {
                //如何释放资源
                return ret;
            }
            hash_cols->add_need_projection_column(col_name, expr);
        }
    }
    _partition_property.hash_partition_propertys.emplace_back(std::move(hash_cols));
    for (auto& arrow_col : es_node.partition_property().need_cast_string_cols()) {
        _partition_property.need_cast_string_columns.insert(arrow_col);
    }

    // 解析schema
    const auto& pb_schema = es_node.schema();
    if (pb_schema.size() > 0) {
        std::shared_ptr<arrow::Buffer> schema_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(pb_schema.data()),
                                                                                   static_cast<int64_t>(pb_schema.size()));
        arrow::io::BufferReader schema_reader(schema_buffer);
        auto schema_ret = arrow::ipc::ReadSchema(&schema_reader, nullptr);
        if (schema_ret.ok()) {
            _arrow_schema = *schema_ret;
        } else {
            DB_FATAL_ES("parser from schema error [%s]. ", schema_ret.status().ToString().c_str());
            return -1;
        }
    }

    for (auto& des : es_node.receiver_destinations()) {
        _receiver_destinations.emplace_back(std::move(des));
    }
    return 0;
}

void ExchangeSenderNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto es_node = pb_node->mutable_derive_node()->mutable_exchange_sender_node();
    es_node->set_log_id(_log_id);
    es_node->set_fragment_id(_fragment_id);
    es_node->set_receiver_fragment_id(_receiver_fragment_id);
    es_node->set_fragment_instance_id(_fragment_instance_id);
    es_node->set_node_id(_node_id);
    for (auto& des : _addresses) {
        auto pb_des = es_node->add_fragment_addresses();
        *pb_des = des;
    }
    for (auto& des : _receiver_destinations) {
        auto pb_des = es_node->add_receiver_destinations();
        *pb_des = des;
    }
    // _pb_node里的type是最准的 
    auto es_type = _pb_node.derive_node().exchange_sender_node().partition_property().type();
    es_node->mutable_partition_property()->set_type(es_type);
    if (_exchange_receiver_node != nullptr && es_type == pb::HashPartitionType) {
        auto receiver_property = _exchange_receiver_node->partition_property();
        if (!receiver_property->hash_partition_propertys.empty()) {
            auto hash_property = receiver_property->hash_partition_propertys[0];
            for (auto& name : hash_property->ordered_hash_columns) {
                es_node->mutable_partition_property()->add_hash_cols(name);
            }
            for (auto& name: hash_property->need_add_projection_columns) {
                es_node->mutable_partition_property()->add_need_project_hash_cols(name);
                ExprNode* expr = hash_property->hash_columns[name];
                if (!expr) {
                    DB_FATAL_ES("hash_columns not contain %s", name.c_str());
                    continue; 
                }
                ExprNode::create_pb_expr(es_node->mutable_partition_property()->add_need_project_hash_exprs(), expr);
            }
            if (!hash_property->need_add_projection_columns.empty()) {
                auto data_schema = _children[0]->data_schema();
                _arrow_schema = VectorizeHelpper::get_arrow_schema(data_schema);
                if (_arrow_schema == nullptr) {
                    DB_FATAL_ES("transfer receiver arrow schema fail");
                    return;
                }
                arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = arrow::ipc::SerializeSchema(*_arrow_schema, arrow::default_memory_pool());
                if (!schema_ret.ok()) {
                    DB_FATAL_ES("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
                    return;
                }
                es_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());
            }
        }
        for (auto& name : _exchange_receiver_node->partition_property()->need_cast_string_columns) {
            es_node->mutable_partition_property()->add_need_cast_string_cols(name);
        }
    }
}

void ExchangeSenderNode::add_address(const std::string& address) {
    pb::ExchangeDestination destination;
    destination.set_address(address);
    destination.set_node_id(_node_id);
    uint64_t fragment_instance_id = (uint64_t)_fragment_id << 32 | _addresses.size();
    destination.set_fragment_instance_id(fragment_instance_id);
    _addresses.emplace_back(std::move(destination));
}

void ExchangeSenderNode::add_destination(const std::string& address) {
    pb::ExchangeDestination destination;
    destination.set_address(address);
    destination.set_node_id(_node_id);
    uint64_t fragment_instance_id = (uint64_t)_receiver_fragment_id << 32 | _receiver_destinations.size();
    destination.set_fragment_instance_id(fragment_instance_id);
    _receiver_destinations.emplace_back(std::move(destination));
}
 
int ExchangeSenderNode::open(RuntimeState* state) {
    int ret = ExecNode::open(state);
    if (ret < 0) {
        DB_FATAL_ES("ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    if (_receiver_destinations.empty()) {
        DB_FATAL_ES("receiver_destinations is empty");
        return -1;
    }
    set_node_exec_type(pb::EXEC_ARROW_ACERO);
    if (_exchange_receiver_node != nullptr) {
        // 主db构建计划的时候, sender的partition_property是本fragment的parition_property
        // 具体执行的时候需要使用对应上游receiver的partition_property
        _partition_property = *(_exchange_receiver_node->partition_property());
    }
    
    _is_db_fragment = (get_node_pass_subquery(pb::EXCHANGE_RECEIVER_NODE) != nullptr);
    _region_id = state->region_id();
    for (int i = 0; i < _receiver_destinations.size(); ++i) {
        bool is_local_pass_through = false;
        if (_is_db_fragment 
                && state->localhost_address == _receiver_destinations[i].address()) {
            is_local_pass_through = true;
        }
        std::shared_ptr<Channel> channel = std::make_shared<Channel>(_log_id,
                                                                     _fragment_instance_id,
                                                                     _receiver_destinations[i],
                                                                     _is_db_fragment,
                                                                     _region_id,
                                                                     is_local_pass_through);
        if (channel == nullptr) {
            DB_FATAL_ES("Fail to make channel");
            return -1;
        }
        if (channel->init() != 0) {
            DB_FATAL_ES("Fail to init channel");
            return -1;
        }
        _channels.emplace_back(channel);
    }
    return 0;
}

int ExchangeSenderNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    // 应该不需要往下get_next, 直接转向量化执行, 注意RocksdbScanNode的reader
    // 应该不会调用get_next
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

void ExchangeSenderNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto& channel : _channels) {
        channel->close();
    }
    _receiver_destinations.clear();
    _addresses.clear();
}

int ExchangeSenderNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    for (auto* c : _children) {
        if (c->build_arrow_declaration(state) != 0) {
            DB_FATAL_ES("Fail to build_arrow_declaration");
            return -1;
        }
    }
    if (_partition_property.hash_partition_propertys.size() > 0
        && _partition_property.hash_partition_propertys[0]->need_add_projection_columns.size() > 0) {
        // 需要额外的project产生hash列
        // 如select xx from (select c1 as k1 from table) group by k1+1; 需要按照c1+1进行hash
        // 本fragment里sender下游算子肯定没有agg/join
        if (_arrow_schema == nullptr) {
            DB_FATAL_ES("arrow schema is nullptr");
            return -1;
        }
        std::vector<std::string> generate_projection_exprs_names;
        std::vector<arrow::compute::Expression> generate_projection_exprs;
        for (auto& c : _arrow_schema->fields()) {
            generate_projection_exprs.emplace_back(arrow::compute::field_ref(c->name()));
            generate_projection_exprs_names.emplace_back(c->name());
        }
        for (auto& column : _partition_property.hash_partition_propertys[0]->need_add_projection_columns) {
            ExprNode* expr = _partition_property.hash_partition_propertys[0]->hash_columns[column];
            if (0 != expr->transfer_to_arrow_expression()) {
                DB_FATAL_ES("Fail to transfer expr to arrow expression");
                return -1;
            }
            generate_projection_exprs.emplace_back(expr->arrow_expr());
            generate_projection_exprs_names.emplace_back(column);
        }
        arrow::acero::Declaration dec{"project", arrow::acero::ProjectNodeOptions{generate_projection_exprs, generate_projection_exprs_names}};
        LOCAL_TRACE_ARROW_PLAN(dec);
        state->append_acero_declaration(dec);
    }
    arrow::acero::Declaration dec {"exchange_sender", AceroExchangeSenderNodeOptions{state, this, _limit}};
    LOCAL_TRACE_ARROW_PLAN(dec);
    state->append_acero_declaration(dec);
    return 0;
}

int ExchangeSenderNode::init_schema(std::shared_ptr<arrow::Schema> arrow_schema) {
    _arrow_schema = arrow_schema;
    if (_arrow_schema == nullptr) {
        DB_FATAL_ES("_arrow_schema is nullptr");
        return -1;
    }
    if (init_repartition_param() != 0) {
        DB_FATAL_ES("Fail to init repartition param");
        return -1;
    }
    return 0;
}

int ExchangeSenderNode::send_record_batch(RuntimeState* state, 
        std::shared_ptr<arrow::RecordBatch> record_batch, const pb::ExchangeState exchange_state) {
    if (record_batch == nullptr) {
        DB_FATAL_ES("record_batch is nullptr");
        return -1;
    }
    bool is_send_query_stat = false;
    // EOF时，发送查询统计信息
    if (exchange_state == pb::ExchangeState::ES_EOF) {
        is_send_query_stat = true;
        update_runtime_state(state);
    }
    if (_partition_property.type == pb::SinglePartitionType) {
        // 将数据直接发送到上游ER
        const int32_t bucket = 0;
        auto channel = get_channel(bucket);
        if (channel == nullptr) {
            DB_FATAL_ES("Channel is nullptr");
            return -1;
        }
        if (channel->send_record_batch(state, record_batch, exchange_state, is_send_query_stat) != 0) {
            DB_FATAL_ES("Fail to send record batch");
            return -1;
        }
    } else if (_partition_property.type == pb::BroadcastPartitionType) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> need_send_record_batches;
        bool need_send_rpc = false;
        if (0 != _broadcast_record_batch_keepper.add_record_batch(state, record_batch, _is_db_fragment, need_send_record_batches, exchange_state, need_send_rpc)) {
            DB_FATAL_ES("Fail to add record batch");
        }
        if (!need_send_rpc) {
            return 0;
        }
        // 一次性处理数据
        TimeCost time_cost;
        int64_t combine_cost = 0;
        int64_t serialize_cost = 0;
        int64_t send_rows = 0;
        int64_t send_cost = 0;
        int64_t attachment_size = 0;
        std::shared_ptr<arrow::RecordBatch> concatenate_record_batch;
        std::shared_ptr<arrow::Buffer> schema_buffer;
        std::shared_ptr<arrow::Buffer> data_buffer;
        if (0 != VectorizeHelpper::concatenate_record_batches(
                                        record_batch->schema(), 
                                        need_send_record_batches, 
                                        concatenate_record_batch)) {
            DB_FATAL_ES("Fail to concatenate record batches");
            return -1;
        }
        combine_cost = time_cost.get_time();
        time_cost.reset();
        send_rows = concatenate_record_batch->num_rows();
        // 序列化
        if (0 != VectorizeHelpper::serialize_record_batch(
                        concatenate_record_batch, 
                        schema_buffer, 
                        data_buffer,
                        static_cast<arrow::Compression::type>(FLAGS_compression_type))) {
            DB_FATAL_ES("Fail to serialize record batches");
            return -1;
        }
        serialize_cost = time_cost.get_time();
        time_cost.reset();
        attachment_size = data_buffer->size();
        // 发送全部上游
        for (auto& channel : _channels) {
            if (0 != channel->send_record_batch_data(state, 
                                        need_send_record_batches, 
                                        schema_buffer, 
                                        data_buffer, 
                                        exchange_state, 
                                        is_send_query_stat, 
                                        send_rows)) {
                DB_FATAL_ES("Fail to send record batch");
                return -1;
            }
            if (is_send_query_stat) {
                is_send_query_stat = false;
            }
        }
        send_cost = time_cost.get_time();
        if (combine_cost + serialize_cost + send_cost > FLAGS_print_time_us) {
            DB_WARNING_ES("combine cost: %ld, serialize cost: %ld, send cost: %ld, send rows: %ld, single attachment size: %ld",
                combine_cost, serialize_cost, send_cost, send_rows, attachment_size);
        }
    } else if (_partition_property.type == pb::HashPartitionType) {
        // 将数据分桶后发送到对应上游ER
        // Step1: 将batch进行hash分桶
        std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> hash_batch_map;
        arrow::Status status = repartition(record_batch, hash_batch_map);
        if (!status.ok()) {
            DB_FATAL_ES("Fail to repartition, errmsg:%s", status.message().c_str());
            return -1;
        }
        // Step2: 将数据发送给上游ER
        for (auto& [bucket, hash_batch] : hash_batch_map) {
            auto channel = get_channel(bucket);
            if (channel == nullptr) {
                DB_FATAL_ES("channel is nullptr");
                return -1;
            }
            if (channel->send_record_batch(state, hash_batch, exchange_state, is_send_query_stat) != 0) {
                DB_FATAL_ES("Fail to send record batch");
                return -1;
            }
            // 如果上游有多个ER节点，只向第一个ER节点发送查询统计信息，避免统计信息重复计算
            if (is_send_query_stat) {
                is_send_query_stat = false;
            }
        }
    } else {
        DB_FATAL_ES("mpp execute fail: partition type[%d] is not support", _partition_property.type);
        return -1;
    }
    return 0;
}

int ExchangeSenderNode::send_curr_record_batch(
        RuntimeState* state, std::shared_ptr<arrow::RecordBatch> record_batch){ 
    return send_record_batch(state, record_batch, pb::ExchangeState::ES_DOING);
}

int ExchangeSenderNode::send_eof_record_batch(
        RuntimeState* state, std::shared_ptr<arrow::RecordBatch> record_batch) {
    int ret = send_record_batch(state, record_batch, pb::ExchangeState::ES_EOF);
    if (ret != 0) {
        DB_WARNING_ES("Fail to send_record_batch");
        return -1;
    }
    TimeCost time_cost;
    // 发送EOF，需要等所有channel成功返回或存在错误才能结束
    for (auto& channel : _channels) {
        if (channel->wait_last_rpc() != 0) {
            DB_WARNING_ES("Fail to send_eof_record_batch");
            return -1;
        }
    }
    if (time_cost.get_time() > FLAGS_print_time_us) {
        DB_WARNING_ES("wait all eof cost: %ld", time_cost.get_time());
    }
    return 0;
}

int ExchangeSenderNode::send_exec_fail() {
    for (auto& channel : _channels) {
        if (channel == nullptr) {
            DB_WARNING_ES("Channel is nullptr");
            return -1;
        }
        channel->send_exec_fail();
    }
    return 0;
}


int ExchangeSenderNode::init_repartition_param() {
    // repartition使用
    // 获取分区列，获取需要转化成字符串的分区列
    _hash_bucket_num = _receiver_destinations.size();

    if (_partition_property.type == pb::SinglePartitionType
        || _partition_property.type == pb::BroadcastPartitionType) {
        return 0;
    }
    if (_partition_property.type != pb::HashPartitionType) {
        DB_FATAL_ES("type: %s is not support", pb::PartitionPropertyType_Name(_partition_property.type).c_str());
        return -1;
    }
    if (_partition_property.hash_partition_propertys.size() != 1) {
        DB_FATAL_ES("hash partition property size is not 1");
        return -1;
    }

    const auto& hash_columns = _partition_property.hash_partition_propertys[0]->hash_columns;
    std::vector<std::string> hash_cols;
    for (const auto& [hash_column_name, _] : hash_columns) {
        hash_cols.emplace_back(hash_column_name);
    }
    const auto& need_cast_string_cols = _partition_property.need_cast_string_columns;
    _hash_indices.resize(hash_cols.size());
    _need_cast_indices.resize(hash_cols.size());
    for (int i = 0; i < hash_cols.size(); ++i) {
        const std::string& hash_col = hash_cols[i];
        const int idx = _arrow_schema->GetFieldIndex(hash_col);
        if (idx < 0) {
            DB_FATAL_ES("Fail to GetFieldIndex, hash_col:%s", hash_col.c_str());
            return -1;
        }
        _hash_indices[i] = idx;
        if (need_cast_string_cols.find(hash_col) != need_cast_string_cols.end()) {
            _need_cast_indices[i] = true;
        } else {
            _need_cast_indices[i] = false;
        }
    }
    // 构造分区列的arrow元信息
    std::vector<arrow::compute::KeyColumnMetadata> metadata_vec;
    _metadata_vec.resize(hash_cols.size());
    const auto& fields = _arrow_schema->fields();
    for (int i = 0; i < _metadata_vec.size(); ++i) {
        if (_hash_indices[i] >= fields.size()) {
            DB_FATAL_ES("hash_indices[i] >= fields.size(), %d vs %d", _hash_indices[i], (int)fields.size());
            return -1;
        }
        if (_need_cast_indices[i]) {
            arrow::Result<arrow::compute::KeyColumnMetadata> metadata_result = 
                    arrow::compute::ColumnMetadataFromDataType(arrow::large_binary());
            if (!metadata_result.ok()) {
                DB_FATAL_ES("Fail to ColumnMetadataFromDataType, %s", metadata_result.status().ToString().c_str());
                return -1;
            }
            _metadata_vec[i] = *metadata_result;
        } else {
            arrow::Result<arrow::compute::KeyColumnMetadata> metadata_result = 
                    arrow::compute::ColumnMetadataFromDataType(fields[_hash_indices[i]]->type());
            if (!metadata_result.ok()) {
                DB_FATAL_ES("Fail to ColumnMetadataFromDataType, %s", metadata_result.status().ToString().c_str());
                return -1;
            }
            _metadata_vec[i] = *metadata_result;
        }
        if (_metadata_vec[i].is_null_type) {
            DB_FATAL_ES("NOT SUPPORT: hash column is null type");
            return -1;
        }
    }

    return 0;
}

arrow::Status ExchangeSenderNode::repartition(
        std::shared_ptr<arrow::RecordBatch> batch, 
        std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>>& hash_batch_map) {
    const int64_t max_batch_size = arrow::util::MiniBatch::kMiniBatchLength; // 1024

    const std::vector<int>& hash_indices = _hash_indices;
    const std::vector<bool>& need_cast_indices = _need_cast_indices;
    const std::vector<arrow::compute::KeyColumnMetadata>& metadata_vec = _metadata_vec;
    const int32_t hash_bucket_num = _hash_bucket_num;
    if (hash_indices.empty()) {
        return arrow::Status::Invalid("hash_indices is empty");
    }
    if (batch == nullptr) {
        return arrow::Status::Invalid("batch is nullptr");
    }
    TimeCost repartition_cost;
    // cast string
    const int64_t batch_length = batch->num_rows();
    std::unordered_map<int, std::shared_ptr<arrow::Array>> cast_array_map;
    for (int i = 0; i < hash_indices.size(); ++i) {
        if (need_cast_indices[i]) {
            if (hash_indices[i] >= batch->column_data().size()) {
                return arrow::Status::Invalid("hash_indices[i] >= batch->num_columns()");
            }
            auto array = batch->column(hash_indices[i]);
            ARROW_ASSIGN_OR_RAISE(cast_array_map[hash_indices[i]], arrow::compute::Cast(*array, arrow::large_binary()));
            if (cast_array_map[hash_indices[i]]->length() != batch_length) {
                return arrow::Status::Invalid("cast_array_map[hash_indices[i]]->length() != batch_length");
            }
        }
    }
    // 构造arrow ctx
    arrow::util::TempVectorStack stack;
    stack.Init(::arrow::default_memory_pool(), 8 * max_batch_size * sizeof(uint64_t));
    arrow::compute::LightContext ctx;
    ctx.hardware_flags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
    ctx.stack = &stack;
    // 计算hash值
    std::vector<uint32_t> hash_vals(batch_length);
    std::vector<arrow::compute::KeyColumnArray> column_arrays;
    column_arrays.resize(hash_indices.size());
    for (int64_t i = 0; i < batch_length; i += max_batch_size) {
        const int64_t length = std::min(batch_length - i, max_batch_size);
        for (int j = 0; j < hash_indices.size(); ++j) {
            std::shared_ptr<arrow::ArrayData> array_data;
            if (need_cast_indices[j]) {
                array_data = cast_array_map[hash_indices[j]]->data();
            } else {
                if (hash_indices[j] >= batch->column_data().size()) {
                    return arrow::Status::Invalid("hash_indices[j] >= batch->num_columns()");;
                }
                array_data = batch->column_data(hash_indices[j]);
            }
            column_arrays[j] = arrow::compute::ColumnArrayFromArrayDataAndMetadata(
                                                    array_data, metadata_vec[j], i, length);
        }
        arrow::compute::Hashing32::HashMultiColumn(column_arrays, &ctx, hash_vals.data() + i);
    }
    // Take不同hash桶的行
    if (hash_bucket_num == 0) {
        return arrow::Status::Invalid("hash_bucket_num is 0");
    }
    std::unordered_map<int, arrow::Int32Builder> hash_buckets;
    for (int i = 0; i < hash_bucket_num; ++i) {
        hash_buckets[i] = arrow::Int32Builder();
        hash_batch_map[i] = batch->Slice(0, 0); // 避免batch行数为空，不生成hash_batch_map
    }
    for (int i = 0; i < hash_vals.size(); ++i) {
        auto status = hash_buckets[hash_vals[i] % hash_bucket_num].Append(i);
        if (!status.ok()) {
            return status;
        }
    }
    for (auto& [bucket, builder] : hash_buckets) {
        std::shared_ptr<arrow::Int32Array> indices;  
        auto status = builder.Finish(&indices);
        if (!status.ok()) {
            return status;
        }
        if (indices == nullptr) {
            return arrow::Status::Invalid("indices is nullptr");
        }
        if (indices->length() == 0) {
            continue;
        }
        // Take指定行
        arrow::Result<arrow::Datum> take_result = 
            arrow::compute::Take(batch, *indices, arrow::compute::TakeOptions::Defaults(), nullptr);
        if (!take_result.ok()) {
            return take_result.status();
        }
        hash_batch_map[bucket] = take_result->record_batch();
    }
    _repartition_cost_us += repartition_cost.get_time();
    _repartition_rows += batch_length;
    return arrow::Status::OK();
}

} // namespace baikaldb
