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

#include <map>
#include "redis_scan_node.h"
#include "filter_node.h"
#include "join_node.h"
#include "schema_factory.h"
#include "scalar_fn_call.h"
#include "slot_ref.h"
#include "runtime_state.h"

namespace baikaldb {
DEFINE_string(redis_addr, "127.0.0.1:6379", "redis addr");
int RedisScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ScanNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    brpc::ChannelOptions option;
    option.protocol = brpc::PROTOCOL_REDIS;
    option.max_retry = 1;
    option.connect_timeout_ms = 3000; 
    option.timeout_ms = -1;
    if (_redis_channel.Init(FLAGS_redis_addr.c_str(), &option) != 0) {
        DB_WARNING("fail to connect redis, %s", FLAGS_redis_addr.c_str());
        return -1;
    }
    return 0;
}

int RedisScanNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ScanNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ScanNode::open fail:%d", ret);
        return ret;
    }
    // 简单kv只有primary
    // 如果数据源支持各种索引，可以在这边做处理
    pb::PossibleIndex pos_index;
    pos_index.ParseFromString(_pb_node.derive_node().scan_node().indexes(0));
    _index_id = pos_index.index_id();
    SchemaFactory* factory = SchemaFactory::get_instance();
    auto index_info_ptr = factory->get_index_info_ptr(_index_id);
    if (index_info_ptr == nullptr) {
        DB_WARNING_STATE(state, "no index_info found for index id: %ld", 
                _index_id);
        return -1;
    }
    if (index_info_ptr->type != pb::I_PRIMARY) {
        DB_WARNING_STATE(state, "index id: %ld not primary: %d", 
                _index_id, index_info_ptr->type);
        return -1;
    }
    for (auto& range : pos_index.ranges()) {
        // 空指针容易出错
        SmartRecord left_record = factory->new_record(_table_id);
        SmartRecord right_record = factory->new_record(_table_id);
        left_record->decode(range.left_pb_record());
        right_record->decode(range.right_pb_record());
        int left_field_cnt = range.left_field_cnt();
        int right_field_cnt = range.right_field_cnt();
        //bool left_open = range.left_open();
        //bool right_open = range.right_open();
        if (range.left_pb_record() != range.right_pb_record()) {
            DB_WARNING("redis only support equeue");
            return -1;
        }
        if (left_field_cnt != 1 || left_field_cnt != right_field_cnt) {
            DB_WARNING("redis only support equeue");
            return -1;
        }
        _primary_records.push_back(left_record);
    }
    return 0;
}

int RedisScanNode::get_by_key(SmartRecord record) {
    auto field_key = record->get_field_by_tag(1);
    // record的交互接口只关注ExprValue结构，这是一种混合类型，可以方便转化
    std::string key = record->get_value(field_key).get_string();
    brpc::RedisRequest request;
    brpc::RedisResponse response;
    brpc::Controller cntl;
    request.AddCommand("GET %s", key.c_str());
    _redis_channel.CallMethod(NULL, &cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        DB_WARNING("Fail to access redis-server, %s", cntl.ErrorText().c_str());
        return -1;
    }
    if (response.reply(0).is_nil() || response.reply(0).is_error()) {
        // key not exist
        return -1;
    }
    //DB_NOTICE("type %d", response.reply(0).type());
    ExprValue value(pb::STRING);
    value.str_val = response.reply(0).c_str();
    auto field_value = record->get_field_by_tag(2);
    record->set_value(field_value, value);
    return 0;
}

int RedisScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    int ret = 0;
    SmartRecord record;
    while (1) {
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }
        if (_idx >= _primary_records.size()) {
            *eos = true;
            return 0;
        } else {
            record = _primary_records[_idx++];
        }
        ret = get_by_key(record);
        if (ret < 0) {
            DB_WARNING("record get value fail, %s", record->debug_string().c_str());
            continue;
        }
        std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
        for (auto slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            row->set_value(slot.tuple_id(), slot.slot_id(),
                    record->get_value(field));
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
    }
}

void RedisScanNode::close(RuntimeState* state) {
    ScanNode::close(state);
    _idx = 0;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
