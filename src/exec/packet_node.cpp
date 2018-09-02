// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include "packet_node.h"
#include "runtime_state.h"
#include "network_socket.h"

namespace baikaldb {
int PacketNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _op_type = node.derive_node().packet_node().op_type();
    for (auto& expr : node.derive_node().packet_node().projections()) {
        ExprNode* projection = nullptr;
        ret = ExprNode::create_tree(expr, &projection);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _projections.push_back(projection);
    }
    for (auto& name : node.derive_node().packet_node().col_names()) {
        ResultField field;
        field.name = name;
        field.org_name = name;
        _fields.push_back(field);
    }
    return 0;
}
int PacketNode::expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs) {
    int ret = 0;
    ret = ExecNode::expr_optimize(tuple_descs);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail:%d", ret);
        return ret;
    }
    int i = 0;
    for (auto expr : _projections) {
        //类型推导
        ret = expr->type_inferer();
        if (ret < 0) {
            DB_WARNING("type_inferer fail");
            return ret;
        }
        //db table_name先不填，后续有影响再填
        _fields[i].type = to_mysql_type(expr->col_type());
        _fields[i].flags = 1;
        if (is_uint(expr->col_type())) {
            _fields[i].flags |= 32;
        }
        //常量表达式计算
        expr->const_pre_calc();
        ++i;
    }
    return 0;
}
int PacketNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::open fail:%d", ret);
        return ret;
    }
    _send_buf = state->send_buf();
    _wrapper = MysqlWrapper::get_instance();
    state->set_num_affected_rows(ret);
    if (op_type() != pb::OP_SELECT) {
        if (op_type() == pb::OP_INSERT) {
            auto client = state->client_conn();
            pack_ok(state->num_affected_rows(), client->last_insert_id);
        } else {
            pack_ok(state->num_affected_rows());
        }
        return 0;
    }
    for (auto expr : _projections) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
    }
    pack_head();
    pack_fields();
    if (_children.size() == 0) {
        if (!reached_limit()) {
            pack_row(nullptr);
        }
    } else {
        bool eos = false;
        int64_t pack_time = 0;
        do {
            RowBatch batch;
            ret = _children[0]->get_next(state, &batch, &eos);
            if (ret < 0) {
                DB_WARNING("children:get_next fail:%d", ret);
                return ret;
            }
            for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
                TimeCost cost;
                ret = pack_row(batch.get_row().get());
                pack_time += cost.get_time();
                cost.reset();
                state->inc_num_returned_rows(1);
                if (ret < 0) {
                    DB_WARNING("pack_row fail:%d", ret);
                    return ret;
                }
            }
        } while (!eos);
        DB_WARNING("txn_id: %lu, pack_time: %ld", state->txn_id, pack_time);
    }
    pack_eof();
    return 0;
}

void PacketNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto expr : _projections) {
        expr->close();
    }
}

int PacketNode::pack_ok(int num_affected_rows, int64_t last_insert_id) {
    if (_send_buf->_size > 0) {
        _send_buf->byte_array_clear();
    }

    DataBuffer tmp_buf;
    tmp_buf.byte_array_append_length_coded_binary(0);
    tmp_buf.byte_array_append_length_coded_binary(num_affected_rows);
    tmp_buf.byte_array_append_length_coded_binary(last_insert_id);
    uint8_t bytes[2];
    bytes[0] = (0 & 0xff);
    bytes[1] = (0 >> 8) & 0xff;
    tmp_buf.byte_array_append_len(bytes, 2);

    bytes[0] = 0 & 0xff;
    bytes[1] = (0 >> 8) & 0xff;
    tmp_buf.byte_array_append_len(bytes, 2);
    return _send_buf->network_queue_send_append(tmp_buf._data, tmp_buf._size, 1, 0);
}

int PacketNode::pack_err() {
    return 0;
}

int PacketNode::pack_head() {
    //Result Set Header Packet
    int start_pos = _send_buf->_size;
    if (!_send_buf->byte_array_append_len((const uint8_t *)"\x01\x00\x00\x01", 4)) {
        DB_FATAL("byte_array_append_len failed.");
        return -1;
    }
    if (_fields.size() == 0) {
        DB_FATAL("fields size is wrong.size:[0]");
        return -1;
    }
    if (!_send_buf->byte_array_append_length_coded_binary(_fields.size())) {
        DB_FATAL("byte_array_append_len failed. len:[%d]", _fields.size());
        return -1;
    }
    int packet_body_len = _send_buf->_size - start_pos - 4;
    _send_buf->_data[start_pos] = packet_body_len & 0xff;
    _send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xff;
    _send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xff;
    return 0;
}

int PacketNode::pack_fields() {
    for (auto& field : _fields) {
        ++_packet_id;
        _wrapper->make_field_packet(_send_buf, &field, _packet_id);
    }
    pack_eof();
    return 0;
}

int PacketNode::pack_row(MemRow* row) {
    ++_packet_id;
    int start_pos = _send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = _packet_id & 0xff;
    if (!_send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len. value:[%s], len:[1]", bytes);
        return -1;
    }

    // package body.
    for (auto expr : _projections) {
        if (!_send_buf->byte_array_append_value(expr->get_value(row).cast_to(expr->col_type()))) {
            DB_FATAL("Failed to append table cell.");
            return -1;
        }
    }
    int packet_body_len = _send_buf->_size - start_pos - 4;
    _send_buf->_data[start_pos] = packet_body_len & 0xff;
    _send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xff;
    _send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xff;
    return 0;
}

int PacketNode::pack_eof() {
    ++_packet_id;
    _wrapper->make_eof_packet(_send_buf, _packet_id);
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
