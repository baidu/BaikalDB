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
#include "expr_node.h"

namespace baikaldb {
class SlotRef : public ExprNode {
public:
    SlotRef()  {
        _is_constant = false;
    }
    virtual int init(const pb::ExprNode& node) {
        ExprNode::init(node);
        _tuple_id = node.derive_node().tuple_id();
        _slot_id = node.derive_node().slot_id();
        _field_id = node.derive_node().field_id();
        return 0;
    }
    virtual ExprValue get_value(MemRow* row) {
        if (row == NULL) {
            return ExprValue::Null();
        }
        ExprValue v = row->get_value(_tuple_id, _slot_id).cast_to(_col_type);
        if (_float_precision_len != -1) {
            v.set_precision_len(_float_precision_len);
        }
        return v;
    }
    virtual ExprValue get_value(const ExprValue& value) {
        return value;
    }
    SlotRef* clone() {
        SlotRef* s = new SlotRef;
        s->_tuple_id = _tuple_id;
        s->_field_id = _field_id;
        s->_slot_id = _slot_id;
        s->_node_type = _node_type;
        s->_col_type = _col_type;
        s->_col_flag = _col_flag;
        return s;
    }

    int32_t field_id() const {
        return _field_id;
    }
    /*
    void set_field_id(int32_t field_id) {
        _field_id = field_id;
    }
    */
    virtual void transfer_pb(pb::ExprNode* pb_node) {
        ExprNode::transfer_pb(pb_node);
        pb_node->mutable_derive_node()->set_tuple_id(_tuple_id);
        pb_node->mutable_derive_node()->set_slot_id(_slot_id);
        pb_node->mutable_derive_node()->set_field_id(_field_id);
    }
    virtual bool can_use_arrow_vector() {
        return true;
    }
    virtual int transfer_to_arrow_expression() override {
        if (_is_vector_index_use) {
            _arrow_expr = arrow::compute::field_ref(std::to_string(_field_id));
        } else {
            _arrow_expr = arrow::compute::field_ref(std::to_string(_tuple_id) + "_" + std::to_string(_slot_id));
        }
        return 0;
    }
    std::string arrow_field_name() {
        return std::to_string(_tuple_id) + "_" + std::to_string(_slot_id);
    }
    void set_tuple_id(int32_t tuple_id) {
        _tuple_id = tuple_id;
    }
    void set_slot_id(int32_t slot_id) {
        _slot_id = slot_id;
    }
    bool contains_specified_fields(const std::unordered_set<int32_t>& fields) override {
        if (fields.find(_field_id) != fields.end()) {
            return true;
        }
        return false;
    }

    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override {
        if (slotid_fieldname_map.find(_slot_id) == slotid_fieldname_map.end()) {
            return "";
        }
        return slotid_fieldname_map.at(_slot_id);
    }

private:
    int32_t _field_id;
    friend ExprNode;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
