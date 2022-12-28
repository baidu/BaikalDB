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
//#include "sql_parser.h"

namespace baikaldb {
class Literal : public ExprNode {
public:
    Literal() : _value(pb::NULL_TYPE) {
        _is_constant = true;
    }

    Literal(ExprValue value) : _value(value) {
        _is_constant = true;
        _has_null = value.is_null();
        value_to_node_type();
    }

    virtual ~Literal() {
    }

    void init(const ExprValue& value) {
        _value = value;
        _is_constant = true;
        _has_null = value.is_null();
        value_to_node_type();
    }
    
    virtual int init(const pb::ExprNode& node) {
        int ret = 0;
        ret = ExprNode::init(node);
        if (ret < 0) {
            return ret;
        }
        switch (node.node_type()) {
            case pb::NULL_LITERAL: {
                _value.type = pb::NULL_TYPE;
                _has_null = true;
                break;
            }
            case pb::INT_LITERAL: {
                _value.type = pb::INT64;
                _value._u.int64_val = node.derive_node().int_val();
                break;
            }
            case pb::BOOL_LITERAL: {
                _value.type = pb::BOOL;
                _value._u.bool_val = node.derive_node().bool_val();
                break;
            }
            case pb::DOUBLE_LITERAL: {
                _value.type = pb::DOUBLE;
                _value._u.double_val = node.derive_node().double_val();
                break;
            }
            case pb::STRING_LITERAL: {
                _value.type = pb::STRING;
                _value.str_val = node.derive_node().string_val();
                break;
            }
            case pb::HEX_LITERAL: {
                _value.type = pb::HEX;
                _value.str_val = node.derive_node().string_val();
                break;
            }
            case pb::HLL_LITERAL: {
                _value.type = pb::HLL;
                _value.str_val = node.derive_node().string_val();
                break;
            }
            case pb::BITMAP_LITERAL: {
                _value.type = pb::BITMAP;
                _value.str_val = node.derive_node().string_val();
                _value.cast_to(pb::BITMAP);
                break;
            }
            case pb::TDIGEST_LITERAL: {
                _value.type = pb::TDIGEST;
                _value.str_val = node.derive_node().string_val();
                break;
            }
            case pb::DATETIME_LITERAL: {
                _value.type = pb::DATETIME;
                _value._u.uint64_val = node.derive_node().int_val();
                break;
            }
            case pb::TIME_LITERAL: {
                _value.type = pb::TIME;
                _value._u.int32_val = node.derive_node().int_val();
                break;
            }
            case pb::TIMESTAMP_LITERAL: {
                _value.type = pb::TIMESTAMP;
                _value._u.uint32_val = node.derive_node().int_val();
                break;
            }
            case pb::DATE_LITERAL: {
                _value.type = pb::DATE;
                _value._u.uint32_val = node.derive_node().int_val();
                break;
            }
            case pb::PLACE_HOLDER_LITERAL: {
                _value.type = pb::NULL_TYPE;
                _is_place_holder = true;
                _place_holder_id = node.derive_node().int_val(); // place_holder id
                break;
            }
            default:
                return -1;
        }
        return 0;
    }

    int64_t used_size() override {
        return sizeof(*this) + _value.size();
    }

    virtual bool is_place_holder() {
        return _is_place_holder;
    }

    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders) {
        if (_is_place_holder) {
            placeholders.insert({_place_holder_id, this});
        }
    }

    virtual void transfer_pb(pb::ExprNode* pb_node) {
        ExprNode::transfer_pb(pb_node);
        switch (node_type()) {
            case pb::NULL_LITERAL:
                break;
            case pb::BOOL_LITERAL:
                pb_node->mutable_derive_node()->set_bool_val(_value.get_numberic<bool>());
                break;
            case pb::INT_LITERAL:
                pb_node->mutable_derive_node()->set_int_val(_value.get_numberic<int64_t>());
                break;
            case pb::DOUBLE_LITERAL:
                pb_node->mutable_derive_node()->set_double_val(_value.get_numberic<double>());
                break;
            case pb::STRING_LITERAL:
            case pb::HEX_LITERAL:
            case pb::HLL_LITERAL:
            case pb::BITMAP_LITERAL:
                pb_node->mutable_derive_node()->set_string_val(_value.get_string());
                break;
            case pb::DATETIME_LITERAL:
            case pb::DATE_LITERAL:
            case pb::TIME_LITERAL:
            case pb::TIMESTAMP_LITERAL:
                pb_node->mutable_derive_node()->set_int_val(_value.get_numberic<int64_t>());
                break;
            case pb::PLACE_HOLDER_LITERAL:
                pb_node->mutable_derive_node()->set_int_val(_place_holder_id); 
                DB_FATAL("place holder need not transfer pb, %d", _place_holder_id);
                break;
            default:
                break;
        }
    }

    // only the following castings are allowed:
    //  STRING_LITERA => TIMESTAMP_LITERAL
    //  STRING_LITERA => DATETIME_LITERAL
    //  STRING_LITERA => DATE_LITERAL
    void cast_to_type(pb::ExprNodeType literal_type) {
        if (literal_type == pb::TIMESTAMP_LITERAL) {
            _value.cast_to(pb::TIMESTAMP);
        } else if (literal_type == pb::DATE_LITERAL) {
            _value.cast_to(pb::DATE);
        } else if (literal_type == pb::DATETIME_LITERAL) {
            _value.cast_to(pb::DATETIME);
        } else if (literal_type == pb::TIME_LITERAL) {
            _value.cast_to(pb::TIME);
        }
        _node_type = literal_type;
        _col_type = _value.type;
    }

    void cast_to_col_type(pb::PrimitiveType type) {
        if (is_datetime_specic(type) && _value.is_numberic()) {
            _value.cast_to(pb::STRING);
        }
        _value.cast_to(type);
        value_to_node_type();
    }

    virtual ExprValue get_value(MemRow* row) {
        return _value.cast_to(_col_type);
    }
    virtual ExprValue get_value(const ExprValue& value) {
        return _value.cast_to(_col_type);
    }

private:
    void value_to_node_type() {
        _col_type = _value.type;
        if (_value.is_timestamp()) {
            _node_type = pb::TIMESTAMP_LITERAL;
        } else if (_value.is_date()) {
            _node_type = pb::DATE_LITERAL;
        } else if (_value.is_datetime()) {
            _node_type = pb::DATETIME_LITERAL;
        } else if (_value.is_time()) {
            _node_type = pb::TIME_LITERAL;
        } else if (_value.is_int()) {
            _node_type = pb::INT_LITERAL;
        } else if (_value.is_string()) {
            _node_type = pb::STRING_LITERAL;
        } else if (_value.is_bool()) {
            _node_type = pb::BOOL_LITERAL;
        } else if (_value.is_double()) {
            _node_type = pb::DOUBLE_LITERAL;
        } else if (_value.is_hll()) {
            _node_type = pb::HLL_LITERAL;
        } else if (_value.is_bitmap()) {
            _node_type = pb::BITMAP_LITERAL;
        } else {
            _node_type = pb::NULL_LITERAL;
        }
    }

private:
    ExprValue _value;
    int _place_holder_id = 0;
    bool _is_place_holder = false;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
