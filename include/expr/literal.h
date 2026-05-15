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
                _place_holder_id = node.derive_node().placeholder_id();
                break;
            }
            case pb::MAXVALUE_LITERAL: {
                _value.type = pb::MAXVALUE_TYPE;
                break;
            }
            case pb::ARRAY_LITERAL: {
                _value.type = node.col_type();
                int ret = init_array_from_pb(node);
                if (ret < 0) {
                    return ret;
                }
                break;
            }
            default:
                return -1;
        }
        // non-prepare plan cache
        if (!_is_place_holder && node.derive_node().has_placeholder_id()) {
            _is_place_holder = true;
            _place_holder_id = node.derive_node().placeholder_id();
        }
        return 0;
    }

    int64_t used_size() override {
        return sizeof(*this) + _value.size();
    }

    virtual bool is_place_holder() {
        return _is_place_holder;
    }

    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
        if (_is_place_holder) {
            placeholders.insert({_place_holder_id, this});
            return;
        }
    }

    template<typename T>
    void add_array_values_impl(pb::ExprNode* pb_node) {
        auto array = _value.get_array<T>();
        if (array == nullptr) {
            DB_FATAL("array is nullptr");
            return;
        }
        auto* derive_node = pb_node->mutable_derive_node();
        
        if constexpr (std::is_same_v<T, bool>) {
            for (auto v : array->data) {
                derive_node->add_array_bool_vals(v);
            }
        } else if constexpr (std::is_same_v<T, std::string>) {
            for (const auto& v : array->data) {
                derive_node->add_array_string_vals(v);
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            for (auto v : array->data) {
                derive_node->add_array_double_vals(v);
            }
        } else {
            for (auto v : array->data) {
                derive_node->add_array_int_vals(v);
            }
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
                pb_node->mutable_derive_node()->set_placeholder_id(_place_holder_id);
                DB_FATAL("place holder need not transfer pb, %d", _place_holder_id);
                break;
            case pb::ARRAY_LITERAL:
                add_array_values(pb_node);
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
    virtual bool can_use_arrow_vector() {
        return true;
    }

    virtual int transfer_to_arrow_expression() {
        switch (_col_type) {
            case pb::NULL_TYPE: {
                // A scalar value for NullType. Never valid
                arrow::NullScalar null_scalar;
                _arrow_expr = arrow::compute::literal(null_scalar);
                break;
            }
            case pb::BOOL: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<bool>());
                break;
            }
            case pb::INT8: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<int8_t>());
                break;
            }
            case pb::INT16: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<int16_t>());
                break;
            }
            case pb::INT32: 
            case pb::TIME: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<int32_t>());
                break;
            }
            case pb::INT64: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<int64_t>());
                break;
            }
            case pb::UINT8: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<uint8_t>());
                break;
            }
            case pb::UINT16: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<uint16_t>());
                break;
            }
            case pb::UINT32:
            case pb::TIMESTAMP:
            case pb::DATE: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<uint32_t>());
                break;
            }
            case pb::UINT64: 
            case pb::DATETIME: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<uint64_t>());
                break;
            }
            case pb::FLOAT: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<float>());
                break;
            }
            case pb::DOUBLE: {
                _arrow_expr = arrow::compute::literal(_value.cast_to(_col_type).get_numberic<double>());
                break;
            }
            case pb::STRING: 
            case pb::HEX:
            case pb::HLL:
            case pb::BITMAP:
            case pb::TDIGEST: {
                _arrow_expr = arrow::compute::literal(std::make_shared<arrow::LargeBinaryScalar>(_value.cast_to(_col_type).get_string()));
                break;
            }
            default:
                return -1;
        }
        //DB_WARNING("col_type: %d, value: %s", _col_type, _arrow_expr.ToString().c_str());
        return 0;
    }

    virtual std::string to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                               baikal::client::MysqlShortConnection* conn) override;

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
        } else if (_value.is_maxvalue()) {
            _node_type = pb::MAXVALUE_LITERAL;
        } else if (_value.is_array()) {
            _node_type = pb::ARRAY_LITERAL;
        } else {
            _node_type = pb::NULL_LITERAL;
        }
    }

    template<typename T>
    int init_array_from_pb_impl(const pb::ExprNode& node, pb::PrimitiveType array_type) {
        _value.init_array(array_type);
        auto array = _value.get_array<T>();
        if (array == nullptr) {
            DB_WARNING("array is nullptr");
            return -1;
        }
        const auto& derive_node = node.derive_node();
        
        if constexpr (std::is_same_v<T, bool>) {
            for (const auto& v : derive_node.array_bool_vals()) {
                array->add_value(v);
            }
        } else if constexpr (std::is_same_v<T, std::string>) {
            for (const auto& v : derive_node.array_string_vals()) {
                array->add_value(v);
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            for (const auto& v : derive_node.array_double_vals()) {
                array->add_value(static_cast<T>(v));
            }
        } else {
            // uint64_t 也从 array_int_vals 读取，通过 static_cast 转换回来
            for (const auto& v : derive_node.array_int_vals()) {
                array->add_value(static_cast<T>(v));
            }
        }
        return 0;
    }

    int init_array_from_pb(const pb::ExprNode& node) {
        pb::PrimitiveType col_type = node.col_type();
        
        switch (col_type) {
            case pb::ARRAY_BOOL:
                return init_array_from_pb_impl<bool>(node, col_type);
            case pb::ARRAY_INT64:
                return init_array_from_pb_impl<int64_t>(node, col_type);
            case pb::ARRAY_UINT64:
                return init_array_from_pb_impl<uint64_t>(node, col_type);
            case pb::ARRAY_FLOAT:
                return init_array_from_pb_impl<float>(node, col_type);
            case pb::ARRAY_DOUBLE:
                return init_array_from_pb_impl<double>(node, col_type);
            case pb::ARRAY_STRING:
                return init_array_from_pb_impl<std::string>(node, col_type);
            default:
                DB_FATAL("unsupported array type: %s", pb::PrimitiveType_Name(col_type).c_str());
                return -1;
        }
    }

    void add_array_values(pb::ExprNode* pb_node) {
        switch (_col_type) {
            case pb::ARRAY_BOOL:
                add_array_values_impl<bool>(pb_node);
                break;
            case pb::ARRAY_INT64:
                add_array_values_impl<int64_t>(pb_node);
                break;
            case pb::ARRAY_UINT64:
                add_array_values_impl<uint64_t>(pb_node);
                break;
            case pb::ARRAY_FLOAT:
                add_array_values_impl<float>(pb_node);
                break;
            case pb::ARRAY_DOUBLE:
                add_array_values_impl<double>(pb_node);
                break;
            case pb::ARRAY_STRING:
                add_array_values_impl<std::string>(pb_node);
                break;
            default:
                DB_FATAL("array type not support: %s", pb::PrimitiveType_Name(_col_type).c_str());
                break;
        }
    }

private:
    ExprValue _value;
    int _place_holder_id = 0;
    bool _is_place_holder = false;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
