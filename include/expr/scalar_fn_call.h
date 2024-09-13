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

#include <functional>
#include "expr_node.h"
#include "fn_manager.h"

namespace baikaldb {
class ScalarFnCall : public ExprNode {
public:
    virtual int init(const pb::ExprNode& node);
    virtual int type_inferer();
    virtual void children_swap();
    virtual int open();
    virtual ExprValue get_value(MemRow* row);
    virtual ExprValue get_value(const ExprValue& value);
    const pb::Function& fn() {
        return _fn;
    }
    virtual void transfer_pb(pb::ExprNode* pb_node) {
        ExprNode::transfer_pb(pb_node);
        pb_node->mutable_fn()->CopyFrom(_fn);
    }
    ExprNode* get_last_insert_id() {
        if (_fn.name() == "last_insert_id") {
            return this;
        }
        return ExprNode::get_last_insert_id();
    }
    ExprNode* get_last_value() {
        if (_fn.name() == "last_value") {
            return this;
        }
        return ExprNode::get_last_value();
    }
    virtual bool is_valid_int_cast(MemRow* row) {
        if (_fn.fn_op() == parser::FT_ADD || _fn.fn_op() == parser::FT_MINUS) {
            if (_children.size() != 2 || !_children[0]->is_valid_int_cast(row) || !_children[1]->is_valid_int_cast(row)) {
                return false;
            }
            auto left = children(0)->get_value(row).cast_to(pb::INT64)._u.int64_val;
            auto right = children(1)->get_value(row).cast_to(pb::INT64)._u.int64_val;
            auto s = get_value(row).cast_to(pb::INT64)._u.int64_val;
            if (_fn.fn_op() == parser::FT_MINUS) {
                right = -right;
            }
            if (left >= 0 && right >= 0 && s < 0) {
                return false; // 上溢
            }
            if (left < 0 && right < 0 && s >= 0) {
                return false; // 下溢
            }
            return true;
        }
        return ExprNode::is_valid_int_cast(row);
    }
    virtual bool is_valid_double_cast(MemRow* row) {
        if (_fn.fn_op() == parser::FT_ADD || _fn.fn_op() == parser::FT_MINUS) {
            if (_children.size() != 2 || !_children[0]->is_valid_double_cast(row) || !_children[1]->is_valid_double_cast(row)) {
                return false;
            }
            auto left = children(0)->get_value(row).cast_to(pb::DOUBLE)._u.double_val;
            auto right = children(1)->get_value(row).cast_to(pb::DOUBLE)._u.double_val;
            auto s = get_value(row).cast_to(pb::DOUBLE)._u.double_val;
            if (_fn.fn_op() == parser::FT_MINUS) {
                right = -right;
            }
            if (left >= 0 && right >= 0 && s < 0) {
                return false; // 上溢
            }
            if (left < 0 && right < 0 && s >= 0) {
                return false; // 下溢
            }
            return true;
        }
        return ExprNode::is_valid_int_cast(row);
    }
private:
    ExprValue multi_eq_value(MemRow* row) {
        for (size_t i = 0; i < children(0)->children_size(); i++) {
            auto left = children(0)->children(i)->get_value(row);
            auto right = children(1)->children(i)->get_value(row);
            if (left.compare_diff_type(right) != 0) {
                return ExprValue::False();
            }
        }
        return ExprValue::True();
    }

    ExprValue multi_ne_value(MemRow* row) {
        for (size_t i = 0; i < children(0)->children_size(); i++) {
            auto left = children(0)->children(i)->get_value(row);
            auto right = children(1)->children(i)->get_value(row);
            if (left.compare_diff_type(right) != 0) {
                return ExprValue::True();
            }
        }
        return ExprValue::False();
    }

    ExprValue multi_lt_value(MemRow* row) {
        for (size_t i = 0; i < children(0)->children_size(); i++) {
            auto left = children(0)->children(i)->get_value(row);
            auto right = children(1)->children(i)->get_value(row);
            if (left.compare_diff_type(right) < 0) {
                return ExprValue::True();
            } else if (left.compare_diff_type(right) > 0) {
                return ExprValue::False();
            }
        }
        return ExprValue::False();
    }

    ExprValue multi_le_value(MemRow* row) {
        for (size_t i = 0; i < children(0)->children_size(); i++) {
            auto left = children(0)->children(i)->get_value(row);
            auto right = children(1)->children(i)->get_value(row);
            if (left.compare_diff_type(right) < 0) {
                return ExprValue::True();
            } else if (left.compare_diff_type(right) > 0) {
                return ExprValue::False();
            }
        }
        return ExprValue::True();
    }

    ExprValue multi_gt_value(MemRow* row) {
        for (size_t i = 0; i < children(0)->children_size(); i++) {
            auto left = children(0)->children(i)->get_value(row);
            auto right = children(1)->children(i)->get_value(row);
            if (left.compare_diff_type(right) < 0) {
                return ExprValue::False();
            } else if (left.compare_diff_type(right) > 0) {
                return ExprValue::True();
            }
        }
        return ExprValue::False();
    }

    ExprValue multi_ge_value(MemRow* row) {
        for (size_t i = 0; i < children(0)->children_size(); i++) {
            auto left = children(0)->children(i)->get_value(row);
            auto right = children(1)->children(i)->get_value(row);
            if (left.compare_diff_type(right) < 0) {
                return ExprValue::False();
            } else if (left.compare_diff_type(right) > 0) {
                return ExprValue::True();
            }
        }
        return ExprValue::True();
    }
protected:
    pb::Function _fn;
    bool _is_row_expr = false;
    std::function<ExprValue(const std::vector<ExprValue>&)> _fn_call;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
