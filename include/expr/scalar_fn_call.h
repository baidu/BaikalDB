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
    virtual ExprNode* transfer();
    virtual int open();
    virtual ExprValue get_value(MemRow* row);
    pb::Function fn() {
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
    ExprNode* transfer_date_format();
    ExprNode* transfer_from_or_to_in();
protected:
    pb::Function _fn;
    bool _is_row_expr = false;
    std::function<ExprValue(const std::vector<ExprValue>&)> _fn_call;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
