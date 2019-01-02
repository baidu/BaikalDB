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

#include "runtime_state.h"
#include "filter_node.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h"

namespace baikaldb {
int FilterNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    for (auto& expr : node.derive_node().filter_node().conjuncts()) {
        ExprNode* conjunct = nullptr;
        ret = ExprNode::create_tree(expr, &conjunct);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _conjuncts.push_back(conjunct);
    }
    return 0;
}

int FilterNode::expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs) {
    int ret = 0;
    ret = ExecNode::expr_optimize(tuple_descs);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail, ret:%d", ret);
        return ret;
    }
    auto iter = _conjuncts.begin();
    while (iter != _conjuncts.end()) {
        auto expr = *iter;
        //类型推导
        ret = expr->type_inferer();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        //常量表达式计算
        expr->const_pre_calc();
        if (expr->is_constant()) {
            ret = expr->open();
            if (ret < 0) {
                DB_WARNING("expr open fail:%d", ret);
                return ret;
            }
            ExprValue value = expr->get_value(nullptr);
            expr->close();
            //有一个false则应该返回0行，true则直接删除
            if (value.is_null() || value.get_numberic<bool>() == false) {
                return -2;
            } else {
                ExprNode::destroy_tree(expr);
                iter = _conjuncts.erase(iter);
                continue;
            }
        }
        ++iter;
    }
    return 0;
}
int FilterNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    //having条件暂时不下推
    if (_node_type == pb::HAVING_FILTER_NODE) {
        ExecNode::predicate_pushdown(input_exprs);
        return 0;
    }
    for (auto& expr : _conjuncts) {
        input_exprs.push_back(expr);     
    }
    if (_children.size() != 1) {
        DB_WARNING("filter node pushdown fail");
        return -1;
    }
    _children[0]->predicate_pushdown(input_exprs);
    _conjuncts.clear();
    for (auto& expr: input_exprs) {
        _conjuncts.push_back(expr);
    }
    input_exprs.clear();
    return 0;
}

int FilterNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }

    std::vector<int64_t>& scan_indices = state->scan_indices();
    for (auto conjunct : _conjuncts) {
        //如果该条件已经被扫描使用的index包含，则不需要再过滤该条件
        if (conjunct->contained_by_index(scan_indices)) {
            continue;
        }
        ret = conjunct->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
        _pruned_conjuncts.push_back(conjunct);
    }
    //DB_WARNING_STATE(state, "_conjuncts_size:%ld, pruned_conjuncts_size:%ld", _conjuncts.size(), _pruned_conjuncts.size());
    return 0;
}

void FilterNode::transfer_pb(pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(pb_node);
    auto filter_node = pb_node->mutable_derive_node()->mutable_filter_node();
    filter_node->clear_conjuncts();
    for (auto expr : _conjuncts) {
        ExprNode::create_pb_expr(filter_node->add_conjuncts(), expr);
    }
}

inline bool FilterNode::need_copy(MemRow* row) {
    for (auto conjunct : _pruned_conjuncts) {
        ExprValue value = conjunct->get_value(row);
        if (value.is_null() || value.get_numberic<bool>() == false) {
            return false;
        }
    }
    return true;
}

int FilterNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    while (1) {
        if (batch->is_full()) {
            return 0;
        }
        if (_child_row_batch.is_traverse_over()) {
            if (_child_eos) {
                *eos = true;
                return 0;
            } else {
                //TimeCost cost;
                _child_row_batch.clear();
                _child_row_batch.set_capacity(state->multiple_row_batch_capacity());
                auto ret = _children[0]->get_next(state, &_child_row_batch, &_child_eos);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "_children get_next fail");
                    return ret;
                }
                //DB_WARNING_STATE(state, "_child_row_batch:%u %u", _child_row_batch.capacity(), _child_row_batch.size());
                //DB_NOTICE("scan cost:%ld", cost.get_time());
                continue;
            }
        }
        if (reached_limit()) {
            DB_WARNING_STATE(state, "reach limit size:%u", batch->size());
            *eos = true;
            return 0;
        }
        std::unique_ptr<MemRow>& row = _child_row_batch.get_row();
        if (need_copy(row.get())) {
            batch->move_row(std::move(row));
            ++_num_rows_returned;
        }
        _child_row_batch.next();
    }
    return 0;
}

void FilterNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto conjunct : _conjuncts) {
        conjunct->close();
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
