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

#include "filter_node.h"
#include "scalar_fn_call.h"
#include "literal.h"
#include "parser.h"
#include "runtime_state.h"
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

struct SlotPredicate {
    std::vector<ExprNode*> eq_preds;
    std::vector<ExprNode*> lt_le_preds;
    std::vector<ExprNode*> gt_ge_preds;
    std::vector<ExprNode*> in_preds;
    bool need_cut() {
        return (eq_preds.size() + lt_le_preds.size() + gt_ge_preds.size() + in_preds.size()) > 1;
    }
    static bool lt_le_less(ExprNode* left, ExprNode* right) {
        ExprValue l = left->children(1)->get_value(nullptr);
        ExprValue r = right->children(1)->get_value(nullptr);
        if (l.compare_diff_type(r) < 0) {
            return true;
        } else if (l.compare_diff_type(r) == 0) {
            if (static_cast<ScalarFnCall*>(left)->fn().fn_op() == parser::FT_LT) {
                return true;
            }
        }
        return false;
    }
    static bool gt_ge_less(ExprNode* left, ExprNode* right) {
        ExprValue l = left->children(1)->get_value(nullptr);
        ExprValue r = right->children(1)->get_value(nullptr);
        if (l.compare_diff_type(r) > 0) {
            return true;
        } else if (l.compare_diff_type(r) == 0) {
            if (static_cast<ScalarFnCall*>(left)->fn().fn_op() == parser::FT_GT) {
                return true;
            }
        }
        return false;
    }
};

static int predicate_cut(SlotPredicate& preds, std::set<ExprNode*>& cut_preds) {
    auto replace_slot = [](ExprValue eq, ExprNode* expr) {
        Literal* lit = new Literal(eq);
        expr->replace_child(0, lit);
        expr->const_pre_calc();
    };
    // has eq, replace all other exprs can be constant
    if (preds.eq_preds.size() > 0) {
        DB_NOTICE("enter eq_preds");
        ExprValue eq = preds.eq_preds[0]->children(1)->get_value(nullptr);
        for (size_t i = 1; i < preds.eq_preds.size(); i++) {
            replace_slot(eq, preds.eq_preds[i]);
        }
        for (auto expr : preds.lt_le_preds) {
            replace_slot(eq, expr);
        }
        for (auto expr : preds.gt_ge_preds) {
            replace_slot(eq, expr);
        }
        for (auto expr : preds.in_preds) {
            replace_slot(eq, expr);
        }
        return 0;
    }
    // 多个in条件合并,只有in条件都是常量才处理
    if (preds.in_preds.size() > 1) {
        DB_NOTICE("enter in_preds");
        // 取交集
        std::map<std::string, ExprNode*> and_map;
        std::map<std::string, ExprNode*> out_map;
        for (uint32_t i = 1; i < preds.in_preds[0]->children_size(); i++) {
            auto lit = preds.in_preds[0]->children(i);
            if (!lit->is_constant()) {
                return 0;
            }
            and_map[lit->get_value(nullptr).get_string()] = lit;
        }
        for (uint32_t j = 1; j < preds.in_preds.size(); j++) {
            for (uint32_t i = 1; i < preds.in_preds[j]->children_size(); i++) {
                auto lit = preds.in_preds[j]->children(i);
                if (!lit->is_constant()) {
                    cut_preds.clear();
                    return 0;
                }
                const auto& key = lit->get_value(nullptr).get_string();
                if (and_map.count(key) == 1) {
                    out_map[key] = and_map[key];
                }
            }
            out_map.swap(and_map);
            out_map.clear();
            cut_preds.insert(preds.in_preds[j]);
        }
        if (and_map.empty()) {
            return -1;
        }
        std::set<ExprNode*> keep;
        for (auto& pair : and_map) {
            keep.insert(pair.second);
        }
        // 反过来删
        for (uint32_t i = preds.in_preds[0]->children_size() - 1; i >= 1; i--) {
            auto lit = preds.in_preds[0]->children(i);
            if (keep.count(lit) == 0) {
                preds.in_preds[0]->del_child(i);
            }
        }
        DB_NOTICE("in size:%lu", preds.in_preds[0]->children_size());

        return 0;
    }
    ExprNode* lt_le = nullptr;
    ExprNode* gt_ge = nullptr;
    if (preds.lt_le_preds.size() > 0) {
        std::sort(preds.lt_le_preds.begin(), preds.lt_le_preds.end(), SlotPredicate::lt_le_less);
        lt_le = preds.lt_le_preds[0];
        for (size_t i = 1; i < preds.lt_le_preds.size(); i++) {
            cut_preds.insert(preds.lt_le_preds[i]);
        }
    }
    if (preds.gt_ge_preds.size() > 0) {
        std::sort(preds.gt_ge_preds.begin(), preds.gt_ge_preds.end(), SlotPredicate::gt_ge_less);
        gt_ge = preds.gt_ge_preds[0];
        for (size_t i = 1; i < preds.gt_ge_preds.size(); i++) {
            cut_preds.insert(preds.gt_ge_preds[i]);
        }
    }
    // a < 50 and a > 60 => false
    // a <=50 and a >= 50 => a = 50
    if (lt_le != nullptr && gt_ge != nullptr) {
        ExprValue l = lt_le->children(1)->get_value(nullptr);
        ExprValue g = gt_ge->children(1)->get_value(nullptr);
        if (l.compare_diff_type(g) < 0) {
            return -1;
        } else if (l.compare_diff_type(g) == 0) {
            if (static_cast<ScalarFnCall*>(lt_le)->fn().fn_op() == parser::FT_LE &&
                static_cast<ScalarFnCall*>(gt_ge)->fn().fn_op() == parser::FT_GE) {
                // transfer to eq
                pb::ExprNode node;
                node.set_node_type(pb::FUNCTION_CALL);
                node.set_col_type(pb::INVALID_TYPE);
                pb::Function* func = node.mutable_fn();
                func->set_name("eq");
                func->set_fn_op(parser::FT_EQ);
                lt_le->init(node);
                lt_le->expr_optimize();
                cut_preds.insert(gt_ge);
                return 0;
            } else {
                return -1;
            }
        }
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
    std::map<int64_t, SlotPredicate> pred_map;
    for (auto& expr : _conjuncts) {
        //类型推导
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        if (expr->children_size() < 2) {
            continue;
        }
        if (!expr->children(0)->is_slot_ref()) {
            continue;
        }
        SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
        int64_t sign = (slot_ref->tuple_id() << 16) + slot_ref->slot_id();
        bool all_const = true;
        for (uint32_t i = 1; i < expr->children_size(); i++) { 
            if (!expr->children(i)->is_constant()) {
                all_const = false;
                break;
            }
        }
        if (!all_const) {
            continue;
        }
        if (expr->node_type() == pb::IN_PREDICATE) {
            pred_map[sign].in_preds.push_back(expr);
        } else if (expr->node_type() == pb::FUNCTION_CALL) {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ:
                    pred_map[sign].eq_preds.push_back(expr);
                    break;
                case parser::FT_GE:
                case parser::FT_GT:
                    pred_map[sign].gt_ge_preds.push_back(expr);
                    break;
                case parser::FT_LE:
                case parser::FT_LT:
                    pred_map[sign].lt_le_preds.push_back(expr);
                    break;
                default:
                    break;
            }
        }
    }
    // a < 100 and a < 200 => a <100
    std::set<ExprNode*> cut_preds;
    for (auto& pair : pred_map) {
        if (pair.second.need_cut()) {
            ret = predicate_cut(pair.second, cut_preds);
            if (ret < 0) {
                DB_WARNING("expr is always false");
                return -2;
            }
        }
    }

    auto iter = _conjuncts.begin();
    while (iter != _conjuncts.end()) {
        auto expr = *iter;
        if (cut_preds.count(expr) == 1) {
            ExprNode::destroy_tree(expr);
            iter = _conjuncts.erase(iter);
            continue;
        }
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
                DB_WARNING("expr is always false");
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
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }

    std::vector<int64_t>& scan_indices = state->scan_indices();
    for (auto conjunct : _conjuncts) {
        //如果该条件已经被扫描使用的index包含，则不需要再过滤该条件
        // TODO 后续baikaldb直接过滤掉条件，不需要这个了
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
    return 0;
}

void FilterNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto filter_node = pb_node->mutable_derive_node()->mutable_filter_node();
    filter_node->clear_conjuncts();
    for (auto expr : _pruned_conjuncts) {
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
    int64_t where_filter_cnt = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this, &where_filter_cnt](TraceLocalNode& local_node) {
        local_node.add_where_filter_rows(where_filter_cnt);
        local_node.set_affect_rows(_num_rows_returned);
    }));
    
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
        if (_is_explain || need_copy(row.get())) {
            batch->move_row(std::move(row));
            ++_num_rows_returned;
        } else {
            state->inc_num_filter_rows();
            ++where_filter_cnt;
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
    _pruned_conjuncts.clear();
    _child_row_batch.clear();
    _child_row_idx = 0;
    _child_eos = false;
}
void FilterNode::show_explain(std::vector<std::map<std::string, std::string>>& output) {
    ExecNode::show_explain(output);
    if (output.empty()) {
        return;
    }
    if (!_pruned_conjuncts.empty()) {
        output.back()["Extra"] += "Using where; ";
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
