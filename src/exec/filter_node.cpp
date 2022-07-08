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
#include "predicate.h"
#include "literal.h"
#include "parser.h"
#include "runtime_state.h"
#include "query_context.h"
#include "row_expr.h"
#include "scan_node.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h"

namespace baikaldb {

DECLARE_bool(open_nonboolean_sql_forbid);
DECLARE_bool(open_nonboolean_sql_statistics);

int FilterNode::init(const pb::PlanNode& node) {
    int ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    pb::FilterNode filter_node;
    filter_node.ParseFromString(node.derive_node().filter_node());
    for (auto& expr : filter_node.conjuncts()) {
        ExprNode* conjunct = nullptr;
        ret = ExprNode::create_tree(expr, &conjunct);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _conjuncts.emplace_back(conjunct);
    }
    for (auto& expr : node.derive_node().raw_filter_node().conjuncts()) {
        ExprNode* conjunct = nullptr;
        ret = ExprNode::create_tree(expr, &conjunct);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _conjuncts.emplace_back(conjunct);
    }

    _pb_node.mutable_derive_node()->clear_raw_filter_node();
    return 0;
}

struct SlotPredicate {
    std::vector<ExprNode*> eq_preds;
    std::vector<ExprNode*> lt_le_preds;
    std::vector<ExprNode*> gt_ge_preds;
    std::vector<ExprNode*> in_preds;
    SlotRef* slot_ref = nullptr;
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
    auto get_slot_ref_idx = [&preds](ExprNode* expr) {
        if (!expr->is_row_expr()) {
            return -1;
        }
        RowExpr* row = static_cast<RowExpr*>(expr);
        int ret = row->open();
        if (ret < 0) {
            DB_WARNING("expr open fail:%d", ret);
            return -1;
        }
        return row->get_slot_ref_idx(preds.slot_ref->tuple_id(), preds.slot_ref->slot_id());
    };
    auto replace_slot = [&get_slot_ref_idx](ExprValue eq, ExprNode* expr) {
        Literal* lit = new Literal(eq);
        if (expr->children(0)->is_row_expr()) {
            int idx = get_slot_ref_idx(expr->children(0));
            if (idx < 0) {
                DB_WARNING("get_slot_ref_idx fail:%d", idx);
                return;
            }
            expr->children(0)->replace_child(idx, lit);
        } else {
            expr->replace_child(0, lit);
        }
        expr->const_pre_calc();
    };
    // has eq, replace all other exprs can be constant
    if (preds.eq_preds.size() > 0) {
        DB_DEBUG("enter eq_preds");
        ExprValue eq;
        if (preds.eq_preds[0]->children(0)->is_row_expr()) {
            int idx = get_slot_ref_idx(preds.eq_preds[0]->children(0));
            if (idx < 0) {
                DB_WARNING("get_slot_ref_idx fail:%d", idx);
                return 0;
            }
            eq = static_cast<RowExpr*>(preds.eq_preds[0]->children(1))->get_value(nullptr, idx);
        } else {
            eq = preds.eq_preds[0]->children(1)->get_value(nullptr);
        }
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
    if (preds.in_preds.size() > 0) {
        DB_DEBUG("enter in_preds");
        // 取交集
        std::map<std::string, ExprNode*> and_map;
        std::map<std::string, ExprNode*> out_map;
        int idx0 = -1;
        if (preds.in_preds[0]->children(0)->is_row_expr()) {
            idx0 = get_slot_ref_idx(preds.in_preds[0]->children(0));
            if (idx0 < 0) {
                DB_WARNING("get_slot_ref_idx fail:%d", idx0);
                return 0;
            }
            for (uint32_t i = 1; i < preds.in_preds[0]->children_size(); i++) {
                auto lit = preds.in_preds[0]->children(i);
                if (!lit->is_constant()) {
                    return 0;
                }
                and_map[static_cast<RowExpr*>(lit)->get_value(nullptr, idx0).get_string()] = lit;
            }
        } else {
            for (uint32_t i = 1; i < preds.in_preds[0]->children_size(); i++) {
                auto lit = preds.in_preds[0]->children(i);
                if (!lit->is_constant()) {
                    return 0;
                }
                and_map[lit->get_value(nullptr).get_string()] = lit;
            }
        }
        for (uint32_t j = 1; j < preds.in_preds.size(); j++) {
            // TODO (a , b) in (("a1", "b1")) and (b, c) in (("b1", "c1")) 合并为 (a,b,c) in (("a1","b1","c1"))
            if (preds.in_preds[j]->children(0)->is_row_expr()) {
                int idx = get_slot_ref_idx(preds.in_preds[j]->children(0));
                if (idx < 0) {
                    DB_WARNING("get_slot_ref_idx fail:%d", idx);
                    return 0;
                }
                for (uint32_t i = 1; i < preds.in_preds[j]->children_size(); i++) {
                    auto lit = preds.in_preds[j]->children(i);
                    if (!lit->is_constant()) {
                        cut_preds.clear();
                        return 0;
                    }
                    const auto& key = static_cast<RowExpr*>(lit)->get_value(nullptr, idx).get_string();
                    if (and_map.count(key) == 1) {
                        out_map[key] = and_map[key];
                    }
                }
            } else {
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
                cut_preds.emplace(preds.in_preds[j]);
            }
            out_map.swap(and_map);
            out_map.clear();
        }
        if (and_map.empty()) {
            return -1;
        }
        std::set<std::string> keep;
        for (auto& pair : and_map) {
            auto v = pair.second->get_value(nullptr);
            bool need_keep = true;
            for (auto expr : preds.lt_le_preds) {
                replace_slot(v, expr);
                int ret = expr->open();
                if (ret < 0) {
                    DB_WARNING("expr open fail:%d", ret);
                    return ret;
                }
                auto value = expr->get_value(nullptr);
                if (value.is_null() || value.get_numberic<bool>() == false) {
                    need_keep = false;
                }
                cut_preds.emplace(expr);
            }
            for (auto expr : preds.gt_ge_preds) {
                replace_slot(v, expr);
                int ret = expr->open();
                if (ret < 0) {
                    DB_WARNING("expr open fail:%d", ret);
                    return ret;
                }
                auto value = expr->get_value(nullptr);
                if (value.is_null() || value.get_numberic<bool>() == false) {
                    need_keep = false;
                }
                cut_preds.emplace(expr);
            }
            if (need_keep) {
                keep.emplace(pair.first);
            }
        }
        if (keep.size() == 0) {
            return -1;
        }
        // 反过来删
        for (uint32_t i = preds.in_preds[0]->children_size() - 1; i >= 1; i--) {
            auto lit = preds.in_preds[0]->children(i);
            const auto& key = lit->is_row_expr() ?
                    static_cast<RowExpr*>(lit)->get_value(nullptr, idx0).get_string():
                    lit->get_value(nullptr).get_string();
            if (keep.count(key) == 0) {
                preds.in_preds[0]->del_child(i);
            }
        }
        // in_row_expr因为条件里还有其它字段,不能剪切整个preds,可以剪切部分行
        // 例如 a in ("a1", "a2") and (a, b) in (("a1","b1"), ("a3","b3"))
        // in_row_pred (a, b)可以剪切为:(("a1","b1"))
        for (uint32_t j = 1; j < preds.in_preds.size(); j++) {
            if (preds.in_preds[j]->children(0)->is_row_expr()) {
                int idx = get_slot_ref_idx(preds.in_preds[j]->children(0));
                // 反过来删
                for (uint32_t i = preds.in_preds[j]->children_size() - 1; i >= 1; i--) {
                    auto lit = preds.in_preds[j]->children(i);
                    const auto& key = static_cast<RowExpr*>(lit)->get_value(nullptr, idx).get_string();
                    if (keep.count(key) == 0) {
                        preds.in_preds[j]->del_child(i);
                    }
                }
            }
        }
        DB_DEBUG("in size:%lu", preds.in_preds[0]->children_size() -1);

        return 0;
    }
    ExprNode* lt_le = nullptr;
    ExprNode* gt_ge = nullptr;
    if (preds.lt_le_preds.size() > 0) {
        std::sort(preds.lt_le_preds.begin(), preds.lt_le_preds.end(), SlotPredicate::lt_le_less);
        lt_le = preds.lt_le_preds[0];
        for (size_t i = 1; i < preds.lt_le_preds.size(); i++) {
            cut_preds.emplace(preds.lt_le_preds[i]);
        }
    }
    if (preds.gt_ge_preds.size() > 0) {
        std::sort(preds.gt_ge_preds.begin(), preds.gt_ge_preds.end(), SlotPredicate::gt_ge_less);
        gt_ge = preds.gt_ge_preds[0];
        for (size_t i = 1; i < preds.gt_ge_preds.size(); i++) {
            cut_preds.emplace(preds.gt_ge_preds[i]);
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
                cut_preds.emplace(gt_ge);
                return 0;
            } else {
                return -1;
            }
        }
    }
    return 0;
}

int FilterNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail, ret:%d", ret);
        return ret;
    }
    // sign => pred
    std::map<int64_t, SlotPredicate> pred_map;
    size_t idx = 0;
    for (auto& expr : _conjuncts) {
        //类型推导
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        //非bool型表达式判断
        if (expr->col_type() != pb::BOOL) {
            ExprNode::_s_non_boolean_sql_cnts << 1;
            DB_WARNING("current sql [%s] is not bool type, current exprnode_type is [%s]", 
                ctx->sql.c_str(), pb::ExprNodeType_Name(expr->node_type()).c_str());
            if (FLAGS_open_nonboolean_sql_forbid) {
                return NOT_BOOL_ERRCODE;
            }
        }
        // TODO 除了not in外，其他计算null的地方在index_selector判断了，应该统一处理
        if (expr->node_type() == pb::NOT_PREDICATE) {
            if (static_cast<NotPredicate*>(expr)->always_null_or_false()) {
                DB_WARNING("expr not is always null or false");
                ctx->return_empty = true;
                return 0;
            }
        }

        ExprNode* e = expr->transfer();
        if (e != expr) {
            delete expr;
            _conjuncts[idx] = e;
            expr = e;
        }
        idx++;
        if (expr->children_size() < 2) {
            continue;
        }

        // 处理in_row_expr
        // in_row_expr的preds只有涉及字段全部匹配索引才会在index_selector环节别裁剪掉, 此处仅裁剪 in里面的记录
        // 例如： key idx_abcd(a, b, c, d),
        // 条件: (a, b) in (("a1", "b1"), ("a2","b2")) and (b , c) in (("b1", "c1"), ("b2","c2")，("b3","c3"))，
        // b字段同时对应了2个in条件，则b字段只会使用in的值较少的一个，本例为会使用(a,b)
        // 条件:(a,b)全部字段匹配索引将会在index_selector环节被裁剪掉
        // 条件:(b, c)只会索引匹配字段c,因为b字段已经被(a, b)匹配了, 条件保留,但("b3","c3")会被裁剪掉,裁剪后:(b,c) in (("b1", "c1"),("b2","c2"));
        // 索引的ranges将被展开为,相当于:(a,b) in (("a1", "b1"), ("a2","b2")) and c in ("c1","c2")
        // a1, b1, c1
        // a2, b2, c1
        // a1, b1, c2
        // a2, b2, c2
        if (expr->children(0)->is_row_expr()) {
            RowExpr* row_expr = static_cast<RowExpr*>(expr->children(0));
            std::map<size_t, SlotRef*> slots;
            std::map<size_t, std::vector<ExprValue>> values;
            row_expr->get_all_slot_ref(&slots);
            // TODO 对于非全部slot的也可以处理
            if (slots.size() != row_expr->children_size()) {
                continue;
            }
            bool all_const = true;
            bool all_row_expr = true;
            for (uint32_t i = 1; i < expr->children_size(); i++) {
                if (!expr->children(i)->is_constant()) {
                    all_const = false;
                    break;
                }
                if (!expr->children(i)->is_row_expr()) {
                    all_row_expr = false;
                    break;
                }
            }
            if (!all_const || !all_row_expr) {
                continue;
            }
            for (auto& pair : slots) {
                SlotRef* slot_ref = pair.second;
                int64_t sign = (slot_ref->tuple_id() << 16) + slot_ref->slot_id();
                pred_map[sign].slot_ref = slot_ref;
                if (expr->node_type() == pb::IN_PREDICATE) {
                    pred_map[sign].in_preds.emplace_back(expr);
                } else if (expr->node_type() == pb::FUNCTION_CALL) {
                    int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
                    switch (fn_op) {
                        case parser::FT_EQ:
                            pred_map[sign].eq_preds.emplace_back(expr);
                            break;
                        case parser::FT_GE:
                        case parser::FT_GT:
                        case parser::FT_LE:
                        case parser::FT_LT:
                            // TODO
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        if (!expr->children(0)->is_slot_ref()) {
            continue;
        }
        SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
        int64_t sign = (slot_ref->tuple_id() << 16) + slot_ref->slot_id();
        bool all_const = true;
        for (uint32_t i = 1; i < expr->children_size(); i++) { 
            // place holder被替换会导致下一次exec参数对不上
            if (!expr->children(i)->is_constant() || expr->children(i)->has_place_holder()) {
                all_const = false;
                break;
            }
        }
        if (!all_const) {
            continue;
        }
        // TODO 整块剪枝逻辑挪到index selector
        if (expr->node_type() == pb::IN_PREDICATE) {
            pred_map[sign].in_preds.emplace_back(expr);
        } else if (expr->node_type() == pb::FUNCTION_CALL) {
            int32_t fn_op = static_cast<ScalarFnCall*>(expr)->fn().fn_op();
            switch (fn_op) {
                case parser::FT_EQ:
                    pred_map[sign].eq_preds.emplace_back(expr);
                    break;
                case parser::FT_GE:
                case parser::FT_GT:
                    pred_map[sign].gt_ge_preds.emplace_back(expr);
                    break;
                case parser::FT_LE:
                case parser::FT_LT:
                    pred_map[sign].lt_le_preds.emplace_back(expr);
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
                ctx->return_empty = true;
                return 0;
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
            // TODO 对于非全部constant的也可以处理
            // 例如: a = "a1" and (a, b) in ("a2", "b1"), 会被替换换为("a1", b) in ("a2", "b1")
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
                ctx->return_empty = true;
                return 0;
            } else if (!expr->has_place_holder()) {
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
        input_exprs.emplace_back(expr);     
    }
    if (_children.size() != 1) {
        DB_WARNING("filter node pushdown fail");
        return -1;
    }
    _children[0]->predicate_pushdown(input_exprs);
    _conjuncts.clear();
    for (auto& expr: input_exprs) {
        _conjuncts.emplace_back(expr);
    }
    input_exprs.clear();
    return 0;
}

int FilterNode::open(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    
    if (_return_empty) {
        return 0;
    }
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    bool check_memory = false;
    int64_t expr_used_size = 0;
    if (_conjuncts.size() > 10000) {
        check_memory = true;
    }
    for (auto conjunct : _conjuncts) {
        ret = conjunct->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
        if (check_memory) {
            expr_used_size += conjunct->used_size();
        }
        _pruned_conjuncts.emplace_back(conjunct);
    }
    if (check_memory && 0 != state->memory_limit_exceeded(std::numeric_limits<int>::max(), expr_used_size)) {
        return -1;
    }
    return 0;
}

void FilterNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    std::vector<ExecNode*> scan_nodes;
    ScanNode* scan_node = nullptr;
    get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() == 1) {
        scan_node = static_cast<ScanNode*>(scan_nodes[0]);
    }
    if (!_is_explain) {
        if (scan_node != nullptr && scan_node->current_use_global_backup()) {
            pb::FilterNode filter_node;
            for (const auto& conjunct : _raw_filter_node.conjuncts_learner()) {
                auto c = filter_node.add_conjuncts();
                c->CopyFrom(conjunct);
            }
            std::string filter_string;
            filter_node.SerializeToString(&filter_string);
            pb_node->mutable_derive_node()->set_filter_node(filter_string);
        } else {
#ifdef BAIDU_INTERNAL
#ifndef NDEBUG
            // 调试日志
            pb::FilterNode filter_node;
            std::string str = _filter_node;
            filter_node.ParseFromString(str);
            DB_DEBUG("filter_node: %s", filter_node.ShortDebugString().c_str());
#endif
#else //BAIDU_INTERNAL
#ifndef NDEBUG
            if (FLAGS_enable_debug) {
                // 调试日志
                pb::FilterNode filter_node;
                std::string str = _filter_node;
                filter_node.ParseFromString(str);
                DB_DEBUG("filter_node: %s", filter_node.ShortDebugString().c_str());
            }
#endif
#endif
            pb_node->mutable_derive_node()->set_filter_node(_filter_node);
        }
    } else {
        auto filter_node = pb_node->mutable_derive_node()->mutable_raw_filter_node();
        filter_node->CopyFrom(_raw_filter_node);
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
    if (_return_empty) {
        DB_WARNING_STATE(state, "return_empty");
        state->set_eos();
        *eos = true;
        return 0;
    }
    
    while (1) {
        if (batch->is_full()) {
            return 0;
        }
        if (_child_row_batch.is_traverse_over()) {
            // 兼容fullexport多轮计算
            if (_child_eos && 
                (state->is_eos() || !state->is_full_export)) {
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
        std::unique_ptr<MemRow>& row = _child_row_batch.get_row();
        if (_is_explain || need_copy(row.get())) {
            batch->move_row(std::move(row));
            ++_num_rows_returned;
        } else {
            state->inc_num_filter_rows();
            ++where_filter_cnt;
            state->memory_limit_release(state->num_scan_rows(), row->used_size());
        }
        if (reached_limit()) {
            DB_WARNING_STATE(state, "reach limit size:%lu", batch->size());
            *eos = true;
            return 0;
        }
        _child_row_batch.next();
    }
    return 0;
}

void FilterNode::remove_additional_predicate(std::vector<ExprNode*>& input_exprs) {
    auto iter1 = _conjuncts.begin();
    while (iter1 != _conjuncts.end()) {
        auto iter2 = input_exprs.begin();
        while (iter2 != input_exprs.end()) {
            if (*iter1 == *iter2) {
                iter1 = _conjuncts.erase(iter1);
                auto remove_expr = *iter2;
                remove_expr->close();
                iter2 = input_exprs.erase(iter2);
                delete remove_expr;
                break;
            } else {
                ++iter2;
            }
        }
        if (iter2 == input_exprs.end()) {
            break;
        }
    }
    _pruned_conjuncts.clear();
    _child_row_batch.clear();
    _raw_filter_node.Clear();
    _filter_node.clear();
    _child_row_idx = 0;
    _child_eos = false;
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
