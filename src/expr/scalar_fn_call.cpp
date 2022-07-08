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

#include "scalar_fn_call.h"
#include "slot_ref.h"
#include "row_expr.h"
#include "literal.h"
#include "parser.h"
#include "predicate.h"

namespace baikaldb {
DEFINE_bool(open_nonboolean_sql_forbid, false, "open nonboolean sqls forbid default:false");
int ScalarFnCall::init(const pb::ExprNode& node) {
    int ret = 0;
    ret = ExprNode::init(node);
    if (ret < 0) {
        return ret;
    }
    //scalar函数默认是true，只有参数中有非const的才是false
    _is_constant = true;
    if (!node.has_fn()) {
        return -1;
    }
    _fn = node.fn();
    // rand不是const
    if (node_type() == pb::FUNCTION_CALL && _fn.name() == "rand") {
        _is_constant = false;
    }
    return 0;
}

int ScalarFnCall::type_inferer() {
    int ret = 0;
    ret = ExprNode::type_inferer();
    if (ret < 0) {
        return ret;
    }
    if (_fn_call != NULL) {
        // 避免重复执行后续逻辑
        return ret;
    }
    // 兼容mysql， predicate 处理成列的类型
    switch (_fn.fn_op()) { 
        case parser::FT_EQ:
        case parser::FT_IN:
        case parser::FT_NE:
        case parser::FT_GE:
        case parser::FT_GT:
        case parser::FT_LE:
        case parser::FT_LT: {
            if (_children[0]->is_slot_ref() && _children[1]->is_constant()) {
                for (size_t i = 1; i < children_size(); i++) {
                    _children[i]->set_col_type(_children[0]->col_type());
                }
            } else if (_children[0]->is_row_expr() && !_children[0]->is_constant() && _children[1]->is_constant()) {
                std::map<size_t, SlotRef*> slots;
                static_cast<RowExpr*>(_children[0])->get_all_slot_ref(&slots);
                for (uint32_t i = 1; i < children_size(); i++) {
                    for (auto& pair : slots) {
                        size_t idx = pair.first;
                        pb::PrimitiveType tp = pair.second->col_type();
                        _children[i]->children(idx)->set_col_type(tp);
                    }
                }
            }
            break;
        } 
        default:
            break;
    }
    std::vector<pb::PrimitiveType> types;
    for (auto c : _children) {
        if (c->col_type() == pb::INVALID_TYPE && !c->is_row_expr()) {
            DB_WARNING("_children is pb::INVALID_TYPE, node:%d", c->node_type());
            return -1;
        }
        if (is_logical_and_or_not()) {
            //类型推导过程中分析表达式节点类型是否为bool型
            if (c->col_type() != pb::BOOL) {
                DB_WARNING("_children is not bool type, ScalarFnCall ExprNode type is [%s], children node_type_is [%s]", 
                    pb::ExprNodeType_Name(_node_type).c_str(),
                    pb::ExprNodeType_Name(c->node_type()).c_str());
                    ExprNode::_s_non_boolean_sql_cnts << 1;
                if (FLAGS_open_nonboolean_sql_forbid) {
                    return NOT_BOOL_ERRCODE;
                }
            }
        }
        types.push_back(c->col_type());
    }
    ret = FunctionManager::complete_fn(_fn, types);

    if (_col_type == pb::INVALID_TYPE) {
        _col_type = _fn.return_type();
    }

    // Literal type cast
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        if (_children[i]->is_literal()) {
            static_cast<Literal*>(_children[i])->cast_to_col_type(_fn.arg_types(i));
        }
    }
    return 0;
}

// 111 > aaa => aaa < 111 
// (11,22) < (a,b) => (a,b) > (11,22)
void ScalarFnCall::children_swap() {
    if (_children.size() != 2) {
        return;
    }
    if (_children[0]->is_constant() && !_children[1]->is_constant() &&
        (_children[1]->is_slot_ref() || _children[1]->is_row_expr())) {
        FunctionManager* fn_manager = FunctionManager::instance();
        if (fn_manager->swap_op(_fn)) {
            std::swap(_children[0], _children[1]);
        }
    }
}

int ScalarFnCall::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    if ((int)_children.size() < _fn.arg_types_size()) {
        DB_WARNING("_children.size:%lu < _fn.arg_types_size:%d",
                _children.size(), _fn.arg_types_size());
        return -1;
    }
    if (children_size() > 0 && children(0)->is_row_expr()) {
        if (_fn.fn_op() != parser::FT_EQ &&
            _fn.fn_op() != parser::FT_NE &&
            _fn.fn_op() != parser::FT_GE &&
            _fn.fn_op() != parser::FT_GT &&
            _fn.fn_op() != parser::FT_LE &&
            _fn.fn_op() != parser::FT_LT &&
            _fn.fn_op() != parser::FT_MATCH_AGAINST) {
            DB_FATAL("Operand should contain 1 column(s)");
            return -1;
        }
        size_t col_size = children(0)->children_size();
        if (_fn.fn_op() == parser::FT_MATCH_AGAINST) {
            if (col_size > 1) {
                DB_FATAL("MATCH_AGAINST column list support only 1, size:%lu", col_size);
                return -1;
            }
            return 0;
        }
        _is_row_expr = true;
        for (size_t i = 1; i < children_size(); i++) {
            if (!children(i)->is_row_expr() ||
                children(i)->children_size() != col_size) {
                DB_FATAL("Operand should contain %lu column(s)", col_size);
                return -1;
            }
        }
    }
    /*
    if (_fn.return_type() != _col_type) {
        DB_WARNING("_fn.return_type:%d != _col_type:%d", _fn.return_type(), _col_type);
        return -1;
    }
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        if (_fn.arg_types(i) != _children[i]._col_type) {
            return -1;
        }
    }*/
    FunctionManager* fn_manager = FunctionManager::instance();
    _fn_call = fn_manager->get_object(_fn.name());
    if (node_type() == pb::FUNCTION_CALL && _fn_call == NULL) {
        DB_WARNING("fn call is null, name:%s", _fn.name().c_str());
    }
    return 0;
}

ExprValue ScalarFnCall::get_value(MemRow* row) {
    if (_is_row_expr) {
        switch (_fn.fn_op()) {
            case parser::FT_EQ:
                return multi_eq_value(row);
            case parser::FT_NE:
                return multi_ne_value(row);
            case parser::FT_GE:
                return multi_ge_value(row);
            case parser::FT_GT:
                return multi_gt_value(row);
            case parser::FT_LE:
                return multi_le_value(row);
            case parser::FT_LT:
                return multi_lt_value(row);
            default:
                return ExprValue::Null();
        }
    }
    if (_fn_call == NULL) {
        return ExprValue::Null();
    }
    std::vector<ExprValue> args;
    for (auto c : _children) {
        args.push_back(c->get_value(row));
    }
    //类型转化
    for (int i = 0; i < _fn.arg_types_size(); i++) {
        args[i].cast_to(_fn.arg_types(i));
    }
    return _fn_call(args).cast_to(_col_type);
}

ExprNode* ScalarFnCall::transfer() {
    for (size_t i = 0; i < _children.size(); i++) {
        ExprNode* e = _children[i]->transfer();
        if (e != _children[i]) {
            delete _children[i];
            _children[i] = e;
        }
    }
    ExprNode* to = this;
    switch (_fn.fn_op()) {
//        case parser::FT_EQ:
//        case parser::FT_NE:
        case parser::FT_GE:
        case parser::FT_GT:
        case parser::FT_LE:
        case parser::FT_LT: {
            if (_children[0]->node_type() == pb::FUNCTION_CALL) {
                ScalarFnCall* e = static_cast<ScalarFnCall*>(_children[0]);
                if (e->fn().name() == "date_format") {
                    to = transfer_date_format();
                }
            }
            break;
        }
        case parser::FT_LOGIC_OR: {
            to = transfer_from_or_to_in();
            break;
        }
        default:
            break;
    }
    return to;
}

ExprNode* ScalarFnCall::transfer_date_format() {
    // [YY]YY-MM-DD HH:MM:SS.xxxxxx => %[Y|y]-%m-%d %H:%i:%[s|S].%f
    // [YY]YYMMDDHHMMSS.xxxxxx => %[Y|y]%m%d%H%i%[s|S].%f
    ExprNode* to = this;
    ScalarFnCall* e = static_cast<ScalarFnCall*>(_children[0]);
    if (e->children_size() != 2) {
        return to;
    }
    if (!e->children(0)->is_slot_ref() || !is_datetime_specic(e->children(0)->col_type())) {
        return to;
    }
    if (!e->children(1)->is_literal() || e->children(1)->col_type() != pb::STRING) {
        return to;
    }
    if (!_children[1]->is_literal()) {
        return to;
    }
    std::string fmt = e->children(1)->get_value(nullptr).get_string();
    std::string date = _children[1]->get_value(nullptr).get_string();
    // 求出前缀匹配精度，例如fmt="%Y-%m-%d %H:%i", date="2022-07-15 16 12"
    // 则精度 = 小时，fmt="%Y-%m-%d %H" + ":%i", date="2022-07-15 16" + " 12"
    // fmt与date将从精度处拆分为匹配部分，不匹配部分比较大小即可
    size_t fi = 0;
    size_t di = 0;
    size_t di_matched = 0;
    bool is_format = false;
    bool matched = true;
    int time_unit = 0;
    int cmp = 0;
    auto match = [&fmt, &fi, &date, &di, &time_unit](char c, size_t len)->bool {
        if (fmt[fi] != c) {
            return false;
        }
        for (size_t i = 0; i < len; i++) {
            if (!isdigit(date[di])) {
                return false;
            }
            di++;
        }
        time_unit++;
        return true;
    };
    while (matched) {
        if (is_format) {
            switch (time_unit) {
                case 0: matched = match('Y', 4) || match('y', 2); break;
                case 1: matched = match('m', 2); break;
                case 2: matched = match('d', 2); break;
                case 3: matched = match('H', 2); break;
                case 4: matched = match('i', 2); break;
                case 5: matched = match('s', 2) || match('S', 2); break;
                case 6: matched = match('f', 6); break;
                default: break;
            }
            di_matched = di;
            is_format = false;
        } else {
            if (fmt[fi] == '%' ) {
                is_format = true;
            } else {
                cmp = fmt[fi] - date[di];
                if (fmt[fi] == '\0' || cmp != 0) {
                    break; // fmt[fi] == '\0' or date[di] == '\0' 都会退出循环
                } else {
                    di++;
                }
            }
        }
        fi++;
    }
    if (!matched || time_unit == 0) {
        return to;
    }
    ExprValue value = children(1)->get_value(nullptr);
    value.str_val = date.substr(0, di_matched);
    ExprValue v = value.cast_to(pb::DATETIME);
    if (value._u.uint64_val == 0) { // 非法日期
        return to;
    }
    // "2022-02-29"经过2次转化后,变为"2022-03-01"
    v.cast_to(pb::TIMESTAMP).cast_to(pb::DATETIME);
    if ((v._u.uint64_val >> 24) != (value._u.uint64_val >> 24)) {
        return to;
    }
    ExprNode* slot =  static_cast<SlotRef*>(e->children(0))->clone();
    delete _children[0];
    _children[0] = slot;

    auto date_add = [](ExprValue& date, int time_unit) {
        uint64_t& datetime = date._u.uint64_val;
        if (time_unit == 1 || time_unit == 2) {

        }
        switch (time_unit) {
            case 1: // year
            case 2: { // month
                uint64_t year_month = ((datetime >> 46) & 0x1FFFF);
                uint64_t year = year_month / 13;
                uint64_t month = year_month % 13;
                if (time_unit == 1) {
                    year++;
                } else {
                    month++;
                    if (month == 13) {
                        month = 0;
                        year++;
                    }
                }
                year_month = year * 13 + month;
                datetime = (datetime << 18) >> 18;
                datetime |= (year_month << 46);
                break;
            }
            case 3: // day
            case 4: // hour
            case 5: // minuter
            case 6: { // second
                date.cast_to(pb::TIMESTAMP);
                uint32_t val = 0;
                if (time_unit == 3) {
                    val = 24 * 60 * 60;
                } else if (time_unit == 4) {
                    val = 60 * 60;
                } else if (time_unit == 5) {
                    val = 60;
                } else if (time_unit == 6) {
                    val = 1;
                }
                date._u.uint32_val += val;
                date.cast_to(pb::DATETIME);
                break;
            }
            case 7: {
                uint64_t macrosec = (datetime & 0xFFFFFF);
                datetime = (datetime >> 24) << 24;
                macrosec = (macrosec + 1) % 1000000;
                if (macrosec == 0) { // +1s
                    date.cast_to(pb::TIMESTAMP);
                    date._u.uint32_val += 1;
                    date.cast_to(pb::DATETIME);
                }
                datetime |= macrosec;
                break;
            }
            default: break;
        }
    };
    auto date_sub_macrosec = [](ExprValue& date) {
        uint64_t& datetime = date._u.uint64_val;
        uint64_t macrosec = (datetime & 0xFFFFFF);
        if (macrosec != 0) {
            date._u.uint64_val--;
        } else {
            macrosec = 999999;
            datetime = (datetime >> 24) << 24;
            date.cast_to(pb::TIMESTAMP);
            date._u.uint32_val--;
            date.cast_to(pb::DATETIME);
            datetime |= macrosec;
        }
    };
    //  +1: 代表原时间单位+1,例如 2022-07-28, time_unit=天, +1后为 2022-07-29
    //  -Δ: 代表时间的最小刻度%f -1,即-1us
    //  op  cmp=0       cmp>0       cmp<0
    //  >=  [0, +∞)     [0, +∞)     [1, +∞)
    //  >   (1-Δ, +∞)   (0-Δ, +∞)   (1-Δ, +∞)
    //  <=  (-∞, 1-Δ]   (-∞, 0-Δ]   (-∞, 1-Δ]
    //  <   (-∞, 0)     (-∞, 0)     (-∞, 1)
    switch (_fn.fn_op()) {
        case parser::FT_GE:
        case parser::FT_LT: {
            if (cmp < 0) {
                date_add(value, time_unit);
            }
            break;
        }
        case parser::FT_GT:
        case parser::FT_LE: {
            if (cmp <= 0) {
                date_add(value, time_unit);
            }
            date_sub_macrosec(value);
            break;
        }
        default:
            break;
    }
    delete  _children[1];;
    _children[1] = new Literal(value);
    return to;
}

ExprNode* ScalarFnCall::transfer_from_or_to_in() {
    ExprNode* to = this;
    if (_children.size() != 2) {
        return to;
    }
    ExprNode * slot = nullptr;
    std::vector<ExprNode*> literal_list;
    for (size_t i = 0; i < _children.size(); ++ i) {
        if (_children[i]->node_type() != pb::FUNCTION_CALL && _children[i]->node_type() != pb::IN_PREDICATE) {
            return to;
        }
        auto child = static_cast<ScalarFnCall*>(_children[i]);
        if (child->_fn.fn_op() != parser::FT_IN && child->_fn.fn_op() != parser::FT_EQ) {
            return to;
        }
        int slot_cnt = 0;
        for (size_t j = 0; j < child->children_size(); ++ j) {
            auto c = child->children(j);
            if (c->is_slot_ref()) {
                if (slot == nullptr) {
                    slot = c;
                } else if (c->tuple_id() != slot->tuple_id() || c->slot_id() != slot->slot_id()) {
                    return to;
                }
                slot_cnt += 1;
            } else {
                if (!c->is_literal()) {
                    return to;
                }
                literal_list.push_back(c);
            }
        }
        if (slot_cnt != 1) {
            return to;
        }
    }
    pb::ExprNode node;
    node.set_col_type(pb::BOOL);
    node.set_node_type(pb::IN_PREDICATE);
    pb::Function* fn = node.mutable_fn();
    fn->set_name("in");
    fn->set_fn_op(parser::FT_IN);
    to = new InPredicate();
    to->init(node);
    // add child
    to->add_child(static_cast<SlotRef*>(slot)->clone());
    for (auto c : literal_list) {
        to->add_child(new Literal(c->get_value(nullptr)));
    }
    to->expr_optimize();
    return to;
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
