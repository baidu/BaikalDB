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

#include "agg_fn_call.h"
#include <unordered_map>
#include "hll_common.h"
#include "slot_ref.h"

namespace baikaldb {

DEFINE_bool(transfor_hll_raw_to_sparse, false, "try transfor raw hll to sparse");

int AggFnCall::init(const pb::ExprNode& node) {
    ExprNode::init(node);
    if (!node.has_fn()) {
        return -1;
    }
    static std::unordered_map<std::string, AggType> name_type_map = {
        {"count_star", COUNT_STAR},
        {"count", COUNT},
        {"count_distinct", COUNT},
        {"sum", SUM},
        {"sum_distinct", SUM},
        {"avg", AVG},
        {"avg_distinct", AVG},
        {"min", MIN},
        {"max", MAX},
        {"hll_add_agg", HLL_ADD_AGG},
        {"hll_merge_agg", HLL_MERGE_AGG},
        {"rb_or_agg", RB_OR_AGG},
        //{"rb_or_cardinality_agg", RB_OR_CARDINALITY_AGG},
        {"rb_and_agg", RB_AND_AGG},
        //{"rb_and_cardinality_agg", RB_AND_CARDINALITY_AGG},
        {"rb_xor_agg", RB_XOR_AGG},
        //{"rb_xor_cardinality_agg", RB_XOR_CARDINALITY_AGG},
        {"rb_build_agg", RB_BUILD_AGG},
        {"tdigest_agg", TDIGEST_AGG},
        {"tdigest_build_agg", TDIGEST_BUILD_AGG},
        {"group_concat", GROUP_CONCAT},
        {"group_concat_distinct", GROUP_CONCAT},
    };
    //所有agg都是非const的
    _is_constant = false;
    _fn = node.fn();
    _tuple_id = node.derive_node().tuple_id();
    _final_slot_id = node.derive_node().slot_id();
    _intermediate_slot_id = node.derive_node().intermediate_slot_id();
    if (name_type_map.count(_fn.name())) {
        _agg_type = name_type_map[_fn.name()];
    } else {
        _agg_type = OTHER;
        return -1;
    }
    if (_fn.name() == "count_distinct" ||
            _fn.name() == "sum_distinct" ||
            _fn.name() == "avg_distinct" ||
            _fn.name() == "group_concat_distinct") {
        _is_distinct = true;
    }
    return 0;
}
int AggFnCall::type_inferer() {
    int ret = 0;
    ret = ExprNode::type_inferer();
    if (ret < 0) {
        DB_FATAL("ExprNode::type_inferer error");
        return ret;
    }
    switch (_agg_type) {
        case COUNT_STAR:
        case COUNT:
            _col_type = pb::INT64;
            return 0;
        case AVG: 
            _col_type = pb::DOUBLE;
            return 0;
        case SUM:
            if (_children.size() == 0) {
                return -1;
            }
            if (is_double(_children[0]->col_type())) {
                _col_type = pb::DOUBLE;
            } else {
                _col_type = pb::INT64;
            }
            return 0;
        case MIN:
        case MAX:
            if (_children.size() == 0) {
                return -1;
            }
            _col_type = _children[0]->col_type();
            return 0;
        case HLL_ADD_AGG:
        case HLL_MERGE_AGG:
            if (_children.size() == 0) {
                DB_FATAL("children.size is 0");
                return -1;
            }
            _col_type = pb::HLL;
            return 0;
        case RB_OR_AGG:
        case RB_AND_AGG:
        case RB_XOR_AGG:
        case RB_BUILD_AGG: {
            if (_children.size() == 0) {
                DB_FATAL("children.size is 0");
                return -1;
            }
            _col_type = pb::BITMAP;
            return 0;
        }
        case RB_AND_CARDINALITY_AGG:
        case RB_XOR_CARDINALITY_AGG:
        case RB_OR_CARDINALITY_AGG: {
            if (_children.size() == 0) {
                DB_FATAL("children.size is 0");
                return -1;
            }
            _col_type = pb::UINT64;
            return 0;
        }
        case TDIGEST_AGG:
        case TDIGEST_BUILD_AGG: {
            if (_children.size() == 0) {
                DB_FATAL("children.size is 0");
                return -1;
            }
            _col_type = pb::TDIGEST;
            return 0;
        }
        case GROUP_CONCAT: {
            if (_children.size() == 0) {
                DB_FATAL("children.size is 0");
                return -1;
            }
            _col_type = pb::STRING;
            return 0;
        }
        default:
            DB_WARNING("un-support agg type:%d", _agg_type);
            return -1;
    }
}

int AggFnCall::type_inferer(pb::TupleDescriptor* tuple_desc) {
    int ret = type_inferer();
    if (ret < 0) {
        return ret;
    }
    for (auto& slot : *tuple_desc->mutable_slots()) {
        if (slot.slot_id() == _final_slot_id) {
            slot.set_slot_type(_col_type);
            break;
        }
    }
    return 0;
}

ExprNode* AggFnCall::create_slot_ref() {
    pb::ExprNode node;
    node.set_node_type(pb::SLOT_REF);
    node.set_col_type(_col_type);
    node.mutable_derive_node()->set_tuple_id(_tuple_id);
    node.mutable_derive_node()->set_slot_id(_final_slot_id);
    SlotRef* expr = new SlotRef;
    expr->init(node);
    return expr;
}
void AggFnCall::transfer_pb(pb::ExprNode* pb_node) {
    ExprNode::transfer_pb(pb_node);
    pb_node->mutable_fn()->CopyFrom(_fn);
    pb_node->mutable_derive_node()->set_tuple_id(_tuple_id);
    pb_node->mutable_derive_node()->set_slot_id(_final_slot_id);
    pb_node->mutable_derive_node()->set_intermediate_slot_id(_intermediate_slot_id);
}

int AggFnCall::open() {
    int ret = 0;
    ret = ExprNode::open();
    if (ret < 0) {
        DB_WARNING("ExprNode::open fail:%d", ret);
        return ret;
    }
    switch (_agg_type) {
        case COUNT:
        case AVG: 
        case SUM:
        case MIN:
        case MAX:
        case HLL_ADD_AGG:
        case HLL_MERGE_AGG:
        case RB_OR_AGG:
        case RB_OR_CARDINALITY_AGG:
        case RB_AND_AGG:
        case RB_AND_CARDINALITY_AGG:
        case RB_XOR_AGG:
        case RB_XOR_CARDINALITY_AGG:
        case RB_BUILD_AGG:
        case TDIGEST_AGG:
        case TDIGEST_BUILD_AGG: 
        case GROUP_CONCAT: {
            if (_children.size() == 0) {
                DB_WARNING("_agg_type:%d , _children.size() == 0", _agg_type);
                return -1;
            }
            break;
        }
        default:
            return 0;
    }
    if (_agg_type == COUNT && _children[0]->is_literal()) {
        if (!_children[0]->get_value(nullptr).is_null()) {
            _agg_type = COUNT_STAR;
        }
    }
    if (_agg_type == GROUP_CONCAT) {
        int children_size = _children.size();
        if (children_size < 2) {
            DB_WARNING("children_size %d less than 2", children_size);
            return -1;
        }
        if (_children[1]->is_literal()) {
            ExprValue value = _children[1]->get_value(nullptr);
            if (!value.is_null()) {
                _sep = value.get_string();
            }
        }
        if (children_size == 4) {
            if (!_children[2]->is_row_expr() || !_children[3]->is_row_expr() ||
                _children[2]->children_size() != _children[3]->children_size()) {
                DB_WARNING("children_size not equal");
                return -1;
            }
            pb::TupleDescriptor order_tuple;
            _order_tuple_id = 0;
            order_tuple.set_tuple_id(_order_tuple_id);
            for (size_t i = 0; i < _children[2]->children_size(); i++) {
                ExprNode* order_expr = _children[2]->children(i);
                bool is_asc = !_children[3]->children(i)->get_value(nullptr)._u.bool_val;
                int ret = order_expr->expr_optimize();
                if (ret < 0) {
                    return ret;
                }
                pb::SlotDescriptor* slot = order_tuple.add_slots();
                slot->set_slot_id(i + 1);
                slot->set_tuple_id(_order_tuple_id);
                slot->set_slot_type(order_expr->col_type());

                ExprNode* slot_order_expr = nullptr;
                //create slot ref
                pb::Expr slot_expr;
                pb::ExprNode* node = slot_expr.add_nodes();
                node->set_node_type(pb::SLOT_REF);
                node->set_col_type(order_expr->col_type());
                node->set_num_children(0);
                node->mutable_derive_node()->set_tuple_id(_order_tuple_id);
                node->mutable_derive_node()->set_slot_id(i + 1);
                ret = ExprNode::create_tree(slot_expr, &slot_order_expr);
                if (ret < 0) {
                    //如何释放资源
                    return ret;
                }
                _order_exprs.push_back(order_expr);
                _slot_order_exprs.push_back(slot_order_expr);
                _is_asc.push_back(is_asc);
                _is_null_first.push_back(is_asc);
            }
            // agg dst expr
            pb::SlotDescriptor* slot = order_tuple.add_slots();
            slot->set_slot_id(_slot_order_exprs.size() + 1);
            slot->set_tuple_id(_order_tuple_id);
            slot->set_slot_type(pb::STRING);

           _mem_row_desc = std::make_shared<MemRowDescriptor>();
           std::vector<pb::TupleDescriptor> tuple_descs;
           tuple_descs.push_back(order_tuple);
           int ret = _mem_row_desc->init(tuple_descs);
           if (ret < 0) {
               DB_WARNING("_mem_row_desc init fail");
               return -1;
           }
            _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
        }
    }
    return 0;
}

struct AvgIntermediate {
    double sum;
    int64_t count;
    AvgIntermediate() : sum(0.0), count(0) {
    }
};

bool AggFnCall::is_initialize(const std::string& key, MemRow* dst) {
    if (_is_distinct) {
        return false;
    }

    if (dst->get_value(_tuple_id, _intermediate_slot_id).is_null()) {
        return true;
    }
    switch (_agg_type) {
        case COUNT_STAR:
        case COUNT: {
            if (dst->get_value(_tuple_id, _intermediate_slot_id).get_numberic<int64_t>() == 0) {
                return true;
            } else {
                return false;
            }
        }
        case AVG: {
            ExprValue value(pb::STRING);
            AvgIntermediate avg;
            value.str_val.assign((char*)&avg, sizeof(avg));
            if (dst->get_value(_tuple_id, _intermediate_slot_id).compare(value) == 0) {
                return true;
            } else {
                return false;
            }
        }
        default:
            break;
    }
    return false;
}

// 聚合函数逻辑
int AggFnCall::initialize(const std::string& key, MemRow* dst, int64_t& used_size, bool only_count) {
    ExprValue dst_val = dst->get_value(_tuple_id, _intermediate_slot_id);
    if (only_count) {
        if (_agg_type == COUNT_STAR || _agg_type == COUNT) {
            if (dst_val.is_null()) {
                dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue(pb::INT64));
            }
        }
        return 0;
    }
    switch (_agg_type) {
        case COUNT_STAR:
        case COUNT: {
            if (dst_val.is_null()) {
                dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue(pb::INT64));
            }
            return 0;
        }
        case SUM: {
            if (dst_val.is_null()) {
                dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue::Null());
            }
            return 0;
        }
        case AVG: {
            if (dst_val.is_null()) {
                ExprValue value(pb::STRING);
                AvgIntermediate avg;
                value.str_val.assign((char*)&avg, sizeof(avg));
                dst->set_value(_tuple_id, _intermediate_slot_id, value);
                dst->set_value(_tuple_id, _final_slot_id, ExprValue::Null());
            }
            return 0;
        }
        case MIN:
        case MAX: {
            if (dst_val.is_null()) {
                dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue::Null());
            }
            return 0;
        }
        case HLL_ADD_AGG:
        case HLL_MERGE_AGG: {
            if (_intermediate_val_map.count(key) == 0) {
                auto& intermediate_val = _intermediate_val_map[key];
                intermediate_val.val = hll::hll_row_init();
                if (dst_val.is_null()) {
                    dst->set_value(_tuple_id, _intermediate_slot_id, hll::hll_row_init());
                } else {
                    dst_val.cast_to(pb::HLL);
                    hll::hll_merge_agg(intermediate_val.val.str_val, dst_val.str_val);
                    dst->set_value(_tuple_id, _intermediate_slot_id, intermediate_val.val);
                }
                used_size += intermediate_val.val.size();
                intermediate_val.is_assign = true;
            }
            return 0;
        }
        case RB_OR_AGG:
        case RB_OR_CARDINALITY_AGG:
        case RB_AND_AGG:
        case RB_AND_CARDINALITY_AGG:
        case RB_XOR_AGG:
        case RB_XOR_CARDINALITY_AGG:
        case RB_BUILD_AGG: {
            if (_intermediate_val_map.count(key) == 0) {
                auto& intermediate_val = _intermediate_val_map[key];
                if (dst_val.is_null()) {
                    intermediate_val.val = ExprValue::Bitmap();
                    dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue::Bitmap());
                } else {
                    dst_val.cast_to(pb::BITMAP);
                    intermediate_val.val = dst_val;
                    dst->set_value(_tuple_id, _intermediate_slot_id, dst_val);
                }
                if (_agg_type != RB_AND_AGG && _agg_type != RB_AND_CARDINALITY_AGG) {
                    // and第一次需要特殊处理
                    intermediate_val.is_assign = true;
                }
                used_size += intermediate_val.val.size();
            }
            return 0;
        }
        case TDIGEST_AGG:
        case TDIGEST_BUILD_AGG: {
            if (_intermediate_val_map.count(key) == 0) {
                auto& intermediate_val = _intermediate_val_map[key];
                if (dst_val.is_null()) {
                    intermediate_val.val = ExprValue::Tdigest();
                    dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue::Tdigest());
                } else {
                    dst_val.cast_to(pb::TDIGEST);
                    intermediate_val.val = dst_val;
                    tdigest::td_normallize(intermediate_val.val.str_val);
                    dst->set_value(_tuple_id, _intermediate_slot_id, dst_val);
                }
                intermediate_val.is_assign = true;
                used_size += intermediate_val.val.size();
            }
            return 0;
        }
        case GROUP_CONCAT: {
            if (_mem_row_compare != nullptr) {
                if (_intermediate_row_batch_map.count(key) == 0) {
                    _intermediate_row_batch_map[key] = std::make_shared<RowBatch>();
                }
            }
            if (dst_val.is_null()) {
                dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue::Null());
            }
            return 0;
        }
        default:
            return -1;
    }
}

int AggFnCall::update(const std::string& key, MemRow* src, MemRow* dst, int64_t& used_size) {
    switch (_agg_type) {
        case COUNT_STAR: {
            ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
            result._u.int64_val++;
            dst->set_value(_tuple_id, _intermediate_slot_id, result);
            return 0;
        }
        case COUNT: {
            for (auto child : _children) {
                if (child->get_value(src).is_null()) {
                    return 0;
                }
            }

            ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
            result._u.int64_val++;
            dst->set_value(_tuple_id, _intermediate_slot_id, result);
            return 0;
        }
        case SUM: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                result.add(value);
                dst->set_value(_tuple_id, _intermediate_slot_id, result);
            }
            return 0;
        }
        case AVG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                AvgIntermediate* avg = (AvgIntermediate*)result.str_val.c_str();
                avg->sum += value.get_numberic<double>();
                avg->count++;
                dst->set_value(_tuple_id, _intermediate_slot_id, result);
            }
            return 0;
        }
        case MIN: {
            ExprValue value = _children[0]->get_value(src).cast_to(_col_type);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id).cast_to(_col_type);
                if (result.is_null() || result.compare(value) > 0) {
                    dst->set_value(_tuple_id, _intermediate_slot_id, value);
                }
            }
            return 0;
        }
        case MAX: {
            ExprValue value = _children[0]->get_value(src).cast_to(_col_type);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id).cast_to(_col_type);
                if (result.is_null() || result.compare(value) < 0) {
                    dst->set_value(_tuple_id, _intermediate_slot_id, value);
                }
            }
            return 0;
        }
        case HLL_ADD_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null()) {
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                hll::hll_add(intermediate_val.val.str_val, value.hash());
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case HLL_MERGE_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null() && value.is_hll()) {
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                hll::hll_merge_agg(intermediate_val.val.str_val, value.str_val);
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case RB_OR_AGG:
        case RB_OR_CARDINALITY_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null() && value.is_bitmap()) {
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                *intermediate_val.val._u.bitmap |= *value._u.bitmap;
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case RB_AND_AGG:
        case RB_AND_CARDINALITY_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null() && value.is_bitmap()) {
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                if (intermediate_val.is_assign) {
                    *intermediate_val.val._u.bitmap &= *value._u.bitmap;
                } else {
                    *intermediate_val.val._u.bitmap = *value._u.bitmap;
                    intermediate_val.is_assign = true;
                }
                used_size += intermediate_val.val.size() - old_used_size;          
            }
            return 0;
        }
        case RB_XOR_AGG:
        case RB_XOR_CARDINALITY_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null() && value.is_bitmap()) {
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                *intermediate_val.val._u.bitmap ^= *value._u.bitmap;
                used_size += intermediate_val.val.size() - old_used_size; 
            }
            return 0;
        }
        case RB_BUILD_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null()) {
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                intermediate_val.val._u.bitmap->add(value.get_numberic<uint32_t>());
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case TDIGEST_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null() && value.is_tdigest()) {
                auto& intermediate_val = _intermediate_val_map[key];
                
                if (tdigest::is_td_object(value.str_val)) {
                    int64_t old_used_size = intermediate_val.val.size();
                    tdigest::td_merge((tdigest::td_histogram_t *)intermediate_val.val.str_val.data(),
                            (tdigest::td_histogram_t *)value.str_val.data());
                    used_size += intermediate_val.val.size() - old_used_size;
                }
            }
            return 0;
        }
        case TDIGEST_BUILD_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null()) {
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                tdigest::td_add((tdigest::td_histogram_t *)intermediate_val.val.str_val.data(),
                        value.get_numberic<double>(), 1);
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case GROUP_CONCAT: {
            std::string val = "";
            bool all_is_null = true;
            for (size_t i = 0; i < _children[0]->children_size(); i++) {
                ExprValue value = _children[0]->children(i)->get_value(src);
                if (!value.is_null()) {
                    all_is_null = false;
                    val += value.get_string();
                }
            }
            if (_mem_row_compare != nullptr) {
                if (!all_is_null) {
                    auto& batch = _intermediate_row_batch_map[key];
                    std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                    for (size_t i = 0; i < _order_exprs.size(); i++) {
                        ExprValue v = _order_exprs[i]->get_value(src);
                        row->set_value(_order_tuple_id, i + 1, v);
                    }
                    ExprValue value = ExprValue(pb::STRING);
                    value.str_val = val;
                    row->set_value(_order_tuple_id, _slot_order_exprs.size() + 1, value);
                    batch->move_row(std::move(row));
                }
                return 0;
            }
            if (!all_is_null) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                if (result.type != pb::STRING) { // is_null
                    result = ExprValue(pb::STRING);
                } else {
                    result.str_val += _sep;
                }
                result.str_val += val;
                used_size += result.size();
                dst->set_value(_tuple_id, _intermediate_slot_id, result);
            }
            return 0;
        }
        default:
            return -1;
    }
}
int AggFnCall::merge(const std::string& key, MemRow* src, MemRow* dst, int64_t& used_size) {
    if (_is_distinct) {
        //distinct agg, 无merge概念
        //普通agg与distinct agg一起出现时，普通agg需要多计算一次，因此需要merge
        return update(key, src, dst, used_size);
    }
    if (_agg_type == GROUP_CONCAT && _mem_row_compare != nullptr) {
        ExprValue value = src->get_value(_tuple_id, _intermediate_slot_id);
        if (!value.is_null()) {
            auto& batch = _intermediate_row_batch_map[key];
            std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
            for (size_t i = 0; i < _order_exprs.size(); i++) {
                ExprValue v = _order_exprs[i]->get_value(src);
                row->set_value(_order_tuple_id, i + 1, v);
            }
            row->set_value(_order_tuple_id, _slot_order_exprs.size() + 1, value);
            batch->move_row(std::move(row));
        }
        return 0;
    }
    //首行不需要merge
    if (src == dst) {
        ExprValue dst_value = src->get_value(_tuple_id, _intermediate_slot_id);
        if (is_bitmap_agg() || is_tdigest_agg() || is_hll_agg()){
            if (is_bitmap_agg()) {
                dst_value.cast_to(pb::BITMAP);
            } else if (is_tdigest_agg()) {
                dst_value.cast_to(pb::TDIGEST);
            }
            auto& intermediate_val = _intermediate_val_map[key];
            intermediate_val.is_assign = true;
            if (is_hll_agg()) {
                hll::hll_merge_agg(intermediate_val.val.str_val, dst_value.str_val);
            } else {
                intermediate_val.val = dst_value;
            }
        }
        return 0;
    }
    switch (_agg_type) {
        case COUNT_STAR:
        case COUNT:
        case SUM: {
            ExprValue value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                result.add(value);
                dst->set_value(_tuple_id, _intermediate_slot_id, result);
            }
            return 0;
        }
        case AVG: {
            ExprValue value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!value.is_null() && value.type == pb::STRING) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                AvgIntermediate* avg_result = (AvgIntermediate*)result.str_val.c_str();
                AvgIntermediate* avg_value = (AvgIntermediate*)value.str_val.c_str();
                avg_result->sum += avg_value->sum;
                avg_result->count += avg_value->count;
                dst->set_value(_tuple_id, _intermediate_slot_id, result);
            }
            return 0;
        }
        case MIN: {
            ExprValue value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                if (result.is_null() || result.compare(value) > 0) {
                    dst->set_value(_tuple_id, _intermediate_slot_id, value);
                }
            }
            return 0;
        }
        case MAX: {
            ExprValue value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                if (result.is_null() || result.compare(value) < 0) {
                    dst->set_value(_tuple_id, _intermediate_slot_id, value);
                }
            }
            return 0;
        }
        case HLL_ADD_AGG:
        case HLL_MERGE_AGG: {
            ExprValue src_hll = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!src_hll.is_null()) {
                auto& intermediate_val = _intermediate_val_map[key];
                hll::hll_merge_agg(intermediate_val.val.str_val, src_hll.str_val);
            }
            return 0;
        }
        case RB_OR_AGG:
        case RB_OR_CARDINALITY_AGG:
        case RB_BUILD_AGG: {
            ExprValue src_value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!src_value.is_null()) {
                src_value.cast_to(pb::BITMAP);
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                *intermediate_val.val._u.bitmap |= *src_value._u.bitmap;
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case RB_AND_AGG:
        case RB_AND_CARDINALITY_AGG: {
            ExprValue src_value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!src_value.is_null()) {
                src_value.cast_to(pb::BITMAP);
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                if (intermediate_val.is_assign) {
                    *intermediate_val.val._u.bitmap &= *src_value._u.bitmap;
                } else {
                    *intermediate_val.val._u.bitmap = *src_value._u.bitmap;
                    intermediate_val.is_assign = true;
                }
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case RB_XOR_AGG:
        case RB_XOR_CARDINALITY_AGG: {
            ExprValue src_value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!src_value.is_null()) {
                src_value.cast_to(pb::BITMAP);
                auto& intermediate_val = _intermediate_val_map[key];
                int64_t old_used_size = intermediate_val.val.size();
                *intermediate_val.val._u.bitmap ^= *src_value._u.bitmap;
                used_size += intermediate_val.val.size() - old_used_size;
            }
            return 0;
        }
        case TDIGEST_AGG:
        case TDIGEST_BUILD_AGG: {
            ExprValue src_value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!src_value.is_null()) {
                auto& intermediate_val = _intermediate_val_map[key];
                if (tdigest::is_td_object(src_value.str_val)) {
                    int64_t old_used_size = intermediate_val.val.size();
                    tdigest::td_merge((tdigest::td_histogram_t *)intermediate_val.val.str_val.data(),
                        (tdigest::td_histogram_t *)src_value.str_val.data());
                    used_size += intermediate_val.val.size() - old_used_size;
                }
            }
            return 0;
        }
        case GROUP_CONCAT: {
            ExprValue value = src->get_value(_tuple_id, _intermediate_slot_id);
            if (!value.is_null()) {
                ExprValue result = dst->get_value(_tuple_id, _intermediate_slot_id);
                if (result.type != pb::STRING) { // is_null
                    result = ExprValue(pb::STRING);
                } else {
                    result.str_val += _sep;
                }
                result.str_val += value.get_string();
                used_size += result.size();
                dst->set_value(_tuple_id, _intermediate_slot_id, result);
            }
            return 0;
        }
        default:
            return -1;
    }
}
int AggFnCall::finalize(const std::string& key, MemRow* dst) {
    if (_agg_type == GROUP_CONCAT && _mem_row_compare != nullptr) {
        auto& intermediate_row_batch = _intermediate_row_batch_map[key];
        if (intermediate_row_batch == nullptr || intermediate_row_batch->size() == 0) {
            dst->set_value(_tuple_id, _final_slot_id,  ExprValue::Null());
            return 0;
        }
        _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
        _sorter->add_batch(intermediate_row_batch);
        _sorter->sort();

        ExprValue result(pb::STRING);
        bool eos = false;
        do {
            std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
            int ret = _sorter->get_next(batch.get(), &eos);
            if (ret < 0) {
                DB_WARNING("get_next fail:%d", ret);
                return ret;
            }
            for (batch->reset(); !batch->is_traverse_over(); batch->next()) {
                ExprValue value = batch->get_row()->get_value(_order_tuple_id, _slot_order_exprs.size() + 1);
                if (!value.is_null()) {
                    if (result.str_val.length() > 0) {
                        result.str_val += _sep;
                    }
                    result.str_val += value.get_string();
                }
            }
        } while (!eos);
        dst->set_value(_tuple_id, _intermediate_slot_id, result);
        return 0;
    }
    if (_intermediate_slot_id == _final_slot_id) {
        if (is_bitmap_agg() || is_tdigest_agg() || is_hll_agg()) {
            auto& val = _intermediate_val_map[key];
            if (val.is_assign) {
                if (is_hll_agg() && FLAGS_transfor_hll_raw_to_sparse) {
                    if (hll::hll_raw_to_sparse(val.val.str_val) < 0) {
                        DB_WARNING("hll raw to sparse failed");
                        return -1;
                    }
                }
                dst->set_value(_tuple_id, _final_slot_id, val.val);
            } else {
                dst->set_value(_tuple_id, _final_slot_id,  ExprValue::Null());
            }
        }
        return 0;
    }
    switch (_agg_type) {
        case AVG: {
            ExprValue value = dst->get_value(_tuple_id, _intermediate_slot_id);
            if (value.is_null()) {
                dst->set_value(_tuple_id, _final_slot_id, ExprValue::Null());
                return 0;
            }
            const AvgIntermediate* avg = (const AvgIntermediate*)value.str_val.c_str();
            if (avg->count != 0) {
                ExprValue result(pb::DOUBLE);
                result._u.double_val = avg->sum / avg->count;
                dst->set_value(_tuple_id, _final_slot_id, result);
            } else {
                dst->set_value(_tuple_id, _final_slot_id, ExprValue::Null());
            }
            return 0;
        }
        case RB_OR_CARDINALITY_AGG:
        case RB_AND_CARDINALITY_AGG:
        case RB_XOR_CARDINALITY_AGG: {
            auto& intermediate_val = _intermediate_val_map[key];
            dst->set_value(_tuple_id, _intermediate_slot_id, _intermediate_val_map[key].val);
            ExprValue result(pb::UINT64);
            result._u.uint64_val = intermediate_val.val._u.bitmap->cardinality();
            dst->set_value(_tuple_id, _final_slot_id, result);
            return 0;
        }
        default:
            return 0;
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
