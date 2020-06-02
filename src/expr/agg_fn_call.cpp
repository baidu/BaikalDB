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
    };
    //所有agg都是非const的
    _is_constant = false;
    _fn = node.fn();
    _tuple_id = node.derive_node().tuple_id();
    _final_slot_id = node.derive_node().slot_id();
    _intermediate_slot_id = node.derive_node().intermediate_slot_id();
    _slot_id = _intermediate_slot_id;
    if (name_type_map.count(_fn.name())) {
        _agg_type = name_type_map[_fn.name()];
    } else {
        _agg_type = OTHER;
        return -1;
    }
    if (_fn.name() == "count_distinct" ||
            _fn.name() == "sum_distinct" ||
            _fn.name() == "avg_distinct") {
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
            if (_children.size() == 0) {
                DB_WARNING("_agg_type:%d , _children.size() == 0", _agg_type)
                return -1;
            }
            break;
        default:
            return 0;
    }
    if (_agg_type == COUNT && _children[0]->is_literal()) {
        if (!_children[0]->get_value(nullptr).is_null()) {
            _agg_type = COUNT_STAR;
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

bool AggFnCall::is_initialize(MemRow* dst) {
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
int AggFnCall::initialize(MemRow* dst) {
    if (!dst->get_value(_tuple_id, _intermediate_slot_id).is_null()) {
        return 0;
    }
    switch (_agg_type) {
        case COUNT_STAR:
        case COUNT:
            dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue(_col_type));
            return 0;
        case SUM:
            dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue::Null());
            return 0;
        case AVG: {
            ExprValue value(pb::STRING);
            AvgIntermediate avg;
            value.str_val.assign((char*)&avg, sizeof(avg));
            dst->set_value(_tuple_id, _intermediate_slot_id, value);
            dst->set_value(_tuple_id, _final_slot_id, ExprValue::Null());
            return 0;
        }
        case MIN:
        case MAX:
            dst->set_value(_tuple_id, _intermediate_slot_id, ExprValue::Null());
            return 0;
        case HLL_ADD_AGG:
        case HLL_MERGE_AGG:
            dst->set_value(_tuple_id, _intermediate_slot_id, hll::hll_init());
            return 0;
        default:
            return -1;
    }
}

int AggFnCall::update(MemRow* src, MemRow* dst) {
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
                std::string* hll = dst->mutable_string(_tuple_id, _intermediate_slot_id);
                if (hll != NULL) {
                    hll::hll_add(hll, value.hash());
                }
            }
            return 0;
        }
        case HLL_MERGE_AGG: {
            ExprValue value = _children[0]->get_value(src);
            if (!value.is_null()) {
                std::string* hll = dst->mutable_string(_tuple_id, _intermediate_slot_id);
                if (hll != NULL) {
                    hll::hll_merge_agg(hll, &value.str_val);
                }
            }
            return 0;
        }
        default:
            return -1;
    }
}
int AggFnCall::merge(MemRow* src, MemRow* dst) {
    if (_is_distinct) {
        //distinct agg, 无merge概念
        //普通agg与distinct agg一起出现时，普通agg需要多计算一次，因此需要merge
        return update(src, dst);
    }
    //首行不需要merge
    if (src == dst) {
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
            std::string* src_hll = src->mutable_string(_tuple_id, _intermediate_slot_id);
            std::string* dst_hll = dst->mutable_string(_tuple_id, _intermediate_slot_id);
            if (src_hll != NULL && dst_hll != NULL) {
                hll::hll_merge_agg(dst_hll, src_hll);
            }
            return 0;
        }
        default:
            return -1;
    }
}
int AggFnCall::finalize(MemRow* dst) {
    if (_intermediate_slot_id == _final_slot_id) {
        return 0;
    }
    switch (_agg_type) {
        case AVG: {
            ExprValue value = dst->get_value(_tuple_id, _intermediate_slot_id);
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
        default:
            return 0;
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
