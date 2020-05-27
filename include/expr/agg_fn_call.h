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

namespace baikaldb {
class AggFnCall : public ExprNode {
public:
    enum AggType {
        COUNT_STAR,
        COUNT,
        SUM,
        AVG, 
        MIN,
        MAX,
        HLL_ADD_AGG,
        HLL_MERGE_AGG,
        OTHER
    };
    AggFnCall() {
    }
    ~AggFnCall() {
    }
    virtual int type_inferer();
    int type_inferer(pb::TupleDescriptor* tuple_desc);
    ExprNode* create_slot_ref();
    virtual void transfer_pb(pb::ExprNode* pb_node);
    virtual int init(const pb::ExprNode& node);
    virtual int open();
    // 在常规表达式中当做slot_ref用
    virtual ExprValue get_value(MemRow* row) {
        if (row == nullptr) {
            return ExprValue::Null();
        }
        return row->get_value(_tuple_id, _final_slot_id);
    }

    bool is_initialize(MemRow* dst);
    // 聚合函数逻辑
    // 初始化分配内存
    int initialize(MemRow* dst);
    // update每次更新一行
    int update(MemRow* src, MemRow* dst);
    // merge表示store预聚合后，最终merge到一起
    int merge(MemRow* src, MemRow* dst);
    // 对于avg这种，需要最终计算结果
    int finalize(MemRow* dst);

    static bool all_is_initialize(std::vector<AggFnCall*>& agg_calls, MemRow* dst) {
        for (auto call : agg_calls) {
            if (!call->is_initialize(dst)) {
                return false;
            }
        }
        return true;
    }

    static void initialize_all(std::vector<AggFnCall*>& agg_calls, MemRow* dst) {
        for (auto call : agg_calls) {
            call->initialize(dst);
        }
    }
    static void update_all(std::vector<AggFnCall*>& agg_calls, MemRow* src, MemRow* dst) {
        for (auto call : agg_calls) {
            call->update(src, dst);
        }
    }
    static void merge_all(std::vector<AggFnCall*>& agg_calls, MemRow* src, MemRow* dst) {
        for (auto call : agg_calls) {
            call->merge(src, dst);
        }
    }
    static void finalize_all(std::vector<AggFnCall*>& agg_calls, MemRow* dst) {
        for (auto call : agg_calls) {
            call->finalize(dst);
        }
    }
private:
    AggType _agg_type;
    pb::Function _fn;
    int32_t _intermediate_slot_id;
    int32_t _final_slot_id;
    bool _is_distinct = false;
    //聚合函数参数列表，count(*)参数为空
    //merge的时候，类型是slotref，size=1
    //std::vector<ExprNode*> _arg_exprs;
    //switch很恶心，后续要用函数指针分离逻辑
    //std::function<ExprValue(const std::vector<ExprValue>&)> _add_fn;
    //std::function<ExprValue(const std::vector<ExprValue>&)> _merge_fn;
    //std::function<ExprValue(const std::vector<ExprValue>&)> _get_value_fn;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
