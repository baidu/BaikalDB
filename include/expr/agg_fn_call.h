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
#include "sorter.h"
#include "mem_row_descriptor.h"

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
        RB_OR_AGG,
        RB_OR_CARDINALITY_AGG,
        RB_AND_AGG,
        RB_AND_CARDINALITY_AGG,
        RB_XOR_AGG,
        RB_XOR_CARDINALITY_AGG,
        RB_BUILD_AGG,
        TDIGEST_AGG,
        TDIGEST_BUILD_AGG,
        GROUP_CONCAT,
        OTHER
    };
    AggFnCall() {
    }
    ~AggFnCall() {
        for (auto expr : _slot_order_exprs) {
            ExprNode::destroy_tree(expr);
        }
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

    bool is_initialize(const std::string& key, MemRow* dst);
    // 聚合函数逻辑
    // 初始化分配内存
    int initialize(const std::string& key, MemRow* dst, int64_t& used_size, bool only_count);
    // update每次更新一行
    int update(const std::string& key, MemRow* src, MemRow* dst, int64_t& used_size);
    // merge表示store预聚合后，最终merge到一起
    int merge(const std::string& key, MemRow* src, MemRow* dst, int64_t& used_size);
    // 对于avg这种，需要最终计算结果
    int finalize(const std::string& key, MemRow* dst);

    static bool all_is_initialize(std::vector<AggFnCall*>& agg_calls,
            const std::string& key,
            MemRow* dst) {
        for (auto call : agg_calls) {
            if (!call->is_initialize(key, dst)) {
                return false;
            }
        }
        return true;
    }

    static void initialize_all(std::vector<AggFnCall*>& agg_calls,
            const std::string& key,
            MemRow* dst,
            int64_t& used_size,
            bool only_count) {
        for (auto call : agg_calls) {
            call->initialize(key, dst, used_size, only_count);
        }
    }
    static void update_all(std::vector<AggFnCall*>& agg_calls, const std::string& key, MemRow* src, MemRow* dst, int64_t& used_size) {
        for (auto call : agg_calls) {
            call->update(key, src, dst, used_size);
        }
    }
    static void merge_all(std::vector<AggFnCall*>& agg_calls, const std::string& key, MemRow* src, MemRow* dst, int64_t& used_size) {
        for (auto call : agg_calls) {
            call->merge(key, src, dst, used_size);
        }
    }
    static void finalize_all(std::vector<AggFnCall*>& agg_calls, const std::string& key, MemRow* dst) {
        for (auto call : agg_calls) {
            call->finalize(key, dst);
        }
    }
    bool is_bitmap_agg() const {
        switch(_agg_type) {
            case RB_OR_AGG:
            case RB_OR_CARDINALITY_AGG:
            case RB_AND_AGG:
            case RB_AND_CARDINALITY_AGG:
            case RB_XOR_AGG:
            case RB_XOR_CARDINALITY_AGG:
            case RB_BUILD_AGG:
                return true;
            default:
                return false;
        }
    }
    bool is_tdigest_agg() const {
        switch(_agg_type) {
            case TDIGEST_AGG:
            case TDIGEST_BUILD_AGG:
                return true;
            default:
                return false;
        }
    }
    bool is_hll_agg() const {
        switch(_agg_type) {
            case HLL_ADD_AGG:
            case HLL_MERGE_AGG:
                return true;
            default:
                return false;
        }
    }
private:
    struct InterVal {
        bool is_assign = false;
        ExprValue  val;    
    };
    AggType _agg_type;
    pb::Function _fn;
    int32_t _intermediate_slot_id;
    int32_t _final_slot_id;
    bool _is_distinct = false;
    bool _is_merge = false;
    std::map<std::string, InterVal> _intermediate_val_map;
    // for group_concat
    std::string _sep = ",";
    std::shared_ptr<MemRowDescriptor> _mem_row_desc = nullptr;
    std::shared_ptr<Sorter> _sorter = nullptr;
    std::shared_ptr<MemRowCompare> _mem_row_compare = nullptr;
    int32_t _order_tuple_id = -1;
    std::vector<ExprNode*> _order_exprs; // not own it
    std::vector<ExprNode*> _slot_order_exprs; // own it
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::map<std::string, std::shared_ptr<RowBatch>> _intermediate_row_batch_map;
    // for group_concat end

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
