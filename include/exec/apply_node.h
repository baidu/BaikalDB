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
#include "exec_node.h"
#include "joiner.h"
#include "mut_table_key.h"
#ifdef BAIDU_INTERNAL 
#include <base/containers/flat_map.h>
#else
#include <butil/containers/flat_map.h>
#endif
#include "slot_ref.h"

namespace baikaldb {

class ApplyNode : public Joiner {
public:
    ApplyNode() {
    }
    virtual  ~ApplyNode() { }

    virtual int init(const pb::PlanNode& node);
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);

    void decorrelate();

    int check_unique_key(RuntimeState* state, const std::map<int32_t, std::set<int32_t>>& tuple_field_ids);

    bool is_correlate_expr(ExprNode* expr, bool& is_equal);

    void get_slot_ref_sign_set(RuntimeState* state, std::set<int64_t>& sign_set);

private:
    void extract_eq_inner_slots(ExprNode* expr_node);
    int hash_apply(RuntimeState* state);
    int loop_hash_apply(RuntimeState* state);
    int nested_loop_apply(RuntimeState* state);
    int fetcher_inner_table_data(RuntimeState* state,
                            MemRow* outer_tuple_data,
                            std::vector<ExecNode*>& scan_nodes,
                            std::vector<MemRow*>& inner_tuple_data);
    int get_next_via_inner_hash_map(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_via_outer_hash_map(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_via_loop_outer_hash_map(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_for_nested_loop_join(RuntimeState* state, RowBatch* batch, bool* eos);
    inline int get_loop_num() {
        return _parent->get_limit() * std::pow(2, _loops);
    }
private:
    bool    _max_one_row = false;
    bool    _has_matched = false;
    bool    _all_matched = true;
    bool    _is_select_field = false;
    std::map<int64_t, std::vector<ExprNode*>> _literal_maps;
    std::set<int64_t> _slot_ref_sign_set;
    std::vector<ExecNode*> _scan_nodes;
    std::vector<SlotRef*> _inner_eq_slot_refs;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
