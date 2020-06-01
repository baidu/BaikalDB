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

#include <string>
#include <map>
#include <unordered_map>
#ifdef BAIDU_INTERNAL 
#include <base/containers/flat_map.h>
#else
#include <butil/containers/flat_map.h>
#endif
#include <vector>
#include "exec_node.h"
#include "agg_fn_call.h"
#include "mut_table_key.h"

namespace baikaldb {
class AggNode : public ExecNode {
public:
    AggNode() {
    }
    virtual ~AggNode() {
        for (auto expr : _group_exprs) {
            ExprNode::destroy_tree(expr);
        }
        for (auto agg : _agg_fn_calls) {
            ExprNode::destroy_tree(agg);
        }
    }
    virtual int init(const pb::PlanNode& node);
    virtual int expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs);
    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    void encode_agg_key(MemRow* row, MutTableKey& key);
    void process_row_batch(RowBatch& batch);
    std::vector<ExprNode*>& group_exprs() {
        return _group_exprs;
    }
private:
    //需要推导_agg_tuple_id内部slot的类型
    std::vector<ExprNode*> _group_exprs;
    int32_t _agg_tuple_id;
    pb::TupleDescriptor* _group_tuple_desc;
    std::vector<AggFnCall*> _agg_fn_calls;
    bool _is_merger = false;
    MemRowDescriptor* _mem_row_desc;
    //用于分组和get_next的定位,用map可与mysql保持一致
    butil::FlatMap<std::string, MemRow*> _hash_map;
    butil::FlatMap<std::string, MemRow*>::iterator _iter;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
