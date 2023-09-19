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
#include "sorter.h"
#include "mem_row_compare.h"
#include "property.h"

namespace baikaldb {
class SortNode : public ExecNode {
public:
    SortNode() : _tuple_id(-1), 
                 _mem_row_desc(nullptr) {
    }
    virtual ~SortNode() {
        for (auto expr : _order_exprs) {
            ExprNode::destroy_tree(expr);
        }
        for (auto expr : _slot_order_exprs) {
            ExprNode::destroy_tree(expr);
        }
    }
    virtual int init(const pb::PlanNode& node);
     
    virtual int expr_optimize(QueryContext* ctx);
    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
        ExecNode::transfer_pb(region_id, pb_node);
        auto sort_node = pb_node->mutable_derive_node()->mutable_sort_node();
        sort_node->clear_order_exprs();
        for (auto expr : _order_exprs) {
            ExprNode::create_pb_expr(sort_node->add_order_exprs(), expr);
        }
        sort_node->clear_slot_order_exprs();
        for (auto expr : _slot_order_exprs) {
            ExprNode::create_pb_expr(sort_node->add_slot_order_exprs(), expr);
        }
    }
    
    void transfer_fetcher_pb(pb::FetcherNode* pb_fetcher) {
        for (auto expr : _slot_order_exprs) {
            ExprNode::create_pb_expr(pb_fetcher->add_slot_order_exprs(), expr);
        }
        for (auto is_asc : _is_asc) {
            pb_fetcher->add_is_asc(is_asc);
        }
        for (auto is_null_first : _is_null_first) {
            pb_fetcher->add_is_null_first(is_null_first);
        }
    }

    Property sort_property() {
        if (_monotonic) {
            return Property{_slot_order_exprs, _is_asc, _limit};
        } else {
            return Property();
        }
    }

    std::vector<bool>& is_asc() {
        return _is_asc;
    }

    std::vector<bool>& is_null_first() {
        return _is_null_first;
    }

    std::vector<ExprNode*>& order_exprs() {
        return _order_exprs;
    }
    std::vector<ExprNode*>& slot_order_exprs() {
        return _slot_order_exprs;
    }

    std::vector<ExprNode*>* mutable_order_exprs() {
        return &_order_exprs;
    }
    std::vector<ExprNode*>* mutable_slot_order_exprs() {
        return &_slot_order_exprs;
    }

    bool is_monotonic() {
        return _monotonic;
    }

    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output) {
        ExecNode::show_explain(output);
        if (output.empty()) {
            return;
        }
        if (output.back()["sort_index"] != "1") {
            output.back()["Extra"] += "Using filesort";
        }
    }

private:
    int fill_tuple(RowBatch* batch);

private:
    std::vector<ExprNode*> _order_exprs;
    std::vector<ExprNode*> _slot_order_exprs;
    int32_t _tuple_id;
    MemRowDescriptor* _mem_row_desc;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::shared_ptr<MemRowCompare> _mem_row_compare;
    std::shared_ptr<Sorter> _sorter;
    bool _monotonic = true; //是否单调(全部升序或降序)
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
