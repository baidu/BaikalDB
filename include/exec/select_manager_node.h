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
#include "table_record.h"
#include "proto/store.interface.pb.h"
#include "sorter.h"
#include "mem_row_compare.h"
#include "fetcher_store.h"

namespace baikaldb {
class SelectManagerNode : public ExecNode {
public:
    SelectManagerNode() {
        _factory = SchemaFactory::get_instance();
    }
    virtual ~SelectManagerNode() {
        for (auto expr : _slot_order_exprs) {
            ExprNode::destroy_tree(expr);
        }
    }
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state) {
        ExecNode::close(state);
        for (auto expr : _slot_order_exprs) {
            expr->close();
        }
        _sorter = nullptr;
        _fetcher_store.clear();
    }
    int init_sort_info(const pb::PlanNode& node) {
        for (auto& expr : node.derive_node().sort_node().slot_order_exprs()) {
            ExprNode* order_expr = nullptr;
            auto ret = ExprNode::create_tree(expr, &order_expr);
            if (ret < 0) {
                //如何释放资源
                return ret;
            }
            _slot_order_exprs.push_back(order_expr);
        }
        for (auto asc : node.derive_node().sort_node().is_asc()) {
            _is_asc.push_back(asc);
        }
        for (auto null_first : node.derive_node().sort_node().is_null_first()) {
            _is_null_first.push_back(null_first);
        } 
        return 0;
    }
    int open_global_index(RuntimeState* state,
                          ExecNode* exec_node,
                          int64_t global_index_id,
                          int64_t main_table_id);
    int construct_primary_possible_index(
                          RuntimeState* state,
                          ExecNode* exec_node,
                          int64_t main_table_id);
private:
    //允许fetcher回来后排序
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::shared_ptr<MemRowCompare> _mem_row_compare;
    std::shared_ptr<Sorter> _sorter;
    FetcherStore    _fetcher_store;
    std::map<int32_t, int32_t> _index_slot_field_map;
    SchemaFactory*  _factory = nullptr;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
