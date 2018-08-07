// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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
#include "scan_node.h"
#include "filter_node.h"
#include "sort_node.h"
#include "query_context.h"
#include "schema_factory.h"

namespace baikaldb {
class IndexSelector {
    enum RangeType {
        NONE, 
        LEFT_OPEN,
        LEFT_CLOSE,
        RIGHT_OPEN,
        RIGHT_CLOSE,
        EQ,
        LIKE,
        OR_LIKE,
        IN,
        INDEX_HAS_NULL
    };

public:
    /* 循环遍历所有索引
     * 对每个索引字段都去表达式中寻找是否能命中
     */
    int analyze(QueryContext* ctx);
    
    void index_selector(const std::function<int32_t(int32_t, int32_t)>& get_slot_id,
                        QueryContext* ctx,
                        ScanNode* scan_node,
                        FilterNode* filter_node,
                        SortNode* sort_node,
                        bool* has_recommend);
private:

    RangeType or_like_index_type(
            ExprNode* expr, int32_t tuple_id, int32_t slot_id, ExprValue* value);
    RangeType index_expr_type(ExprNode* expr, int32_t tuple_id, int32_t slot_id,
            pb::IndexType index_type, std::vector<ExprValue>* values);

    //检查order by是否可以使用索引
    bool check_sort_use_index(const std::function<int(int, int)>& get_slot_id, 
                              IndexInfo& index_info, 
                              const std::vector<ExprNode*>& order_exprs, 
                              int32_t tuple_id, uint32_t field_cnt);
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
