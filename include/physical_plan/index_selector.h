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
#include "row_expr.h"
#include "scan_node.h"
#include "filter_node.h"
#include "sort_node.h"
#include "join_node.h"
#include "query_context.h"
#include "schema_factory.h"
#include "range.h"

namespace baikaldb {

class IndexSelector {
public:
    /* 循环遍历所有索引
     * 对每个索引字段都去表达式中寻找是否能命中
     */
    int analyze(QueryContext* ctx);
    // -2 表示always false
    int64_t index_selector(const std::vector<pb::TupleDescriptor>& tuple_descs,
                        ScanNode* scan_node,
                        FilterNode* filter_node,
                        SortNode* sort_node,
                        JoinNode* join_node,
                        bool* index_has_null,
                        std::map<int32_t, int>& field_range_type,
                        const std::string& sample_sql);
private:

    void hit_row_field_range(ExprNode* expr, std::map<int32_t, range::FieldRange>& field_range_map, bool* index_predicate_is_null);
    void hit_match_against_field_range(ExprNode* expr, 
        std::map<int32_t, range::FieldRange>& field_range_map, FulltextInfoNode* fulltext_index_node, int64_t table_id);
    void hit_field_range(ExprNode* expr, std::map<int32_t, range::FieldRange>& field_range_map, bool* index_predicate_is_null, 
        int64_t table_id, FulltextInfoNode* fulltext_index_node);
    void hit_field_or_like_range(ExprNode* expr, std::map<int32_t, range::FieldRange>& field_range_map, 
        int64_t table_id, FulltextInfoNode* fulltext_index_node);
    
    bool is_field_has_arrow_reverse_index(int64_t table_id, int64_t field_id, int64_t* index_id_ptr) {
        auto table_ptr = _factory->get_table_info_ptr(table_id);
        if (table_ptr != nullptr) {
            auto iter = table_ptr->arrow_reverse_fields.find(field_id);
            if (iter != table_ptr->arrow_reverse_fields.end()) {
                *index_id_ptr = iter->second;
                auto index_ptr = _factory->get_index_info_ptr(*index_id_ptr);
                if (index_ptr != nullptr) {
                    return index_ptr->state == pb::IS_PUBLIC;   
                }
            }
        }
        return false;
    }

    SchemaFactory* _factory = SchemaFactory::get_instance();
    QueryContext*  _ctx = nullptr;

};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
