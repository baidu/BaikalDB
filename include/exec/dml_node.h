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
#include "schema_factory.h"

namespace baikaldb {
class DMLNode : public ExecNode {
public:
    DMLNode() {
        _factory = SchemaFactory::get_instance();
    }
    virtual ~DMLNode() {
    }
    virtual int expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs);
    int insert_row(RuntimeState* state, SmartRecord record, bool is_update = false);
    int delete_row(RuntimeState* state, SmartRecord record);
    int get_lock_row(RuntimeState* state, SmartRecord record, std::string* pk_str);
    int remove_row(RuntimeState* state, SmartRecord record, const std::string& pk_str);
    int update_row(RuntimeState* state, SmartRecord record, MemRow* row);
    int64_t table_id() {
        return _table_id;
    }

protected:
    int init_schema_info(RuntimeState* state);

    int64_t _table_id = -1;
    int64_t _region_id = -1;
    int32_t _tuple_id = -1; // dup key update 原始数据tuple_id
    int32_t _values_tuple_id = -1; // insert values tuple_id
    pb::TupleDescriptor* _tuple_desc = nullptr;
    pb::TupleDescriptor* _values_tuple_desc = nullptr;
    std::unique_ptr<MemRow> _dup_update_row; // calc for on_dup_key_update
    int _num_increase_rows = 0; // update num_table_lines 
    bool _need_ignore = false;
    bool _on_dup_key_update = false;
    //std::vector<int64_t> _index_ids;
    bool _affect_primary = true;
    SchemaFactory* _factory = nullptr;
    std::vector<pb::SlotDescriptor> _primary_slots;
    std::vector<pb::SlotDescriptor> _update_slots;
    std::vector<ExprNode*> _update_exprs;

    TableInfo*               _table_info = nullptr;
    IndexInfo*               _pri_info = nullptr;
    std::vector<int64_t>     _affected_index_ids;
    std::vector<int32_t>     _field_ids;
    std::set<int32_t>        _pri_field_ids;
};
}


/* vim: set ts=4 sw=4 sts=4 tw=100 */
