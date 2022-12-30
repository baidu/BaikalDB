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
#include "transaction.h"
#include "schema_factory.h"

namespace baikaldb {
class DMLNode : public ExecNode {
public:
    DMLNode() {
        _factory = SchemaFactory::get_instance();
    }
    virtual ~DMLNode() {}
    virtual int expr_optimize(QueryContext* ctx);
    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders);
    int insert_row(RuntimeState* state, SmartRecord record, bool is_update = false);
    int delete_row(RuntimeState* state, SmartRecord record, MemRow* row);
    int get_lock_row(RuntimeState* state, SmartRecord record, std::string* pk_str, MemRow* row);
    int remove_row(RuntimeState* state, SmartRecord record, 
            const std::string& pk_str, bool delete_primary = true);
    int update_row(RuntimeState* state, SmartRecord record, MemRow* row);
    bool satisfy_condition_again(RuntimeState* state, MemRow* row);
    int64_t table_id() {
        return _table_id;
    }
    int64_t tuple_id() {
        return _tuple_id;
    }
    int64_t values_tuple_id() {
        return _values_tuple_id;
    }
    std::map<int64_t, std::vector<SmartRecord>>& insert_records_by_region() {
        return _insert_records_by_region;
    }
    std::map<int64_t, std::vector<SmartRecord>>& delete_records_by_region() {
        return _delete_records_by_region;
    }

    int64_t global_index_id() const {
        return _global_index_id;
    }
    pb::LockCmdType lock_type() { return _lock_type; }
    void set_affect_primary(bool affect_primary) {
        _update_affect_primary = affect_primary;
    }
    bool is_replace() {
        return _is_replace;
    }
    bool need_ignore() {
        return _need_ignore;
    }
    std::vector<pb::SlotDescriptor>& update_slots() {
        return _update_slots;
    }
    std::vector<pb::SlotDescriptor>& primary_slots() {
        return _primary_slots;
    }
    std::vector<ExprNode*>& update_exprs() {
        return _update_exprs;
    }
    void clear_update_exprs() {
        _update_exprs.clear();
    }
    SmartTable table_info() {
        return _table_info;
    }

protected:
    int init_schema_info(RuntimeState* state);
    void add_delete_conditon_fields();

    int64_t _table_id = -1; //主表的table_id,不管是二级索引表还是主表
    int64_t _region_id = -1;
    int32_t _tuple_id = -1; // dup key update 原始数据tuple_id
    int32_t _values_tuple_id = -1; // insert values tuple_id
    pb::TupleDescriptor* _tuple_desc = nullptr;
    pb::TupleDescriptor* _values_tuple_desc = nullptr;
    std::unique_ptr<MemRow> _dup_update_row; // calc for on_dup_key_update
    int _num_increase_rows = 0; // update num_table_lines 
    bool _is_replace = false;
    bool _need_ignore = false;
    bool _on_dup_key_update = false;
    bool _update_affect_primary = true;
    SchemaFactory* _factory = nullptr;
    std::vector<pb::SlotDescriptor> _primary_slots;
    std::vector<pb::SlotDescriptor> _update_slots;
    std::vector<ExprNode*> _update_exprs;
    std::set<int32_t> _update_field_ids;

    SmartTransaction         _txn = nullptr; 
    SmartTable               _table_info;
    SmartIndex               _pri_info;
    // insert delete影响索引索引
    // update不修改primary的时候影响部分索引
    // insert on dup key update，在执行update部分时影响部分索引，所以拆出两个变量
    std::vector<SmartIndex>     _all_indexes; 
    std::vector<SmartIndex>     _affected_indexes; 
    std::vector<SmartIndex>*    _indexes_ptr = &_all_indexes;
    std::vector<SmartIndex>     _reverse_indes; 
    std::unordered_set<int64_t> _ignore_index_ids;

    SmartIndex        _global_index_info; 
    pb::LockCmdType   _lock_type;
    int64_t           _global_index_id = 0; //如果是二级索引表则为索引ID，主表为主表id
    std::map<int64_t, std::vector<SmartRecord>> _insert_records_by_region;
    std::map<int64_t, std::vector<SmartRecord>> _delete_records_by_region;
    std::map<int32_t, FieldInfo*> _field_ids;
    std::set<int32_t> _pri_field_ids;
    int64_t _row_ttl_duration = 0; //insert语句可以带上ttl用来覆盖表的配置
    int64_t _ttl_timestamp_us = 0; //ttl写入时间，0表示无ttl
    bool  _ddl_need_write = false;
    int64_t  _ddl_index_id = -1;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
