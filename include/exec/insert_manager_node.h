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
// Brief:  truncate table exec node

#pragma once
#include "dml_manager_node.h"
#include "exec_node.h"
#include "schema_factory.h"

namespace baikaldb {
class UpdateManagerNode;
class InsertNode;

class InsertManagerNode : public DmlManagerNode {
public:
    InsertManagerNode() {
    }
    virtual ~InsertManagerNode() {
        for (auto expr : _update_exprs) {
            ExprNode::destroy_tree(expr);
        }
        for (auto expr : _insert_values) {
            ExprNode::destroy_tree(expr);
        }
    }
    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;
    virtual void close(RuntimeState* state) override {
        ExecNode::close(state);
        for (auto expr : _update_exprs) {
            expr->close();
        }
        _origin_records.clear();
        _store_records.clear();
        _on_dup_key_update_records.clear();
        _record_ids.clear();
        _index_keys_record_map.clear();
        _index_info_map.clear();

        _has_conflict_record = true;
        _main_table_reversed = false;
        _affected_rows = 0;
    }
    virtual int expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs);
    int init_insert_info(UpdateManagerNode* update_manager_node);
    int init_insert_info(InsertNode* insert_node);
    bool need_ignore() {
        return _need_ignore;
    }
    bool is_replace() {
        return _is_replace;
    }
    bool on_dup_key_update() {
        return _on_dup_key_update; 
    }

    int basic_insert(RuntimeState* state);
    int insert_ignore(RuntimeState* state);

    int get_record_from_store(RuntimeState* state);
    int reverse_main_table(RuntimeState* state);
    int insert_replace(RuntimeState* state);
    int insert_on_dup_key_update(RuntimeState* state);

    void add_store_records() {
        for (auto pair : _fetcher_store.index_records) {
            int64_t index_id = pair.first;
            _store_records[index_id].insert(_store_records[index_id].end(),
                pair.second.begin(), pair.second.end());
        }
    }

    void set_err_message(IndexInfo& index_info,
                         SmartRecord& record,
                         RuntimeState* state);
    int process_records_before_send(RuntimeState* state);
    void set_records(std::vector<SmartRecord>& records) {
        _origin_records.swap(records);
    }

private:
    void update_record(SmartRecord record);
    int64_t     _table_id = -1;
    int32_t     _tuple_id = -1;
    int32_t     _values_tuple_id = -1;
    bool        _is_replace = false;
    bool        _need_ignore = false;
    bool        _on_dup_key_update = false;
    pb::TupleDescriptor* _tuple_desc = nullptr;
    pb::TupleDescriptor* _values_tuple_desc = nullptr;
    std::unique_ptr<MemRow> _dup_update_row; // calc for on_dup_key_update
    SmartTable      _table_info;
    SmartIndex      _pri_info;
    std::vector<ExprNode*>            _update_exprs;
    std::vector<pb::SlotDescriptor>   _update_slots;
    std::vector<ExprNode*>   _insert_values;
    std::vector<SmartRecord> _origin_records;
    // index_id -> records
    std::map<int64_t, std::vector<SmartRecord>> _store_records;
    /* on_dup_key_update冲突时更新的是原表数据，_on_dup_key_update_records保存原表记录
       index_id -> <key -> record> 
    */
    std::map<int64_t, std::map<std::string, SmartRecord>> _on_dup_key_update_records;
    std::set<int32_t>            _record_ids;
    // index_id -> <key->record_id>
    std::map<int64_t, std::map<std::string, std::set<int32_t>>> _index_keys_record_map;
    std::map<int64_t, SmartIndex> _index_info_map;

    bool   _has_conflict_record = true;
    bool   _main_table_reversed = false;
    int    _affected_rows = 0;
};

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
