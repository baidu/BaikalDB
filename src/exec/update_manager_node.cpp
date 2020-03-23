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
#include "runtime_state.h"
#include "update_manager_node.h"
#include "delete_manager_node.h"
#include "insert_manager_node.h"
#include "update_node.h"
#include "network_socket.h"

namespace baikaldb {
int UpdateManagerNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _node_type = pb::UPDATE_MANAGER_NODE; 
    return 0;
}

int UpdateManagerNode::init_update_info(UpdateNode* update_node) {
    _op_type = pb::OP_UPDATE;
    _table_id =  update_node->table_id();
    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("get table info failed, table_id:%ld", _table_id);
        return -1;
    }
    _affected_index_ids = _table_info->indices;
    _primary_slots = update_node->primary_slots();
    _update_slots = update_node->update_slots();
    std::set<int32_t> affect_field_ids;
    for (auto& slot : _update_slots) {
        affect_field_ids.insert(slot.field_id());
    }
    _affect_primary = false;
    std::vector<int64_t> affected_indices;
    for (auto index_id : _affected_index_ids) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("get index info failed, index_id:%ld", index_id);
            return -1;
        }
        IndexInfo& info = *info_ptr;
        bool has_id = false;
        for (auto& field : info.fields) {
            if (affect_field_ids.count(field.id) == 1) {
                has_id = true;
                break;
            }
        }
        if (has_id) {
            if (info.id == _table_id) {
                _affect_primary = true;
            } else {
                affected_indices.push_back(index_id);
            }
        }
    }
    
    // 如果更新主键，那么影响了全部索引
    if (!_affect_primary) {
        _affected_index_ids.swap(affected_indices);
    }
    for (auto index_id : _affected_index_ids) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("index info not found index_id:%ld", index_id);
            return -1;
        }
        if (info_ptr->is_global) {
            _affect_global_index = true;
            if (info_ptr->type == pb::I_UNIQ) {
                _uniq_index_number++;
            }
        }
        //DB_WARNING("index:%ld %d", index_id, _affect_global_index);
    }
    
    return 0;
}

int UpdateManagerNode::open(RuntimeState* state) {
    int ret = 0;
    if (_children[0]->node_type() == pb::UPDATE_NODE) {
        return DmlManagerNode::open(state);
    }
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr sxn_id:%lu", state->txn_id);
        return -1;
    }
    if (state->tuple_descs().size() > 0) {
        _tuple_desc = const_cast<pb::TupleDescriptor*> (&(state->tuple_descs()[0]));
    }
    _update_row = state->mem_row_desc()->fetch_mem_row();
    for (auto expr : _update_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("expr open fail, log_id:%lu ret:%d", state->log_id(), ret);
            return ret;
        }
    }
    DeleteManagerNode* delete_manager = static_cast<DeleteManagerNode*>(_children[0]);
    ret = delete_manager->open(state);
    if (ret < 0) {
        DB_WARNING("fetch store failed, log_id:%lu ret:%d ", state->log_id(), ret);
        return -1;
    }
    std::vector<SmartRecord> delete_records = delete_manager->get_real_delete_records();
    if (delete_records.size() == 0) {
        DB_WARNING("no record return");
        return 0;
    }
    for (auto record : delete_records) {
        update_record(record);
    }
    InsertManagerNode* insert_manager = static_cast<InsertManagerNode*>(_children[1]);
    insert_manager->init_insert_info(this);
    insert_manager->set_records(delete_records);
    ret = insert_manager->open(state);
    if (ret < 0) {
        DB_WARNING("fetch store failed, log_id:%lu ret:%d ", state->log_id(), ret);
        // insert失败需要回滚之前的delete操作
        auto seq_ids = delete_manager->seq_ids();
        for (auto seq_id : seq_ids) {
            client_conn->need_rollback_seq.insert(seq_id);
        }
        return -1;
    }
    return ret;
}

void UpdateManagerNode::update_record(SmartRecord record) {
    _update_row->clear();
    MemRow* row = _update_row.get();
    if (_tuple_desc != nullptr) {
        for (auto& slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            row->set_value(slot.tuple_id(), slot.slot_id(),
                           record->get_value(field));
        }
    }
    for (size_t i = 0; i < _update_exprs.size(); i++) {
        auto& slot = _update_slots[i];
        auto expr = _update_exprs[i];
        record->set_value(record->get_field_by_tag(slot.field_id()),
            expr->get_value(row).cast_to(slot.slot_type()));
    }
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
