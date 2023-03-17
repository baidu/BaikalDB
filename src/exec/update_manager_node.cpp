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
#include "query_context.h"

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
    _primary_slots = update_node->primary_slots();
    _update_slots = update_node->update_slots();
    std::set<int32_t> affect_field_ids;
    for (auto& slot : _update_slots) {
        affect_field_ids.insert(slot.field_id());
    }
    _affect_primary = false;
    std::vector<int64_t> local_affected_indices;
    std::vector<int64_t> global_affected_indices;
    std::vector<int64_t> g_unique_indexs;
    std::vector<int64_t> g_non_unique_indexs;
    std::vector<int64_t> g_affected_unique_indexs;
    std::vector<int64_t> g_affected_non_unique_indexs;
    for (auto index_id : _table_info->indices) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("get index info failed, index_id:%ld", index_id);
            return -1;
        }
        IndexInfo& info = *info_ptr;
        if (info.state == pb::IS_NONE) {
            DB_NOTICE("index info is NONE, skip.");
            continue;
        }
        if (info.index_hint_status == pb::IHS_VIRTUAL) {
            DB_NOTICE("index info is virtual, skip.");
            continue;
        }
        if (info.is_global) {
            if (info.type == pb::I_UNIQ) {
                g_unique_indexs.emplace_back(index_id);
            } else {
                g_non_unique_indexs.emplace_back(index_id);
            }
        } else {
            _local_affected_index_ids.emplace_back(index_id);
        }
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
            } else if (info.is_global) {
                if (info.type == pb::I_UNIQ) {
                    g_affected_unique_indexs.emplace_back(index_id);
                } else {
                    g_affected_non_unique_indexs.emplace_back(index_id);
                }
            } else {
                local_affected_indices.push_back(index_id);
            }
        }
    }

    global_affected_indices.insert(global_affected_indices.end(), g_affected_unique_indexs.begin(), g_affected_unique_indexs.end());
    global_affected_indices.insert(global_affected_indices.end(), g_affected_non_unique_indexs.begin(), g_affected_non_unique_indexs.end());
    
    // 如果更新主键或者ttl，那么影响了全部索引
    if (_affect_primary || (_table_info->ttl_info.ttl_duration_s > 0)) {
        _global_affected_index_ids.insert(_global_affected_index_ids.end(), g_unique_indexs.begin(), g_unique_indexs.end());
        _global_affected_index_ids.insert(_global_affected_index_ids.end(), g_non_unique_indexs.begin(), g_non_unique_indexs.end());
    } else {
        _global_affected_index_ids.swap(global_affected_indices);
        _local_affected_index_ids.swap(local_affected_indices);
    }
    for (auto index_id : _global_affected_index_ids) {
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
        ret =  DmlManagerNode::open(state);
        if (ret >= 0) {
            if (process_binlog(state, true) < 0) {
                return -1;
            }
        }
        return ret;
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
    bool open_binlog = false;
    if (state->open_binlog() && _table_info->is_linked) {
        open_binlog = true;
    }
    state->set_open_binlog(false);
    DeleteManagerNode* delete_manager = static_cast<DeleteManagerNode*>(_children[0]);
    ret = delete_manager->open(state);
    if (ret < 0) {
        DB_WARNING("fetch store failed, log_id:%lu ret:%d ", state->log_id(), ret);
        return -1;
    }
    std::vector<SmartRecord>& delete_records = delete_manager->get_real_delete_records();
    if (delete_records.size() == 0) {
        DB_WARNING("no record return");
        return 0;
    }

    for (auto record : delete_records) {
        if (open_binlog) {
            std::string* row = _update_binlog.add_deleted_rows();
            record->encode(*row);
            _partition_record = record;
        }
        update_record(state, record);
        if (open_binlog) {
            std::string* row = _update_binlog.add_insert_rows();
            record->encode(*row);
        }
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
    if (open_binlog) {
        state->set_open_binlog(true);
        process_binlog(state, false);
    }
    return ret;
}

int UpdateManagerNode::process_binlog(RuntimeState* state, bool is_local) {
    if (state->open_binlog() && _table_info->is_linked) {
        // update没有数据删除跳过
        if (is_local && _fetcher_store.return_str_old_records.size() == 0) {
            return 0;
        }
        auto client = state->client_conn();
        auto binlog_ctx = client->get_binlog_ctx();
        auto ctx = client->get_query_ctx();
        pb::PrewriteValue* binlog_value = binlog_ctx->mutable_binlog_value();
        auto mutation = binlog_value->add_mutations();
        binlog_ctx->set_table_info(_table_info);
        if (is_local) {
            for (auto& str_record : _fetcher_store.return_str_records) {
                mutation->add_insert_rows(str_record);
            }
            SmartRecord record_template = _factory->new_record(_table_id);
            bool need_set_partition_record = true;
            for (auto& str_record : _fetcher_store.return_str_old_records) {
                if (need_set_partition_record) {
                    SmartRecord record = record_template->clone(false);
                    auto ret = record->decode(str_record);
                    if (ret < 0) {
                        DB_FATAL("decode to record fail");
                        return -1;
                    }
                    binlog_ctx->set_partition_record(record);
                    need_set_partition_record = false;
                }
                mutation->add_deleted_rows(str_record);
            }
        } else {
            mutation->CopyFrom(_update_binlog);
            binlog_ctx->set_partition_record(_partition_record);
        }

        // 上面CopyFrom会覆盖mutation中其他值，所以公共部分放到CopyFrom之后设置(和InsertManagerNode、DeleteManagerNode不同)
        if (ctx != nullptr) {
            mutation->set_sql(ctx->sql);
            auto stat_info = &(ctx->stat_info);
            mutation->set_sign(stat_info->sign);
            binlog_ctx->add_sql_info(stat_info->family, stat_info->table, stat_info->sign);
        }

        mutation->add_sequence(pb::MutationType::UPDATE);
        mutation->set_table_id(_table_id);
    }
    return 0;
}

void UpdateManagerNode::update_record(RuntimeState* state, SmartRecord record) {
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
        auto last_insert_id_expr = expr->get_last_insert_id();
        if (last_insert_id_expr != nullptr) {
            state->last_insert_id = last_insert_id_expr->get_value(row).get_numberic<int64_t>();
            state->client_conn()->last_insert_id = state->last_insert_id;
        }
    }
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
