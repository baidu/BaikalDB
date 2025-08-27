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
#include "dml_node.h"

namespace baikaldb {

DEFINE_bool(replace_no_get, false, "no get before replace if true");

int DMLNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("expr type_inferer fail:%d", ret);
        return ret;
    }
    ret = common_expr_optimize(&_update_exprs);
    if (ret < 0) {
        DB_WARNING("common_expr_optimize fail");
        return ret;
    }
    return 0;
}

void DMLNode::add_delete_conditon_fields() {
    if (_node_type == pb::UPDATE_NODE || _node_type == pb::DELETE_NODE || _node_type == pb::LOCK_PRIMARY_NODE) {
        std::set<int32_t> cond_field_ids;
        for (auto& slot : _tuple_desc->slots()) {
            cond_field_ids.insert(slot.field_id());
        }
        for (auto& field_info : _table_info->fields) {
            if (cond_field_ids.count(field_info.id) > 0
                && _pri_field_ids.count(field_info.id) == 0) {
                _field_ids[field_info.id] = &field_info;
            }
        }
    }
}

int DMLNode::init_schema_info(RuntimeState* state) {
    _region_id = state->region_id();
    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id); 
    if (_table_info == nullptr) {
        DB_WARNING("get table info failed table_id: %ld", _table_id);
        return -1;
    }
    _pri_info = SchemaFactory::get_instance()->get_index_info_ptr(_table_id);
    if (_pri_info == nullptr) {
        DB_WARNING("get primary index info failed table_id: %ld", _table_id);
        return -1;
    }

    int64_t ttl_duration = _row_ttl_duration > 0 ? _row_ttl_duration : _table_info->ttl_info.ttl_duration_s;
    if (ttl_duration > 0) {
        _ttl_timestamp_us = butil::gettimeofday_us() + ttl_duration * 1000 * 1000LL;
    }
    DB_DEBUG("table_id: %ld, region_id: %ld, _row_ttl_duration: %ld, table ttl duration: %ld", 
        _table_id, _region_id, _row_ttl_duration, _table_info->ttl_info.ttl_duration_s);
    bool ttl = ttl_duration > 0;

    if (_global_index_id != 0) {
        _global_index_info = SchemaFactory::get_instance()->get_index_info_ptr(_global_index_id);
        if (_global_index_info == nullptr) {
            DB_WARNING("get global index info failed _global_index_id: %ld", _global_index_id);
            return -1;
        }
    }
    for (auto& field_info : _pri_info->fields) {
        _pri_field_ids.insert(field_info.id);
    }
    if (_all_indexes.empty()) {
        bool ddl_index_id_synced = false;
        for (auto index_id : _table_info->indices) {
            auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
            if (index_info == nullptr) {
                DB_WARNING("get index info failed index_id: %ld", index_id);
                return -1;
            }
            if (index_info->type == pb::I_FULLTEXT) {
                _reverse_indexes.push_back(index_info);
            }
            if (!index_info->is_global && index_info->index_hint_status != pb::IHS_VIRTUAL) {
                _all_indexes.push_back(index_info);
                if (_ddl_need_write && _ddl_index_id == index_id) {
                    ddl_index_id_synced = true;
                }
            }
        }
        // db认为需要写入,store还未同步ddl信息,写失败处理
        if (_ddl_need_write && !ddl_index_id_synced) {
            DB_WARNING("table_id:%ld ddl index info not found index:%ld", _table_id, _ddl_index_id);
            return -1;
        }
    }
    
    bool has_rollup_index = SchemaFactory::get_instance()->has_rollup_index(_table_id);
    // update and on_dup_key_update need all fields
    // delete and insert/replace need get index fields
    // replace/delete binlog need all old fields
    // 对rollup也要拿表所有的列
    if (_node_type == pb::UPDATE_NODE || _on_dup_key_update
        || ((_local_index_binlog || has_rollup_index) && (_is_replace || _node_type == pb::DELETE_NODE))) {
        //保存所有字段，主键不在pb里，不需要传入
        for (auto& field_info : _table_info->fields) {
            if (_pri_field_ids.count(field_info.id) == 0) {
                _field_ids[field_info.id] = &field_info;

            }
        }
    } else {
        for (auto index_info : _all_indexes) {
            if (index_info->is_global) {
                continue;
            }
            for (auto& field_info : index_info->fields) {
                if (_pri_field_ids.count(field_info.id) == 0) {
                    _field_ids[field_info.id] = &field_info;
                }
            }
        }
    }
    _txn = state->txn();
    if (_txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr: region:%ld", _region_id);
        return -1;
    }
    if (_node_type == pb::UPDATE_NODE || _node_type == pb::DELETE_NODE || _node_type == pb::LOCK_PRIMARY_NODE) {
        if (state->tuple_id >= 0) {
            _tuple_desc = state->get_tuple_desc(state->tuple_id);
            if (_tuple_desc == nullptr) {
                DB_WARNING_STATE(state, "_tuple_desc nullptr: tuple_id:%d", state->tuple_id);
                return -1;
            }
            add_delete_conditon_fields();
        }
    }
    if (!_update_slots.empty()) {
        std::set<int32_t> affect_field_ids;
        for (auto& slot : _update_slots) {
            affect_field_ids.insert(slot.field_id());
        }
        for (auto& field_info : _table_info->fields) {
            if (affect_field_ids.count(field_info.id) == 1) {
                _update_fields[field_info.id] = &field_info;
            }
        }
        _update_affect_primary = false;

        for (auto& info_ptr : _all_indexes) {
            IndexInfo& info = *info_ptr;
            bool has_id = false;
            for (auto& field : info.fields) {
                if (affect_field_ids.count(field.id) == 1) {
                    has_id = true;
                    break;
                }
            }
            // 如果是ROLLUP索引, 一定是被更改了, 后续如果只修改维度列有可能没有被更改
            if (info.type == pb::I_ROLLUP) {
                has_id = true;
            }
            // 向量索引缓存包含标量字段
            if (info.type == pb::I_VECTOR) {
                has_id = true;
            }
            if (has_id) {
                if (info.type == pb::I_PRIMARY) {
                    _update_affect_primary = true;
                    break;
                } else {
                    _affected_indexes.push_back(info_ptr);
                }
            }
        }
        // 如果更新主键或ttl表，那么影响了全部索引
        if (!_update_affect_primary && !ttl) {
            // cstore下只更新涉及列
            if (_table_info->engine == pb::ROCKSDB_CSTORE) {
                for (size_t i = 0; i < _update_slots.size(); i++) {
                    auto field_id = _update_slots[i].field_id();
                    if (_pri_field_ids.count(field_id) == 0 &&
                            _update_field_ids.count(field_id) == 0) {
                        _update_field_ids.insert(field_id);
                    }
                }
                _field_ids.clear();
                for (auto& index_info : _affected_indexes) {
                    for (auto& field_info : index_info->fields) {
                        if (_pri_field_ids.count(field_info.id) == 0) {
                            _field_ids[field_info.id] = &field_info;
                        }
                    }
                }
                add_delete_conditon_fields();
            }
        } else {
            _affected_indexes = _all_indexes;
            if (ttl || _local_index_binlog) {
                // cstore_update_fields_partly=true时，需要用到_update_field_ids，修复ttl列存表更新默认字段不生效的bug
                if (_table_info->engine == pb::ROCKSDB_CSTORE) {
                    for (auto iter : _field_ids) {
                        _update_field_ids.insert(iter.first);
                    }
                }
            }
        }
    } else {
        _affected_indexes = _all_indexes;
    }
    return 0;
}

int DMLNode::insert_row(RuntimeState* state, SmartRecord record, bool is_update) {
    _ignore_index_ids.clear();
    //DB_WARNING_STATE(state, "insert record: %s", record->debug_string().c_str());
    int ret = 0;
    int affected_rows = 0;
    // update更新部分索引，会在update_row里指定索引
    // insert/relace语义需要更新全部索引
    // LOCK_PRIMARY_NODE在全局索引中相当于update
    if (!is_update && _node_type != pb::LOCK_PRIMARY_NODE) {
        _indexes_ptr = &_all_indexes;
    }
    // LOCK_PRIMARY_NODE目前无法区分update与insert，暂用update兼容
    // 由于cstore的字段是分开存储的,不涉及主键时,可以优化为更新部分涉及字段.
    bool cstore_update_fields_partly = !_update_affect_primary &&
            (is_update || _node_type == pb::LOCK_PRIMARY_NODE);
    bool need_increase = true;
    auto& reverse_index_map = state->reverse_index_map();
    auto& vector_index_map = state->vector_index_map();
    if (_on_dup_key_update) {
        _dup_update_row->clear();
        if (_values_tuple_desc != nullptr) {
            for (auto slot : _values_tuple_desc->slots()) {
                auto field = record->get_field_by_tag(slot.field_id());
                _dup_update_row->set_value(slot.tuple_id(), slot.slot_id(),
                        record->get_value(field));
                //DB_WARNING_STATE(state, "_on_dup_key_update: tuple:%d slot:%d %d", slot.tuple_id(), slot.slot_id(), record->get_value(field).get_numberic<int32_t>());
            }
        }
    }
    _txn->set_write_ttl_timestamp_us(_ttl_timestamp_us);
    DB_DEBUG("ttl_timestamp_us: %ld", _ttl_timestamp_us);
    MutTableKey pk_key;
    ret = record->encode_key(*_pri_info, pk_key, -1, false);
    if (ret < 0) {
        DB_WARNING_STATE(state, "encode key failed, ret:%d", ret);
        return ret;
    }
    std::string pk_str = pk_key.data();
    if ((!is_update && _node_type != pb::LOCK_PRIMARY_NODE) || _update_affect_primary) {
        //no field need to decode here, only check key exist and get lock
        //std::vector<int32_t> field_ids;
        SmartRecord old_record = record;
        if (_is_replace) {
            old_record = record->clone(true);
        }
        if (FLAGS_replace_no_get && _is_replace && _all_indexes.size() == 1) {
            if (!_txn->fits_region_range_for_primary(*_pri_info, pk_key)) {
                // DB_DEBUG("replace_no_get fail to fit: %s", rocksdb::Slice(pk_key.data()).ToString(true).c_str());
                return 0;
            }
            ret = -2;
        } else if (_is_merge) {
            _txn->set_leader_merge_in_raft(FLAGS_leader_merge_in_raft);
            if (!_txn->fits_region_range_for_primary(*_pri_info, pk_key)) {
                // DB_DEBUG("replace_no_get fail to fit: %s", rocksdb::Slice(pk_key.data()).ToString(true).c_str());
                return 0;
            }
            ret = -2;
        } else {
            ret = _txn->get_update_primary(_region_id, *_pri_info, old_record, _field_ids, GET_LOCK, true);
        }
        if (ret == -3) {
            //DB_WARNING_STATE(state, "key not in this region:%ld, %s", _region_id, record->to_string().c_str());
            return 0;
        }
        if (ret == -4) {
            //过期的数据被覆盖，但是num_table_lines已经计算过旧数据了
            need_increase = false;
        }
        if (ret != -2 && ret != -4) {
            if (ret == 0) { 
                if (_need_ignore) {
                    return 0;
                }
                if (is_update) {
                    DB_WARNING_STATE(state, "update new primary row must not exist, "
                            "index:%ld, ret:%d", _table_id, ret);
                    state->error_code = ER_DUP_ENTRY;
                    state->error_msg << "Duplicate entry: '" << 
                        old_record->get_index_value(*_pri_info) << "' for key 'PRIMARY'";
                    return -1;
                } else if (_on_dup_key_update) {
                    ret = update_row(state, record, _dup_update_row.get());
                    if (ret == 1) {
                        ++ret;
                    }
                    return ret;
                } else if (_is_replace) {
                    for (auto& info_ptr : _reverse_indexes) {
                        int64_t index_id = info_ptr->id;
                        std::string old_word;
                        old_record->get_reverse_word(*info_ptr, old_word);
                        std::string new_word;
                        record->get_reverse_word(*info_ptr, new_word);
                        if (old_word == new_word) {
                            _ignore_index_ids.insert(index_id);
                        }
                    }
                    // 对于主键replace，可以不删除旧数据，直接用新数据覆盖
                    ret = remove_row(state, old_record, pk_str, false);
                    if (ret < 0) {
                        DB_WARNING_STATE(state, "remove fail, table_id:%ld ,ret:%d", _table_id, ret);
                        return -1;
                    }
                    if (_local_index_binlog) {
                        _return_old_records[_pri_info->id].emplace_back(old_record);
                    }
                    cstore_update_fields_partly = true;
                    ++affected_rows;
                } else {
                    DB_WARNING_STATE(state, "insert row must not exist, index:%ld, ret:%d", _table_id, ret);
                    state->error_code = ER_DUP_ENTRY;
                    state->error_msg << "Duplicate entry: '" << 
                        old_record->get_index_value(*_pri_info) << "' for key 'PRIMARY'";
                    return -1;
                }
            } else {
                DB_WARNING_STATE(state, "insert row rocksdb error, index:%ld, ret:%d", _table_id, ret);
                if (ret == -5) {
                    state->error_code = ER_LOCK_WAIT_TIMEOUT;
                    state->error_msg << "Lock '" << 
                        old_record->get_index_value(*_pri_info) << "' for key 'PRIMARY' Timeout";
                }
                return -1;
            }
        }
    }

    // lock secondary keys
    for (auto& info_ptr : *_indexes_ptr) {
        IndexInfo& info = *info_ptr;

        auto index_state = info.state;
        //DB_DEBUG("dml_insert_record prime+index string[%s] state[%s] index_id[%ld] index_name[%s] region_%ld", 
        //    record->to_string().c_str(), pb::IndexState_Name(index_state).c_str(), info.id, info.name.c_str(), _region_id);

        if (!_ddl_need_write && (index_state != pb::IS_PUBLIC && index_state != pb::IS_WRITE_ONLY &&
            index_state != pb::IS_WRITE_LOCAL)) {
            DB_DEBUG("DDL_LOG skip index [%ld] state [%s] ", 
                info.id, pb::IndexState_Name(index_state).c_str());
            continue;
        }

        // 只有unique会冲突
        if (info.type != pb::I_UNIQ) {
            continue;
        }
        SmartRecord old_record = record;
        if (_is_replace) {
            old_record = record->clone(true);
        }
        ret = _txn->get_update_secondary(_region_id, *_pri_info, info, old_record, GET_LOCK, true);
        if (ret == 0) {
            if (is_update) {
                DB_WARNING_STATE(state, "update uniq key must not exist, "
                        "index:%ld, ret:%d", info.id, ret);
                state->error_code = ER_DUP_ENTRY;
                state->error_msg << "Duplicate entry: '" << 
                    old_record->get_index_value(info) << "' for key '" << info.short_name << "'";
                return -1;
            } else if (_need_ignore) {
                return 0;
            } else if (_on_dup_key_update) {
                ret = update_row(state, old_record, _dup_update_row.get());
                if (ret == 1) {
                    ++ret;
                }
                return ret;
            } else if (_is_replace) {
                ret = delete_row(state, old_record, nullptr);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "remove fail, index:%ld ,ret:%d", info.id, ret);
                    return -1;
                }
                ++affected_rows;
                continue;
            } else {
                DB_WARNING_STATE(state, "insert uniq key must not exist, "
                        "index:%ld, ret:%d", info.id, ret);
                state->error_code = ER_DUP_ENTRY;
                state->error_msg << "Duplicate entry: '" << 
                    old_record->get_index_value(info) << "' for key '" << info.short_name << "'";
                return -1;
            }
        }
        // ret == -3 means the primary_key returned by get_update_secondary is out of the region
        // (dirty data), this does not affect the insertion
        if (ret != -2 && ret != -3 && ret != -4) {
            if (ret == -5) {
                state->error_code = ER_LOCK_WAIT_TIMEOUT;
                state->error_msg << "Lock '" << 
                     old_record->get_index_value(info) << "' for key '" << info.short_name << "' Timeout";
                DB_WARNING_STATE(state, "insert rocksdb get lock failed, index:%ld, ret:%d", info.id, ret);
                return -1;
            }
            if (_need_ignore) {
                return 0;
            }
            DB_WARNING_STATE(state, "insert rocksdb failed, index:%ld, ret:%d", info.id, ret);
            return -1;
        }
    }
    for (auto& info_ptr: *_indexes_ptr) {
        IndexInfo& info = *info_ptr;
        if (_ignore_index_ids.count(info.id) == 1) {
            continue;
        }
        if (info.type == pb::I_PRIMARY) {
            continue;
        }
        auto index_state = info.state;
        if (!_ddl_need_write && (index_state != pb::IS_PUBLIC && index_state != pb::IS_WRITE_ONLY &&
            index_state != pb::IS_WRITE_LOCAL)) {
            DB_DEBUG("DDL_LOG skip index [%ld] state [%s] ", 
                info.id, pb::IndexState_Name(index_state).c_str());
            continue;
        }
        // 全文索引信息未同步到store,写失败处理
        if (_ddl_need_write && info.type == pb::I_FULLTEXT && reverse_index_map.count(info.id) == 0) {
            DB_WARNING_STATE(state, "table_id:%ld full index info not found index:%ld", _table_id, info.id);
            return -1;
        }
        if (_ddl_need_write && info.type == pb::I_VECTOR && vector_index_map.count(info.id) == 0) {
            DB_WARNING_STATE(state, "table_id:%ld vector index info not found index:%ld", _table_id, info.id);
            return -1;
        }
        if (reverse_index_map.count(info.id) == 1) {
            // inverted index only support single field
            if (info.id == -1 || info.fields.size() != 1) {
                return -1;
            }
            auto field = record->get_field_by_idx(info.fields[0].pb_idx);
            if (record->is_null(field)) {
                continue;
            }
            std::string word;
            ret = record->get_reverse_word(info, word);
            if (ret < 0) {
                DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", 
                                 info.id);
                return ret;
            }
            //DB_NOTICE("word:%s", str_to_hex(word).c_str());
            ret = reverse_index_map[info.id]->insert_reverse(_txn, 
                                                            word, pk_str, record);
            if (ret < 0) {
                return ret;
            }
            continue;
        } else if (vector_index_map.count(info.id) == 1) {
            // vector index only can support one or two field
            if (info.id == -1 || (info.fields.size() != 1 && info.fields.size() != 2)) {
                return -1;
            }
            int field_idx = 0;
            if (info.fields.size() > 1) {
                field_idx = 1;
            }
            auto field = record->get_field_by_idx(info.fields[field_idx].pb_idx);
            if (record->is_null(field)) {
                continue;
            }
            std::string word;
            ret = record->get_reverse_word(info, word);
            if (ret < 0) {
                DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", 
                                 info.id);
                return ret;
            }
            //DB_NOTICE("word:%s", str_to_hex(word).c_str());
            ret = vector_index_map[info.id]->insert_vector(_txn, word, pk_str, record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "vector_index fail insert, index_id: %ld", info.id);
                return ret;
            }
            continue;
        }
        ret = _txn->put_secondary(_region_id, info, record);
        if (ret < 0) {
            DB_WARNING_STATE(state, "put index:%ld fail:%d, table_id:%ld", info.id, ret, _table_id);
            return ret;
        }
    }

    if (_local_index_binlog && (_is_replace || _need_ignore || _on_dup_key_update || _node_type == pb::UPDATE_NODE)) {
        _return_records[_pri_info->id].emplace_back(record->clone(true));
    }
    // 列存为节省空间, 插入默认值或空值时不会put
    // cstore_update_fields_partly为true时更新前旧值尚未被删除
    _txn->set_watt_stats_version(_watt_stats_version);
    ret = _txn->put_primary(_region_id, *_pri_info, record,
                            cstore_update_fields_partly ? &_update_field_ids : nullptr, _is_merge);
    if (ret < 0) {
        DB_WARNING_STATE(state, "put table:%ld fail:%d", _table_id, ret);
        return -1;
    }
    //DB_WARNING_STATE(state, "insert succes:%ld, %s", _region_id, record->to_string().c_str());
    if (need_increase) {
        ++_num_increase_rows;
    }
    return ++affected_rows;
}

int DMLNode::get_lock_row(RuntimeState* state, SmartRecord record, std::string* pk_str, MemRow* row, int64_t& ttl_ts) {
    int ret = 0;
    MutTableKey pk_key;
    ret = record->encode_key(*_pri_info, pk_key, -1, false);
    if (ret < 0) {
        DB_WARNING_STATE(state, "encode key failed, ret:%d", ret);
        return ret;
    }
    *pk_str = pk_key.data();
    if (_on_dup_key_update) {
        // clear the record data beforehand in case of field conflict 
        // between record in db and the inserting record,
        record->clear();
        record->decode_key(*_pri_info, *pk_str);
    }
    bool check_region = !_table_info->is_binlog;
    //delete requires all fields (index and non-index fields)
    ret = _txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, check_region, ttl_ts);
    if (ret < 0) {
        return ret;
    }
    if (row != nullptr && _tuple_desc != nullptr
        && (_node_type == pb::DELETE_NODE || _node_type == pb::UPDATE_NODE || _node_type == pb::LOCK_PRIMARY_NODE)) {
        for (auto slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            row->set_value(slot.tuple_id(), slot.slot_id(),
                    record->get_value(field));
        }
    }
    return 0;
}

int DMLNode::remove_row(RuntimeState* state, SmartRecord record, 
        const std::string& pk_str, bool delete_primary) {
    int ret = 0;
    if (delete_primary) {
        ret = _txn->remove(_region_id, *_pri_info, record);
        if (ret != 0) {
            DB_WARNING_STATE(state, "remove fail, index:%ld ,ret:%d", _table_id, ret);
            return -1;
        }
    }
    auto& reverse_index_map = state->reverse_index_map();
    auto& vector_index_map = state->vector_index_map();
    for (auto& info_ptr : *_indexes_ptr) {
        IndexInfo& info = *info_ptr;
        int64_t index_id = info.id;
        // replace主表冲突才忽略索引
        // unique冲突不能忽略
        if (!delete_primary && _ignore_index_ids.count(index_id) == 1) {
            continue;
        }
        auto index_state = info.state;
        if (index_state == pb::IS_NONE) {
            DB_DEBUG("DDL_LOG skip index [%ld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }
        if (info.index_hint_status == pb::IHS_DISABLE
            && info.state == pb::IS_DELETE_LOCAL) {
            continue;
        }

        if (info.type == pb::I_PRIMARY) {
            continue;
        }
        if (reverse_index_map.count(info.id) == 1) {
            // inverted index only support single field
            if (info.id == -1 || info.fields.size() != 1) {
                DB_WARNING_STATE(state, "indexinfo get fail, index_id:%ld", info.id);
                return -1;
            }
            auto field = record->get_field_by_idx(info.fields[0].pb_idx);
            if (record->is_null(field)) {
                continue;
            }
            std::string word;
            ret = record->get_reverse_word(info, word);
            if (ret < 0) {
                DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", info.id);
                return ret;
            }
            ret = reverse_index_map[info.id]->delete_reverse(_txn,
                                                           word, pk_str, record);
            if (ret < 0) {
                return ret;
            }
            continue;
        } else if (vector_index_map.count(info.id) == 1) {
            // vector index only can support one or two field
            if (info.id == -1 || (info.fields.size() != 1 && info.fields.size() != 2)) {
                DB_WARNING_STATE(state, "indexinfo get fail, index_id:%ld", info.id);
                return -1;
            }
            int field_idx = 0;
            if (info.fields.size() > 1) {
                field_idx = 1;
            }
            auto field = record->get_field_by_idx(info.fields[field_idx].pb_idx);
            if (record->is_null(field)) {
                continue;
            }
            std::string word;
            ret = record->get_reverse_word(info, word);
            if (ret < 0) {
                DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", info.id);
                return ret;
            }
            ret = vector_index_map[info.id]->delete_vector(_txn,
                                                           word, pk_str, record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "vector_index fail delete, index_id: %ld", info.id);
                return ret;
            }
            continue;
        }
        ret = _txn->get_update_secondary(_region_id, *_pri_info, info, record, LOCK_ONLY, false);
        if (ret != 0 && ret != -2) {
            DB_WARNING_STATE(state, "lock fail, index:%ld, ret:%d", info.id, ret);
            return -1;
        }
        ret = _txn->remove(_region_id, info, record);
        if (ret != 0) {
            DB_WARNING_STATE(state, "remove index:%ld failed", info.id);
            return -1;
        }
    }
    --_num_increase_rows;
    return 1;
}

// return -1 :执行失败
// retrun 0 : 数据已经删除或者不存在
// retrun 1 : 数据真正删除
int DMLNode::delete_row(RuntimeState* state, SmartRecord record, MemRow* row) {
    int ret = 0;
    std::string pk_str;
    int64_t ttl_ts = 0;
    ret = get_lock_row(state, record, &pk_str, row, ttl_ts);
    if (ret == -3) {
        //DB_WARNING_STATE(state, "key not in this region:%ld", _region_id);
        return 0;
    }else if (ret == -2 || ret == -4) {
        // deleted or expired
        return 0;
    } else if (ret != 0) {
        DB_WARNING_STATE(state, "lock table:%ld failed", _table_id);
        return -1;
    }
    if (!satisfy_condition_again(state, row)) {
        DB_WARNING_STATE(state, "condition changed when delete record:%s", record->debug_string().c_str());
        // UndoGetForUpdate(pk_str)?
        return 0;
    }
    return remove_row(state, record, pk_str, true);
}

// todo : 全局索引update/delete流程不同，重新判断条件待完善
bool DMLNode::satisfy_condition_again(RuntimeState* state, MemRow* row) {
    if (!state->need_condition_again) {
        return true;
    }
    if (row == nullptr) {
        return true;
    }
    if (_node_type != pb::DELETE_NODE &&  _node_type != pb::UPDATE_NODE && _node_type != pb::LOCK_PRIMARY_NODE) {
        return true;
    }
    return check_satisfy_condition(row);
}

int DMLNode::update_row(RuntimeState* state, SmartRecord record, MemRow* row) {
    int ret = 0;
    std::string pk_str;
    int64_t ttl_ts = 0;
    ret = get_lock_row(state, record, &pk_str, row, ttl_ts);
    if (ret == -3) {
        //DB_WARNING_STATE(state, "key not in this region:%ld", _region_id);
        return 0;
    } else if (ret == -2 || ret == -4) {
        // row deleted or expired
        return 0;
    } else if (ret != 0) {
        DB_WARNING_STATE(state, "lock table:%ld failed", _table_id);
        return -1;
    }
    if (!satisfy_condition_again(state, row)) {
        DB_WARNING_STATE(state, "condition changed when update record:%s", record->debug_string().c_str());
        // UndoGetForUpdate(pk_str)? 同一个txn GetForUpdate与UndoGetForUpdate之间不要写pk_str
        return 0;
    }
    // _row_ttl_duration == -1 代表保持原ttl意思
    // TODO: 全局索引 keep ttl功能
    if (_row_ttl_duration == -1 && _ttl_timestamp_us > 0 && ttl_ts > 0) {
        _ttl_timestamp_us = ttl_ts;
        _txn->set_write_ttl_timestamp_us(_ttl_timestamp_us);
        DB_DEBUG("keep ttl_timestamp_us: %ld", _ttl_timestamp_us);
    }
    _indexes_ptr = &_affected_indexes;
    // 影响了主键需要删除旧的行
    ret = remove_row(state, record, pk_str, _update_affect_primary);
    if (ret < 0) {
        DB_WARNING_STATE(state, "remove_row fail");
        return -1;
    } else if (ret == 0) {
        // update null row
        return 0;
    }
    if (_local_index_binlog) {
        _return_old_records[_pri_info->id].emplace_back(record->clone(true));
    }
    // if the updating field has no change, the update can be skipped.
    if (_on_dup_key_update) {
        if (_tuple_desc != nullptr) {
            for (auto slot : _tuple_desc->slots()) {
                auto field = record->get_field_by_tag(slot.field_id());
                row->set_value(slot.tuple_id(), slot.slot_id(),
                        record->get_value(field));
                //DB_WARNING_STATE(state, "_on_dup_key_update: tuple:%d slot:%d %d", slot.tuple_id(), slot.slot_id(), record->get_value(field).get_numberic<int32_t>());
            }
        }
    }
    for (size_t i = 0; i < _update_exprs.size(); i++) {
        auto& slot = _update_slots[i];
        auto expr = _update_exprs[i];

        if (_update_fields.count(slot.field_id()) == 0) {
            DB_WARNING("vector out of range, region_id: %ld, field_id: %d", _region_id, slot.field_id());
            continue;
        }
        auto field = _update_fields[slot.field_id()];
        if (field == nullptr) {
            state->error_code = ER_BAD_FIELD_ERROR;
            state->error_msg << "Unknown column id " << slot.field_id() << " in 'field list'";
            DB_WARNING_STATE(state, "Unknown column, table_id: %d, field_id: %d", slot.table_id(), slot.field_id());
            return -1;
        }
        if (field->type == pb::FLOAT || field->type == pb::DOUBLE || field->type == pb::DATETIME) {
            auto& expr_value = expr->get_value(row).cast_to(slot.slot_type());
            expr_value.set_precision_len(field->float_precision_len);
            record->set_value(record->get_field_by_tag(slot.field_id()), expr_value);
        } else {
            record->set_value(record->get_field_by_tag(slot.field_id()),
                expr->get_value(row).cast_to(slot.slot_type()));
        }
        auto last_insert_id_expr = expr->get_last_insert_id();
        if (last_insert_id_expr != nullptr) {
            state->last_insert_id = last_insert_id_expr->get_value(row).get_numberic<int64_t>();
        }
    }
    ret = insert_row(state, record, true);
    if (ret < 0) {
        DB_WARNING_STATE(state, "insert_row fail");
        return -1;
    }
    return 1;
}

void DMLNode::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _update_exprs) {
        expr->find_place_holder(placeholders);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
