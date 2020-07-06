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

DEFINE_bool(disable_writebatch_index, false,
    "disable the indexing of transaction writebatch, if true the uncommitted data cannot be read");

int DMLNode::expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs) {
    int ret = 0;
    ret = ExecNode::expr_optimize(tuple_descs);
    if (ret < 0) {
        DB_WARNING("expr type_inferer fail:%d", ret);
        return ret;
    }
    for (auto expr : _update_exprs) {
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
    }
    return 0;
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

    int64_t ttl_duration = _row_ttl_duration > 0 ? _row_ttl_duration : _table_info->ttl_duration;
    if (ttl_duration > 0) {
        _ttl_timestamp_us = butil::gettimeofday_us() + ttl_duration * 1000 * 1000;
    }

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
    if (_affected_index_ids.size() == 0) {
        for (auto index_id : _table_info->indices) {
            auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
            if (index_info == nullptr) {
                DB_WARNING("get index info failed index_id: %ld", index_id);
                return -1;
            }
            if (!index_info->is_global) {
                _affected_index_ids.push_back(index_id);
            }
        }
    }
    // update and on_dup_key_update need all fields
    // delete and insert/replace need get index fields
    if (_node_type == pb::UPDATE_NODE || _on_dup_key_update) {
        //保存所有字段，主键不在pb里，不需要传入
        for (auto& field_info : _table_info->fields) {
            if (_pri_field_ids.count(field_info.id) == 0) {
                _field_ids[field_info.id] = &field_info;
            }
        }
    } else {
        for (auto index_id : _table_info->indices) {
            auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
            if (index_info == nullptr) {
                DB_WARNING("get index info failed index_id: %ld", index_id);
                return -1;
            }
            for (auto& field_info : index_info->fields) {
                if (_pri_field_ids.count(field_info.id) == 0) {
                    _field_ids[field_info.id] = &field_info;
                }
            }
        }
    }
    _txn = state->txn();;
    if (_txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr: region:%ld", _region_id);
        return -1;
    }
    return 0;
}

int DMLNode::insert_row(RuntimeState* state, SmartRecord record, bool is_update) {
    //DB_WARNING_STATE(state, "insert record: %s", record->debug_string().c_str());
    int ret = 0;
    int affected_rows = 0;
    // LOCK_PRIMARY_NODE目前无法区分update与insert，暂用update兼容
    bool delete_before_put_primary = !_affect_primary &&
            (is_update || _node_type == pb::LOCK_PRIMARY_NODE);
    bool need_increase = true;
    auto& reverse_index_map = state->reverse_index_map();
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
    if (_affect_primary) {
        //no field need to decode here, only check key exist and get lock
        //std::vector<int32_t> field_ids;
        SmartRecord old_record = record;
        if (_is_replace) {
            old_record = record->clone(true);
        }
        ret = _txn->get_update_primary(_region_id, *_pri_info, old_record, _field_ids, GET_LOCK, true);
        if (ret == -3) {
            //DB_WARNING_STATE(state, "key not in this region:%ld, %s", _region_id, record->to_string().c_str());
            return 0;
        }
        if (ret == -4) {
            //过期的数据被覆盖，但是num_table_lines已经计算过旧数据了
            need_increase = false;
        }
        if (ret != -2 && ret != -4) {
            if (_need_ignore) {
                return 0;
            }
            if (ret == 0) { 
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
                    /*
                    std::string old_s;
                    std::string new_s;
                    old_record->encode(old_s);
                    record->encode(new_s);
                    if (old_s == new_s) {
                        DB_WARNING_STATE(state, "table_id:%ld, region_id: %ld, old new is same, need not replace", 
                                _table_id, state->region_id());
                        return 0;
                    }*/
                    // 对于主键replace，可以不删除旧数据，直接用新数据覆盖
                    ret = remove_row(state, old_record, pk_str, false);
                    if (ret < 0) {
                        DB_WARNING_STATE(state, "remove fail, table_id:%ld ,ret:%d", _table_id, ret);
                        return -1;
                    }
                    delete_before_put_primary = true;
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
                return -1;
            }
        }
    }

    // lock secondary keys
    for (auto& index_id: _affected_index_ids) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("index info not found index_id:%ld", index_id);
            return -1;
        }
        IndexInfo& info = *info_ptr;

        auto index_state = info.state;
        //DB_DEBUG("dml_insert_record prime+index string[%s] state[%s] index_id[%lld] index_name[%s] region_%lld", 
        //    record->to_string().c_str(), pb::IndexState_Name(index_state).c_str(), info.id, info.name.c_str(), _region_id);

        if (index_state != pb::IS_PUBLIC && index_state != pb::IS_WRITE_ONLY &&
            index_state != pb::IS_WRITE_LOCAL) {
            DB_DEBUG("index_selector skip index [%lld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }

        if (info.id == _table_id) {
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
                ret = delete_row(state, old_record);
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
            if (_need_ignore) {
                return 0;
            }
            DB_WARNING_STATE(state, "insert rocksdb failed, index:%ld, ret:%d", info.id, ret);
            return -1;
        }
    }
    for (auto& index_id : _affected_index_ids) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("index info not found index_id:%ld", index_id);
            return -1;
        }
        IndexInfo& info = *info_ptr;
        if (info.id == _table_id) {
            continue;
        }
        auto index_state = info.state;
        if (index_state != pb::IS_PUBLIC && index_state != pb::IS_WRITE_ONLY &&
            index_state != pb::IS_WRITE_LOCAL) {
            DB_DEBUG("DDL_LOG index_selector skip index [%lld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }
        if (reverse_index_map.count(info.id) == 1) {
            // inverted index only support single field
            if (info.id == -1 || info.fields.size() != 1) {
                return -1;
            }
            auto field = record->get_field_by_tag(info.fields[0].id);
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
            pb::StoreReq* req = nullptr;
            if (_txn->_is_separate) {
                req = _txn->get_raftreq();
            }
            ret = reverse_index_map[info.id]->insert_reverse(_txn->get_txn(), req, 
                                                            word, pk_str, record);
            if (ret < 0) {
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
    // 列存为节省空间, 插入默认值或空值时不会put
    // delete_before_put_primary为true时表示更新前旧值尚未被删除
    ret = _txn->put_primary(_region_id, *_pri_info, record,
                            delete_before_put_primary ? &_update_field_ids : nullptr);
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

int DMLNode::get_lock_row(RuntimeState* state, SmartRecord record, std::string* pk_str) {
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
    //delete requires all fields (index and non-index fields)
    return _txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true);
}

int DMLNode::remove_row(RuntimeState* state, SmartRecord record, 
        const std::string& pk_str, bool delete_primary) {
    int ret = 0;
    if (_affect_primary && delete_primary) {
        ret = _txn->remove(_region_id, *_pri_info, record);
        if (ret != 0) {
            DB_WARNING_STATE(state, "remove fail, index:%ld ,ret:%d", _table_id, ret);
            return -1;
        }
    }
    auto& reverse_index_map = state->reverse_index_map();
    for (auto& index_id : _affected_index_ids) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("index info not found index_id:%ld", index_id);
            return -1;
        }
        IndexInfo& info = *info_ptr;
        auto index_state = info.state;
        if (index_state == pb::IS_NONE) {
            DB_DEBUG("DDL_LOG index_selector skip index [%lld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }

        if (info.id == _table_id) {
            continue;
        }
        if (reverse_index_map.count(info.id) == 1) {
            // inverted index only support single field
            if (info.id == -1 || info.fields.size() != 1) {
                DB_WARNING_STATE(state, "indexinfo get fail, index_id:%ld", info.id);
                return -1;
            }
            auto field = record->get_field_by_tag(info.fields[0].id);
            if (record->is_null(field)) {
                continue;
            }
            std::string word;
            ret = record->get_reverse_word(info, word);
            if (ret < 0) {
                DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", info.id);
                return ret;
            }
            pb::StoreReq* req = nullptr;
            if (_txn->_is_separate) {
                req = _txn->get_raftreq();
            }
            ret = reverse_index_map[info.id]->delete_reverse(_txn->get_txn(), req, 
                                                           word, pk_str, record);
            if (ret < 0) {
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

int DMLNode::delete_row(RuntimeState* state, SmartRecord record) {
    int ret = 0;
    std::string pk_str;
    ret = get_lock_row(state, record, &pk_str);
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
    return remove_row(state, record, pk_str);
}

int DMLNode::update_row(RuntimeState* state, SmartRecord record, MemRow* row) {
    int ret = 0;
    std::string pk_str;
    ret = get_lock_row(state, record, &pk_str);
    if (ret == -3) {
        //DB_WARNING_STATE(state, "key not in this region:%ld", _region_id);
        return 0;
    }else if (ret == -2 || ret == -4) {
        // row deleted or expired
        return 0;
    } else if (ret != 0) {
        DB_WARNING_STATE(state, "lock table:%ld failed", _table_id);
        return -1;
    }
    ret = remove_row(state, record, pk_str);
    if (ret < 0) {
        DB_WARNING_STATE(state, "remove_row fail");
        return -1;
    } else if (ret == 0) {
        // update null row
        return 0;
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
        record->set_value(record->get_field_by_tag(slot.field_id()),
                expr->get_value(row).cast_to(slot.slot_type()));
    }
    /*
    std::string old_s;
    std::string new_s;
    old_record->encode(old_s);
    record->encode(new_s);
    if (old_s == new_s) {
        DB_WARNING_STATE(state, "table_id:%ld, region_id: %ld, old new is same, need not update", 
                _table_id, state->region_id());
        return 0;
    }*/
    ret = insert_row(state, record, true);
    if (ret < 0) {
        DB_WARNING_STATE(state, "insert_row fail");
        return -1;
    }
    return 1;
}

void DMLNode::find_place_holder(std::map<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _update_exprs) {
        expr->find_place_holder(placeholders);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
