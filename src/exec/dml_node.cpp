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
        ret = expr->type_inferer();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        expr->const_pre_calc();
    }
    return 0;
}
int DMLNode::init_schema_info(RuntimeState* state) {
    _region_id = state->region_id();
    _table_info = &(state->resource()->table_info);
    _pri_info = &(state->resource()->pri_info);
    for (auto& field_info : _pri_info->fields) {
        _pri_field_ids.insert(field_info.id);
    }
    _affected_index_ids = _table_info->indices;

    //保存所有字段，主键不在pb里，不需要传入
    for (auto& field_info : _table_info->fields) {
        if (_pri_field_ids.count(field_info.id) == 0) {
            _field_ids.push_back(field_info.id);
        }
    }
    std::sort(_field_ids.begin(), _field_ids.end());
    return 0;
}

int DMLNode::insert_row(RuntimeState* state, SmartRecord record, bool is_update) {
    //DB_WARNING_STATE(state, "insert record: %s", record->debug_string().c_str());
    int ret = 0;
    int affected_rows = 0;
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING_STATE(state, "txn is null, region:%ld", _region_id);
        return -1;
    }
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
    MutTableKey pk_key;
    ret = record->encode_key(*_pri_info, pk_key, -1, false);
    if (ret < 0) {
        DB_WARNING_STATE(state, "encode key failed, ret:%d", ret);
        return ret;
    }
    std::string pk_str = pk_key.data();
    if (_affect_primary) {
        //no field need to decode here, only check key exist and get lock
        std::vector<int32_t> field_ids;
        SmartRecord old_record = record;
        if (_is_replace) {
            old_record = record->clone(true);
        }
        ret = txn->get_update_primary(_region_id, *_pri_info, old_record, field_ids, GET_LOCK, true);
        if (ret == -3) {
            //DB_WARNING_STATE(state, "key not in this region:%ld, %s", _region_id, record->to_string().c_str());
            return 0;
        }
        if (ret != -2) {
            if (_need_ignore) {
                return 0;
            }
            if (ret == 0 && !is_update) { 
                if (_on_dup_key_update) {
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
                    ret = remove_row(state, old_record, pk_str, false);
                    if (ret < 0) {
                        DB_WARNING_STATE(state, "remove fail, table_id:%ld ,ret:%d", _table_id, ret);
                        return -1;
                    }
                    ++affected_rows;
                } else {
                    DB_WARNING_STATE(state, "insert row must not exist, index:%ld, ret:%d", _table_id, ret);
                    state->error_code = ER_DUP_KEY;
                    state->error_msg << "Cannot write duplicate key: " 
                                     << old_record->debug_string();
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
        IndexInfo& info = state->resource()->get_index_info(index_id);
        if (info.id == _table_id) {
            continue;
        }
        SmartRecord old_record = record;
        if (_is_replace ) {
            old_record = record->clone(true);
        }
        ret = txn->get_update_secondary(_region_id, *_pri_info, info, old_record, GET_LOCK, true);
        if (ret == 0 && !is_update) {
            if (_on_dup_key_update) {
                ret = update_row(state, old_record, _dup_update_row.get());
                if (ret == 1) {
                    ++ret;
                }
                return ret;
            } if (_is_replace) {
                ret = delete_row(state, old_record);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "remove fail, index:%ld ,ret:%d", info.id, ret);
                    return -1;
                }
                ++affected_rows;
                continue;
            }
        }
        // ret == -3 means the primary_key returned by get_update_secondary is out of the region
        // (dirty data), this does not affect the insertion
        if (ret != -2 && ret != -3) {
            if (_need_ignore) {
                return 0;
            }
            DB_WARNING_STATE(state, "insert index row must not exist, index:%ld, ret:%d", info.id, ret);
            return -1;
        }
    }
    for (auto& index_id : _affected_index_ids) {
        IndexInfo& info = state->resource()->get_index_info(index_id);
        if (info.id == _table_id) {
            continue;
        }
        if (reverse_index_map.count(info.id) == 1) {
            //IndexInfo info = _factory->get_index_info(index_id);
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
                DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", info.id);
                return ret;
            }
            //DB_NOTICE("word:%s", str_to_hex(word).c_str());
            ret = reverse_index_map[info.id]->insert_reverse(txn->get_txn(), word, pk_str, record);
            if (ret < 0) {
                return ret;
            }
            continue;
        }
        ret = txn->put_secondary(_region_id, info, record);
        if (ret < 0) {
            DB_WARNING_STATE(state, "put index:%ld fail:%d, table_id:%ld", info.id, ret, _table_id);
            return ret;
        }
    }
    ret = txn->put_primary(_region_id, *_pri_info, record);
    if (ret < 0) {
        DB_WARNING_STATE(state, "put table:%ld fail:%d", _table_id, ret);
        return -1;
    }
    //DB_WARNING_STATE(state, "insert succes:%ld, %s", _region_id, record->to_string().c_str());
    ++_num_increase_rows;
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
    auto txn = state->txn();
    //delete requires all fields (index and non-index fields)
    return txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true);
}

int DMLNode::remove_row(RuntimeState* state, SmartRecord record, 
        const std::string& pk_str, bool delete_primary) {
    int ret = 0;
    auto txn = state->txn();
    if (_affect_primary && delete_primary) {
        ret = txn->remove(_region_id, *_pri_info, record);
        if (ret != 0) {
            DB_WARNING_STATE(state, "remove fail, index:%ld ,ret:%d", _table_id, ret);
            return -1;
        }
    }
    auto& reverse_index_map = state->reverse_index_map();
    for (auto& index_id : _affected_index_ids) {
        IndexInfo& info = state->resource()->get_index_info(index_id);
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
            ret = reverse_index_map[info.id]->delete_reverse(txn->get_txn(), word, pk_str, record);
            if (ret < 0) {
                return ret;
            }
            continue;
        }
        ret = txn->get_update_secondary(_region_id, *_pri_info, info, record, LOCK_ONLY, false);
        if (ret != 0 && ret != -2) {
            DB_WARNING_STATE(state, "lock fail, index:%ld, ret:%d", info.id, ret);
            return -1;
        }
        ret = txn->remove(_region_id, info, record);
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
    }else if (ret == -2) {
        // deleted
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
    }else if (ret == -2) {
        // row deleted
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
    //SmartRecord old_record = record->clone(true);
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
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
