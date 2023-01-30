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
#include "lock_primary_node.h"

namespace baikaldb {

DEFINE_bool(check_condition_again_for_global_index, false, "avoid write skew for global index if true");

int LockPrimaryNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    const pb::LockPrimaryNode& lock_primary_node = node.derive_node().lock_primary_node();
    _table_id = lock_primary_node.table_id();
    _lock_type = lock_primary_node.lock_type();
    _global_index_id = lock_primary_node.table_id();
    _update_affect_primary = lock_primary_node.affect_primary();
    _row_ttl_duration = lock_primary_node.row_ttl_duration_s();
    if (lock_primary_node.affect_index_ids_size() > 0) {
        for (auto i = 0; i < lock_primary_node.affect_index_ids_size(); i++) {
            _affected_index_ids.emplace_back(lock_primary_node.affect_index_ids(i));
        }
    }

    if (lock_primary_node.conjuncts_size() > 0) {
        _conjuncts.clear();
        _conjuncts_need_destory = true;
        for (auto i = 0; i < lock_primary_node.conjuncts_size(); i++) {
            ExprNode* conjunct = nullptr;
            int ret = ExprNode::create_tree(lock_primary_node.conjuncts(i), &conjunct);
            if (ret < 0) {
                //如何释放资源
                return ret;
            }
            _conjuncts.emplace_back(conjunct);
        }
    }
    return 0;
}

void LockPrimaryNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto lock_primary_node = pb_node->mutable_derive_node()->mutable_lock_primary_node();
    lock_primary_node->set_table_id(_table_id);
    lock_primary_node->set_lock_type(_lock_type);
    lock_primary_node->clear_put_records();
    if (_insert_records_by_region.count(region_id) != 0) {
        for (auto& record : _insert_records_by_region[region_id]) {
            std::string* str = lock_primary_node->add_put_records();
            record->encode(*str);
        }
    }
    if (_delete_records_by_region.count(region_id) != 0) {
        lock_primary_node->clear_delete_records();
        for (auto& record : _delete_records_by_region[region_id]) {
            std::string* str = lock_primary_node->add_delete_records();
            record->encode(*str);
        }
    }
    lock_primary_node->set_affect_primary(_update_affect_primary);
    lock_primary_node->set_row_ttl_duration_s(_row_ttl_duration);
    for (auto id : _affected_index_ids) {
        lock_primary_node->add_affect_index_ids(id);
    }
    for (auto expr : _conjuncts) {
        ExprNode::create_pb_expr(lock_primary_node->add_conjuncts(), expr);
    }
}

void LockPrimaryNode::reset(RuntimeState* state) {
    _insert_records_by_region.clear();
    _delete_records_by_region.clear();
    _conjuncts.clear();
}

int LockPrimaryNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    for (auto conjunct : _conjuncts) {
         ret = conjunct->open();
         if (ret < 0) {
             DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
             return ret;
         }
    }
    state->tuple_id = state->tuple_descs()[0].tuple_id();
    ret = init_schema_info(state);
    if (ret == -1) {
        DB_WARNING_STATE(state, "init schema failed fail:%d", ret);
        return ret;
    }
    _affected_indexes.clear();
    for (auto index_id : _affected_index_ids) {
        auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        // 新加索引还未同步到store
        if (index_info == nullptr) {
            DB_WARNING("get index info failed index_id: %ld", index_id);
            continue;
        }
        if (index_info->index_hint_status == pb::IHS_DISABLE
            && index_info->state == pb::IS_DELETE_LOCAL) {
            continue;
        }
        if (!index_info->is_global) {
            _affected_indexes.emplace_back(index_info);
        }
    }
    _indexes_ptr = &_affected_indexes;
    //对于update 和 insert on duplicate key fields_ids需要全部返回
    //对于replace 和delete是用户如果指定了binlog要全部返回
    //对于ignore 不需要返回
    //目前为了简单，先全部返回。如果后续有性能问题（比如有大宽表）再考虑优化
    _field_ids.clear();
    //保存所有字段，主键不在pb里，不需要传入
    for (auto& field_info : _table_info->fields) {
        if (_pri_field_ids.count(field_info.id) == 0) {
            _field_ids[field_info.id] = &field_info;
        }
    }
    // cstore下只更新涉及列，暂时全部涉及，原因同_field_ids
    if (_table_info->engine == pb::ROCKSDB_CSTORE) {
        for (auto& field_info : _table_info->fields) {
            if (_pri_field_ids.count(field_info.id) == 0 &&
                    _update_field_ids.count(field_info.id) == 0) {
                _update_field_ids.insert(field_info.id);
            }
        }
    }
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr: region:%ld", _region_id);
        return -1;
    }
    txn->set_write_ttl_timestamp_us(_ttl_timestamp_us);
    SmartRecord record_template = _factory->new_record(_table_id);
    std::vector<SmartRecord> put_records;
    std::vector<SmartRecord> delete_records;
    for (auto& str_record : _pb_node.derive_node().lock_primary_node().put_records()) {
        SmartRecord record = record_template->clone(false);
        record->decode(str_record);
        bool fit_region = true;
        if (txn->fits_region_range_for_primary(*_pri_info, record, fit_region) < 0) {
            DB_WARNING("fits_region_range_for_primary fail, region_id: %ld, table_id: %ld",
                    _region_id, _table_id);
            return -1;
        }
        if (fit_region) {
            put_records.emplace_back(record);
        }
    }
    for (auto& str_record : _pb_node.derive_node().lock_primary_node().delete_records()) {
        SmartRecord record = record_template->clone(false);
        record->decode(str_record);
        bool fit_region = true;
        if (txn->fits_region_range_for_primary(*_pri_info, record, fit_region) < 0) {
            DB_WARNING("fits_region_range_for_primary fail, region_id: %ld, table_id: %ld",
                    _region_id, _table_id);
            return -1;
        }
        if (fit_region) {
            delete_records.emplace_back(record);
        }
    }
    int num_affected_rows = 0;
    switch (_lock_type) {
        //对主表加锁，同时对局部二级索引表加锁
        case pb::LOCK_GET: {
            txn->set_separate(false); // 只加锁不走kv模式
            for (auto& record : put_records) {
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                auto ret = lock_get_main_table(state, record);
                if (ret < 0) {
                    return ret;
                }
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
            }
            break;
        }
        //只对主表加锁返回
        case pb::LOCK_GET_ONLY_PRIMARY: {
            txn->set_separate(false); // 只加锁不走kv模式
            for (auto& record : put_records) {
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                auto ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true);
                if (ret == -3) {
                    continue;
                }
                //代表存在
                if (ret == 0) {
                    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                    _return_records[_pri_info->id].emplace_back(record);
                } else {
                    //replace onduplicatekey update的反查主表，主表必须存在
                    DB_WARNING_STATE(state, "reverse main table must exist");
                    return -1;
                }
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
            }
            break;
        }
        //对主表加锁操作，同时对局部二级索引加锁操作, 若是是get操作，删除的数据需要返回
        case pb::LOCK_DML:
        case pb::LOCK_GET_DML: {
            for (auto& record : delete_records) {
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                ret = delete_row(state, record, nullptr);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "delete_row fail");
                    return -1;
                }
                if (ret == 1 && _lock_type == pb::LOCK_GET_DML) {
                    _return_records[_pri_info->id].emplace_back(record);
                }
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                num_affected_rows += ret;
            }
            for (auto& record : put_records) {
                //加锁写入
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                ret = insert_row(state, record);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "insert_row fail");
                    return -1;
                }
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                num_affected_rows += ret;
            }
            break;
        }
        //不加锁，直接进行写入和删除
        case pb::LOCK_NO: {
            for (auto& record : put_records) {
                //加锁写入
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                ret = put_row(state, record);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "insert_row fail");
                    return -1;
                }
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                num_affected_rows += ret;
            }
            break;
        }
        default:
            DB_WARNING("error _lock_type:%s", LockCmdType_Name(_lock_type).c_str());
            break;
    }
    state->set_num_increase_rows(_num_increase_rows);
    if (state->need_txn_limit) {
        int row_count = put_records.size() + delete_records.size();
        bool is_limit = TxnLimitMap::get_instance()->check_txn_limit(state->txn_id, row_count);
        if (is_limit) {
            DB_FATAL("Transaction too big, region_id:%ld, txn_id:%ld, txn_size:%d", 
                state->region_id(), state->txn_id, row_count);
            return -1;
        }
    }
    return num_affected_rows;
}

//对主表以及局部二级索引加锁返回
int LockPrimaryNode::lock_get_main_table(RuntimeState* state, SmartRecord record) {
    auto txn = state->txn();
    SmartRecord primary_record = record->clone(true);
    auto ret = txn->get_update_primary(_region_id, *_pri_info, primary_record, _field_ids, GET_LOCK, true);
    if (ret == -1 || ret == -5) {
        DB_WARNING("get lock fail txn_id: %lu", txn->txn_id());
        return -1;
    }
    //代表存在
    if (ret == 0) {
        //DB_WARNING_STATE(state,"record:%s", primary_record->debug_string().c_str());
        _return_records[_pri_info->id].emplace_back(primary_record);
    } 
    for (auto& index_ptr: _affected_indexes) {
        if (index_ptr->id == _table_id) {
            continue;
        }
        //因为这边查到的值可能会被修改，后边锁二级索引还要用到输入的record, 所以要复制出来
        SmartRecord get_record = record->clone(true);
        auto ret = txn->get_update_secondary(_region_id, *_pri_info, *index_ptr, get_record, GET_LOCK, true);
        if (ret == -3 || ret == -2 || ret == -4) {
            continue;
        }
        if (ret == -1 || ret == -5) {
            DB_WARNING("get lock fail txn_id: %lu", txn->txn_id());
            return -1;
        }
        _return_records[index_ptr->id].emplace_back(get_record);
        //DB_WARNING_STATE(state,"record:%s", get_record->debug_string().c_str());
    }
    return 0;
}
int LockPrimaryNode::put_row(RuntimeState* state, SmartRecord record) {
    int ret = 0;
    auto txn = state->txn();
    auto& reverse_index_map = state->reverse_index_map();
    for (auto& info_ptr : _affected_indexes) {
        if (info_ptr->id == _table_id) {
            continue;
        }
        if (reverse_index_map.count(info_ptr->id) == 1) {
            // inverted index only support single field
            if (info_ptr->id == -1 || info_ptr->fields.size() != 1) {
                return -1;
            }
            auto field = record->get_field_by_idx(info_ptr->fields[0].pb_idx);
            if (record->is_null(field)) {
                continue;
            }
            std::string word;
            ret = record->get_reverse_word(*info_ptr, word);
            if (ret < 0) {
                DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", 
                                 info_ptr->id);
                return ret;
            }
            MutTableKey pk_key;
            ret = record->encode_key(*_pri_info, pk_key, -1, false);
            if (ret < 0) {
                DB_WARNING_STATE(state, "encode key failed, ret:%d", ret);
                return ret;
            }
            std::string pk_str = pk_key.data();
            ret = reverse_index_map[info_ptr->id]->insert_reverse(txn,
                                                            word, pk_str, record);
            if (ret < 0) {
                return ret;
            }
            continue;
        }
        ret = txn->put_secondary(_region_id, *info_ptr, record);
        if (ret < 0) {
            DB_WARNING_STATE(state, "put index:%ld fail:%d, table_id:%ld", info_ptr->id, ret, _table_id);
            return ret;
        }
    }
    ret = txn->put_primary(_region_id, *_pri_info, record);
    if (ret < 0) {
        DB_WARNING_STATE(state, "put table:%ld fail:%d", _table_id, ret);
        return -1;
    }
    ++_num_increase_rows;
    return 1;
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
