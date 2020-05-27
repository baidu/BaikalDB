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
    _affect_primary = lock_primary_node.affect_primary();
    if (lock_primary_node.affect_index_ids_size() > 0) {
        for (auto i = 0; i < lock_primary_node.affect_index_ids_size(); i++) {
            _affected_index_ids.push_back(lock_primary_node.affect_index_ids(i));
        }
    }
    return 0;
}

void LockPrimaryNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    if (region_id == 0) {
        return;
    }
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
    lock_primary_node->set_affect_primary(_affect_primary);
    for (auto id : _affected_index_ids) {
        lock_primary_node->add_affect_index_ids(id);
    }
}

int LockPrimaryNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    ret = init_schema_info(state);
    if (ret == -1) {
        DB_WARNING_STATE(state, "init schema failed fail:%d", ret);
        return ret;
    }
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
    AtomicManager<std::atomic<long>> ams[state->reverse_index_map().size()];
    int i = 0;
    for (auto& pair : state->reverse_index_map()) {
        pair.second->sync(ams[i]);
        i++;
    }
    SmartRecord record_template = _factory->new_record(_table_id);
    std::vector<SmartRecord> put_records;
    std::vector<SmartRecord> delete_records;
    for (auto& str_record : _pb_node.derive_node().lock_primary_node().put_records()) {
        SmartRecord record = record_template->clone(false);
        record->decode(str_record);
        put_records.push_back(record);
    }
    for (auto& str_record : _pb_node.derive_node().lock_primary_node().delete_records()) {
        SmartRecord record = record_template->clone(false);
        record->decode(str_record);
        delete_records.push_back(record);
    }
    int num_affected_rows = 0;
    //对主表加锁，同时对局部二级索引表加锁
    if (_lock_type == pb::LOCK_GET) {
        for (auto& record : put_records) {
            //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
            auto ret = lock_get_main_table(state, record);
            if (ret < 0) {
                return ret;
            }
            //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
        }
    }
    //只对主表加锁返回
    if (_lock_type == pb::LOCK_GET_ONLY_PRIMARY) {
        for (auto& record : put_records) {
            //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
            auto ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true);
            if (ret == -3) {
                continue;
            }
            //代表存在
            if (ret == 0) {
                //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                _return_records[_pri_info->id].push_back(record);
            } else {
                //replace onduplicatekey update的反查主表，主表必须存在
                DB_WARNING_STATE(state, "reverse main table must exist");
                return -1;
            }
            //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
        }
    }
    //对主表加锁操作，同时对局部二级索引加锁操作, 若是是get操作，删除的数据需要返回
    if (_lock_type == pb::LOCK_DML || _lock_type == pb::LOCK_GET_DML) {
        for (auto& record : delete_records) {
            //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
            ret = delete_row(state, record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "insert_row fail");
                return -1;
            }
            if (ret == 1 && _lock_type == pb::LOCK_GET_DML) {
                _return_records[_pri_info->id].push_back(record);
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
    }
    //不加锁，直接进行写入和删除
    if (_lock_type == pb::LOCK_NO) {
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
    if (ret == -1) {
        DB_WARNING("get lock fail txn_id: %lu", txn->txn_id());
        return -1;
    }
    //代表存在
    if (ret == 0) {
        //DB_WARNING_STATE(state,"record:%s", primary_record->debug_string().c_str());
        _return_records[_pri_info->id].push_back(primary_record);
    } 
    for (auto& index_id: _affected_index_ids) {
        //因为这边查到的值可能会被修改，后边锁二级索引还要用到输入的record, 所以要复制出来
        auto index_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (index_ptr == nullptr) {
            DB_WARNING("index info not found index_id:%ld", index_id);
            return -1;
        }
        if (index_ptr->id == _table_id) {
            continue;
        }
        SmartRecord get_record = record->clone(true);
        auto ret = txn->get_update_secondary(_region_id, *_pri_info, *index_ptr, get_record, GET_LOCK, true);
        if (ret == -3 || ret == -2) {
            continue;
        }
        if (ret == -1) {
            DB_WARNING("get lock fail txn_id: %lu", txn->txn_id());
            return -1;
        }
        _return_records[index_ptr->id].push_back(get_record);
        //DB_WARNING_STATE(state,"record:%s", get_record->debug_string().c_str());
    }
    return 0;
}
int LockPrimaryNode::put_row(RuntimeState* state, SmartRecord record) {
    int ret = 0;
    auto txn = state->txn();
    bool fit_region = true;
    if (txn->fits_region_range_for_primary(*_pri_info, record, fit_region) < 0) {
        DB_WARNING("fits_region_range_for_primary fail, region_id: %ld, table_id: %ld",
            _region_id, _table_id);
        return -1;
    }
    if (!fit_region) {
        //DB_WARNING("table_id: %ld not in region_id: %d, record: %s",
            //_table_id, _region_id, record->debug_string().c_str());
        return 0;
    }
    for (auto& index_id : _affected_index_ids) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING_STATE(state, "index info is null, index:%ld", index_id);
            return -1;
        }
        if (info_ptr->id == _table_id) {
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
