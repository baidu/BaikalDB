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
#include "lock_secondary_node.h"

namespace baikaldb {

int LockSecondaryNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    const pb::LockSecondaryNode& lock_secondary_node = node.derive_node().lock_secondary_node();
    _table_id = lock_secondary_node.table_id();
    _lock_type = lock_secondary_node.lock_type();
    _global_index_id = lock_secondary_node.global_index_id();
    _lock_secondary_type = lock_secondary_node.lock_secondary_type();
    _row_ttl_duration = lock_secondary_node.row_ttl_duration_s();

    DB_DEBUG("table_id: %ld, global_index_id: %ld, row_ttl_duration: %ld", 
        _table_id, _global_index_id, _row_ttl_duration);
    return 0;
}

void LockSecondaryNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto lock_secondary_node = pb_node->mutable_derive_node()->mutable_lock_secondary_node();
    lock_secondary_node->set_global_index_id(_global_index_id);
    lock_secondary_node->set_table_id(_table_id);
    lock_secondary_node->set_lock_type(_lock_type);
    lock_secondary_node->set_lock_secondary_type(_lock_secondary_type);
    lock_secondary_node->clear_put_records();
    lock_secondary_node->set_row_ttl_duration_s(_row_ttl_duration);
    bool global_ddl_with_ttl = false;
    if (!_record_ttl_map.empty()) {
        global_ddl_with_ttl = true;
    } 
    if (_insert_records_by_region.count(region_id) != 0) {
        for (auto& record : _insert_records_by_region[region_id]) {
            std::string* str = lock_secondary_node->add_put_records();
            record->encode(*str);
            if (global_ddl_with_ttl) {
                auto iter = _record_ttl_map.find(*str);
                if (iter != _record_ttl_map.end()) {
                    DB_DEBUG("record: %s, timestamp: %ld", str->c_str(), iter->second);
                    lock_secondary_node->add_global_ddl_ttl_timestamp_us(iter->second);
                } else {
                    DB_FATAL("recod: %s can't find in ttl map", str->c_str());
                }
            }
        }
    }
    lock_secondary_node->clear_delete_records();
    if (_delete_records_by_region.count(region_id) != 0) {
        for (auto& record : _delete_records_by_region[region_id]) {
            std::string* str = lock_secondary_node->add_delete_records();
            record->encode(*str);
        }
    }
}

void LockSecondaryNode::reset(RuntimeState* state) {
    _insert_records_by_region.clear();
    _delete_records_by_region.clear();
}

int LockSecondaryNode::open(RuntimeState* state) {
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
    _global_index_info = _factory->get_index_info_ptr(_global_index_id);
    //索引还未同步到store，返回成功。
    if (_global_index_info == nullptr) {
        DB_WARNING("get index info fail, index_id: %ld", _global_index_id);
        return 0;
    }
    bool can_write = _global_index_info->state != pb::IS_NONE;
    bool can_delete = _global_index_info->state != pb::IS_NONE;

    if (_lock_type == pb::LOCK_NO_GLOBAL_DDL || _lock_type == pb::LOCK_GLOBAL_DDL) {
        // 和baikaldb 索引状态同步，store状态可能会比db状态迟，不同步会丢数据
        // 分裂发送日志时，索引状态已经是 IS_PUBLIC，在该状态下，必须可以写入。
        if (!can_write) {
            DB_WARNING_STATE(state, "state is not write_local or write_only or public");
            return -1;
        }
    }
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr: region:%ld", _region_id);
        return -1;
    }
    txn->set_write_ttl_timestamp_us(_ttl_timestamp_us);
    SmartRecord record_template = _factory->new_record(_table_id);
    int num_affected_rows = 0;
    std::vector<SmartRecord> put_records;
    std::vector<int64_t> put_record_ttl;
    put_record_ttl.reserve(100);
    std::vector<SmartRecord> delete_records;
    int ttl_idx = 0;
    for (auto& str_record : _pb_node.derive_node().lock_secondary_node().put_records()) {
        const auto& ttl_vec = _pb_node.derive_node().lock_secondary_node().global_ddl_ttl_timestamp_us();
        int64_t timestamp = ttl_vec.size() > ttl_idx ? ttl_vec[ttl_idx] : _ttl_timestamp_us;
        ttl_idx++;
        SmartRecord record = record_template->clone(false);
        record->decode(str_record);
        bool fit_region = true;
        if (txn->fits_region_range_for_global_index(*_pri_info, *_global_index_info, record, fit_region) < 0) {
            DB_WARNING("fits_region_range_for_global_index fail, region_id: %ld, index_id: %ld",
                _region_id, _global_index_id);
            return -1;
        }
        if (fit_region) {
            put_records.emplace_back(record);
            put_record_ttl.emplace_back(timestamp);
        }
    }

    for (auto& str_record : _pb_node.derive_node().lock_secondary_node().delete_records()) {
        SmartRecord record = record_template->clone(false);
        record->decode(str_record);
        bool fit_region = true;
        if (txn->fits_region_range_for_global_index(*_pri_info, *_global_index_info, record, fit_region) < 0) {
            DB_WARNING("fits_region_range_for_global_index fail, region_id: %ld, index_id: %ld",
                _region_id, _global_index_id);
            return -1;
        }
        if (fit_region) {
            delete_records.push_back(record);
        } else {
            //DB_WARNING("index_id: %ld not in region_id: %d, record: %s",
            //    _global_index_id, _region_id, record->debug_string().c_str());
        }
    }
    switch (_lock_type) {
        //对全局二级索引加锁返回
        case pb::LOCK_GET: {
            txn->set_separate(false); // 只加锁不走kv模式
            if (can_write) {
                int ttl_idx = 0;
                for (auto& record : put_records) {
                    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                    txn->set_write_ttl_timestamp_us(put_record_ttl[ttl_idx++]);
                    auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, record, GET_LOCK, true);
                    if (ret == -3 || ret == -2 || ret == -4) {
                        continue;
                    }
                    if (ret == -1 || ret == -5) {
                        DB_WARNING("get lock fail");
                        return -1;
                    }
                    _return_records[_global_index_info->id].push_back(record);
                    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
                }
            }
            break;
        }
        //对全局二级索引进行加锁写入 or  删除
        case pb::LOCK_DML:
        case pb::LOCK_GLOBAL_DDL: {
            if (can_delete) {
                for (auto& record : delete_records) {
                    ret = delete_global_index(state, record);
                    if (ret < 0) {
                        DB_WARNING_STATE(state, "delete_global_index fail");
                        return -1;
                    }
                    num_affected_rows += ret;
                }
            }
            if (can_write) {
                int ttl_idx = 0;
                for (auto& record : put_records) {
                    //加锁写入
                    txn->set_write_ttl_timestamp_us(put_record_ttl[ttl_idx++]);
                    ret = insert_global_index(state, record);
                    if (ret < 0) {
                        DB_WARNING_STATE(state, "insert_global_index fail");
                        return -1;
                    }
                    num_affected_rows += ret;
                }
            }
            break;
        }
        case pb::LOCK_NO:
        case pb::LOCK_NO_GLOBAL_DDL: {
            if (can_write) {
                int ttl_idx = 0;
                for (auto& record : put_records) {
                    txn->set_write_ttl_timestamp_us(put_record_ttl[ttl_idx++]);
                    ret = put_global_index(state, record);
                    if (ret < 0) {
                        DB_WARNING_STATE(state, "put_row fail");
                        return -1;
                    }
                    num_affected_rows += ret;
                }
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
int LockSecondaryNode::insert_global_index(RuntimeState* state, SmartRecord record) {
    auto txn = state->txn();
    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
    //DB_WARNING("record:%s", record->debug_string().c_str());

    SmartRecord exist_record = record->clone();
    auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, exist_record, GET_LOCK, true);
    if (ret == 0) {
        if (_lock_type == pb::LOCK_GLOBAL_DDL) {
            MutTableKey key;
            MutTableKey exist_key;
            if (record->encode_key(*_pri_info, key, -1, false, false) == 0 && 
                exist_record->encode_key(*_pri_info, exist_key, -1, false, false) == 0) {

                if (key.data().compare(exist_key.data()) == 0) {
                    DB_NOTICE("same pk val.");
                    ++_num_increase_rows;
                    return 1;
                } else {
                    DB_WARNING("not same pk value record %s exist_record %s.", record->to_string().c_str(), 
                        exist_record->to_string().c_str());
                    state->error_code = ER_DUP_ENTRY;
                    state->error_msg << "Duplicate entry: '" << 
                            record->get_index_value(*_global_index_info) << "' for key '" << _global_index_info->short_name << "'";
                    return -1;
                }
            } else {
                DB_FATAL("encode key error record %s exist_record %s.", record->to_string().c_str(), 
                    exist_record->to_string().c_str());
                state->error_code = ER_DUP_ENTRY;
                state->error_msg << "Duplicate entry: '" << 
                        record->get_index_value(*_global_index_info) << "' for key '" << _global_index_info->short_name << "'";
                return -1;
            }
        } else {
            DB_WARNING_STATE(state, "insert uniq key must not exist, "
                            "index:%ld, ret:%d", _global_index_info->id, ret);
                state->error_code = ER_DUP_ENTRY;
                state->error_msg << "Duplicate entry: '" << 
                        record->get_index_value(*_global_index_info) << "' for key '" << _global_index_info->short_name << "'";
            return -1;
        }
    }
    // ret == -3 means the primary_key returned by get_update_secondary is out of the region
    // (dirty data), this does not affect the insertion
    if (ret != -2 && ret != -3 && ret != -4) {
        DB_WARNING_STATE(state, "insert rocksdb failed, index:%ld, ret:%d", _global_index_info->id, ret);
        return -1;
    }
    ret = txn->put_secondary(_region_id, *_global_index_info, record);
    if (ret < 0) {
        DB_WARNING_STATE(state, "put index:%ld fail:%d, table_id:%ld", _global_index_info->id, ret, _table_id);
        return ret;
    }
    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
    ++_num_increase_rows;
    return 1;
}
int LockSecondaryNode::delete_global_index(RuntimeState* state, SmartRecord record) {
    auto txn = state->txn();
    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
    auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, record, GET_LOCK, true);
    if (ret == -1 || ret == -5) {
        DB_WARNING_STATE(state, "insert rocksdb failed, index:%ld, ret:%d", _global_index_info->id, ret);
        return -1;
    }
    if (ret == -3) {
        return 0;
    }
    ret = txn->remove(_region_id, *_global_index_info, record);
    if (ret != 0) {
        DB_WARNING_STATE(state, "remove index:%ld failed", _global_index_info->id);
        return -1;
    }
    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
    --_num_increase_rows;
    return 1;
}
int LockSecondaryNode::put_global_index(RuntimeState* state, SmartRecord record) {
    int ret = 0;
    auto txn = state->txn();
    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
    ret = txn->put_secondary(_region_id, *_global_index_info, record);
    if (ret < 0) {
        DB_WARNING_STATE(state, "put index:%ld fail:%d, table_id:%ld", 
            _global_index_info->id, ret, _table_id);
        return ret;
    }
    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
    ++_num_increase_rows;
    return 1;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
