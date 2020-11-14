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
    return 0;
}

void LockSecondaryNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    if (region_id == 0) {
        return;
    }
    auto lock_secondary_node = pb_node->mutable_derive_node()->mutable_lock_secondary_node();
    lock_secondary_node->set_global_index_id(_global_index_id);
    lock_secondary_node->set_table_id(_table_id);
    lock_secondary_node->set_lock_type(_lock_type);
    lock_secondary_node->clear_put_records();
    if (_insert_records_by_region.count(region_id) != 0) {
        for (auto& record : _insert_records_by_region[region_id]) {
            std::string* str = lock_secondary_node->add_put_records();
            record->encode(*str);
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
    if (_global_index_info == nullptr) {
        DB_WARNING("get index info fail, index_id: %ld", _global_index_id);
        return -1;
    }
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr: region:%ld", _region_id);
        return -1;
    }
    SmartRecord record_template = _factory->new_record(_table_id);
    int num_affected_rows = 0;
    std::vector<SmartRecord> put_records;
    std::vector<SmartRecord> delete_records;
    for (auto& str_record : _pb_node.derive_node().lock_secondary_node().put_records()) {
        SmartRecord record = record_template->clone(false);
        record->decode(str_record);
        bool fit_region = true;
        if (txn->fits_region_range_for_global_index(*_pri_info, *_global_index_info, record, fit_region) < 0) {
            DB_WARNING("fits_region_range_for_global_index fail, region_id: %ld, index_id: %ld",
                _region_id, _global_index_id);
            return -1;
        }
        if (fit_region) {
            put_records.push_back(record);
        } else {
            //DB_WARNING("index_id: %ld not in region_id: %d, record: %s",
            //    _global_index_id, _region_id, record->debug_string().c_str());
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
    //对全局二级索引加锁返回
    if (_lock_type == pb::LOCK_GET) {
        for (auto& record : put_records) {
            //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
            auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, record, GET_LOCK, true);
            if (ret == -3 || ret == -2) {
                continue;
            }
            if (ret == -1) {
                DB_WARNING("get lock fail");
                return -1;
            }
            _return_records[_global_index_info->id].push_back(record);
            //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
        }
    }
    //对全局二级索引进行加锁写入 or  删除
    if (_lock_type == pb::LOCK_DML ) {
        for (auto& record : delete_records) {
            ret = delete_global_index(state, record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "insert_row fail");
                return -1;
            }
            num_affected_rows += ret;
        }
        for (auto& record : put_records) {
            //加锁写入
            ret = insert_global_index(state, record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "insert_row fail");
                return -1;
            }
            num_affected_rows += ret;
        }  
    }
    if (_lock_type == pb::LOCK_NO) {
        for (auto& record : put_records) {
            ret = put_global_index(state, record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "put_row fail");
                return -1;
            }
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
int LockSecondaryNode::insert_global_index(RuntimeState* state, SmartRecord record) {
    auto txn = state->txn();
    //DB_WARNING_STATE(state,"record:%s", record->debug_string().c_str());
    auto ret = txn->get_update_secondary(_region_id, *_pri_info, *_global_index_info, record, GET_LOCK, true);
    if (ret == 0) {
        DB_WARNING_STATE(state, "insert uniq key must not exist, "
                        "index:%ld, ret:%d", _global_index_info->id, ret);
            state->error_code = ER_DUP_ENTRY;
            state->error_msg << "Duplicate entry: '" << 
                    record->get_index_value(*_global_index_info) << "' for key '" << _global_index_info->short_name << "'";
        return -1;
    }
    // ret == -3 means the primary_key returned by get_update_secondary is out of the region
    // (dirty data), this does not affect the insertion
    if (ret != -2 && ret != -3) {
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
    if (ret == -1) {
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
