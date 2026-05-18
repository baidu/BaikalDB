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

#include "log_entry_reader.h"
#include "my_raft_log_storage.h"
#include "common.h"
#include "table_key.h"
#include "mut_table_key.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
DEFINE_bool(enable_new_raft_log_storage, false, "whether use NewMyRaftLogStorage");

int LogEntryReader::read_log_entry(int64_t region_id, int64_t log_index, std::string& log_entry) {
    MutTableKey log_data_key;
    log_data_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(log_index);
    std::string log_value;
    rocksdb::ReadOptions options;

    if (FLAGS_enable_new_raft_log_storage) {
        // 早于first_log_index的记录都是无效的
        int64_t first_log_index = 0;
        int ret = NewMyRaftLogStorage::get_region_first_log_idx(region_id, first_log_index);
        if (ret < 0) {
            return -1;
        }
        if (log_index < first_log_index) {
            DB_FATAL("read log entry fail: log expired. region_id: %ld, log_index: %ld", region_id, log_index);
            return -1;
        }
    }

    RocksdbVars::get_instance()->raft_log_read_log_entry_count << 1;
    bool found = false;
    if (FLAGS_enable_new_raft_log_storage) {
        // 优先找新的，新的没有再fallback到老的
        auto status = _rocksdb->new_log_get(options, _new_log_cf, rocksdb::Slice(log_data_key.data()), &log_value);
        if (status.ok()) {
            found = true;
        } else if (status.IsNotFound()) {
            DB_WARNING("log entry not found in new raft log storage, region_id: %ld, log_index: %ld", region_id, log_index);
        } else {
            DB_FATAL("error occurred when read log entry from new_log_cf, region_id: %ld, log_index: %ld", region_id, log_index);
            return -1;
        }
    }
    if (!found) {
        auto status = _rocksdb->get(options, _log_cf, rocksdb::Slice(log_data_key.data()), &log_value);
        if (!status.ok()) {
            DB_FATAL("read log entry fail, region_id: %ld, log_index: %ld", region_id, log_index);
            return -1;
        }
    }
    rocksdb::Slice slice(log_value);
    LogHead head(slice);
    if (head.type != braft::ENTRY_TYPE_DATA) {
        DB_FATAL("log entry is not data, log_index:%ld, region_id: %ld", log_index, region_id);
        return -1;
    }
    slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
    log_entry.assign(slice.data(), slice.size());
    return 0;
}

int LogEntryReader::read_log_entry(int64_t region_id,
        int64_t start_log_index,
        int64_t end_log_index,
        std::set<uint64_t>& txn_ids,
        std::map<int64_t, std::string>& log_entrys) {
    if (txn_ids.empty()) {
        return 0;
    }
    if (start_log_index > end_log_index) {
        DB_FATAL("region_id:%ld, start_log_index:%ld, end_log_index:%ld", region_id, start_log_index, end_log_index);
        return -1;
    }
    TimeCost cost;
    RocksdbVars::get_instance()->raft_log_read_log_entry_count << 1;
    auto process_single_log_entry = [&](const rocksdb::Slice& key, const rocksdb::Slice& value, bool& /*need_break*/) {
        int64_t log_index = TableKey(key).extract_i64(sizeof(int64_t) + 1);
        rocksdb::Slice value_slice(value);
        LogHead head(value);
        value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
        if (head.type != braft::ENTRY_TYPE_DATA) {
            DB_WARNING("log entry is not data, region_id: %ld head.type: %d", region_id, head.type);
            return 0;
        }
        pb::StoreReq store_req;
        if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
            DB_FATAL("Fail to parse request fail, region_id: %ld", region_id);
            return -1;
        }

        if (!is_txn_op_type(store_req.op_type())) {
            //DB_WARNING("log entry is not txn, region_id: %ld head.type: %d", region_id, store_req.op_type());
            return 0;
        }

        if (store_req.txn_infos_size() > 0) {
            uint64_t txn_id = store_req.txn_infos(0).txn_id();
            if (txn_ids.count(txn_id) == 1) {
                log_entrys[log_index] = value_slice.ToString();
                DB_WARNING("read txn log entry region_id:%ld, log_index:%ld, txn_id:%ld", region_id, log_index, txn_id);
            }
        }
        return 0;
    };

    int ret = process_logs(region_id, start_log_index, end_log_index, process_single_log_entry);
    if (ret != 0) {
        DB_WARNING("read txn log entries failed, region_id: %ld", region_id);
        return -1;
    }
    DB_WARNING("read txn log entry region_id:%ld, time_cost:%ld", region_id, cost.get_time());
    return 0;
}

int LogEntryReader::read_txn_last_log_entry(int64_t region_id, int64_t start_log_index, int64_t end_log_index,
                std::set<uint64_t>& txn_ids, std::map<uint64_t, std::string>& log_entrys) {
    if (txn_ids.empty()) {
        return 0;
    }
    TimeCost cost;

    auto process_single_log_entry = [&](const rocksdb::Slice& key, const rocksdb::Slice& value, bool& /*need_break*/) {
        int64_t log_index = TableKey(key).extract_i64(sizeof(int64_t) + 1);
        rocksdb::Slice value_slice(value);
        LogHead head(value_slice);
        value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
        if (head.type != braft::ENTRY_TYPE_DATA) {
            DB_WARNING("log entry is not data, region_id: %ld head.type: %d", region_id, head.type);
            return 0;
        }
        pb::StoreReq store_req;
        if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
            DB_FATAL("Fail to parse request fail, region_id: %ld", region_id);
            return -1;
        }

        if (!is_txn_op_type(store_req.op_type())) {
            //DB_WARNING("log entry is not txn, region_id: %ld head.type: %d", region_id, store_req.op_type());
            return 0;
        }

        if (store_req.txn_infos_size() > 0) {
            uint64_t txn_id = store_req.txn_infos(0).txn_id();
            if (txn_ids.count(txn_id) == 1) {
                log_entrys[txn_id] = value_slice.ToString();
                DB_WARNING("read txn log entry region_id:%ld, log_index:%ld, txn_id:%ld", region_id, log_index, txn_id);
            }
        }
        return 0;
    };

    int ret = process_logs(region_id, start_log_index, end_log_index, process_single_log_entry);
    if (ret != 0) {
        DB_WARNING("read tnx log entry failed. region_id: %ld", region_id);
        return ret;
    }
    DB_WARNING("read txn log entry region_id:%ld, time_cost:%ld", region_id, cost.get_time());
    return 0;
}

int LogEntryReader::process_logs(int64_t region_id,
                    int64_t start_index,
                    int64_t end_index,
                    KV_PROCESS_FUNC process_func) {
    TimeCost cost;
    int64_t first_log_index = 0;

    RocksdbVars::get_instance()->raft_log_baikal_scan_times_count << 1;
    // 早于first_log_index的记录都是无效的
    if (FLAGS_enable_new_raft_log_storage) {
        int ret = NewMyRaftLogStorage::get_region_first_log_idx(region_id, first_log_index);
        if (ret < 0) {
            DB_WARNING("get first log index failed");
            return -1;
        }
        start_index = std::max(start_index, first_log_index);
    }
    MutTableKey log_data_key;
    log_data_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(start_index);
    MutTableKey prefix;
    MutTableKey end;
    prefix.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY);
    end.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(end_index + 1);
    rocksdb::ReadOptions options;
    rocksdb::Slice upper_bound_slice = end.data();
    if (end_index > 0) {
        options.iterate_upper_bound = &upper_bound_slice;
    }
    options.prefix_same_as_start = true;
    options.total_order_seek = false;
    options.fill_cache = false;

    bool need_break = false;
    bool skip_old_iter = false;
    if (FLAGS_enable_new_raft_log_storage) {
        int64_t new_log_start_idx = NewMyRaftLogStorage::get_region_new_first_log_idx(region_id);
        if (new_log_start_idx < 0) {
            // should not happen
            DB_WARNING("new_log_start_idx not found");
        } else if (start_index >= new_log_start_idx) {
            skip_old_iter = true;
        }
    }
    if (!skip_old_iter) {
        auto old_iter_ptr = _rocksdb->new_iterator(options, _log_cf);
        if (old_iter_ptr == nullptr) {
            return -1;
        }
        std::unique_ptr<rocksdb::Iterator> old_iter(old_iter_ptr);
        old_iter->Seek(log_data_key.data());
        for (; old_iter->Valid(); old_iter->Next()) {
            auto key = old_iter->key();
            // 不设置end_key则需要根据前缀结束循环
            if (!key.starts_with(prefix.data())) {
                DB_WARNING("read end info, region_id: %ld, key:%s",
                    region_id, key.ToString(true).c_str());
                break;
            }
            int64_t log_index = TableKey(key).extract_i64(sizeof(int64_t) + 1);
            if (end_index > 0 && log_index > end_index) {
                DB_WARNING("region_id:%ld, log_index:%ld, end_log_index:%ld",
                    region_id, log_index, end_index);
                need_break = true;
                break;
            }

            int ret = process_func(key, old_iter->value(), need_break);
            if (0 != ret) {
                DB_WARNING("process single raft log failed, region: %ld", region_id);
                return ret;
            }
            if (need_break) {
                break;
            }
        }
        if (!old_iter->status().ok()) {
            DB_FATAL("Iterator error during read old raft log: %s, region_id: %ld",
                     old_iter->status().ToString().c_str(), region_id);
            return -1;
        }
    }
    if (FLAGS_enable_new_raft_log_storage && !need_break) {
        auto new_iter_ptr = _rocksdb->new_log_iterator(options, RocksWrapper::NEW_RAFT_LOG_CF);
        if (new_iter_ptr == nullptr) {
            return -1;
        }
        std::unique_ptr<rocksdb::Iterator> new_iter(new_iter_ptr);
        new_iter->Seek(log_data_key.data());
        for (; new_iter->Valid(); new_iter->Next()) {
            auto key = new_iter->key();
            if (!key.starts_with(prefix.data())) {
                DB_WARNING("read end info, region_id: %ld, key:%s",
                    region_id, key.ToString(true).c_str());
                break;
            }
            int64_t log_index = TableKey(key).extract_i64(sizeof(int64_t) + 1);
            if (end_index > 0 && log_index > end_index) {
                DB_WARNING("region_id:%ld, log_index:%ld, end_log_index:%ld",
                    region_id, log_index, end_index);
                break;
            }
            if (0 != process_func(new_iter->key(), new_iter->value(), need_break)) {
                DB_WARNING("process new storage single raft log failed, region: %ld", region_id);
                return -1;
            }
            if (need_break) {
                break;
            }
        }
        if (!new_iter->status().ok()) {
            DB_FATAL("Iterator error during read new raft log: %s, region_id: %ld",
                     new_iter->status().ToString().c_str(), region_id);
            return -1;
        }
    }
    return 0;
}

int LogEntryReader::remove_log_entry(int64_t drop_region_id) {
    TimeCost cost;
    rocksdb::WriteOptions options;
    MutTableKey start_key;
    MutTableKey end_key;
    start_key.append_i64(drop_region_id);

    end_key.append_i64(drop_region_id);
    end_key.append_u64(UINT64_MAX);
    auto rocksdb = RocksWrapper::get_instance();
    // sleep会，等待异步日志刷盘
    bthread_usleep(1000 * 1000);
    auto status = _rocksdb->remove_range(options,
                                    rocksdb->get_raft_log_handle(),
                                    start_key.data(),
                                    end_key.data(),
                                    true);
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), drop_region_id);
        return -1;
    }
    if (FLAGS_enable_new_raft_log_storage) {
        NewMyRaftLogStorage::remove_region_first_log_idx(drop_region_id);
        NewMyRaftLogStorage::remove_region_new_first_log_idx(drop_region_id);
    }
    DB_WARNING("remove raft log entry, region_id: %ld, cost: %ld", drop_region_id, cost.get_time());
    // MutTableKey log_data_key;
    // log_data_key.append_i64(drop_region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(1);
    // rocksdb::ReadOptions opt;
    // opt.prefix_same_as_start = true;
    // opt.total_order_seek = false;
    // opt.fill_cache = false;
    // std::unique_ptr<rocksdb::Iterator> iter(rocksdb->new_iterator(opt, rocksdb->get_raft_log_handle()));
    // iter->Seek(log_data_key.data());
    // if (iter->Valid()) {
    //     int64_t log_index = TableKey(iter->key()).extract_i64(sizeof(int64_t) + 1);
    //     rocksdb::Slice value(iter->value());
    //     LogHead head(value);
    //     value.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
    //     pb::StoreReq req;
    //     req.ParseFromArray(value.data(), value.size());
    //     DB_WARNING("remove raft log entry, region_id: %ld, cost:%ld, log_index:%ld, type:%d, %s",
    //             drop_region_id, cost.get_time(), log_index, head.type, req.ShortDebugString().c_str());
    // }
    return 0;
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */