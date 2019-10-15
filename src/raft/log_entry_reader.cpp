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
#include "mut_table_key.h"

namespace baikaldb {
int LogEntryReader::read_log_entry(int64_t region_id, int64_t log_index, std::string& log_entry) {
    MutTableKey log_data_key;
    log_data_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(log_index);
    std::string log_value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _log_cf, rocksdb::Slice(log_data_key.data()), &log_value);
    if (!status.ok()) {
        DB_FATAL("read log entry fail, region_id: %ld, log_index: %ld", region_id, log_index);
        return -1;
    }
    rocksdb::Slice slice(log_value);
    LogHead head(slice);
    if (head.type != braft::ENTRY_TYPE_DATA) {
        DB_FATAL("log entry is not data, log_index:%ld, region_id: %ld", log_index, region_id);
        return -1;
    }
    log_entry = log_value.substr(MyRaftLogStorage::LOG_HEAD_SIZE);
    return 0;
}
}


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
