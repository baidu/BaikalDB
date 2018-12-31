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

#pragma once

#include "rocks_wrapper.h"

namespace baikaldb {
class LogEntryReader {
public:
    virtual ~LogEntryReader() {}
   
    static LogEntryReader* get_instance() {
        static LogEntryReader _instance;
        return &_instance;
    } 
    void init(RocksWrapper* rocksdb, 
            rocksdb::ColumnFamilyHandle* log_cf) {
        _rocksdb = rocksdb;
        _log_cf = log_cf;
    }
    int read_log_entry(int64_t region_id, int64_t log_index, std::string& log_entry); 

private:
    LogEntryReader() {}

private:
    RocksWrapper*       _rocksdb;
    rocksdb::ColumnFamilyHandle* _log_cf;    
};

} // end of namespace
