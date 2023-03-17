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

#pragma once

#include "rocksdb/listener.h"
#include "rocks_wrapper.h"

namespace baikaldb {
class MyListener : public rocksdb::EventListener {
public:
    virtual ~MyListener() {}
    virtual void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) {
        bool is_stall = info.condition.cur != rocksdb::WriteStallCondition::kNormal;
        DB_WARNING("OnStallConditionsChanged, cf:%s is_stall:%d", info.cf_name.c_str(), is_stall);
    }
    virtual void OnFlushCompleted(rocksdb::DB* /*db*/, const rocksdb::FlushJobInfo& info) {
        uint64_t file_number = info.file_number;
        RocksWrapper::get_instance()->set_flush_file_number(info.cf_name, file_number);
        DB_WARNING("OnFlushCompleted, cf:%s file_number:%lu", info.cf_name.c_str(), file_number);
    }
    virtual void OnExternalFileIngested(rocksdb::DB* /*db*/, const rocksdb::ExternalFileIngestionInfo& info) {
        DB_WARNING("OnExternalFileIngested, cf:%s table_properties:%s", 
                info.cf_name.c_str(), info.table_properties.ToString().c_str());
    }
};
}
