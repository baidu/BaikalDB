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
 
#include <string>
#include "rocks_wrapper.h"

namespace baikaldb {

class SstFileWriter {
public:
    SstFileWriter(const rocksdb::Options& option)
            : _sst_writer(rocksdb::EnvOptions(), option, nullptr, true) {}
    rocksdb::Status open(const std::string& sst_file) {
        return _sst_writer.Open(sst_file);
    }
    rocksdb::Status put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
        return _sst_writer.Put(key, value);
    }
    rocksdb::Status finish(rocksdb::ExternalSstFileInfo* file_info = nullptr) {
        return _sst_writer.Finish(file_info);
    }
    virtual ~SstFileWriter() {}
private:
    rocksdb::SstFileWriter _sst_writer;
};
}
