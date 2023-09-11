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
    SstFileWriter(const rocksdb::Options& options, bool force_lz4 = true) : _options(options) {
        if (force_lz4) {
            _options.bottommost_compression = rocksdb::kLZ4Compression;
            _options.bottommost_compression_opts = rocksdb::CompressionOptions();
        }
        _sst_writer.reset(new rocksdb::SstFileWriter(rocksdb::EnvOptions(), _options, nullptr, true));
    }
    rocksdb::Status open(const std::string& sst_file) {
        return _sst_writer->Open(sst_file);
    }
    rocksdb::Status put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
        return _sst_writer->Put(key, value);
    }
    rocksdb::Status finish(rocksdb::ExternalSstFileInfo* file_info = nullptr) {
        return _sst_writer->Finish(file_info);
    }
    uint64_t file_size() {
        return _sst_writer->FileSize();
    }
    virtual ~SstFileWriter() {}
private:
    rocksdb::Options _options;
    std::unique_ptr<rocksdb::SstFileWriter> _sst_writer = nullptr;
};
}
