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

#include "gtest/gtest.h"
#include "meta_rocksdb.h"
#include <gflags/gflags.h>
#include "sst_file_writer.h"
using namespace baikaldb;
class RocksdbTEST : public testing::Test {
public:
    ~RocksdbTEST() {}
protected:
    virtual void SetUp() {
        _rocksdb = RocksWrapper::get_instance();
        if (!_rocksdb) {
            DB_FATAL("create rocksdb handler failed");
            return;
        }
        int ret = _rocksdb->init("./rocks_db");
        if (ret != 0) {
            DB_FATAL("rocksdb init failed: code:%d", ret);
            return;
        }
    }
    virtual void TearDown() {}
    baikaldb::RocksWrapper*  _rocksdb;
};

TEST_F(RocksdbTEST, ingest_test) {
    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(_rocksdb->get_options(_rocksdb->get_data_handle())));
    std::unique_ptr<SstFileWriter> writer2(new SstFileWriter(_rocksdb->get_options(_rocksdb->get_data_handle())));
    std::string path1 = "./test_ingest1.sst";
    std::string path2 = "./test_ingest2.sst";
    auto s = writer->open(path1);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s", path1.c_str(), s.ToString().c_str());
        return;
    }
    writer->put(rocksdb::Slice("1"), rocksdb::Slice("1"));
    writer->put(rocksdb::Slice("4"), rocksdb::Slice("4"));
    writer->put(rocksdb::Slice("7"), rocksdb::Slice("7"));
    writer->put(rocksdb::Slice("9"), rocksdb::Slice("9"));
    writer->finish();

    s = writer2->open(path2);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s", path2.c_str(), s.ToString().c_str());
        return;
    }
    writer2->put(rocksdb::Slice("11"), rocksdb::Slice("11"));
    writer2->put(rocksdb::Slice("3"), rocksdb::Slice("3"));
    writer2->put(rocksdb::Slice("6"), rocksdb::Slice("6"));
    writer2->put(rocksdb::Slice("9"), rocksdb::Slice("9_2"));
    writer2->put(rocksdb::Slice("99"), rocksdb::Slice("99"));
    writer2->finish();

    rocksdb::IngestExternalFileOptions ifo;
    ifo.move_files = true;
    ifo.write_global_seqno = false;
    ifo.allow_blocking_flush = false;
    s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {path1}, ifo);
    if (!s.ok()) {
        DB_WARNING("ingest fail: %s", path1.c_str());
    }
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    int idx = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rocksdb::Slice key_slice(iter->key());
        rocksdb::Slice value_slice(iter->value());
        DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
        idx++;
    }
    ASSERT_EQ(idx, 4);

    s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {path2}, ifo);
    if (!s.ok()) {
        DB_WARNING("ingest fail: %s", path1.c_str());
    }
    std::unique_ptr<rocksdb::Iterator> iter2(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    idx = 0;
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        rocksdb::Slice key_slice(iter2->key());
        rocksdb::Slice value_slice(iter2->value());
        DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
        idx++;
    }
    ASSERT_EQ(idx, 8);
} // TEST_F

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
