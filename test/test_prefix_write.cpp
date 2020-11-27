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

#include <cstdio>
#include <string>
#include <vector>
#include <iostream>
#include <sys/time.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "common.h"
#include "mut_table_key.h"

int main(int argc, char** argv) {

    std::string db_path = "/tmp/rocksdb_prefix_write_example";

    // open DB
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db);
    assert(s.ok());

    // rocksdb::ColumnFamilyOptions cf_option;
    // cf_option.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(4));
    // cf_option.OptimizeLevelStyleCompaction();

    // rocksdb::ColumnFamilyHandle* cf_handle;
    // s = db->CreateColumnFamily(cf_option, "test_cf", &cf_handle);

    /*
    // put and get from non-default column family
    uint32_t size = 1000;
    srand((unsigned)time(NULL));
    std::cout << RAND_MAX << std::endl;
    baikaldb::TimeCost cost;
    for (uint64_t idx = 0; idx < 10000000; idx++) {
        // uint64_t key1 = rand();
        // uint64_t key2 = rand();
        // uint64_t key = ((key1 << 32) | key2);
        // std::string prefix = std::string("longlonglonglonglonglonglongprefix") 
        //   + std::to_string(idx % size) + std::to_string(key);

        baikaldb::MutTableKey key;
        //key.append_string("prefix_");
        key.append_i32(idx / 50000);
        key.append_u64(idx);

        std::string value("hahavalue");
        value += std::to_string(idx);

        s = db->Put(rocksdb::WriteOptions(), cf_handle, key.data(), value);
        assert(s.ok());
    }
    //s = db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
    //assert(s.ok());
    //DB_NOTICE("put cost: %lu", cost.get_time());

    baikaldb::MutTableKey key1;
    key1.append_i32(5000000 / 50000);
    key1.append_u64(5000000);

    std::string value;

    cost.reset();
    s = db->Get(rocksdb::ReadOptions(), cf_handle, key1.data(), &value);
    DB_NOTICE("get cost: %lu, %s", cost.get_time(), rocksdb::Slice(value).ToString().c_str());

    cost.reset();
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    auto iter = db->NewIterator(read_options, cf_handle);
    baikaldb::MutTableKey key2;
    key2.append_i32(5000000 / 50000);
    iter->Seek(key2.data());
    DB_NOTICE("seek cost: %lu", cost.get_time());
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
        //std::cout << iter->key().ToString() << " | " << iter->value().ToString() << std::endl;
        count++;
    }
    DB_NOTICE("count: %d", count);*/

    rocksdb::ColumnFamilyOptions cf_option;
    cf_option.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(8));
    cf_option.OptimizeLevelStyleCompaction();

    rocksdb::ColumnFamilyHandle* cf_handle;
    s = db->CreateColumnFamily(cf_option, "test_cf", &cf_handle);

    rocksdb::WriteOptions write_options;
    int64_t prefix = 630152;
    for (int idx = 0; idx < 10000000; ++idx) {
        baikaldb::MutTableKey key;
        key.append_i64(idx/1000000).append_i64(idx);
        s = db->Put(write_options, cf_handle, key.data(), key.data());
        assert(s.ok());
    }
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("key1"), rocksdb::Slice("value1"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("keyt2"), rocksdb::Slice("value2"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("keytt3"), rocksdb::Slice("value3"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("keyttt4"), rocksdb::Slice("value4"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("keytzz"), rocksdb::Slice("value5"));
    // //s = db->Put(write_options, cf_handle, rocksdb::Slice("keyzzz"), rocksdb::Slice("value6"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("tepp"), rocksdb::Slice("value7"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("texaa"), rocksdb::Slice("value8"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("texab"), rocksdb::Slice("value9"));
    // s = db->Put(write_options, cf_handle, rocksdb::Slice("zzzzz"), rocksdb::Slice("value10"));

    //rocksdb::FlushOptions flush_option;
    //s = db->Flush(flush_option);

    // rocksdb::Range range;
    // range.start = "key1";
    // range.limit = "zzzzz";
    // uint64_t range_size = 0;
    // db->GetApproximateSizes(cf_handle, &range, 1, &range_size, (uint8_t)3);
    // std::cout << "range size:" << range_size << std::endl;

    //////
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    auto iter = db->NewIterator(read_options, cf_handle);

    baikaldb::TimeCost cost;
    iter->SeekToFirst();
    DB_NOTICE("SeekToFirst cost: %ld", cost.get_time());
    cost.reset();
    int count = 0;
    for (; iter->Valid(); iter->Next()) {
        count++;
    }
    DB_NOTICE("forward next cost: %ld, count:%d", cost.get_time(), count);
    delete iter;

    //////
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    iter = db->NewIterator(read_options, cf_handle);

    cost.reset();
    iter->SeekToLast();
    DB_NOTICE("SeekToLast cost: %ld", cost.get_time());
    cost.reset();
    count = 0;
    for (; iter->Valid(); iter->Prev()) {
        count++;
    }
    DB_NOTICE("backward prev cost: %ld, count:%d", cost.get_time(), count);
    delete iter;

    //////
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    iter = db->NewIterator(read_options, cf_handle);

    baikaldb::MutTableKey key1;
    key1.append_i64(5).append_i64(5000000);
    cost.reset();
    iter->Seek(key1.data());
    DB_NOTICE("Seek cost: %ld", cost.get_time());
    cost.reset();
    count = 0;
    for (; iter->Valid(); iter->Next()) {
        count++;
    }
    DB_NOTICE("forward next cost: %ld, count:%d", cost.get_time(), count);
    delete iter;

    //////
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    iter = db->NewIterator(read_options, cf_handle);

    baikaldb::MutTableKey key2;
    key2.append_i64(5).append_i64(5999999);
    cost.reset();
    iter->SeekForPrev(key2.data());
    DB_NOTICE("SeekForPrev cost: %ld", cost.get_time());
    cost.reset();
    count = 0;
    for (; iter->Valid(); iter->Prev()) {
        count++;
    }
    DB_NOTICE("backward prev cost: %ld, count:%d", cost.get_time(), count);
    delete iter;
    //return 0;


    // for (iter->Seek(key); iter->Valid(); iter->Next()) {
    //     // do something
    //     std::cout << iter->key().ToString() << " | " << iter->value().ToString() << std::endl;
    // }
    // std::cout << std::endl;

    // s = db->DeleteRange(write_options, cf_handle, "keyt", "keyz");
    // assert(s.ok());
    // read_options.prefix_same_as_start = false;
    // iter = db->NewIterator(read_options, cf_handle);
    // for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    //     // do something
    //     std::cout << iter->key().ToString() << " ==> " << iter->value().ToString() << std::endl;
    //     if (iter->value().ToString() == "value10") {
    //         break;
    //     }
    // }
    // std::cout << std::endl;

    // for (; iter->Valid(); iter->Prev()) {
    //     std::cout << iter->key().ToString() << " ==> " << iter->value().ToString() << std::endl;
    // }

    // close db
    delete db;
    DestroyDB(db_path, options);
    return 0;
}
