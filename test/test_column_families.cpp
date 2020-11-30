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
#include "common.h"

using namespace rocksdb;

int main(int argc, char** argv) {
  
    std::string db_path = "/tmp/rocksdb_column_families_example";

    // open DB
    Options options;
    options.create_if_missing = true;
    DB* db = NULL;
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());
    
    uint32_t family_count = 1000;
    if (argc >= 2) {
        family_count = atoi(argv[1]);
    }
    DB_NOTICE("family count: %u", family_count);

    // create column family
    std::vector<ColumnFamilyHandle*> cf_vec;
    std::vector<std::string> family_names;
    for (uint32_t idx = 0; idx < family_count; ++idx) {
        family_names.push_back("new_cf_" + std::to_string(idx));
    }
    s = db->CreateColumnFamilies(ColumnFamilyOptions(), family_names, &cf_vec);
    assert(s.ok());
    DB_NOTICE("create cfs success");

    // close DB
    for (uint32_t idx = 0; idx < cf_vec.size(); ++idx) {
        delete cf_vec[idx];
    }
    delete db;

    family_names.clear();
    s = DB::ListColumnFamilies(DBOptions(), db_path, &family_names);
    assert(s.ok());
    DB_NOTICE("column family count: %lu", family_names.size());

    // open DB with two column families
    std::vector<ColumnFamilyDescriptor> column_families;
    // have to open default column family
    ColumnFamilyOptions option;
    option.write_buffer_size = 64 << 20;
    for (int idx = 0; idx < family_names.size(); ++idx) {
        column_families.push_back(ColumnFamilyDescriptor(family_names[idx], option));
    }
    std::vector<ColumnFamilyHandle*> handles;
    s = DB::Open(DBOptions(), db_path, column_families, &handles, &db);
    assert(s.ok());

    // put and get from non-default column family
    uint32_t size = handles.size();
    srand((unsigned)time(NULL));
    std::cout << RAND_MAX << std::endl;
    baikaldb::TimeCost cost;
    for (uint64_t idx = 0; idx < 1000; idx++) {
        uint64_t key1 = rand();
        uint64_t key2 = rand();
        uint64_t key = ((key1 << 32) | key2);
        s = db->Put(WriteOptions(), handles[idx%size], Slice(std::to_string(key)), Slice(std::to_string(idx) + "value"));
        assert(s.ok());
    }

    for (auto handle : handles) {
        s = db->CompactRange(CompactRangeOptions(), handle, nullptr, nullptr);
    }
    assert(s.ok());
    DB_NOTICE("put cost: %lu", cost.get_time());
    // s = db->Put(WriteOptions(), handles[0], Slice("key1"), Slice("value1"));
    // s = db->Put(WriteOptions(), handles[0], Slice("key2"), Slice("value2"));
    // s = db->Put(WriteOptions(), handles[0], Slice("key3"), Slice("value3"));
    // s = db->Put(WriteOptions(), handles[0], Slice("key4"), Slice("value4"));
    // s = db->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    // close db
    for (auto handle : handles) {
        delete handle;
    }
    delete db;

    return 0;
}
