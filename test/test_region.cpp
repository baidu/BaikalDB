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

#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <vector>
#include "region.h"
#include "rocks_wrapper.h"

namespace baikaldb{

int main(int argc, char* argv[]) {
    /*
    auto factory = SchemaFactory::get_instance();

    pb::SchemaInfo info;
    info.set_namespace_("test_namespace");
    info.set_database("test_database");
    info.set_table_name("test_table_name");

    info.set_namespace_id(111);
    info.set_database_id(222);

    uint32_t col_cnt = 6;
    uint32_t row_cnt = 1;
    if (argc >= 2) {
        col_cnt = atoi(argv[1]);
    }
    if (argc >= 3) {
        row_cnt = atoi(argv[2]);
    }

    // test table 1
    // column1, int32
    // column2, uint32
    // column3, int64
    // column4, uint64
    // column5, double
    // column6, string
    // pk field (col1, col2, col3)
    pb::Field *field1 = info.add_fields();
    field1->set_field_name("column1");
    field1->set_field_id(1);
    field1->set_mysql_type(pb::INT32);

    pb::Field *field2 = info.add_fields();
    field2->set_field_name("column2");
    field2->set_field_id(2);
    field2->set_mysql_type(pb::UINT32);

    pb::Field *field3 = info.add_fields();
    field3->set_field_name("column3");
    field3->set_field_id(3);
    field3->set_mysql_type(pb::INT64);

    pb::Field *field4 = info.add_fields();
    field4->set_field_name("column4");
    field4->set_field_id(4);
    field4->set_mysql_type(pb::UINT64);

    pb::Field *field5 = info.add_fields();
    field5->set_field_name("column5");
    field5->set_field_id(5);
    field5->set_mysql_type(pb::DOUBLE);

    pb::Field *field6 = info.add_fields();
    field6->set_field_name("column6");
    field6->set_field_id(6);
    field6->set_mysql_type(pb::STRING);

    pb::Index *index_pk = info.add_indexs();
    index_pk->set_index_type(pb::I_PRIMARY);
    index_pk->set_index_name("pk_index");
    index_pk->add_field_ids(1);
    //index_pk->add_field_ids(2);
    //index_pk->add_field_ids(3);

    info.set_table_id(1);
    info.set_version(2);

    factory->init();

    TimeCost cost;
    factory->reload_schema(info);
    DB_NOTICE("reload_schema cost: %lu", cost.get_time());
    cost.reset();

    auto _rocksdb = baikaldb::RocksWrapper::get_instance();
    _rocksdb->init("/tmp/rocksdb/");

    auto region = new baikaldb::Region();
    region->init();

    std::vector<SmartRecord> rows_vec;

    srand((unsigned)time(NULL));
    for (int row_idx = 0; row_idx < row_cnt; ++row_idx) {
        auto record = factory->create_record_by_tableid(1);
        assert(record != NULL);

        record->set_int32(1,  rand()%UINT_MAX);
        record->set_uint32(2, rand()%UINT_MAX);
        record->set_int64(3,  rand()%UINT_MAX);
        record->set_uint64(4, rand()%UINT_MAX);
        record->set_double(5, (rand() + 0.0 )/UINT_MAX);
        record->set_string(6, "test_string" + std::to_string(row_idx));
        rows_vec.push_back(record);
    }

    rocksdb::WriteOptions woptions;
    for (int row_idx = 0; row_idx < row_cnt; ++row_idx) {
        auto res = region->put(1, rows_vec[row_idx], woptions);
    }

    auto pk_record = factory->create_record_by_tableid(1);

    int32_t val;
    int rand_idx = rand()%row_cnt;
    auto res = rows_vec[rand_idx]->get_int32(1, val);
    pk_record->set_int32(1, val);
    std::cout << "input: " << rows_vec[rand_idx]->to_string() << std::endl;

    std::vector<baikaldb::SmartTableRecord> out_records;

    rocksdb::ReadOptions roptions;
    roptions.prefix_same_as_start = true;
    res = region->get(1, pk_record, out_records, roptions);

    for (auto re : out_records) {
        std::cout << "output:" << re->to_string() << std::endl;
    }

    res = region->remove(1, pk_record, woptions);
    std::cout << region->dump_hex() << std::endl;*/
    return 0;
}
} // end of namespace
