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
#include "schema_factory.h"
#include "region.h"
int main(int argc, char* argv[]) {
    auto val_encoder = baikaldb::RecordEncoder::get_instance();

    baikaldb::pb::SchemaInfo info;
    info.set_namespace_("test_namespace");
    info.set_database("test_database");
    info.set_table_name("test_table_name");

    info.set_namespace_id(111);
    info.set_database_id(222);

    uint32_t col_cnt = 10;
    uint32_t row_cnt = 10000;
    if (argc >= 2) {
        col_cnt = atoi(argv[1]);
    }
    if (argc >= 3) {
        row_cnt = atoi(argv[2]);
    }

    for (int idx = 1; idx < col_cnt; idx++) {
        baikaldb::pb::Field *field_string = info.add_fields();
        field_string->set_field_name("column" + std::to_string(idx));
        field_string->set_field_id(idx);
        if (idx % 5 == 0) {
            field_string->set_mysql_type(baikaldb::pb::INT32);
        } else if (idx % 5 == 1) {
            field_string->set_mysql_type(baikaldb::pb::UINT32);
        } else if (idx % 5 == 2) {
            field_string->set_mysql_type(baikaldb::pb::INT64);
        } else if (idx % 5 == 3) {
            field_string->set_mysql_type(baikaldb::pb::UINT64);
        } else if (idx % 5 == 4) {
            field_string->set_mysql_type(baikaldb::pb::DOUBLE);
        }
    }

    baikaldb::pb::Index *index_pk = info.add_indexs();
    index_pk->set_index_type(baikaldb::pb::I_PRIMARY);
    index_pk->set_index_name("pk_index");
    index_pk->add_field_ids(1);
    index_pk->add_field_ids(3);

    info.set_table_id(1);
    info.set_version(2);

    comcfg::ConfigUnit conf;
    val_encoder->init(conf);

    baikaldb::TimeCost cost;
    //val_encoder->reload_schema_test(info, row_cnt);
    DB_NOTICE("reload_schema cost: %lu", cost.get_time());
    cost.reset();

    return 0;
    //std::vector<baikaldb::TableRecord*> rows_vec;
    //rows_vec.reserve(row_cnt);
    //sleep(10);
    for (int row_idx = 0; row_idx < row_cnt; ++row_idx) {
        auto message = val_encoder->_get_table_message(1);
        auto record = new (std::nothrow)baikaldb::TableRecord(message);
        assert(record != NULL);

        srand((unsigned)time(NULL));
        for (int idx = 1; idx < col_cnt; idx++) {
            if (idx % 5 == 0) {
                record->set_int32(idx, rand()%INT_MAX);
            } else if (idx % 5 == 1) {
                record->set_uint32(idx, rand()%UINT_MAX);
            } else if (idx % 5 == 2) {
                record->set_int64(idx, rand());
            } else if (idx % 5 == 3) {
                record->set_uint64(idx, rand());
            } else if (idx % 5 == 4) {
                record->set_double(idx, 1.111);
            }
        }
        if (row_idx == 1) {
            std::string out;
            record->encode(out);
            std::cout << "encode size: " << out.size() << std::endl;
        }
        //rows_vec.push_back(record);
    }
    DB_NOTICE("set_field cost: %lu", cost.get_time());
    cost.reset();

    std::vector<baikaldb::FieldInfo> infos;
    auto res = val_encoder->get_index_fields(1, infos);
    if (res == -1) {
        DB_WARNING("get pk fields failed: %lu", 1);
        return -1;
    }
    for (auto info : infos) {
        std::cout << "field: " << info.field << std::endl;
    }

    auto message = val_encoder->new_record(1);
    double value = 100.123456789;
    
    //message->set_double(4, 9.8135);

    res = message->get_double(4, value);
    std::cout << res << "\t" << value << std::endl;

    /*
    std::string message_str;
    int res = record->encode(message_str);
    assert(res == true);
    DB_NOTICE("encode_fields cost: %lu", cost.get_time());
    cost.reset();

    auto out_record = val_encoder->create_record_by_tableid(1);
    res = out_record->decode(message_str);
    assert(res == true);

    DB_NOTICE("decode_fields cost: %lu", cost.get_time());
    cost.reset();

    std::cout << "sizeof(float): " << sizeof(float) << std::endl;
    std::cout << "sizeof(double): " << sizeof(double) << std::endl;
    std::cout << "sizeof(uint32_t): " << sizeof(uint32_t) << std::endl;

    std::cout << out_record->to_string() << std::endl;
    */
    return 0;
}
