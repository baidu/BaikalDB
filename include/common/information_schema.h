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
#include "common.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
class TableRecord;
class RuntimeState;
class ExprNode;
using SmartRecord = std::shared_ptr<TableRecord>;
using FieldVec = std::vector<std::pair<std::string, pb::PrimitiveType>>;
class InformationSchema {
public:
    static InformationSchema* get_instance() {
        static InformationSchema _instance;
        return &_instance;
    }
    int init();
    std::vector<SmartRecord> call_table(int64_t table_id, 
            RuntimeState* state, std::vector<ExprNode*>& conditions) {
        auto iter = _calls.find(table_id);
        if (iter == _calls.end()) {
            return std::vector<SmartRecord>();
        }
        if (iter->second == nullptr) {
            return std::vector<SmartRecord>();
        }
        return iter->second(state, conditions);
    }
    int64_t db_id() const {
        return _db_id;
    }
private:
    InformationSchema() {
    }
    int64_t construct_table(const std::string& table_name, FieldVec& fields);
    void init_partition_split_info();
    void init_region_status();
    void init_columns();
    void init_statistics();
    void init_schemata();
    void init_tables();
    void init_virtual_index_influence_info();
    void init_sign_list();
    void init_routines();
    void init_key_column_usage();
    void init_referential_constraints();
    void init_triggers();
    void init_views();

    std::unordered_map<std::string, pb::SchemaInfo> _tables;
    //InformationSchema在baikaldb端运行
    //处理逻辑函数指针参数state和相关的conditions可选用
    //返回符合条件的全部record
    std::unordered_map<int64_t, std::function<
            std::vector<SmartRecord>(RuntimeState* state, std::vector<ExprNode*>& conditions)>
        > _calls;
    int32_t _max_table_id = INT32_MAX;
    int32_t _db_id = INT32_MAX;
};
}
