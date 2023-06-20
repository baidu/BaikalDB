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

// Brief:  the class for generating and executing DDL SQL
#pragma once

#include "logical_planner.h"
#include "scan_node.h"
#include "query_context.h"
#include "single_txn_manager_node.h"

namespace baikaldb {

class DDLWorkPlanner : public LogicalPlanner {
public:
    DDLWorkPlanner(QueryContext* ctx) : LogicalPlanner(ctx), _ctx(ctx) {}
    ~DDLWorkPlanner() { }
    int plan(); 
    int execute();

    const pb::RegionDdlWork& get_ddlwork() const {
        return _work;
    }

    int set_ddlwork(const pb::RegionDdlWork& work) {
        _work = work;
        _table_id = _work.table_id();
        _index_id = _work.index_id();
        _partition_id = _work.partition();
        _task_id = std::to_string(work.table_id()) + "_" + std::to_string(work.region_id());
        if (work.op_type() == pb::OP_ADD_INDEX) {
            auto index_ptr = SchemaFactory::get_instance()->get_index_info_ptr(_index_id);
            if (index_ptr == nullptr) {
                DB_FATAL("task_%s get index info error.", _task_id.c_str());
                return -1;
            }
            _is_uniq = index_ptr->type == pb::I_UNIQ;
            _is_global_index = index_ptr->is_global;
        }
        _is_column_ddl = (_work.op_type() == pb::OP_MODIFY_FIELD);
        auto pri_index_ptr = SchemaFactory::get_instance()->get_index_info_ptr(_table_id);
        if (pri_index_ptr == nullptr) {
            DB_FATAL("task_%s get index info error.", _task_id.c_str());
            return -1;
        }
        _field_num = pri_index_ptr->fields.size();

        _router_start_key = _work.start_key();
        _router_end_key = _work.end_key();
        DB_NOTICE("process table_id_%ld index_id_%ld _is_global_index:%d field_num %d", _table_id,
            _index_id, _is_global_index, _field_num);
        return 0;
    }

    int create_index_ddl_plan();
    int create_column_ddl_plan();

    int create_txn_dml_node(std::unique_ptr<SingleTxnManagerNode>& tnx_node, std::unique_ptr<ScanNode> scan_node);
    std::unique_ptr<ScanNode> create_scan_node();
private:
    pb::RegionDdlWork _work;
    int64_t _table_id = 0;
    int64_t _index_id = 0;
    QueryContext* _ctx = nullptr;
    bool    _ddl_pk_key_is_full = true;
    std::string _start_key;
    std::string _end_key;
    std::string _router_start_key;
    std::string _router_end_key;
    int64_t _limit = 100;
    int64_t _last_num = 100;
    bool _is_uniq = false;
    bool _is_global_index = false;
    bool _is_column_ddl = false;
    int64_t _partition_id = 0;
    std::string _task_id;
    int32_t _field_num = 0;
    pb::PossibleIndex _pos_index;
};
    
} // namespace baikaldb
