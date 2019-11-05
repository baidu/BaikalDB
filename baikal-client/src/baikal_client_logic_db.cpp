// Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
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
 
/**
 * @file baikal_client_logic_db.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/16 11:25:18
 * @brief 
 *  
 **/

#include "baikal_client_logic_db.h"
#include <boost/algorithm/string.hpp>
#ifdef BAIDU_INTERNAL
#include "com_log.h"
#endif
#include "shard_operator_mgr.h"

using std::vector;
using std::string;

namespace baikal {
namespace client {
LogicDB::LogicDB(const string& logic_db_name) :
        _name(logic_db_name) {}

int LogicDB::get_table_id(
            const string& table_name, 
            uint32_t partition_key,
            bool* split_table, 
            int* table_id) {
    vector<TableSplit>::iterator iter = _table_infos.begin();
    for (; iter != _table_infos.end(); ++iter) {
        if (iter->table_name == table_name) {
            if (iter->table_split_function.empty()) {
                *split_table = false;
                *table_id = 0;
                return SUCCESS;
            }
            ShardOperatorMgr* mgr = ShardOperatorMgr::get_s_instance();
            uint32_t id = 0;
            int ret = mgr->evaluate(iter->table_split_function, partition_key, &id);
            if (ret < 0) {
                CLIENT_WARNING("table id compute fail, table_name:%s", table_name.c_str());
                return ret;
            }
            *table_id = id;
            *split_table = true;
            return SUCCESS;
        }
    }
    //table_name表在配置文件中未配置，说明不分表
    *split_table = false;
    *table_id = 0;
    return SUCCESS; 
}

string LogicDB::get_name() const {
    return _name;
}

int LogicDB::add_table_split(
        const string& table_name, 
        const vector<string>& table_split_function, 
        int table_split_count) {
    if (table_name.empty() 
            || table_split_count <= 0 
            || (table_split_count != 1 && table_split_function.empty())) {
        CLIENT_WARNING("table split configure is not right");
        return CONFPARAM_ERROR;
    }
    vector<string> names;
    boost::split(names, table_name, boost::is_any_of("|"), boost::token_compress_on);
    vector<string>::iterator it = names.begin();
    for (; it != names.end(); ++it) {
        string name(*it);
        boost::trim(name);
        TableSplit table_split(name, table_split_count, table_split_function);
        _table_infos.push_back(table_split);
    }
    return SUCCESS;
}

vector<TableSplit> LogicDB::get_table_infos() const {
    return _table_infos;
}
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
