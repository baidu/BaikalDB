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
 * @file baikal_client_logic_db.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/09 14:31:01
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_LOGIC_DB_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_LOGIC_DB_H

#include <string>
#include <vector>
#include "baikal_client_define.h"

namespace baikal {
namespace client {
class LogicDB {

public:
    LogicDB(const std::string& logic_db_name);
    
    //split_table, true, 代表分表，分表id为table_id，false，代表不分表
    int get_table_id(const std::string& table_name,
            uint32_t partition_key,
            bool* split_table,
            int* table_id);
    
    std::string get_name() const;
    
    int add_table_split(
            const std::string& table_name,
            const std::vector<std::string>& table_split_function,
            int table_split_count);
    
    std::vector<TableSplit> get_table_infos() const;
private:
    std::string _name; // 逻辑库名
    std::vector<TableSplit> _table_infos; //该库的分表信息
};
}
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_LOGIC_DB_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
