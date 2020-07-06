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

#include <unordered_map>
#include "common.h"
#include "mysql_err_code.h"

namespace baikaldb {

struct MysqlErrorItem {
    int  err_code;
    std::string err_name;
    std::string state_odbc;
    std::string state_jdbc;

};

class MysqlErrHandler {
public:
    virtual ~MysqlErrHandler();

    static MysqlErrHandler* get_instance() {
        static MysqlErrHandler err_handler;
        return &err_handler;
    }

    bool init();

    MysqlErrorItem* get_error_item_by_code(MysqlErrCode code);

private:
    MysqlErrHandler() {};
    MysqlErrHandler& operator=(const MysqlErrHandler& other);

    std::unordered_map<int, MysqlErrorItem*> _error_mapping;
    bool _is_init = false;
};

} // namespace baikal

