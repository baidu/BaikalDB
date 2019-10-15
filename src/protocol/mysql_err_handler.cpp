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

#include "mysql_err_handler.h"
#include "mysql_err_item.h"

namespace baikaldb {

bool MysqlErrHandler::init() {
    if (_is_init) {
        return true;
    }
    for (auto& item : mysql_err_item) {
        if (item.state_odbc == "" && item.state_jdbc == "") {
            item.state_odbc = "HY000";
            item.state_jdbc = "";
        }
        _error_mapping[item.err_code] = &item;
    }
    _is_init = true;
    return true;
}

MysqlErrHandler::~MysqlErrHandler() {}

MysqlErrorItem* MysqlErrHandler::get_error_item_by_code(MysqlErrCode code) {
    if (_error_mapping.count(code) == 0) {
        return nullptr;
    }
    return _error_mapping[code];
}
} // end of namespace baikal
