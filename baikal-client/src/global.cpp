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
 * @file ../src/global.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2016/02/16 14:35:39
 * @brief 
 *  
 **/
#include "global.h"

namespace baikal {
namespace client {

char* INSTANCE_STATUS_CSTR[] = {
    "NONE",
    "ON_LINE",
    "OFF_LINE",
    "FAULTY",
    "DELAY"
};
char* CONN_TYPE_CSTR[] = {
    "NONE",
    "MYSQL CONNECTION",
    "REDIS CONNECTION"
};

int DEFAULT_READ_TIMEOUT = 10000;
int DEFAULT_WRITE_TIMEOUT = 10000;
int DEFAULT_CONNECT_TIMEOUT = 10;
std::string DEFAULT_CHARSET = "gbk";

}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
