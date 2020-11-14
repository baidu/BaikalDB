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
 * @file global.h
 * @author liuhuicong(com@baidu.com)
 * @date 2016/02/16 14:35:20
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_GLOBAL_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_GLOBAL_H

#include <string>

namespace baikal {
namespace client {
extern char* INSTANCE_STATUS_CSTR[5];
extern int DEFAULT_READ_TIMEOUT;
extern int DEFAULT_WRITE_TIMEOUT;
extern int DEFAULT_CONNECT_TIMEOUT;
extern std::string DEFAULT_CHARSET;
extern char* CONN_TYPE_CSTR[3];    
}
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_GLOBAL_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
