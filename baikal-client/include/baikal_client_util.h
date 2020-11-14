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
 * @file baikal_client_util.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/05 21:04:22
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_UTIL_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_UTIL_H

#include "baikal_client_define.h"
#include <string>
#include <vector>
#include <sys/time.h>
#ifdef BAIDU_INTERNAL
#include "webfoot_naming.h"
#include "naming.pb.h"
#endif

namespace baikal {
namespace client {
// @brief 通过bns提供的服务得到bns上的实例信息
#ifdef BAIDU_INTERNAL
extern int get_instance_from_bns(const std::string& bns_name, std::vector<InstanceInfo>& result);
#endif
extern int get_shard_id_by_bns_tag(std::string tag);

extern int32_t divide_ceil(int32_t dividend, int32_t dividor, bool* result);

class HeapSort {
public:
    HeapSort();
    int init_heap(int64_t count, const int64_t& init_value);
    int64_t get_min_value();
    int64_t heap_down(const int64_t& value);
    void clear();
    ~HeapSort();
private:
    void _min_heapify(int64_t index);

    int64_t* _data;
    int64_t _init_value;
};
}
}
#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_UTIL_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
