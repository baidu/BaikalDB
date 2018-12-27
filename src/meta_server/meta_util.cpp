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

#include "meta_util.h"
#include <boost/lexical_cast.hpp>

namespace baikaldb {
int get_instance_from_bns(int* ret,
                          const std::string& bns_name,
                          std::vector<std::string>& instances,
                          bool need_alive) {
#ifdef BAIDU_INTERNAL
    instances.clear();
    BnsInput input;
    BnsOutput output;
    input.set_service_name(bns_name);
    input.set_type(0);
    *ret = webfoot::get_instance_by_service(input, &output);
    // bns service not exist
    if (*ret == webfoot::WEBFOOT_RET_SUCCESS ||
            *ret == webfoot::WEBFOOT_SERVICE_BEYOND_THRSHOLD) {
        for (int i = 0; i < output.instance_size(); ++i) {
            if (output.instance(i).status() == 0 || !need_alive) {
                instances.push_back(output.instance(i).host_ip() + ":" 
                        + boost::lexical_cast<std::string>(output.instance(i).port()));
            }   
        }   
        return 0;
    }   
    DB_WARNING("get instance from service fail, bns_name:%s, ret:%d",
            bns_name.c_str(), *ret);
    return -1; 
#else
    return -1;
#endif
}

}//namespace
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
