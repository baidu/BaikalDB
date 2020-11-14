
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

#include "store_interact.hpp"
#include <gflags/gflags.h>

namespace baikaldb {
DEFINE_int32(store_request_timeout, 60000, 
            "store as server request timeout, default:60000ms");
DEFINE_int32(store_connect_timeout, 5000, 
            "store as server connect timeout, default:5000ms");
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
