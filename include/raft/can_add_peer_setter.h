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
#include <cstdint>

namespace baikaldb {
class CanAddPeerSetter {
public:
    virtual ~CanAddPeerSetter() {}

    static CanAddPeerSetter* get_instance() {
        static CanAddPeerSetter _instance;
        return &_instance;
    }
    void set_can_add_peer(int64_t region_id); 
private:
    CanAddPeerSetter() {}
};
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
