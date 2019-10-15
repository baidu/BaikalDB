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
#include <string>

namespace baikaldb {
//T是被管理类，Derived是继承ObjectManager的管理器
template <typename T, typename Derived>
class ObjectManager {
public:
    static Derived* instance() {
        static Derived manager;
        return &manager;
    }

    virtual ~ObjectManager() {
    }

    T get_object(const std::string& name) {
        if (_objects.count(name) == 1) {
            return _objects[name];
        } 
        return NULL;
    }

    int register_object(const std::string& name, T object) {
        _objects[name] = object;
        return 0;
    }

protected:
    ObjectManager() {
    }

    std::unordered_map<std::string, T> _objects;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
