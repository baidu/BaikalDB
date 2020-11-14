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

#include <my_raft_log.h>
#include <my_raft_log_storage.h>
#include <my_raft_meta_storage.h>
#include <pthread.h> 

namespace baikaldb {

static pthread_once_t g_register_once = PTHREAD_ONCE_INIT;

struct MyRaftExtension {
    MyRaftLogStorage my_raft_log_storage;
    MyRaftLogStorage my_bin_log_storage;
    MyRaftMetaStorage my_raft_meta_storage;
};

static void register_once_or_die() {
    static MyRaftExtension* s_ext = new MyRaftExtension;
    braft::log_storage_extension()->RegisterOrDie("myraftlog", &s_ext->my_raft_log_storage);
    braft::log_storage_extension()->RegisterOrDie("mybinlog", &s_ext->my_bin_log_storage);
#ifdef BAIDU_INTERNAL
    braft::stable_storage_extension()->RegisterOrDie("myraftmeta", &s_ext->my_raft_meta_storage);
#else
    braft::meta_storage_extension()->RegisterOrDie("myraftmeta", &s_ext->my_raft_meta_storage);
#endif
    DB_WARNING("Registered myraft extension");
}

int register_myraft_extension() {
    return pthread_once(&g_register_once, register_once_or_die);
}

} //namespace raft
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
