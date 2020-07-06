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

#pragma once

#include "common.h"

#include <atomic>
#include <vector>

#include "proto/meta.interface.pb.h"

namespace baikaldb {

struct DllParam {
    DllParam () = default;
    TimeCost total_cost;
    bool is_start = false;
    uint32_t begin_timestamp = 0;
    bool is_doing = false;
    bool is_waiting = false;
    std::atomic<uint64_t> delete_only_count{0};
    std::atomic<uint64_t> none_count{0};
    std::atomic<uint64_t> public_count{0};
    std::atomic<uint64_t> write_only_count{0};
    std::atomic<uint64_t> write_local_count{0};
    std::atomic<uint64_t> delete_local_count{0};
    int64_t index_id = 0;
    void reset() {
        is_start = false;
        is_doing = false;
        is_waiting = false;
        begin_timestamp = 0;
        total_cost.reset();
        delete_only_count = 0;
        none_count = 0;
        public_count = 0;
        write_only_count = 0;
        write_local_count = 0;
        delete_local_count = 0;
        index_id = 0;
    }
};

class DdlHelper {
public:
static bool ddlwork_is_finish(pb::OpType optype, pb::IndexState state) {
    switch (optype) {
    case pb::OP_ADD_INDEX:
        if (state == pb::IS_PUBLIC) {
            return true;
        }
        break;
    case pb::OP_DROP_INDEX:
        if (state == pb::IS_NONE) {
            return true;
        }
        break;
    default:
        DB_WARNING("UNKNOWN optype [%s]", pb::OpType_Name(optype).c_str());
    }
    return false;
}

static bool can_init_ddlwork(pb::OpType op_type, pb::IndexState state) {
    switch (op_type) {
    case pb::OP_ADD_INDEX:
        if (state == pb::IS_NONE || state == pb::IS_DELETE_LOCAL || state == pb::IS_DELETE_ONLY) {
            return true;
        }
        break;
    case pb::OP_DROP_INDEX:
        if (state != pb::IS_DELETE_LOCAL && state != pb::IS_NONE) {
            return true;
        }
        break;
    default:
        DB_FATAL("unknown op");
    }
    return false;
}
};
}