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

#include "exec_node.h"
#include "scan_node.h"
#include "table_record.h"
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#include <baidu/rpc/redis.h>
#else
#include <brpc/channel.h>
#include <brpc/redis.h>
#endif

namespace baikaldb {
// 一个支持redis get的简单示例
class RedisScanNode : public ScanNode {
public:
    RedisScanNode() {
    }
    virtual int init(const pb::PlanNode& node);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual int open(RuntimeState* state);
    virtual void close(RuntimeState* state);
private:
    // 对于kv来说，只需要改这个函数，就能支持其他的类似kv需求
    // SmartRecord是kv数据源与BaikalDB的接口，是一个pb封装
    // 与建表的字段一一对应
    int get_by_key(SmartRecord record);

private:
    brpc::Channel _redis_channel;
    std::vector<SmartRecord> _primary_records;
    size_t _idx = 0;
    int64_t _index_id;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
