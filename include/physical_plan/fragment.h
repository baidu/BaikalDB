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
#include "runtime_state.h"
#ifdef BAIDU_INTERNAL
#include <bthread.h>
#else
#include <bthread/bthread.h>
#endif
#include <bthread/execution_queue.h>
#include <bthread/condition_variable.h>

namespace baikaldb {

struct FragmentExecutor {
    bthread::Mutex mu;
    bthread::ConditionVariable cond;
    bool done = false;
    arrow::Result<std::shared_ptr<arrow::Table>> result;
    arrow::Future<std::shared_ptr<arrow::Table>> future;
    std::shared_ptr<arrow::Table> table;

    void wait() {
        std::unique_lock<decltype(mu)> lock(mu);
        if (!done) {
            cond.wait(lock);
        }
    }
};

struct FragmentInfo {
    uint64_t log_id = 0;
    int fragment_id = 0;                                    // fragment id
    uint64_t fragment_instance_id = 0;                      // fragment instance id

    NodePartitionProperty* partition_property = nullptr;    // 本fragment的分区属性
    FragmentInfo* parent = nullptr;                         // 上游fragment

    ExecNode* root = nullptr;                               // 本fragment的根节点, [PacketNode or ExchangeSender]
    std::vector<ExecNode*> receivers;                       // receiver和下游fragment一一对应
    std::vector<std::shared_ptr<FragmentInfo>> children;    // 下游fragment

    RuntimeState* runtime_state = nullptr;                  // 本fragment的RuntimeState
    RuntimeState* last_runtime_state = nullptr;             // 本fragment最后一个dualscannode的ctx
    SmartState  smart_state = nullptr;                      // 非主db中用到,自动析构

    std::shared_ptr<FragmentExecutor> executor;             // 执行本fragment的线程

    TimeCost time_cost;
    int64_t open_cost = 0;
    int64_t exec_cost = 0;

    arrow::Result<std::shared_ptr<arrow::Table>> wait() {
        if (executor) {
            executor->wait();
            return executor->result;
        }
        return arrow::Result<std::shared_ptr<arrow::Table>>(arrow::Status::IOError("invalid executor"));
    }

    void set_open_cost() {
        open_cost = time_cost.get_time();
        time_cost.reset();
    }

    void set_exec_cost() {
        exec_cost = time_cost.get_time();
        time_cost.reset();
    }

    void close() {
        for (auto& c : children) {
            c->close();
        }
        children.clear();
    }
};

using SmartFragment = std::shared_ptr<FragmentInfo>;

struct ExecQueryFragments {
    int64_t start_time = 0;
    bool is_main_db = false;
    std::vector<SmartFragment> fragments;
    int doing_cnt = 0;

    ExecQueryFragments(int64_t t, bool is_main_db, std::vector<SmartFragment> fragments) 
            : is_main_db(is_main_db), fragments(fragments), start_time(t) {
        doing_cnt = fragments.size();
    }

    void close() {
        if (is_main_db) {
            return;
        }
        for (auto& fragment : fragments) {
            fragment->wait();
            if (fragment->root != nullptr) {
                fragment->root->close(fragment->runtime_state);
                ExecNode::destroy_tree(fragment->root);
            }
        }
        fragments.clear();
    }
};

}