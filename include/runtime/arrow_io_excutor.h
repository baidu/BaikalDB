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

#include "arrow/util/thread_pool.h"
#include "arrow/util/functional.h"
#include "arrow/util/cancel.h"
#include "common.h"
#include "fragment.h"
#ifdef BAIDU_INTERNAL
#include <bthread.h>
#else
#include <bthread/bthread.h>
#endif
#include <bthread/execution_queue.h>
#include <bthread/condition_variable.h>


/// An Executor implementation spawning tasks in FIFO manner on a fixed-size
/// pool of worker threads.
///
/// Note: Any sort of nested parallelism will deadlock this executor.  Blocking waits are
/// fine but if one task needs to wait for another task it must be expressed as an
/// asynchronous continuation.
namespace baikaldb {
struct IndexCollectorCond {
    int64_t index_cnt = 0;
    BthreadCond cond{0};
};

// 并行执行下, BthreadArrowExecutor有问题, 如acero join执行使用了pthread local
class BthreadArrowExecutor : public arrow::internal::Executor {
public:
    // Construct a thread pool with the given number of worker threads
    static arrow::Result<std::shared_ptr<BthreadArrowExecutor>> Make(int threads);

    // Destroy thread pool; the pool will first be shut down
    ~BthreadArrowExecutor() override;

    // Return the desired number of worker threads.
    // The actual number of workers may lag a bit before being adjusted to
    // match this value.
    int GetCapacity() override;

    // Dynamically change the number of worker threads.
    //
    // This function always returns immediately.
    // If fewer threads are running than this number, new threads are spawned
    // on-demand when needed for task execution.
    // If more threads are running than this number, excess threads are reaped
    // as soon as possible.
    arrow::Status SetCapacity(int threads);

    // Heuristic for the default capacity of a thread pool for CPU-bound tasks.
    // This is exposed as a static method to help with testing.
    static int DefaultCapacity();

    // Shutdown the pool.  Once the pool starts shutting down, new tasks
    // cannot be submitted anymore.
    // If "wait" is true, shutdown waits for all pending tasks to be finished.
    // If "wait" is false, workers are stopped as soon as currently executing
    // tasks are finished.
    arrow::Status Shutdown(bool wait = true);

    struct State;

    std::shared_ptr<BthreadArrowExecutor> MakeSinglePool();

 protected:
    BthreadArrowExecutor();
    arrow::Status SpawnReal(arrow::internal::TaskHints hints, arrow::internal::FnOnce<void()> task, arrow::StopToken,
                    StopCallback&&) override;
    // Collect finished worker threads, making sure the OS threads have exited
    void CollectFinishedWorkersUnlocked();
    // Launch a given number of additional workers
    void LaunchWorkersUnlocked(int threads);
    // Get the current actual capacity
    int GetActualCapacity();
    std::shared_ptr<State> sp_state_;
    State* state_;
};

// pthread
class GlobalArrowExecutor {
public:
    static int init();
    static void execute(RuntimeState* state, arrow::Result<std::shared_ptr<arrow::Table>>* result);
    static void execute_fragment(RuntimeState* state, FragmentInfo* fragment);
};
};