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

#include "arrow_io_excutor.h"
#include <algorithm>
#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "exchange_sender_node.h"

namespace baikaldb {
static bool validate_arrow_multi_threads(const char*, int32_t val) {
    return val >= 2;
}

DEFINE_int32(arrow_multi_threads, 10, "arrow_use_multi_thread, default 10");
const int ALLOW_UNUSED register_FLAGS_bthread_concurrency = 
    ::google::RegisterFlagValidator(&FLAGS_arrow_multi_threads, validate_arrow_multi_threads);

struct BthreadArrowExecutorTask {
    arrow::internal::FnOnce<void()> callable;
    arrow::StopToken stop_token;
    arrow::internal::Executor::StopCallback stop_callback;
};

struct BthreadArrowExecutor::State {
    State() = default;

    // NOTE: in case locking becomes too expensive, we can investigate lock-free FIFOs
    // such as https://github.com/cameron314/concurrentqueue

    bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;
    bthread::ConditionVariable cv_shutdown_;

    std::list<Bthread> workers_;
    // Trashcan for finished threads
    std::vector<Bthread> finished_workers_;
    std::deque<BthreadArrowExecutorTask> pending_tasks_;

    // Desired number of threads
    int desired_capacity_ = 0;

    // Total number of tasks that are either queued or running
    int tasks_queued_or_running_ = 0;

    // Are we shutting down?
    bool please_shutdown_ = false;
    bool quick_shutdown_ = false;
};

// The worker loop is an independent function so that it can keep running
// after the ThreadPool is destroyed.
static void WorkerLoop(
        std::shared_ptr<BthreadArrowExecutor::State> state,
        std::list<Bthread>::iterator it) {
    std::unique_lock<bthread::Mutex> lock(state->mutex_);

    // Since we hold the lock, `it` now points to the correct thread object
    // (LaunchWorkersUnlocked has exited)
    DCHECK_EQ(bthread_self(), it->id());

    // If too many threads, we should secede from the pool
    const auto should_secede = [&]() -> bool {
        return state->workers_.size() > static_cast<size_t>(state->desired_capacity_);
    };

    while (true) {
        // By the time this thread is started, some tasks may have been pushed
        // or shutdown could even have been requested.  So we only wait on the
        // condition variable at the end of the loop.

        // Execute pending tasks if any
        while (!state->pending_tasks_.empty() && !state->quick_shutdown_) {
            // We check this opportunistically at each loop iteration since
            // it releases the lock below.
            if (should_secede()) {
                break;
            }

            DCHECK_GE(state->tasks_queued_or_running_, 0);
            {
                BthreadArrowExecutorTask task = std::move(state->pending_tasks_.front());
                state->pending_tasks_.pop_front();
                arrow::StopToken* stop_token = &task.stop_token;
                lock.unlock();
                if (!stop_token->IsStopRequested()) {
                    std::move(task.callable)();
                } else {
                    if (task.stop_callback) {
                        std::move(task.stop_callback)(stop_token->Poll());
                    }
                }
                ARROW_UNUSED(std::move(task));  // release resources before waiting for lock
                lock.lock();
            }
            state->tasks_queued_or_running_--;
        }
        // Now either the queue is empty *or* a quick shutdown was requested
        if (state->please_shutdown_ || should_secede()) {
            break;
        }
        // Wait for next wakeup
        state->cv_.wait(lock);
    }
    DCHECK_GE(state->tasks_queued_or_running_, 0);

    // We're done.  Move our thread object to the trashcan of finished
    // workers.  This has two motivations:
    // 1) the thread object doesn't get destroyed before this function finishes
    //    (but we could call thread::detach() instead)
    // 2) we can explicitly join() the trashcan threads to make sure all OS threads
    //    are exited before the ThreadPool is destroyed.  Otherwise subtle
    //    timing conditions can lead to false positives with Valgrind.
    DCHECK_EQ(bthread_self(), it->id());
    state->finished_workers_.push_back(std::move(*it));
    state->workers_.erase(it);
    if (state->please_shutdown_) {
        // Notify the function waiting in Shutdown().
        state->cv_shutdown_.notify_one();
    }
}

BthreadArrowExecutor::BthreadArrowExecutor() :
        sp_state_(std::make_shared<BthreadArrowExecutor::State>()), state_(sp_state_.get()) {}

BthreadArrowExecutor::~BthreadArrowExecutor() {
    ARROW_UNUSED(Shutdown(false /* wait */));
}

arrow::Status BthreadArrowExecutor::SetCapacity(int threads) {
    std::unique_lock<bthread::Mutex> lock(state_->mutex_);
    if (state_->please_shutdown_) {
        return arrow::Status::Invalid("operation forbidden during or after shutdown");
    }
    if (threads <= 0) {
        return arrow::Status::Invalid("ThreadPool capacity must be > 0");
    }
    CollectFinishedWorkersUnlocked();

    state_->desired_capacity_ = threads;
    // See if we need to increase or decrease the number of running threads
    const int required = std::min(
            static_cast<int>(state_->pending_tasks_.size()),
            threads - static_cast<int>(state_->workers_.size()));
    if (required > 0) {
        // Some tasks are pending, spawn the number of needed threads immediately
        LaunchWorkersUnlocked(required);
    } else if (required < 0) {
        // Excess threads are running, wake them so that they stop
        state_->cv_.notify_all();
    }
    return arrow::Status::OK();
}

int BthreadArrowExecutor::GetCapacity() {
    std::unique_lock<bthread::Mutex> lock(state_->mutex_);
    return state_->desired_capacity_;
}

int BthreadArrowExecutor::GetActualCapacity() {
    std::unique_lock<bthread::Mutex> lock(state_->mutex_);
    return static_cast<int>(state_->workers_.size());
}

arrow::Status BthreadArrowExecutor::Shutdown(bool wait) {
    std::unique_lock<bthread::Mutex> lock(state_->mutex_);

    if (state_->please_shutdown_) {
        return arrow::Status::Invalid("Shutdown() already called");
    }
    state_->please_shutdown_ = true;
    state_->quick_shutdown_ = !wait;
    state_->cv_.notify_all();
    while (!state_->workers_.empty()) {
        state_->cv_shutdown_.wait(lock);
    }
    if (!state_->quick_shutdown_) {
        DCHECK_EQ(state_->pending_tasks_.size(), 0);
    } else {
        state_->pending_tasks_.clear();
    }
    CollectFinishedWorkersUnlocked();
    return arrow::Status::OK();
}

void BthreadArrowExecutor::CollectFinishedWorkersUnlocked() {
    for (auto& thread : state_->finished_workers_) {
        // Make sure OS thread has exited
        thread.join();
    }
    state_->finished_workers_.clear();
}

void BthreadArrowExecutor::LaunchWorkersUnlocked(int threads) {
    std::shared_ptr<State> state = sp_state_;

    for (int i = 0; i < threads; i++) {
        state_->workers_.emplace_back();
        auto it = --(state_->workers_.end());
        *it = Bthread([state, it] { WorkerLoop(state, it); });
    }
}

arrow::Status BthreadArrowExecutor::SpawnReal(
        arrow::internal::TaskHints hints,
        arrow::internal::FnOnce<void()> task,
        arrow::StopToken stop_token,
        StopCallback&& stop_callback) {
    {
        std::lock_guard<bthread::Mutex> lock(state_->mutex_);
        if (state_->please_shutdown_) {
            return arrow::Status::Invalid("operation forbidden during or after shutdown");
        }
        CollectFinishedWorkersUnlocked();
        state_->tasks_queued_or_running_++;
        if (static_cast<int>(state_->workers_.size()) < state_->tasks_queued_or_running_ &&
            state_->desired_capacity_ > static_cast<int>(state_->workers_.size())) {
            // We can still spin up more workers so spin up a new worker
            LaunchWorkersUnlocked(/*threads=*/1);
        }
        state_->pending_tasks_.push_back(
                {std::move(task), std::move(stop_token), std::move(stop_callback)});
    }
    state_->cv_.notify_one();
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<BthreadArrowExecutor>> BthreadArrowExecutor::Make(int threads) {
    auto pool = std::shared_ptr<BthreadArrowExecutor>(new BthreadArrowExecutor());
    RETURN_NOT_OK(pool->SetCapacity(threads));
    return pool;
}

int GlobalArrowExecutor::init() {
    int thread_num = FLAGS_arrow_multi_threads;
    int default_capacity = arrow::GetCpuThreadPoolCapacity();
    auto s = arrow::SetCpuThreadPoolCapacity(thread_num);
    if (!s.ok()) {
        DB_FATAL("set cpu thread pool capacity failed, thread_num: %d", thread_num);
        return -1;
    }
    DB_NOTICE("set cpu thread pool capacity: %d -> %d", default_capacity, thread_num);
    return 0;
}

void GlobalArrowExecutor::execute(RuntimeState* state, arrow::Result<std::shared_ptr<arrow::Table>>* result) {
    if (state->vectorlized_parallel_execution == false) {
        // 不开启pipeline并行
        *result = arrow::acero::DeclarationToTable(arrow::acero::Declaration::Sequence(std::move(state->acero_declarations)), false); 
        state->vectorlized_parallel_execution = false;
    } else {
        // 开启pipeline并行, 异步模式
        arrow::compute::ExecContext exec_context(arrow::default_memory_pool(), arrow::internal::GetCpuThreadPool());
        exec_context.set_use_threads(true);
        bthread::Mutex mu;
        bthread::ConditionVariable cond;
        bool done = false;
        auto table_future = arrow::acero::DeclarationToTableAsync(arrow::acero::Declaration::Sequence(std::move(state->acero_declarations)), exec_context);
        table_future.AddCallback([&] (arrow::Result<std::shared_ptr<arrow::Table>> result_) {
            *result = std::move(result_);
            std::lock_guard<bthread::Mutex> lock(mu);
            done = true;
            cond.notify_one();
        });
        {
            std::unique_lock<decltype(mu)> lock(mu);
            if (!done) {
                cond.wait(lock);
            }
        }
        state->vectorlized_parallel_execution = true;
    }
}
}  // namespace baikaldb
