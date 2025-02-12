#include "arrow/testing/gtest_util.h"
#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "bthread_arrow_executor.h"

#include "gtest/gtest.h"

#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

template <typename T>
static void task_add(T x, T y, T* out) {
  *out = x + y;
}

template <typename T>
struct task_slow_add {
  void operator()(T x, T y, T* out) {
    bthread_usleep(seconds_ * 1000 * 1000);
    *out = x + y;
  }

  const double seconds_;
};

typedef std::function<void(int, int, int*)> AddTaskFunc;

template <typename T>
static T add(T x, T y) {
  return x + y;
}

template <typename T>
static T slow_add(double seconds, T x, T y) {
  bthread_usleep(seconds * 1000 * 1000);
  return x + y;
}

template <typename T>
static T inplace_add(T& x, T y) {
  return x += y;
}

// A class to spawn "add" tasks to a pool and check the results when done

class AddTester {
 public:
  explicit AddTester(int nadds, arrow::StopToken stop_token = arrow::StopToken::Unstoppable())
      : nadds_(nadds), stop_token_(stop_token), xs_(nadds), ys_(nadds), outs_(nadds, -1) {
    int x = 0, y = 0;
    std::generate(xs_.begin(), xs_.end(), [&] {
      ++x;
      return x;
    });
    std::generate(ys_.begin(), ys_.end(), [&] {
      y += 10;
      return y;
    });
  }

  AddTester(AddTester&&) = default;

  void SpawnTasks(BthreadArrowExecutor* pool, AddTaskFunc add_func) {
    for (int i = 0; i < nadds_; ++i) {
      ASSERT_OK(pool->Spawn([=] { add_func(xs_[i], ys_[i], &outs_[i]); }, stop_token_));
    }
  }

  void CheckResults() {
    for (int i = 0; i < nadds_; ++i) {
      ASSERT_EQ(outs_[i], (i + 1) * 11);
    }
  }

  void CheckNotAllComputed() {
    for (int i = 0; i < nadds_; ++i) {
      if (outs_[i] == -1) {
        return;
      }
    }
    ASSERT_TRUE(0) << "all values were computed";
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(AddTester);

  int nadds_;
  arrow::StopToken stop_token_;
  std::vector<int> xs_;
  std::vector<int> ys_;
  std::vector<int> outs_;
};

class TestBthreadArrowExecutor : public ::testing::Test {
 public:
  void TearDown() override {
    fflush(stdout);
    fflush(stderr);
  }

  std::shared_ptr<BthreadArrowExecutor> MakeThreadPool() { return MakeThreadPool(4); }

  std::shared_ptr<BthreadArrowExecutor> MakeThreadPool(int threads) {
    return *BthreadArrowExecutor::Make(threads);
  }

  void DoSpawnAdds(BthreadArrowExecutor* pool, int nadds, AddTaskFunc add_func,
                   arrow::StopToken stop_token = arrow::StopToken::Unstoppable(),
                   arrow::StopSource* stop_source = nullptr) {
    AddTester add_tester(nadds, stop_token);
    add_tester.SpawnTasks(pool, add_func);
    if (stop_source) {
      stop_source->RequestStop();
    }
    ASSERT_OK(pool->Shutdown());
    if (stop_source) {
      add_tester.CheckNotAllComputed();
    } else {
      add_tester.CheckResults();
    }
  }

  void SpawnAdds(BthreadArrowExecutor* pool, int nadds, AddTaskFunc add_func,
                 arrow::StopToken stop_token = arrow::StopToken::Unstoppable()) {
    DoSpawnAdds(pool, nadds, std::move(add_func), std::move(stop_token));
  }

  void SpawnAddsAndCancel(BthreadArrowExecutor* pool, int nadds, AddTaskFunc add_func,
                          arrow::StopSource* stop_source) {
    DoSpawnAdds(pool, nadds, std::move(add_func), stop_source->token(), stop_source);
  }

  void DoSpawnAddsThreaded(BthreadArrowExecutor* pool, int nthreads, int nadds,
                           AddTaskFunc add_func,
                           arrow::StopToken stop_token = arrow::StopToken::Unstoppable(),
                           arrow::StopSource* stop_source = nullptr) {
    // Same as SpawnAdds, but do the task spawning from multiple threads
    std::vector<AddTester> add_testers;
    std::vector<std::thread> threads;
    for (int i = 0; i < nthreads; ++i) {
      add_testers.emplace_back(nadds, stop_token);
    }
    for (auto& add_tester : add_testers) {
      threads.emplace_back([&] { add_tester.SpawnTasks(pool, add_func); });
    }
    if (stop_source) {
      stop_source->RequestStop();
    }
    for (auto& thread : threads) {
      thread.join();
    }
    ASSERT_OK(pool->Shutdown());
    for (auto& add_tester : add_testers) {
      if (stop_source) {
        add_tester.CheckNotAllComputed();
      } else {
        add_tester.CheckResults();
      }
    }
  }

  void SpawnAddsThreaded(BthreadArrowExecutor* pool, int nthreads, int nadds, AddTaskFunc add_func,
                         arrow::StopToken stop_token = arrow::StopToken::Unstoppable()) {
    DoSpawnAddsThreaded(pool, nthreads, nadds, std::move(add_func),
                        std::move(stop_token));
  }

  void SpawnAddsThreadedAndCancel(BthreadArrowExecutor* pool, int nthreads, int nadds,
                                  AddTaskFunc add_func, arrow::StopSource* stop_source) {
    DoSpawnAddsThreaded(pool, nthreads, nadds, std::move(add_func), stop_source->token(),
                        stop_source);
  }
};

void SleepFor(double seconds) {
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(static_cast<int64_t>(seconds * 1e9)));
}

void BusyWait(double seconds, std::function<bool()> predicate) {
  const double period = 0.001;
  for (int i = 0; !predicate() && i * period < seconds; ++i) {
    SleepFor(period);
  }
}

TEST_F(TestBthreadArrowExecutor, ConstructDestruct) {
  // Stress shutdown-at-destruction logic
  for (int threads : {1, 2, 3, 8, 32, 70}) {
    auto pool = this->MakeThreadPool(threads);
  }
}

// Correctness and stress tests using Spawn() and Shutdown()

TEST_F(TestBthreadArrowExecutor, Spawn) {
  auto pool = this->MakeThreadPool(3);
  SpawnAdds(pool.get(), 7, task_add<int>);
}

TEST_F(TestBthreadArrowExecutor, StressSpawn) {
  auto pool = this->MakeThreadPool(30);
  SpawnAdds(pool.get(), 1000, task_add<int>);
}

TEST_F(TestBthreadArrowExecutor, StressSpawnThreaded) {
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, task_add<int>);
}

TEST_F(TestBthreadArrowExecutor, SpawnSlow) {
  // This checks that Shutdown() waits for all tasks to finish
  auto pool = this->MakeThreadPool(2);
  SpawnAdds(pool.get(), 7, task_slow_add<int>{/*seconds=*/0.02});
}

TEST_F(TestBthreadArrowExecutor, StressSpawnSlow) {
  auto pool = this->MakeThreadPool(30);
  SpawnAdds(pool.get(), 1000, task_slow_add<int>{/*seconds=*/0.002});
}

TEST_F(TestBthreadArrowExecutor, StressSpawnSlowThreaded) {
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, task_slow_add<int>{/*seconds=*/0.002});
}

TEST_F(TestBthreadArrowExecutor, SpawnWithStopToken) {
  arrow::StopSource stop_source;
  auto pool = this->MakeThreadPool(3);
  SpawnAdds(pool.get(), 7, task_add<int>, stop_source.token());
}

TEST_F(TestBthreadArrowExecutor, StressSpawnThreadedWithStopToken) {
  arrow::StopSource stop_source;
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, task_add<int>, stop_source.token());
}

TEST_F(TestBthreadArrowExecutor, SpawnWithStopTokenCancelled) {
  arrow::StopSource stop_source;
  auto pool = this->MakeThreadPool(3);
  SpawnAddsAndCancel(pool.get(), 100, task_slow_add<int>{/*seconds=*/0.02}, &stop_source);
}

TEST_F(TestBthreadArrowExecutor, StressSpawnThreadedWithStopTokenCancelled) {
  arrow::StopSource stop_source;
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreadedAndCancel(pool.get(), 20, 100, task_slow_add<int>{/*seconds=*/0.02},
                             &stop_source);
}

TEST_F(TestBthreadArrowExecutor, QuickShutdown) {
  AddTester add_tester(100);
  {
    auto pool = this->MakeThreadPool(3);
    add_tester.SpawnTasks(pool.get(), task_slow_add<int>{/*seconds=*/0.02});
    ASSERT_OK(pool->Shutdown(false /* wait */));
    add_tester.CheckNotAllComputed();
  }
  add_tester.CheckNotAllComputed();
}

class GatingTask: public std::enable_shared_from_this<GatingTask> {
 public:
  explicit GatingTask(double timeout_seconds)
      : timeout_seconds_(timeout_seconds), status_(), unlocked_(false) {}

  ~GatingTask() {
    if (num_running_ != num_launched_) {
      ADD_FAILURE()
          << "A GatingTask instance was destroyed but some underlying tasks did not "
             "start running"
          << std::endl;
    } else if (num_finished_ != num_launched_) {
      ADD_FAILURE()
          << "A GatingTask instance was destroyed but some underlying tasks did not "
             "finish running"
          << std::endl;
    }
  }

  std::function<void()> Task() {
    num_launched_++;
    auto self = shared_from_this();
    return [self] { self->RunTask(); };
  }

  void RunTask() {
    std::unique_lock<bthread::Mutex> lk(mx_);
    num_running_++;
    running_cv_.notify_all();
    if (!unlocked_) {
        if (ETIMEDOUT == unlocked_cv_.wait_for(
                lk, timeout_seconds_ * 1000 * 1000)) {
        status_ &= arrow::Status::Invalid("Timed out (" + std::to_string(timeout_seconds_) + "," +
                                    std::to_string(unlocked_) +
                                    " seconds) waiting for the gating task to be unlocked");
        }
    }
    num_finished_++;
    finished_cv_.notify_all();
  }

  arrow::Status WaitForRunning(int count) {
    std::unique_lock<bthread::Mutex> lk(mx_);
    while (num_running_ < count) {
        if (ETIMEDOUT == running_cv_.wait_for(
                lk, timeout_seconds_ * 1000 * 1000)) {
            return arrow::Status::Invalid("Timed out waiting for tasks to launch");
        }
    }
    return arrow::Status::OK();
  }

  arrow::Status Unlock() {
    std::lock_guard<bthread::Mutex> lk(mx_);
    unlocked_ = true;
    unlocked_cv_.notify_all();
    return status_;
  }

 private:
  double timeout_seconds_;
  arrow::Status status_;
  bool unlocked_;
  int num_launched_ = 0;
  int num_running_ = 0;
  int num_finished_ = 0;
  bthread::Mutex mx_;
  bthread::ConditionVariable running_cv_;
  bthread::ConditionVariable unlocked_cv_;
  bthread::ConditionVariable finished_cv_;
};


TEST_F(TestBthreadArrowExecutor, SetCapacity) {
  auto pool = this->MakeThreadPool(5);

  // Thread spawning is on-demand
  ASSERT_EQ(pool->GetCapacity(), 5);
  ASSERT_EQ(pool->GetActualCapacity(), 0);

  ASSERT_OK(pool->SetCapacity(3));
  ASSERT_EQ(pool->GetCapacity(), 3);
  ASSERT_EQ(pool->GetActualCapacity(), 0);

  auto gating_task = std::make_shared<GatingTask>(10);

  ASSERT_OK(pool->Spawn(gating_task->Task()));
  ASSERT_OK(gating_task->WaitForRunning(1));
  ASSERT_EQ(pool->GetActualCapacity(), 1);
  ASSERT_OK(gating_task->Unlock());

  gating_task = std::make_shared<GatingTask>(10);
  // Spawn more tasks than the pool capacity
  for (int i = 0; i < 6; ++i) {
    ASSERT_OK(pool->Spawn(gating_task->Task()));
  }
  ASSERT_OK(gating_task->WaitForRunning(3));
  bthread_usleep(1000);  // Sleep a bit just to make sure it isn't making any threads
  ASSERT_EQ(pool->GetActualCapacity(), 3);  // maxxed out

  // The tasks have not finished yet, increasing the desired capacity
  // should spawn threads immediately.
  ASSERT_OK(pool->SetCapacity(5));
  ASSERT_EQ(pool->GetCapacity(), 5);
  ASSERT_EQ(pool->GetActualCapacity(), 5);

  // Thread reaping is eager (but asynchronous)
  ASSERT_OK(pool->SetCapacity(2));
  ASSERT_EQ(pool->GetCapacity(), 2);

  // Wait for workers to wake up and secede
  ASSERT_OK(gating_task->Unlock());
  BusyWait(0.5, [&] { return pool->GetActualCapacity() == 2; });
  ASSERT_EQ(pool->GetActualCapacity(), 2);

  // Downsize while tasks are pending
  ASSERT_OK(pool->SetCapacity(5));
  ASSERT_EQ(pool->GetCapacity(), 5);
  gating_task = std::make_shared<GatingTask>(10);
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(pool->Spawn(gating_task->Task()));
  }
  ASSERT_OK(gating_task->WaitForRunning(5));
  ASSERT_EQ(pool->GetActualCapacity(), 5);

  ASSERT_OK(pool->SetCapacity(2));
  ASSERT_EQ(pool->GetCapacity(), 2);
  ASSERT_OK(gating_task->Unlock());
  BusyWait(0.5, [&] { return pool->GetActualCapacity() == 2; });
  ASSERT_EQ(pool->GetActualCapacity(), 2);

  // Ensure nothing got stuck
  ASSERT_OK(pool->Shutdown());
}

// Test Submit() functionality

TEST_F(TestBthreadArrowExecutor, Submit) {
  auto pool = this->MakeThreadPool(3);
  {
    ASSERT_OK_AND_ASSIGN(arrow::Future<int> fut, pool->Submit(add<int>, 4, 5));
    arrow::Result<int> res = fut.result();
    ASSERT_OK_AND_EQ(9, res);
  }
  {
    ASSERT_OK_AND_ASSIGN(arrow::Future<std::string> fut,
                         pool->Submit(add<std::string>, "foo", "bar"));
    ASSERT_OK_AND_EQ("foobar", fut.result());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto fut, pool->Submit(slow_add<int>, /*seconds=*/0.01, 4, 5));
    ASSERT_OK_AND_EQ(9, fut.result());
  }
  {
    // Reference passing
    std::string s = "foo";
    ASSERT_OK_AND_ASSIGN(auto fut,
                         pool->Submit(inplace_add<std::string>, std::ref(s), "bar"));
    ASSERT_OK_AND_EQ("foobar", fut.result());
    ASSERT_EQ(s, "foobar");
  }
  {
    // `void` return type
    ASSERT_OK_AND_ASSIGN(auto fut, pool->Submit([]{
        bthread_usleep(1000);
    }));
    ASSERT_OK(fut.status());
  }
}

TEST_F(TestBthreadArrowExecutor, SubmitWithStopToken) {
  auto pool = this->MakeThreadPool(3);
  {
    arrow::StopSource stop_source;
    ASSERT_OK_AND_ASSIGN(arrow::Future<int> fut,
                         pool->Submit(stop_source.token(), add<int>, 4, 5));
    arrow::Result<int> res = fut.result();
    ASSERT_OK_AND_EQ(9, res);
  }
}

TEST_F(TestBthreadArrowExecutor, SubmitWithStopTokenCancelled) {
  auto pool = this->MakeThreadPool(3);
  {
    const int n_futures = 100;
    arrow::StopSource stop_source;
    arrow::StopToken stop_token = stop_source.token();
    std::vector<arrow::Future<int>> futures;
    for (int i = 0; i < n_futures; ++i) {
      ASSERT_OK_AND_ASSIGN(
          auto fut, pool->Submit(stop_token, slow_add<int>, 0.01 /*seconds*/, i, 1));
      futures.push_back(std::move(fut));
    }
    SleepFor(0.05);  // Let some work finish
    stop_source.RequestStop();
    int n_success = 0;
    int n_cancelled = 0;
    for (int i = 0; i < n_futures; ++i) {
      arrow::Result<int> res = futures[i].result();
      if (res.ok()) {
        ASSERT_EQ(i + 1, *res);
        ++n_success;
      } else {
        ASSERT_RAISES(Cancelled, res);
        ++n_cancelled;
      }
    }
    ASSERT_GT(n_success, 0);
    ASSERT_GT(n_cancelled, 0);
  }
}