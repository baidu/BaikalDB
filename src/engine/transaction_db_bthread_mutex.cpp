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

#ifdef BAIDU_INTERNAL
#include <bthread.h>
#else
#include <bthread/bthread.h>
#endif
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include "transaction_db_bthread_mutex.h"

namespace baikaldb {

class TransactionDBBthreadMutex : public rocksdb::TransactionDBMutex {
public:
    TransactionDBBthreadMutex() {}
    ~TransactionDBBthreadMutex() override {}

    rocksdb::Status Lock() override;

    rocksdb::Status TryLockFor(int64_t timeout_time) override;

    void UnLock() override { _mutex.unlock(); }

    friend class TransactionDBBthreadCond;

private:
    bthread::Mutex _mutex;
};

class TransactionDBBthreadCond : public rocksdb::TransactionDBCondVar {
public:
    TransactionDBBthreadCond() {}
    ~TransactionDBBthreadCond() override {}

    rocksdb::Status Wait(std::shared_ptr<rocksdb::TransactionDBMutex> mutex) override;

    rocksdb::Status WaitFor(std::shared_ptr<rocksdb::TransactionDBMutex> mutex,
                    int64_t timeout_time) override;

    void Notify() override { _cv.notify_one(); }

    void NotifyAll() override { _cv.notify_all(); }

private:
    bthread::ConditionVariable _cv;
};

std::shared_ptr<rocksdb::TransactionDBMutex>
TransactionDBBthreadFactory::AllocateMutex() {
    return std::shared_ptr<rocksdb::TransactionDBMutex>(new TransactionDBBthreadMutex());
}

std::shared_ptr<rocksdb::TransactionDBCondVar>
TransactionDBBthreadFactory::AllocateCondVar() {
    return std::shared_ptr<rocksdb::TransactionDBCondVar>(new TransactionDBBthreadCond());
}

rocksdb::Status TransactionDBBthreadMutex::Lock() {
    _mutex.lock();
    return rocksdb::Status::OK();
}

rocksdb::Status TransactionDBBthreadMutex::TryLockFor(int64_t timeout_time) {
    bool locked = true;

    if (timeout_time == 0) {
        locked = _mutex.try_lock();
    } else {
        _mutex.lock();
    }

    if (!locked) {
        // timeout acquiring mutex
        return rocksdb::Status::TimedOut(rocksdb::Status::SubCode::kMutexTimeout);
    }

    return rocksdb::Status::OK();
}

rocksdb::Status TransactionDBBthreadCond::Wait(
    std::shared_ptr<rocksdb::TransactionDBMutex> mutex) {
    auto bthread_mutex = reinterpret_cast<TransactionDBBthreadMutex*>(mutex.get());

    std::unique_lock<bthread_mutex_t> lock(*(bthread_mutex->_mutex.native_handler()), std::adopt_lock);
    _cv.wait(lock);

    // Make sure unique_lock doesn't unlock mutex when it destructs
    lock.release();

    return rocksdb::Status::OK();
}

rocksdb::Status TransactionDBBthreadCond::WaitFor(
    std::shared_ptr<rocksdb::TransactionDBMutex> mutex, int64_t timeout_time) {
    rocksdb::Status s;

    auto bthread_mutex = reinterpret_cast<TransactionDBBthreadMutex*>(mutex.get());
    std::unique_lock<bthread_mutex_t> lock(*(bthread_mutex->_mutex.native_handler()), std::adopt_lock);

    if (timeout_time < 0) {
        // If timeout is negative, do not use a timeout
        _cv.wait(lock);
    } else {
        // auto duration = std::chrono::microseconds(timeout_time);
        auto cv_status = _cv.wait_for(lock, timeout_time);

        // Check if the wait stopped due to timing out.
        if (cv_status == ETIMEDOUT) {
            s = rocksdb::Status::TimedOut(rocksdb::Status::SubCode::kMutexTimeout);
        }
    }

    // Make sure unique_lock doesn't unlock mutex when it destructs
    lock.release();

    // CV was signaled, or we spuriously woke up (but didn't time out)
    return s;
}

}  // namespace baikaldb