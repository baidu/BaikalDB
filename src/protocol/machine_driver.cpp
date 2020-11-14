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

#include "machine_driver.h"
#include "state_machine.h"
#include <sys/syscall.h>
#include <linux/unistd.h>

namespace baikaldb {

int MachineDriver::init(uint32_t thread_num, std::vector<ThreadTimeStamp> &time_stamp) {
    _time_stamp = &time_stamp;
    for (uint32_t idx = 0; idx < thread_num; ++idx) {
        // thread id initialized to -1
        (*_time_stamp)[idx].first = -1;
        (*_time_stamp)[idx].second = time(NULL);
    }
    _thread_num = thread_num;
    _max_idx = new std::atomic<int>(0);
    return 0;
}

MachineDriver::~MachineDriver() {
    delete _max_idx;
}

void* MachineDriver::bthread_callback(void* void_arg) {
    DriverTask *task = static_cast<DriverTask*>(void_arg);
    std::unique_ptr<DriverTask> cntl_guard(task);

    MachineDriver* _driver = task->driver;

    // // get pthread tid (first column of `ps x`)
    pid_t tid = (pid_t)syscall(SYS_gettid);
    // if (tid >= PID_MAX) {
    //     DB_FATAL("tid: %d overflow PID_MAX", tid);
    //     exit(-1);
    // }
    static thread_local int thread_idx = -1;
    if (thread_idx == -1) {
        _driver->_mutex.lock();
        thread_idx = (*_driver->_max_idx)++;
        _driver->_mutex.unlock();
        DB_NOTICE("tid: %d, idx: %d", tid, thread_idx);
    }

    task->socket->thread_idx = thread_idx;
    if ((uint32_t)thread_idx >= _driver->_thread_num) {
        DB_FATAL("tid: %d, thread_idx: %d, _thread_num: %d", tid, thread_idx, _driver->_thread_num);
        exit(-1);
    }
    // get real thread id only for the first thread call
    if ((*_driver->_time_stamp)[thread_idx].first == -1) {
        (*_driver->_time_stamp)[thread_idx].first = tid;
    }
    (*_driver->_time_stamp)[thread_idx].second = time(NULL);

    StateMachine::get_instance()->run_machine(task->socket, task->epoll_info, task->shutdown);
    task->socket->mutex.unlock();
    task->is_succ = true;
    return NULL;
}

int MachineDriver::dispatch(SmartSocket client, EpollInfo* epoll_info, 
        bool shutdown, bool nonblock) {
    if (nonblock) {
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        DriverTask* task = new DriverTask(client, epoll_info, this, shutdown);
        bthread_start_background(&task->tid, &attr, bthread_callback, task);
    } else {
        DriverTask* task = new DriverTask(client, epoll_info, this, shutdown);
        bthread_callback(task);
    }
    return 0;
}

}
