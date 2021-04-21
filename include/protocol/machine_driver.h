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

#include <atomic>
#include <mutex>
#include "common.h"
#include "network_socket.h"
#include "epoll_info.h"

namespace baikaldb {

class DriverTask;
class DriverTaskHandler;
class MachineDriver;
//const int PID_MAX = 65536;

typedef std::shared_ptr<DriverTask> SmartDriverTask;

// 将ExecutorUnit封装成可以执行的任务
struct DriverTask {
    // 任务相关
    SmartSocket     socket;            // client socket
    EpollInfo*      epoll_info;        // Epoll info and fd mapping.
    bthread_t       tid;

    // 同步相关
    MachineDriver*  driver;            // 调度器

    bool shutdown;

    bool is_succ;                     // 任务处理是否成功

    // 当前任务组完成数量，用于同步，当前任务完成时会将该变量加1
    //boost::atomic<uint32_t>& count;
    // 同步条件，任务分配完后，主线程阻塞等待子任务完成
    // 当所有子任务完成时，最后一个子任务唤醒主线程
    //boost::condition_variable& cond;
    //size_t executor_units_size;             // 任务总数
    DriverTask(
            SmartSocket         _sock,
            EpollInfo*          _epoll,
            MachineDriver*      _driver,
            bool                _shutdown) :
        socket(_sock),
        epoll_info(_epoll),
        driver(_driver),
        shutdown(_shutdown),
        is_succ(false) {}
        //count(_count),
        //cond(_cond) {}

    ~DriverTask() {}
};

// 任务调度器
class MachineDriver {
public:
    virtual ~MachineDriver();

    static MachineDriver* get_instance() {
        static MachineDriver _driver;
        return &_driver;
    }

    static void* bthread_callback(void* void_arg);

    int init(uint32_t thread_num);
    void stop() {}
    void join() {}

    int dispatch(SmartSocket client, EpollInfo* epoll_info, bool shutdown, bool nonblock = true);

    size_t task_size() {
        //return _pool.task_size();
        return 0;
    }

public:
    // map from tid(sys allocated) to thread index (0 to n-1) 
    std::mutex                      _mutex;
    std::atomic<int>*               _max_idx;
    uint32_t                        _thread_num;

private:
    explicit MachineDriver() {}
};

}  // namespace baikal

