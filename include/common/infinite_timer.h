// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include <sys/time.h>
#include <map>
#include <vector>
#include <string>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include "common.h"

namespace baikaldb {

//定时器超时回调函数
typedef void* (*timer_func)(void* param);

//使用boost::asio库实现的可多次超时的定时器
//超时回调函数可以在相同线程或者子线程中执行
//interval: 超时间隔，单位秒
//is_other_thread: 是否在子线程中启动回调函数
class InfiniteTimer {
public:
    InfiniteTimer(int interval, 
                  boost::asio::io_service& ios, 
                  timer_func func, 
                  void* param, 
                  bool is_other_thread) : 
    _interval(interval),
    _timer(ios, boost::posix_time::seconds(interval)),
    _func(func),
    _param(param),
    _is_other_thread(is_other_thread) {
        _timer.async_wait(boost::bind(&InfiniteTimer::call_func, 
                                      this, 
                                      boost::asio::placeholders::error));
    }

    virtual ~InfiniteTimer() {
    }

private:
    //超时回调函数，根据is_other_thread参数的设置，call_func会在主线程或者子线程中
    //调用外部回调函数 void* (*timer_func)(void* param);
    void call_func(const boost::system::error_code& e) {
        DB_TRACE("[time trace] start timer [otherthread %d]", _is_other_thread);
        if (_is_other_thread) {
            bthread_t tid;
            if (bthread_start_background(&tid, NULL, _func, _param) != 0) {
                DB_WARNING("[timer error]start timer _func thread error");
            }
        } else {
            _func(_param);
        }
        _timer.expires_at(_timer.expires_at() + boost::posix_time::seconds(_interval));
        _timer.async_wait(boost::bind(&InfiniteTimer::call_func, 
                                      this, 
                                      boost::asio::placeholders::error));
    }
private:
    int _interval;
    boost::asio::deadline_timer _timer;
    timer_func _func;
    void* _param;
    bool _is_other_thread;
};
} //namespace baikaldb 

/* vim: set ts=4 sw=4 sts=4 tw=100 */
