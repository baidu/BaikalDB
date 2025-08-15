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
#include <google/protobuf/descriptor.h>
#include "proto/meta.interface.pb.h"
#include "common.h"
#include "proto_process.hpp"
#include "meta_server_interact.hpp"
#include <optional>
#include <utility>

namespace baikaldb {

struct TsoRequestTask {
    TsoRequestTask(const pb::TsoRequest* request, pb::TsoResponse* response, BthreadCond* cond, int* send_request_ret)
            : request(request), response(response), cond(cond), send_request_ret(send_request_ret), timeout_flag(false) {}

    explicit TsoRequestTask(int64_t timer_id) : timeout_flag(true), timer_id(timer_id) {}

    const pb::TsoRequest* request;
    pb::TsoResponse* response;
    int* send_request_ret = nullptr;
    BthreadCond* cond;
    bool timeout_flag = false;
    int64_t timer_id = 0;
};

class TsoProxy {
public:
    /**
     * @brief 通过TsoProxy获取tso，对外接口为同步，内部实现为异步
     * @param request tso_request
     * @param response 返回的tso_response
     * @return 操作是否成功
     */
    int get_tso(const pb::TsoRequest& request, pb::TsoResponse& response);

    int init();

    static TsoProxy* get_instance() {
        static TsoProxy instance;
        return &instance;
    }

    void add_to_cached_tasks(const TsoRequestTask& task);

    /**
     * @brief 将多个请求拼成一个，只发送一次rpc
     * @param requests 批量发送的请求
     * @param response rpc响应
     * @return 操作是否成功
     */
    int send_requests(const std::vector<TsoRequestTask>& requests, pb::TsoResponse& response);

    /**
     * @brief 根据rpc的返回结果，按请求加入channel的顺序生成response返回，并 唤醒/decrease 条件变量
     * @param requests 批量发送的请求
     * @param response send_request的响应
     * @param ret send_request的返回值
     * @return 操作是否成功
     */
    static int process_response(const std::vector<TsoRequestTask>& requests, const pb::TsoResponse& response, int ret);

    void reset_timer();

    void add_timeout_flag();

private:
    TsoProxy() : _current_timer_id(0),
                 _tso_request_count_per_batch_avg("tso_request_count_per_batch_avg", &_tso_request_count_per_batch, 1),
                 _tso_count_per_batch_avg("tso_count_per_batch_avg", &_tso_count_per_batch, 1){};

private:
    bool _init_success = false;
    BthreadTimer _timer;
    std::vector<TsoRequestTask> _cached_tasks;
    bthread_mutex_t _mutex;
    int64_t _current_timer_id;

    bvar::IntRecorder _tso_request_count_per_batch;
    bvar::Window<bvar::IntRecorder, bvar::SERIES_IN_SECOND> _tso_request_count_per_batch_avg;

    bvar::IntRecorder _tso_count_per_batch;
    bvar::Window<bvar::IntRecorder, bvar::SERIES_IN_SECOND> _tso_count_per_batch_avg;
};
}
