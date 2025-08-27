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

#include "tso_proxy.h"

namespace baikaldb {
DEFINE_int64(max_tso_requests_per_batch, 100, "max_tso_requests_per_batch. default(10000)");
DEFINE_int64(batch_tso_interval_ms, 0, "batch tso wait interval in ms, [0,10]ms, <=0 means no batch tso");

int TsoProxy::init() {
    bthread_mutex_init(&_mutex, NULL);
    _current_timer_id = 0;
    _init_success = true;
    reset_timer();
    return 0;
}

void TsoProxy::reset_timer() {
    if (FLAGS_batch_tso_interval_ms <= 0) {
        return;
    }
    int64_t timer_id = ++_current_timer_id;
    _timer.stop();
    _timer.run(FLAGS_batch_tso_interval_ms,
               [this, timer_id](){
                    add_to_cached_tasks(TsoRequestTask(timer_id));
                });
}

int TsoProxy::get_tso(const pb::TsoRequest& request, pb::TsoResponse& response) {
    if (FLAGS_batch_tso_interval_ms <= 0) {
        // interval小于等于0直接退化为同步处理
        return MetaServerInteract::get_tso_instance()->send_request("tso_service", request, response);
    }
    BthreadCond cond;
    int ret = 0;
    TsoRequestTask request_task(&request, &response, &cond, &ret);
    add_to_cached_tasks(request_task);
    cond.increase_wait();
    return ret;
}

int TsoProxy::send_requests(const std::vector<TsoRequestTask>& requests, pb::TsoResponse& response) {
    if (requests.empty()) {
        return 1;
    }
    pb::TsoRequest request;
    request.CopyFrom(*requests.front().request);
    int64_t count = 0;
    for (const auto& it: requests) {
        count += it.request->count();
    }
    request.set_count(count);
//    DB_NOTICE("tsoproxy send tso_request, requests count: %ld, count: %ld", requests.size(), count);
    _tso_request_count_per_batch << requests.size();
    _tso_count_per_batch << count;
    return MetaServerInteract::get_tso_instance()->send_request("tso_service", request, response);
}

int TsoProxy::process_response(const std::vector<TsoRequestTask>& requests, const pb::TsoResponse& response, int ret) {
    pb::TsoTimestamp tso_timestamp;
    bool success = ret == 0 && response.errcode() == pb::SUCCESS;
    if (success) {
        tso_timestamp.CopyFrom(response.start_timestamp());
    }
    for (auto& it: requests) {
        if (it.send_request_ret != nullptr) {
            // it.send_request_ret should never br nullptr!
            if (ret != 0) {
                *it.send_request_ret = ret;
            } else if (success) {
                *it.send_request_ret = 0;
            } else {
                *it.send_request_ret = -1;
            }
        }
        if (success) {
            it.response->CopyFrom(response);
            auto tso_timestamp_resp = it.response->mutable_start_timestamp();
            tso_timestamp_resp->set_physical(tso_timestamp.physical());
            tso_timestamp_resp->set_logical(tso_timestamp.logical());
            it.response->set_count(it.request->count());
            tso_timestamp.set_logical(tso_timestamp.logical() + it.request->count());
        }
        it.cond->decrease_signal();
    }
    return 0;
}

void TsoProxy::add_to_cached_tasks(const TsoRequestTask& task) {
    std::vector<TsoRequestTask> requests;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (task.timeout_flag) {
            if (task.timer_id != _current_timer_id) {
                // 此时一定有一个更晚启动的定时器在执行
                return;
            }
        } else {
            _cached_tasks.emplace_back(task);
            if (_cached_tasks.size() < FLAGS_max_tso_requests_per_batch) {
                return;
            }
        }

        requests.swap(_cached_tasks);
        // 到这一步不管是因为达到最大batch还是超时，都需要重置timer
        reset_timer();
    }

    if (requests.empty()) {
        return;
    }
    auto process_batch_requests = [requests, this](){
        pb::TsoResponse response;
        // 不重试，重试逻辑在FetchStore::get_tso
        int ret = send_requests(requests, response);
        if (ret == 1) {
            return ;
        }
        if (ret != 0) {
            DB_WARNING("batch get tso failed, batch size: %ld", requests.size());
        }
        process_response(requests, response, ret);
    };
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run(process_batch_requests);
}
}
