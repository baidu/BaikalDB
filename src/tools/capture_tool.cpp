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

#include <chrono>
#include <thread>
#include <gflags/gflags.h>
// #include "datetime.h"
#include "baikal_capturer.h"

DEFINE_int64(start_before_now_h, 0, "partition id");
DEFINE_int64(start_ts, 0, "partition id");
DEFINE_int64(end_ts, 0, "partition id");
DEFINE_int64(sleep_ms, 100, "partition id");
DEFINE_int64(concurrency, 1, "partition id");
DEFINE_int64(fetch_num, 10000, "partition id");
DEFINE_bool(is_debug, false, "is_debug");
DECLARE_bool(capture_print_event);
DEFINE_string(skip_region_ids, "", "skip_region_ids");

namespace baikaldb {
    DECLARE_string(meta_server_bns);
}

struct BaikaldbCheckPoint {
    int64_t binlog_pos;
    uint32_t binlog_time;

    BaikaldbCheckPoint() : binlog_pos(0), binlog_time(0) {}
};

int main(int argc, char** argv) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);

    std::ifstream ifs("./conf/cap");
    std::string fs_content(
        (std::istreambuf_iterator<char>(ifs)),
        std::istreambuf_iterator<char>());
    Json::Value value;
    Json::Reader reader;
    if (!reader.parse(fs_content, value)) {
        fprintf(stderr, "parse error.");
        return -1;
    }

    time_t now = time(nullptr);

    int64_t start_ts = baikaldb::timestamp_to_ts((uint32_t)now - FLAGS_start_before_now_h * 3600);
    if (FLAGS_start_ts != 0) {
        start_ts = FLAGS_start_ts;
    }
    int64_t end_ts = INT64_MAX;
    if (FLAGS_end_ts != 0) {
        end_ts = FLAGS_end_ts;
    }
    baikaldb::Capturer capturer;
    if (capturer.init(value) != 0) {
        DB_FATAL("init capturer error.");
        return -1;
    }

    std::set<int64_t> skip_ids;
    if (!FLAGS_skip_region_ids.empty()) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, FLAGS_skip_region_ids,
                         boost::is_any_of(","), boost::token_compress_on);
        for (std::string id : split_vec) {
            if (!id.empty()) {
                boost::trim(id);
                int64_t region_id = atoll(id.c_str());
                skip_ids.insert(region_id);
                DB_WARNING("skip region_id: %ld", region_id);
            }
        }

        capturer.set_skip_regions(skip_ids);
    }

    auto proc = [&capturer, start_ts, end_ts]() -> int {
        BaikaldbCheckPoint current_check_point;
        current_check_point.binlog_pos = start_ts;
        while (true) {
            baikaldb::CaptureStatus cs;
            std::vector<std::shared_ptr<mysub::Event>> event_vec;
            int64_t start_ts = current_check_point.binlog_pos;
            if (start_ts > end_ts) {
                break;
            }
            cs = capturer.subscribe(event_vec, start_ts, FLAGS_fetch_num, 0);
            DB_NOTICE("request start_ts %ld status[%d]", start_ts, cs);
            DB_NOTICE("get result %lu", event_vec.size());
            switch (cs) {
                case baikaldb::CS_SUCCESS:
                case baikaldb::CS_EMPTY: {
                    current_check_point.binlog_pos = start_ts;
                    if (!FLAGS_is_debug) {
                        break;
                    }
                    // FLAGS_capture_print_event为true时在baikal_capture内部打印event
                    break;
                }
                case baikaldb::CS_LESS_THEN_OLDEST:
                    DB_NOTICE("subscriber failed, ret: CS_LESS_THEN_OLDEST");
                    // current_check_point.binlog_pos = 0;
                    // 如果当前进度点太旧没有数据了，就从最早的binlog点开始订阅
                    break;
                default:
                    break;
            }
            bthread_usleep(FLAGS_sleep_ms * 1000);
        }
        return 0;
    };

    baikaldb::ConcurrencyBthread req_threads {FLAGS_concurrency, &BTHREAD_ATTR_NORMAL};
    for (int i = 0; i < FLAGS_concurrency; ++i) {
        req_threads.run(proc);
    }

    req_threads.join();

    return 0;
}
