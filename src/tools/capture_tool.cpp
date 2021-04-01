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

#include "baikal_capturer.h"

DEFINE_int64(start_ts, 0, "partition id");
namespace baikaldb {
    DECLARE_string(meta_server_bns);
}
int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");

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
    auto start_ts = FLAGS_start_ts;
    if (baikaldb::Capturer::get_instance()->init(value) != 0) {
        DB_FATAL("init capturer error.");
        return -1;
    }
    while (true) {
        baikaldb::CaptureStatus cs;
        std::vector<std::shared_ptr<mysub::Event>> event_vec;
        cs = baikaldb::Capturer::get_instance()->subscribe(event_vec, start_ts, 5000, 0);
        DB_NOTICE("request start_ts %ld status[%d]", start_ts, cs);
        DB_NOTICE("get result %lu", event_vec.size());
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    return 0;
}
