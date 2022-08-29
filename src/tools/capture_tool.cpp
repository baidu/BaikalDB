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

struct BaikaldbCheckPoint {
    int64_t binlog_pos;
    uint32_t binlog_time;

    BaikaldbCheckPoint() : binlog_pos(0), binlog_time(0) {}
};

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

    
    BaikaldbCheckPoint current_check_point;
    current_check_point.binlog_pos = start_ts;

    while (true) {
        baikaldb::CaptureStatus cs;
        std::vector<std::shared_ptr<mysub::Event>> event_vec;
        int64_t start_ts = current_check_point.binlog_pos;
        cs = baikaldb::Capturer::get_instance()->subscribe(event_vec, start_ts, 5000, 0);
        DB_NOTICE("request start_ts %ld status[%d]", start_ts, cs);
        DB_NOTICE("get result %lu", event_vec.size());
        switch (cs) {
            case baikaldb::CS_SUCCESS:
            case baikaldb::CS_EMPTY: {
                current_check_point.binlog_pos = start_ts;
                for (auto& event : event_vec) {
                    std::string fields = "(";
                    std::string values = "(";
                    std::string old_values = "(";
                    for (auto& field : event->row().field()) {
                        fields += field.name() + ",";
                        values += field.new_value() + ",";
                        old_values += field.old_value() + ",";
                    }
                    fields.pop_back();
                    fields += ")";
                    values.pop_back();
                    values += ")";
                    old_values.pop_back();
                    old_values += ")";
                    switch (event->event_type())
                    {
                    case mysub::INSERT_EVENT: {
                        DB_NOTICE("table %s.%s insert rows:%s - %s old_values:%s", event->db().c_str(),
                            event->table().c_str(), fields.c_str(), values.c_str(), old_values.c_str());
                        break;
                    }
                    case mysub::DELETE_EVENT: {
                        DB_NOTICE("table %s.%s delete rows:%s - %s old_values:%s", event->db().c_str(),
                            event->table().c_str(), fields.c_str(), values.c_str(), old_values.c_str());
                        break;
                    }
                    case mysub::UPDATE_EVENT: {
                        DB_NOTICE("table %s.%s update rows:%s - %s old_values:%s", event->db().c_str(),
                            event->table().c_str(), fields.c_str(), values.c_str(), old_values.c_str());
                        break;
                    }
                    default:
                        break;
                    }
                }
                break;
            }
            case baikaldb::CS_LESS_THEN_OLDEST:
                DB_NOTICE("subscriber failed, ret: CS_LESS_THEN_OLDEST");
                current_check_point.binlog_pos = 0;
                // 如果当前进度点太旧没有数据了，就从最早的binlog点开始订阅
                break;
            default:
                break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    return 0;
}
