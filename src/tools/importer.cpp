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

#include "importer_tool.h"
#include "backup_tool.h"
DECLARE_bool(is_backup);
DECLARE_bool(is_send);
namespace baikaldb {
DEFINE_string(done_file, "./data/.done", "hdfs done file");
class LocalImporter : public Importer {
public:
    int import_to_baikaldb();
};

int LocalImporter::import_to_baikaldb() {
    baikal::client::Manager tmp_manager;
    int ret = tmp_manager.init("conf", "baikal_client.conf");
    if (ret != 0) {
        DB_FATAL("baikal client init fail:%d", ret);
        return -1;
    }
    auto baikaldb = tmp_manager.get_service("baikaldb");
    auto baikaldb_gbk = tmp_manager.get_service("baikaldb_gbk");
    auto baikaldb_utf8 = tmp_manager.get_service("baikaldb_utf8");
    //兼容老版本
    if (baikaldb_gbk != NULL && baikaldb_utf8 != NULL) {
        init(baikaldb_gbk, baikaldb_utf8);
    } else if (baikaldb != NULL) {
        init(baikaldb, baikaldb);
    } else {
        DB_FATAL("get_service fail");
        return -1;
    }
    std::ifstream done_ifs(FLAGS_done_file);
    std::string done_config(
        (std::istreambuf_iterator<char>(done_ifs)),
        std::istreambuf_iterator<char>());
    Json::Reader reader;
    Json::Value done_root;
    bool ret1 = reader.parse(done_config, done_root);
    if (!ret1) {
        DB_FATAL("fail parse %d", ret1);
        return -1;
    }

    std::string charset = done_root.get("charset", "gbk").asString();
    //BthreadCond cond;
    if (done_root["delete"].isArray()) {
        for (auto& node : done_root["delete"]) {
            ret = handle(node, DEL, "", "", charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["update"].isArray()) {
        if (done_root.isMember("path")) {
            return handle(done_root, BASE_UP, "", "", charset);
        } else {
            for (auto& node : done_root["update"]) {
                ret = handle(node, UP, "", "", charset);
                if (ret < 0) {
                    return -1;
                }
            }
        }
    }
    if (done_root["dup_key_update"].isArray()) {
        for (auto& node : done_root["dup_key_update"]) {
            ret = handle(node, DUP_UP, "", "", charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["replace"].isArray()) {
        for (auto& node : done_root["replace"]) {
            ret = handle(node, REP, "", "", charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["select"].isArray()) {
        for (auto& node : done_root["select"]) {
            ret = handle(node, SEL, "", "", charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["xbs"].isArray()) {
        for (auto& node : done_root["xbs"]) {
            ret = handle(node, XBS, "", "", charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["xcube"].isArray()) {
        for (auto& node : done_root["xcube"]) {
            ret = handle(node, XCUBE, "", "", charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    return 0;
}
}

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    //brpc::StartDummyServerAt(8800);

    baikaldb::TimeCost cost;
    // Initail log
    
    if (baikaldb::init_log(argv[0]) != 0) {
         fprintf(stderr, "log init failed.");
         return -1;
    }
    DB_WARNING("log file load success");

    if (FLAGS_is_backup || FLAGS_is_send) {
        baikaldb::BackUp bp;
        bp.run();
        return 0;
    }

    baikaldb::LocalImporter importer;
    auto ret = importer.import_to_baikaldb();
    auto time_print = [] (int64_t cost) -> std::string {
        std::ostringstream os;
        cost /= 1000000;
        int days = cost / (3600 * 24);
        if (days > 0) {
            os << days << "d ";
        } 
        cost = cost % (3600 * 24);
        int hours = cost / 3600;
        if (hours > 0) {
            os << hours << "h ";
        }
        cost = cost % 3600;
        int mins = cost / 60;
        if (mins > 0) {
            os << mins << "m ";
        }
        os << cost % 60 << "s";
        return os.str();
    };
    if (ret == 0) {
        DB_NOTICE("import to baikaldb success, cost:%s", time_print(cost.get_time()).c_str());
    } else {
        DB_NOTICE("import to baikaldb failed, cost:%s", time_print(cost.get_time()).c_str());
    }
    com_closelog();
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
