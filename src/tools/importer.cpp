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

#include "importer_handle.h"
#include "backup_tool.h"
#include "flash_back.h"
DECLARE_bool(is_backup);
DECLARE_bool(is_send);
namespace baikaldb {
DECLARE_string(meta_server_bns);
DECLARE_string(input_path);
DECLARE_string(output_path);
DEFINE_string(backup_peer_resource_tag, "", "pefered backup peer resource tag");
DEFINE_int64(interval_days, 1, "backup interval days");
DEFINE_int64(backup_times, 3, "backup times, default 3");
DECLARE_string(done_file);
DEFINE_int32(loop_cnt, 1, "import loops, use for press");
DEFINE_int32(atom_test_concurrency, 10, "");
DEFINE_int32(atom_test_timeout_minutes, 10, "");
DEFINE_int32(atom_test_showword_cnt, 10, "");
class LocalImporter {
public:
    int atom_test(baikal::client::Service* baikaldb);
    int import_to_baikaldb();

private:
    int handle(const Json::Value& node, OpType type,
            baikal::client::Service* baikaldb, baikal::client::Service* backup_db);

};

int LocalImporter::handle(const Json::Value& node, OpType type, 
        baikal::client::Service* baikaldb, baikal::client::Service* backup_db) {
    for (int i = 0; i < FLAGS_loop_cnt; i++) {
        std::unique_ptr<ImporterFileSystemAdaptor> fs(new PosixFileSystemAdaptor());
        std::unique_ptr<ImporterHandle> handler;
        // 线下导入任务，done_path填""，ImporterHandle内部会根据参数生成path
        handler.reset(ImporterHandle::new_handle(type, baikaldb, backup_db, ""));

        int ret = handler->init(node, "/");
        if (ret < 0) {
            DB_FATAL("importer init failed");
            return -1;
        }

        int64_t lines = handler->run(fs.get(), 
                                     "{}",  // config
                                     [](const std::string&){},  // progress_func
                                     [](std::string, int64_t,int64_t, BlockHandleResult*, bool){}); //finish_block_func
        if (lines < 0) {
            DB_FATAL("importer run failed");
            return -1;
        }
    }

    return 0;
}

int LocalImporter::atom_test(baikal::client::Service* baikaldb) {
    TimeCost time_cost;
    BthreadCond concurrency_cond(-FLAGS_atom_test_concurrency);

    while (true) {
        auto func = [this, baikaldb, &time_cost, &concurrency_cond] () {
            std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond, 
                                [](BthreadCond* cond) { cond->decrease_signal();});
            if (time_cost.get_time() > FLAGS_atom_test_timeout_minutes * 60 * 1000 * 1000) {
                exit(0);
            }
            std::string sql = "SELECT wordid, showword FROM Atom.wordinfo_w WHERE showword IN (";
            for (int i = 0; i < FLAGS_atom_test_showword_cnt; i++) {
                int64_t rand = butil::fast_rand();
                sql += "'atom_test_" + std::to_string(rand) + "',";
            }

            sql[sql.size() - 1] = ')';

            baikal::client::ResultSet result_set;
            int ret = baikaldb->query(0, sql, &result_set);
            if (ret != 0) {
                DB_FATAL("atom_test failed");
            } else {
                DB_WARNING("atom_test succ");
            }
        };

        Bthread bth;
        concurrency_cond.increase_wait();
        bth.run(func);
    }
    concurrency_cond.wait(-FLAGS_atom_test_concurrency);
    return 0;

}

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

    auto backup_db = tmp_manager.get_service("backup_db");

    if (baikaldb == nullptr && (baikaldb_gbk == nullptr || baikaldb_utf8 == nullptr)) {
        DB_FATAL("get_service failed");
        return -1;
    }
    if (baikaldb_gbk == nullptr) {
        baikaldb_gbk = baikaldb;
    } 
    if (baikaldb_utf8 == nullptr) {
        baikaldb_utf8 = baikaldb;
    } 
    if (baikaldb == nullptr) {
        baikaldb = baikaldb_gbk;
    } 

    // atom_test(baikaldb);

    if (!FLAGS_input_path.empty() && !FLAGS_output_path.empty()) {
        FLAGS_done_file = FLAGS_input_path + "/done";
    }
    std::ifstream done_ifs(FLAGS_done_file);
    std::string done_config(
        (std::istreambuf_iterator<char>(done_ifs)),
        std::istreambuf_iterator<char>());
    Json::Reader reader;
    Json::Value done_root;
    bool ret1 = reader.parse(done_config, done_root);
    if (!ret1) {
        DB_FATAL("fail parse %d, json:%s", ret1, done_config.c_str());
        return -1;
    }

    Bthread capture_worker;
    time_t now = time(NULL);
    int64_t start_ts = baikaldb::timestamp_to_ts((uint32_t)now);
    if (ImporterHandle::is_launch_capture_task(done_root)) {
        Json::Value node = ImporterHandle::get_node_json(done_root);
        if (LaunchCapture::get_instance()->truncate_old_table(baikaldb, node) != 0) {
            DB_FATAL("truncate old table failed");
            return -1;
        }
        capture_worker.run([this, &done_root, &baikaldb, start_ts]() {
            LaunchCapture::get_instance()->set_capture_initial_param(baikaldb);
            Json::Value node = ImporterHandle::get_node_json(done_root);
            LaunchCapture::get_instance()->init_capture(node, start_ts);
        });
    }

    ON_SCOPE_EXIT(([this, &capture_worker]() {
        capture_worker.join();
        LaunchCapture::get_instance()->destroy();
    }));

    try {
        for (auto& name_type : op_name_type) {
            if (done_root[name_type.name].isArray()) {
                if (done_root.isMember("path") && done_root["update"].isArray()) {
                    if (done_root.isMember("charset")) {
                        auto charset = done_root["charset"].asString();
                        if (charset == "gbk") {
                            baikaldb = baikaldb_gbk;
                        } else {
                            baikaldb = baikaldb_utf8;
                        }
                    }
                    ret = handle(done_root, BASE_UP, baikaldb, backup_db);
                    if (ret < 0) {
                        DB_FATAL("handle fail, json:%s", done_config.c_str());
                        return -1;
                    }
                } else if (done_root.isMember("recovery") && done_root["recovery"].isArray()) {
                    ret = handle(done_root, RECOVERY, baikaldb, backup_db);
                    if (ret < 0) {
                        DB_FATAL("handle fail, json:%s", done_config.c_str());
                        return -1;
                    }
                } else {
                    for (auto& node : done_root[name_type.name]) {
                        ret = handle(node, name_type.type, baikaldb, backup_db);
                        if (ret < 0) {
                            DB_FATAL("handle fail, json:%s", done_config.c_str());
                            return -1;
                        }
                    }
                }

            }
        }
    } catch (Json::LogicError& e) {
        DB_FATAL("fail parse %d, what:%s, json:%s", ret1, e.what(), done_config.c_str());
        return -1;
    }
    
    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {
    if (boost::filesystem::exists("conf/gflags.conf")) {
        google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    }
    google::ParseCommandLineFlags(&argc, &argv, true);
    brpc::StartDummyServerAt(8502);

    baikaldb::TimeCost cost;
    // Initail log
    
    if (baikaldb::init_log(argv[0]) != 0) {
         fprintf(stderr, "log init failed.");
         return -1;
    }
    DB_WARNING("log file load success");

    if (FLAGS_is_backup || FLAGS_is_send) {
        baikaldb::BackUp bp(baikaldb::FLAGS_meta_server_bns, 
                            baikaldb::FLAGS_backup_peer_resource_tag, 
                            baikaldb::FLAGS_interval_days, 
                            baikaldb::FLAGS_backup_times);
        bp.run();
        return 0;
    }
    int ret = baikaldb::flash_back();
    if (ret == 1) {
        return 0;
    } else if (ret == -1) {
        DB_FATAL("flash back fail");
        return -1;
    }
    baikaldb::LocalImporter importer;
    ret = importer.import_to_baikaldb();
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
