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

#include <ctime>
#include <cstdlib>
#include <net/if.h>
#include <sys/ioctl.h>
#include <gflags/gflags.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <boost/filesystem.hpp>
#include "common.h"
#include "my_raft_log.h"
#include "store.h"
#include "reverse_common.h"
#include "fn_manager.h"
#include "schema_factory.h"

namespace baikaldb {
DECLARE_int32(store_port);
DEFINE_string(wordrank_conf, "./config/drpc_client.xml", "wordrank conf path");
} // namespace baikaldb

int main(int argc, char **argv) {
#ifdef BAIKALDB_REVISION
    google::SetVersionString(BAIKALDB_REVISION);
#endif
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    srand((unsigned)time(NULL));
    boost::filesystem::path remove_path("init.success");
    boost::filesystem::remove_all(remove_path); 
    // Initail log
    if (baikaldb::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    DB_WARNING("log file load success");
    baikaldb::register_myraft_extension();
    int ret = 0;
    baikaldb::Tokenizer::get_instance()->init();
#ifdef BAIDU_INTERNAL
    //init wordrank_client
    std::unique_ptr<drpc::NLPCClient> wordrank_client_ptr(new drpc::NLPCClient());
    std::unique_ptr<drpc::NLPCClient> wordseg_client_ptr(new drpc::NLPCClient());
    baikaldb::wordrank_client = wordrank_client_ptr.get();
    baikaldb::wordseg_client = wordseg_client_ptr.get();
    ret = ::drpc::init_env(baikaldb::FLAGS_wordrank_conf);
    if (ret < 0) {
        DB_WARNING("wordrank init_env failed");
        return -1;
    }
    if (baikaldb::wordrank_client->init("nlpc_wordrank_208") != 0) {
        DB_WARNING("init wordrank agent failed");
        return -1;
    }

    if (baikaldb::wordseg_client->init("nlpc_wordseg_3016") != 0) {
        DB_WARNING("init wordseg agent failed");
        return -1;
    }
#endif
    /* 
    auto call = []() {
        std::ifstream extra_fs("test_file");
        std::string word((std::istreambuf_iterator<char>(extra_fs)),
                std::istreambuf_iterator<char>());
        baikaldb::TimeCost tt1;
        for (int i = 0; i < 1000000000; i++) {
            //word+="1";
            std::string word2 = word + "1";
            std::map<std::string, float> term_map;
            baikaldb::Tokenizer::get_instance()->wordrank(word2, term_map);
            if (i%1000==0) {
                DB_WARNING("wordrank:%d",i);
            }
        }
        DB_WARNING("wordrank:%ld", tt1.get_time());
    };
    baikaldb::ConcurrencyBthread cb(100);
    for (int i = 0; i < 100; i++) {
        cb.run(call);
    }
    cb.join();
    return 0;
    */
    // init singleton
    baikaldb::FunctionManager::instance()->init();
    if (baikaldb::SchemaFactory::get_instance()->init() != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    }

    //add service
    brpc::Server server;
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = baikaldb::FLAGS_store_port;
    //将raft加入到baidu-rpc server中
    if (0 != braft::add_service(&server, addr)) { 
        DB_FATAL("Fail to init raft");
        return -1;
    }
    DB_WARNING("add raft to baidu-rpc server success");
    //注册处理Store逻辑的service服务
    baikaldb::Store* store = baikaldb::Store::get_instance();
    std::vector<std::int64_t> init_region_ids;
    ret = store->init_before_listen(init_region_ids);
    if (ret < 0) {
        DB_FATAL("Store instance init_before_listen fail");
        return -1;
    } 
    if (0 != server.AddService(store, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add StoreService");
        return -1;
    }
    if (server.Start(addr, NULL) != 0) {
        DB_FATAL("Fail to start server");
        return -1;
    }
    DB_WARNING("start rpc success");
    ret = store->init_after_listen(init_region_ids);
    if (ret < 0) {
        DB_FATAL("Store instance init_after_listen fail");
        return -1;
    }
    std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    DB_WARNING("store instance init success");
    //server.RunUntilAskedToQuit();
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    DB_WARNING("recevie kill signal, begin to quit");
    store->shutdown_raft();
    store->close();
    DB_WARNING("store close success");
    // exit if server.join is blocked
    baikaldb::Bthread bth;
    bth.run([]() {
            bthread_usleep(2 * 60 * 1000 * 1000);
            DB_FATAL("store forse exit");
            exit(-1);
        });
    // 需要后关端口
    server.Stop(0);
    server.Join();
    DB_WARNING("quit success");
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
