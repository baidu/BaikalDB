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

#include <string>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/server.h>
#else
#include <brpc/server.h>
#endif
#include <gflags/gflags.h>
#include "my_raft_log.h"
#include "common.h"
#include "meta_util.h"
#include "meta_server.h"
#include "rocks_wrapper.h"
#include "memory_profile.h"

namespace baikaldb {
DECLARE_int32(meta_port);
DECLARE_string(meta_server_bns);
DECLARE_int32(meta_replica_number);
}

int main(int argc, char **argv) {
#ifdef BAIKALDB_REVISION
    google::SetVersionString(BAIKALDB_REVISION);
    static bvar::Status<std::string> baikaldb_version("baikaldb_version", "");
    baikaldb_version.set_value(BAIKALDB_REVISION);
#endif
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);
    boost::filesystem::path remove_path("init.success");
    boost::filesystem::remove_all(remove_path); 
    // Initail log
    if (baikaldb::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    DB_WARNING("log file load success");

    // 注册自定义的raft log的存储方式
    baikaldb::register_myraft_extension();

    //add service
    brpc::Server server;
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = baikaldb::FLAGS_meta_port;
    //将raft加入到baidu-rpc server中
    if (0 != braft::add_service(&server, addr)) {
        DB_FATAL("Fail to init raft");
        return -1;
    }
    DB_WARNING("add raft to baidu-rpc server success");
   
    int ret = 0;
    //this step must be before server.Start
    std::vector<braft::PeerId> peers;
    std::vector<std::string> instances;
    bool completely_deploy = false;
    bool use_bns = false;
#ifdef BAIDU_INTERNAL
    //指定的是ip:port的形式
    if (baikaldb::FLAGS_meta_server_bns.find(":") != std::string::npos) {
        std::vector<std::string> list_raft_peers;
        boost::split(list_raft_peers, baikaldb::FLAGS_meta_server_bns, boost::is_any_of(","));
        for (auto & raft_peer : list_raft_peers) {
            DB_WARNING("raft_peer:%s", raft_peer.c_str());
            braft::PeerId peer(raft_peer);
            peers.push_back(peer);
        }
    } else {
        do {
            baikaldb::get_instance_from_bns(&ret, baikaldb::FLAGS_meta_server_bns, instances);
        } while (ret != webfoot::WEBFOOT_RET_SUCCESS &&
                 ret != webfoot::WEBFOOT_SERVICE_NOTEXIST &&
                 ret != webfoot::WEBFOOT_SERVICE_BEYOND_THRSHOLD);
        if (ret == webfoot::WEBFOOT_SERVICE_NOTEXIST || instances.size() == 0) {
            completely_deploy = true;
        }
        use_bns = true;
    }
    DB_WARNING("completely deploy:%d, host:%s", 
                completely_deploy, butil::endpoint2str(addr).c_str());
#else
    //指定的是ip:port的形式
    std::vector<std::string> list_raft_peers;
    boost::split(list_raft_peers, baikaldb::FLAGS_meta_server_bns, boost::is_any_of(","));
    for (auto & raft_peer : list_raft_peers) {
        DB_WARNING("raft_peer:%s", raft_peer.c_str());
        braft::PeerId peer(raft_peer);
        peers.push_back(peer);
    }
#endif
     
    baikaldb::MetaServer* meta_server = baikaldb::MetaServer::get_instance();
    //注册处理meta逻辑的service服务
    if (0 != server.AddService(meta_server, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add idonlyeService");
        return -1;
    }
    //启动端口
    if (server.Start(addr, NULL) != 0) {
        DB_FATAL("Fail to start server");
        return -1;
    }
    DB_WARNING("baidu-rpc server start");
#ifdef BAIDU_INTERNAL
    if (completely_deploy) {
        std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
        while (1) {
            baikaldb::get_instance_from_bns(&ret, baikaldb::FLAGS_meta_server_bns, instances);
            if ((int)instances.size() == baikaldb::FLAGS_meta_replica_number) {
                for (auto &instance : instances) {
                    braft::PeerId peer(instance);
                    peers.push_back(peer);
                }
                break;
            }
            DB_WARNING("bns not generate, bns_name:%s", baikaldb::FLAGS_meta_server_bns.c_str());
            sleep(1);
        }
    }
#endif
    if (meta_server->init(peers) != 0) {
        DB_FATAL("meta server init fail");
        return -1;
    }
    baikaldb::MemoryGCHandler::get_instance()->init();
    if (!completely_deploy && use_bns) {
        // 循环等待数据加载成功, ip_list配置区分不了全新/更新部署
        while (!meta_server->have_data()) {
            bthread_usleep(1000 * 1000);
        }
        std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    }
    DB_WARNING("meta server init success");
    //server.RunUntilAskedToQuit(); 这个方法会先把端口关了，导致丢请求
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    DB_WARNING("recevie kill signal, begin to quit"); 
    meta_server->shutdown_raft();
    meta_server->close();
    baikaldb::MemoryGCHandler::get_instance()->close();
    baikaldb::RocksWrapper::get_instance()->close();
    DB_WARNING("raft shut down, rocksdb close");
    server.Stop(0);
    server.Join();
    DB_WARNING("meta server quit success"); 
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
