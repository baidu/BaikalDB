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

#include <string>
#include <boost/algorithm/string.hpp>
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

namespace baikaldb {
DECLARE_int32(meta_port);
DECLARE_string(meta_server_bns);
DECLARE_int32(meta_replica_number);
}

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    // Initail log
    if (baikaldb::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    DB_WARNING("log file load success");

    // 注册自定义的raft log的存储方式
    baikaldb::register_myraftlog_extension();

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
   
    //this step must be before server.Start
    std::vector<braft::PeerId> peers;
    //指定的是ip:port的形式
    std::vector<std::string> list_raft_peers;
    boost::split(list_raft_peers, baikaldb::FLAGS_meta_server_bns, boost::is_any_of(","));
    for (auto & raft_peer : list_raft_peers) {
        DB_WARNING("raft_peer:%s", raft_peer.c_str());
        braft::PeerId peer(raft_peer);
        peers.push_back(peer);
    }
     
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
    if (meta_server->init(peers) != 0) {
        DB_FATAL("meta server init fail");
        return -1;
    }
    DB_WARNING("meta server init success");
    server.RunUntilAskedToQuit();
    DB_WARNING("recevie kill signal, begin to quit"); 
    meta_server->shutdown_raft();
    baikaldb::RocksWrapper::get_instance()->close();
    DB_WARNING("meta server quit success"); 
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
