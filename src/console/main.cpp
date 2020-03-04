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

#include <stdio.h>
#include <string>
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/server.h>
#else
#include <brpc/server.h>
#endif
#include <gflags/gflags.h>

#include "console_server.h"

namespace baikaldb {
DECLARE_int32(console_port);

void handle_exit_signal() {
    DB_NOTICE("Server shutdown gracefully.");
}
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

    //init service
    brpc::Server server;
    butil::EndPoint addr;
    addr.ip = butil::IP_ANY;
    addr.port = baikaldb::FLAGS_console_port;
    
    baikaldb::ConsoleServer* console_server = baikaldb::ConsoleServer::get_instance();
    if (0 != server.AddService(console_server, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add idonlyeService");
        return -1;
    }
    
    
    if (server.Start(addr, NULL) != 0) {
        DB_FATAL("Fail to start server");
        console_server->stop();
        return -1;
    }
    DB_WARNING("baidu-rpc server start");

    if (console_server->init() != 0) {
        DB_FATAL("console server init fail");
        return -1;
    }
    DB_WARNING("console server init success");
    server.RunUntilAskedToQuit();
    console_server->stop();
    DB_WARNING("console server quit success"); 

    return 0; 
}
