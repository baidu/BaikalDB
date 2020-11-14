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

#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <gflags/gflags.h>
//#include <gperftools/malloc_extension.h>
#include "common.h"
#include "network_server.h"
#include "fn_manager.h"
#include "schema_factory.h"
#include "information_schema.h"

namespace baikaldb {

// Signal handlers.
void handle_exit_signal() {
    NetworkServer::get_instance()->graceful_shutdown();
}
} // namespace baikaldb

int main(int argc, char **argv) {
    // Initail signal handlers.
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, (sighandler_t)baikaldb::handle_exit_signal);
    signal(SIGTERM, (sighandler_t)baikaldb::handle_exit_signal);
#ifdef BAIKALDB_REVISION
    google::SetVersionString(BAIKALDB_REVISION);
#endif
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    // Initail log
    if (baikaldb::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    DB_NOTICE("baikaldb starting");
//    DB_WARNING("log file load success; GetMemoryReleaseRate:%f", 
//            MallocExtension::instance()->GetMemoryReleaseRate());

    // init singleton
    baikaldb::FunctionManager::instance()->init();
    if (baikaldb::SchemaFactory::get_instance()->init() != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    }
    if (baikaldb::InformationSchema::get_instance()->init() != 0) {
        DB_FATAL("InformationSchema init failed");
        return -1;
    }
    if (baikaldb::MetaServerInteract::get_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    if (baikaldb::AutoInc::init_meta_inter() != 0) {
        DB_FATAL("auto incr meta server interact init failed");
        return -1;
    }
    if (baikaldb::TsoFetcher::init_meta_inter() != 0) {
        DB_FATAL("tso meta server interact init failed");
        return -1;
    }
    // Initail server.
    baikaldb::NetworkServer* server = baikaldb::NetworkServer::get_instance();
    if (!server->init()) {
        DB_FATAL("Failed to initail network server.");
        return 1;
    }
    if (!server->start()) {
        DB_FATAL("Failed to start server.");
    }
    DB_NOTICE("Server shutdown gracefully.");

    // Stop server.
    server->stop();
    DB_NOTICE("Server stopped.");
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
