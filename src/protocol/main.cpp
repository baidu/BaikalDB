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
#include <boost/filesystem.hpp>
//#include <gperftools/malloc_extension.h>
#include "common.h"
#include "network_server.h"
#include "fn_manager.h"
#include "task_fetcher.h"
#include "task_manager.h"
#include "schema_factory.h"
#include "information_schema.h"
#include "memory_profile.h"

namespace baikaldb {

// Signal handlers.
void handle_exit_signal(int sig) {
    NetworkServer::get_instance()->graceful_shutdown();
}

void crash_handler(int sig) {
    NetworkServer::get_instance()->fast_stop();
    NetworkServer::get_instance()->graceful_shutdown();
    signal(sig, SIG_DFL);
    kill(getpid(), sig);
}

} // namespace baikaldb

int main(int argc, char **argv) {
    // Initail signal handlers.
    signal(SIGPIPE, SIG_IGN);
    signal(SIGSEGV, (sighandler_t)baikaldb::crash_handler);
    signal(SIGINT, (sighandler_t)baikaldb::handle_exit_signal);
    signal(SIGTERM, (sighandler_t)baikaldb::handle_exit_signal);
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
    if (baikaldb::MetaServerInteract::get_auto_incr_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    if (baikaldb::MetaServerInteract::get_tso_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    if (baikaldb::TsoFetcher::get_instance()->init() != 0) {
        DB_FATAL("TsoFetcher init failed");
        return -1;
    }
    // 可以没有backup
    if (baikaldb::MetaServerInteract::get_backup_instance()->init(true) != 0) {
        DB_FATAL("meta server interact backup init failed");
        return -1;
    }
    if (baikaldb::MetaServerInteract::get_backup_instance()->is_inited()) {
        if (baikaldb::SchemaFactory::get_backup_instance()->init() != 0) {
            DB_FATAL("SchemaFactory init failed");
            return -1;
        }
    }

    if (baikaldb::TaskManager::get_instance()->init() != 0) {
        DB_FATAL("init task manager error.");
        return -1;
    }
    baikaldb::HandleHelper::get_instance()->init();
    baikaldb::ShowHelper::get_instance()->init();
    baikaldb::MemoryGCHandler::get_instance()->init();
    baikaldb::MemTrackerPool::get_instance()->init();
    // Initail server.
    baikaldb::NetworkServer* server = baikaldb::NetworkServer::get_instance();
    if (!server->init()) {
        DB_FATAL("Failed to initail network server.");
        return 1;
    }
    std::ofstream init_fs("init.success", std::ofstream::out | std::ofstream::trunc);
    if (!server->start()) {
        DB_FATAL("Failed to start server.");
    }
    DB_NOTICE("Server shutdown gracefully.");

    // Stop server.
    server->stop();
    baikaldb::MemoryGCHandler::get_instance()->close();
    baikaldb::MemTrackerPool::get_instance()->close();
    DB_NOTICE("Server stopped.");
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
