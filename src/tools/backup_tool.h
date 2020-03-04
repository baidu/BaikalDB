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

#pragma once

#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <atomic>
#include <vector>
#include <string>
#include <Configure.h>
#include <unordered_set>
#include <fstream>
#include <unordered_map>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include "proto/store.interface.pb.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
struct RegionFile {
    std::string region_id;
    std::string date;
    int64_t log_index;
    std::string filename;
};

enum Status {
    Succss,
    NoData,
    SameLogIndex,
    Error
};

struct TaskInfo {
    TaskInfo() = default;
    TaskInfo(int64_t tid, int64_t rid) : table_id(tid), region_id(rid) {}
    int64_t table_id;
    int64_t region_id;
};

class BackUp {
public:
    BackUp() = default;
    explicit BackUp(const std::string& metaserver) : _meta_server_bns(metaserver) {} 
    ~BackUp() = default;

    void init();

    Status request_sst(std::string url, int64_t table_id, int64_t region_id, 
        std::vector<RegionFile>& rfs, std::string table_path);


    void gen_backup_task(std::unordered_map<std::string, std::vector<TaskInfo>>& tasks, 
        std::unordered_set<int64_t>& table_ids, 
        const pb::QueryResponse& response);
    
    void send_sst(std::string url, std::string infile, int64_t table_id, int64_t region_id);
    void run();
    int run_backup(std::unordered_set<int64_t>& table_ids);
    int run_backup_region(int64_t table_id, int64_t region_id, const std::string& peer);
    void run_upload_from_region_id_files(const std::string& meta_server, int64_t table_id, std::unordered_set<int64_t> region_ids);
    void run_upload_from_meta_info(const std::string& meta_server, int64_t table_id);
    void run_load_from_meta_json_file(std::string& meta_server, std::string meta_json_file);

    void run_upload_per_region(const std::string& meta_server, int64_t table_id, 
        int64_t region_id, std::string date_directory, const google::protobuf::RepeatedPtrField< ::std::string>& peers);

private:

    struct SingleUpLoadTask {
        SingleUpLoadTask(int64_t tid, int64_t rid, 
            const google::protobuf::RepeatedPtrField< ::std::string>& ps) :
            table_id(tid), region_id(rid), peers(ps) {}
        int64_t table_id;
        int64_t region_id;
        const google::protobuf::RepeatedPtrField< ::std::string>& peers;
    };

    using SingleUpLoadTaskPtr = std::shared_ptr<SingleUpLoadTask>;

    struct UploadTask {
        std::unordered_map<std::string, std::atomic<int>> store_task_info;
        std::unordered_set<SingleUpLoadTaskPtr> all_tasks;
    };

    void run_upload_task(const std::string& meta_server, UploadTask& task);

    void gen_upload_task(UploadTask& task, 
        const pb::QueryResponse& response, std::function<bool(const pb::RegionInfo& ri)> predicate) {
        for (const auto& region_info : response.region_infos()) {
            if (predicate(region_info)) {
                auto tid = region_info.table_id();
                auto region_id = region_info.region_id();
                SingleUpLoadTaskPtr sut (new SingleUpLoadTask{tid, region_id, region_info.peers()});
                task.all_tasks.insert(sut);
                for (const auto& peer : region_info.peers()) {
                    task.store_task_info[peer] = 0;
                }
            }
        }
    }

private:
    int get_meta_info(pb::QueryResponse& response);
    std::string _upload_date {""};
    std::string _meta_server_bns;
};
}