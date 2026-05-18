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

#pragma once
#include "rocksdb_compaction_ctrl.h"
#include "rocksdb/threadpool.h"

namespace baikaldb {
class RegionCompactFiles {
public:
struct WaitInfo {
    int64_t raft_index = 0;
    int64_t begin_time = 0;
};

    ~RegionCompactFiles() {
        if (_thread_pool != nullptr) {
            _thread_pool->JoinAllThreads();
            delete _thread_pool;
        }
    }
    static RegionCompactFiles* get_instance() {
        static RegionCompactFiles _instance;
        return &_instance;
    }

    int finish_compaction_files(const rocksdb::CompactionJobInfo& job, const BGCompactionOptions& options);

    void bg_compaction_files(const BGCompactionOptions& options, const std::vector<std::string>& input_files);

    void do_compaction_files(const CompactionFiles& compaction_files);

    int check_compaction_files(int64_t region_id, std::vector<rocksdb::LiveFileMetaData*> input_file_metadatas,
        CompactionType& compaction_type, std::vector<std::string>& remote_file_names, 
        std::vector<std::string>& smallest_keys, std::vector<std::string>& largest_keys);
        
    void compaction_files();

    bool need_wait(int64_t region_id, int64_t raft_index);
 
private:
    RegionCompactFiles();
    std::mutex _mutex;
    std::map<int64_t, WaitInfo> _region_wait_time;
    std::atomic<int> _doing_cnt {0};
    rocksdb::ThreadPool* _thread_pool = nullptr;
};
} // namespace baikaldb