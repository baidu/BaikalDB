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
#include "rocksdb/threadpool.h"

namespace baikaldb {

enum CompactionType {
    ONLY_USE_LOCAL = 0,
    USE_LOCAL_AND_COPY_TO_REMOTE = 1,
    USE_LOCAL_AND_DIRECT_REMOTE = 2,
    USE_REMOTE_FILES = 3
};

struct CompactionFiles {
    int output_level;
    std::vector<std::string> input_files;
};

struct BGCompactionOptions {
    bool use_local_compaction() {
        return compaction_type == ONLY_USE_LOCAL || compaction_type == USE_LOCAL_AND_COPY_TO_REMOTE || 
            compaction_type == USE_LOCAL_AND_DIRECT_REMOTE;
    }
    CompactionType compaction_type = ONLY_USE_LOCAL;
    bool need_check_input_files = false;
    int output_level = 6;
    // 以下参数为compaction_type = USE_REMOTE_FILES时所需
    uint64_t min_inputfile_oldest_ancester_time;
    uint64_t min_inputfile_epoch_number;
    std::vector<std::string> remote_file_names;
    std::vector<std::string> smallest_keys;
    std::vector<std::string> largest_keys;
    int64_t region_id = -1;
};

struct RemoteCompactionInfo {
    BGCompactionOptions options;
    std::vector<std::string> input_files;
    std::string to_string() {
        std::string result;
        // 拼接input_files
        result += "input_files:[";
        for (size_t i = 0; i < input_files.size(); ++i) {
            if (i != 0) {
                result += ",";
            }
            result += input_files[i];
        }
        result += "] ";
        
        // 拼接options
        result += "output_level:";
        result += std::to_string(options.output_level);
        result += " need_check_input_files:";
        result += (options.need_check_input_files ? "true" : "false");
        
        // 转换compaction_type为字符串
        std::string type_str;
        switch (options.compaction_type) {
            case ONLY_USE_LOCAL:
                type_str = "ONLY_USE_LOCAL";
                break;
            case USE_LOCAL_AND_COPY_TO_REMOTE:
                type_str = "USE_LOCAL_AND_COPY_TO_REMOTE";
                break;
            case USE_LOCAL_AND_DIRECT_REMOTE:
                type_str = "USE_LOCAL_AND_DIRECT_REMOTE";
                break;
            case USE_REMOTE_FILES:
                type_str = "USE_REMOTE_FILES";
                break;
            default:
                type_str = "UNKNOWN_TYPE";
                break;
        }
        result += " compaction_type: ";
        result += type_str;
        
        return result;
    }
};

// struct CompactionServiceJobInfo {
//   std::string db_name;
//   std::string db_id;
//   std::string db_session_id;
//   uint64_t job_id;  // job_id is only unique within the current DB and session,
//                     // restart DB will reset the job_id. `db_id` and
//                     // `db_session_id` could help you build unique id across
//                     // different DBs and sessions.

//   Env::Priority priority;

//   // Additional Compaction Details that can be useful in the CompactionService
//   CompactionReason compaction_reason;
//   bool is_full_compaction;
//   bool is_manual_compaction;
//   bool bottommost_level;
//   bool is_l0_compaction;
//   CompactionServiceJobInfo(std::string db_name_, std::string db_id_,
//                            std::string db_session_id_, uint64_t job_id_,
//                            Env::Priority priority_,
//                            CompactionReason compaction_reason_,
//                            bool is_full_compaction_, bool is_manual_compaction_,
//                            bool bottommost_level_, bool is_l0_compaction_)
//       : db_name(std::move(db_name_)),
//         db_id(std::move(db_id_)),
//         db_session_id(std::move(db_session_id_)),
//         job_id(job_id_),
//         priority(priority_),
//         compaction_reason(compaction_reason_),
//         is_full_compaction(is_full_compaction_),
//         is_manual_compaction(is_manual_compaction_),
//         bottommost_level(bottommost_level_),
//         is_l0_compaction(is_l0_compaction_) {}
// };

// struct CompactionServiceInput {
//   std::string cf_name;

//   std::vector<SequenceNumber> snapshots;

//   // SST files for compaction, it should already be expended to include all the
//   // files needed for this compaction, for both input level files and output
//   // level files.
//   std::vector<std::string> input_files;
//   int output_level;

//   // db_id is used to generate unique id of sst on the remote compactor
//   std::string db_id;

//   // information for subcompaction
//   bool has_begin = false;
//   std::string begin;
//   bool has_end = false;
//   std::string end;

//   uint64_t options_file_number;

//   // serialization interface to read and write the object
//   static Status Read(const std::string& data_str, CompactionServiceInput* obj);
//   Status Write(std::string* output);

//   bool is_l0_compaction = false;
// };

class RemoteCompactionService : public rocksdb::CompactionService {
public:
    RemoteCompactionService() {

    }

    static const char* kClassName() { return "RemoteCompactionService"; }

    const char* Name() const override { return kClassName(); }

    rocksdb::CompactionServiceScheduleResponse Schedule(const rocksdb::CompactionServiceJobInfo& info,
                                             const std::string& compaction_service_input) override;

// struct CompactionServiceOutputFile {
//   std::string file_name;
//   SequenceNumber smallest_seqno;
//   SequenceNumber largest_seqno;
//   std::string smallest_internal_key;
//   std::string largest_internal_key;
//   uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;
//   uint64_t file_creation_time = kUnknownFileCreationTime;
//   uint64_t epoch_number = kUnknownEpochNumber;
//   std::string file_checksum = kUnknownFileChecksum;
//   std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
//   uint64_t paranoid_hash;
//   bool marked_for_compaction;
//   UniqueId64x2 unique_id{};
// };

// struct CompactionServiceResult {
//   Status status;
//   std::vector<CompactionServiceOutputFile> output_files;
//   int output_level;

//   // location of the output files
//   std::string output_path;

//   uint64_t bytes_read = 0;
//   uint64_t bytes_written = 0;
//   CompactionJobStats stats;

//   // serialization interface to read and write the object
//   static Status Read(const std::string& data_str, CompactionServiceResult* obj);
//   Status Write(std::string* output);
// };

    rocksdb::CompactionServiceJobStatus Wait(const std::string& scheduled_job_id, std::string* result) override;

    void OnInstallation(const std::string& scheduled_job_id,
                        rocksdb::CompactionServiceJobStatus status) override {
        DB_WARNING("scheduled_job_id: %s, compaction_result install", 
                scheduled_job_id.c_str());
    }

    void push_compaction_files(const CompactionFiles& input_files) {
        std::unique_lock<std::mutex> l(_mutex);
        for (const auto& file : input_files.input_files) {
            if (_unique_file.count(file) > 0) {
                return;
            }
        }
        for (const auto& file : input_files.input_files) {
            _unique_file.insert(file);
        }
        _compaction_files.push(input_files);
    }

    int pop_compaction_files(CompactionFiles& files) {
        std::unique_lock<std::mutex> l(_mutex);
        if (_compaction_files.empty()) {
            return -1;
        }

        CompactionFiles compaction_files = _compaction_files.front();
        _compaction_files.pop();
        for (const auto& file : compaction_files.input_files) {
            _unique_file.erase(file);
        }
        files = compaction_files;
        return 0;
    }

    void swap_dsc_table_ids(const std::set<int64_t>& table_ids) {
        std::unique_lock<std::mutex> l(_mutex);
        _dsc_table_ids.clear();
        _dsc_table_ids = table_ids;
    }

    bool enable_dsc(int64_t table_id) {
        std::unique_lock<std::mutex> l(_mutex);
        return _dsc_table_ids.count(table_id) > 0;
    }

    bool enable_user_defined_compaction(const std::vector<rocksdb::CompactInputFileInfo>& input_infos);
private:
    std::mutex _mutex;
    std::set<std::string> _unique_file;
    std::queue<CompactionFiles> _compaction_files;
    std::set<int64_t> _dsc_table_ids; // 打开存算分离的table id
};
} // baikaldb