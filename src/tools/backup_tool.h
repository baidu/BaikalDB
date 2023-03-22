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

#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <atomic>
#include <vector>
#include <string>
#include <cstdint>
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
#include "common.h"
#include "backup_stream.h"
#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#else
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <butil/files/file_enumerator.h>
#endif
#ifdef BAIDU_INTERNAL
namespace butil = base;
#endif

namespace baikaldb {
struct RegionFile {
    std::string region_id;
    std::string date;
    int64_t log_index;
    std::string filename;
};

using TableRegionFiles = std::unordered_map<int64_t, std::vector<RegionFile>>;

enum Status {
    Succss = 0,
    NoData,
    SameLogIndex,
    Error,
    RetryLater
};

struct TaskInfo {
    TaskInfo() = default;
    TaskInfo(int64_t tid, int64_t rid) : table_id(tid), region_id(rid) {}
    int64_t table_id;
    int64_t region_id;
};

class BackupStreamReceiver : public CommonStreamReceiver {
public:
    virtual int on_received_messages(baidu::rpc::StreamId id, 
        base::IOBuf *const messages[], 
        size_t size) {
            
        int retry_time = 0;
        while (!_ofs.is_open()) {
            DB_WARNING("wait open stream [%lu] file.", id);
            bthread_usleep(1000 * 1000LL);
            retry_time++;
            if (retry_time > 600) {
                DB_FATAL("open stream [%lu] file error.", id);
                _status = pb::StreamState::SS_FAIL;
                return -1;
            }
        }
        size_t current_index = 0;
        switch (_status) {
        case pb::StreamState::SS_INIT: {
            DB_NOTICE("stream %lu request size %lu", id, _to_process_size);
            multi_iobuf_action(id, messages, size, &current_index,
            [this](base::IOBuf *const message, size_t size) {
                return message->cutn((char*)(&_all_size) + 
                    (sizeof(int64_t) - _to_process_size), size);
            }, &_to_process_size);
            DB_NOTICE("stream %lu left size %lu", id, _to_process_size);
            if (_to_process_size == 0) {
                DB_NOTICE("stream %lu all_size %ld", id, _all_size);
                _status = pb::StreamState::SS_PROCESSING;
            } else {
                break;
            }
        }
        default: {
            for (; current_index < size; ++current_index) {
                _recived_size += messages[current_index]->size();
                _ofs << *messages[current_index] << std::flush;
                DB_DEBUG("streaming %lu recived_size %ld all_size %ld", id, _recived_size, _all_size);
            }
            if (_all_size == _recived_size) {
                _ofs.flush();
                DB_NOTICE("streaming %lu recived_size %ld all_size %ld", id, _recived_size, _all_size);
                _status = _ofs.bad() ? pb::StreamState::SS_FAIL : pb::StreamState::SS_SUCCESS;
                DB_NOTICE("streaming %lu status[%d]", id, int(_status));
            }
        }
        }
        
        return 0;
    }
    virtual void on_idle_timeout(baidu::rpc::StreamId id) {
        DB_WARNING("on_idle_timeout streaming %lu", id);
        _status = pb::StreamState::SS_FAIL;
    }
    bool init(const char* filename) {
        int retry_time = 0;
        while (retry_time++ < 60) {
            _ofs.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);
            if (_ofs.good() && _ofs.is_open()) {
                DB_WARNING("open file success: %s", filename);
                break;
            } else {
                DB_WARNING("open file fail: %s, retry: %d", filename, retry_time);
                bthread_usleep(1000 * 1000LL);
            } 
        }
        return _ofs.good() && _ofs.is_open();
    }

    bool is_closed() const {
        return _is_closed;
    }

    virtual void on_closed(brpc::StreamId id) override {
        CommonStreamReceiver::on_closed(id);
        _is_closed = true;
    }
private:
    std::ofstream _ofs;
    int64_t _streaming_size {0};
    int64_t _recived_size {0};
    int64_t _all_size {0};
    bool _is_closed {false};
    size_t _to_process_size {sizeof(int64_t)};
};

class FileReader {
public:
    const static int BUF_SIZE {2 * 1024 * 1024LL};
    
    FileReader() { }
    virtual ~FileReader() { }
    virtual int open() = 0;
    virtual int64_t read(int64_t pos, char** buf, bool* eof) = 0;
    virtual void close() = 0;
};

class PosixFileReader : public FileReader {
public:
    PosixFileReader(const std::string& infile) : _infile(infile), 
        _f(butil::FilePath{infile}, butil::File::FLAG_OPEN) { }
    virtual ~PosixFileReader() {

    }
    virtual int open() override {
        if (!_f.IsValid()) {
            DB_WARNING("file[%s] is not valid.", _infile.c_str());
            return -1;
        }

        _buf = new char[BUF_SIZE];
        return 0;
    }

    //不可并发读取
    virtual int64_t read(int64_t pos, char** buf, bool* eof) override {
        int64_t read_ret = _f.Read(pos, _buf, BUF_SIZE);
        if (read_ret == -1) {
            DB_WARNING("read file[%s] error.", _infile.c_str());
            return -1;
        } 

        *buf = _buf;
        if (read_ret < BUF_SIZE) {
            *eof = true;
        } else {
            *eof = false;
        }

        return read_ret;
    }

    virtual void close() override { 
        if (_buf != nullptr) {
            delete[] _buf;
        }
    }
private:
    std::string _infile;
    butil::File _f;
    char* _buf = nullptr;
};

class BackUp {
public:
    BackUp() = default;
    explicit BackUp(const std::string& metaserver, const std::string& resource_tag, int64_t interval_days, int64_t backup_times) : 
        _meta_server_bns(metaserver), 
        _pefered_peer_resource_tag(resource_tag),
        _interval_days(interval_days),
        _backup_times(backup_times) {} 
    ~BackUp() = default;

    void init();

    static void shutdown() {
        _shutdown = true;
    }

    void run();
    int run_backup(std::unordered_set<int64_t>& table_ids, 
        const std::function<int(int64_t, int64_t, const std::string&, std::vector<RegionFile>&)>& backup_proc);

    void run_upload_from_region_id_files(const std::string& meta_server, int64_t table_id, std::unordered_set<int64_t> region_ids);
    void run_upload_from_meta_info(const std::string& meta_server, int64_t table_id);
    void run_upload_from_meta_json_file(std::string& meta_server, std::string meta_json_file);
    
    int backup_region_streaming(int64_t table_id, int64_t region_id, const std::string& peer, std::vector<RegionFile>&);
    int backup_region_streaming_with_retry(int64_t table_id, int64_t region_id, const std::string& target_peer, std::vector<RegionFile>&);
    static Status send_sst_streaming(std::string peer, std::string infile, int64_t table_id, int64_t region_id,
                                     bool only_data_sst = false, int64_t row_size = 0);
    static Status get_region_peers_from_leader(const std::string& peer, int64_t region_id, 
                                    std::set<std::string>& peers, std::set<std::string>& unstable_followers);
    static pb::StreamState get_streaming_result_from_store(const std::string& peer, brpc::StreamId server_id, int64_t region_id);
private:
    void send_sst(std::string url, std::string infile, int64_t table_id, int64_t region_id);
    Status request_sst(std::string url, int64_t table_id, int64_t region_id, 
        std::vector<RegionFile>& rfs, std::string table_path);

    void run_upload_per_region(const std::string& meta_server, int64_t table_id, 
        int64_t region_id, std::string date_directory, const google::protobuf::RepeatedPtrField<std::string>& peers);

    int run_backup_region(int64_t table_id, int64_t region_id, const std::string& peer);

    void request_sst_streaming(std::string url, int64_t table_id, int64_t region_id, 
        std::vector<RegionFile>& rfs, std::string table_path, int64_t log_index, Status& status);

    int run_upload_per_region_streaming(const std::string& meta_server, int64_t table_id, 
        int64_t region_id, const google::protobuf::RepeatedPtrField<std::string>& peers, const std::vector<RegionFile>& region_files);

    void gen_backup_task(std::unordered_map<std::string, std::vector<TaskInfo>>& tasks, 
        std::unordered_set<int64_t>& table_ids, 
        const pb::QueryResponse& response,
        std::unordered_map<int64_t, std::set<int64_t>>& regions_from_meta);
    
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

    void run_upload_task(const std::string& meta_server, UploadTask& task, std::unordered_map<int64_t, TableRegionFiles>&);

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
    int get_meta_info(const pb::QueryRequest& request, pb::QueryResponse& response);
    std::string _upload_date {""};
    std::string _meta_server_bns;
    std::string _pefered_peer_resource_tag;
    std::unordered_map<std::string, std::set<std::string>> _resource_tag_to_instance;
    static bool _shutdown;
    int64_t _interval_days = 0;
    int64_t _backup_times = 0;
};
}
