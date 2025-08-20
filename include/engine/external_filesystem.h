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
#include <iostream>
#include <istream>
#include <streambuf>
#include <string>
#include <vector>
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#ifdef BAIDU_INTERNAL
#include "baidu/inf/afs-api/client/afs_filesystem.h"
#include "baidu/inf/afs-api/common/afs_common.h"
#include "baidu/inf/afs-api/client/afs_impl.h"
#include <baidu/rpc/channel.h>
#include <baidu/rpc/server.h>
#include <baidu/rpc/controller.h>
#else
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#endif
#include "common.h"
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include "lru_cache.h"

namespace baikaldb {
DECLARE_int64(compaction_sst_cache_max_block);

int get_size_by_external_file_name(uint64_t* size, uint64_t* lines, const std::string& external_file);
struct AfsStatis {
    explicit AfsStatis(const std::string& cluster_name) : afs_cluster(cluster_name), 
        read_time_cost(afs_cluster + "_afs_read_time_cost", 60),
        open_time_cost(afs_cluster + "_afs_open_time_cost", 60),
        close_time_cost(afs_cluster + "_afs_close_time_cost", 60),
        read_bytes_per_second(afs_cluster + "_afs_read_bytes_per_second", &read_bytes),
        write_bytes_per_second(afs_cluster + "_afs_write_bytes_per_second", &write_bytes),
        read_fail_count(afs_cluster + "_afs_read_fail_count"),
        write_fail_count(afs_cluster + "_afs_write_fail_count"),
        reader_open_count(afs_cluster + "_afs_reader_open_count") {

    }
    std::string afs_cluster;
    bvar::Adder<int64_t>  read_bytes;
    bvar::Adder<int64_t>  write_bytes;
    bvar::LatencyRecorder read_time_cost;
    bvar::LatencyRecorder open_time_cost;
    bvar::LatencyRecorder close_time_cost;
    bvar::PerSecond<bvar::Adder<int64_t> > read_bytes_per_second;
    bvar::PerSecond<bvar::Adder<int64_t> > write_bytes_per_second;
    bvar::Adder<int64_t>     read_fail_count;
    bvar::Adder<int64_t>     write_fail_count;
    bvar::Adder<int64_t>     reader_open_count;
};

class ExtFileReader {
public:
    virtual ~ExtFileReader() {}

    virtual int64_t read(char* buf, uint32_t count, uint32_t offset, bool* eof) = 0;

    virtual int64_t skip(uint32_t n, bool* eof) {
        DB_FATAL("ExtFileReader::skip not implemented");
        return -1;
    }
    // Close the descriptor of this file adaptor
    virtual bool close() { return true; }

    virtual std::string file_name() { return ""; }

protected:

    ExtFileReader() {}

private:
    DISALLOW_COPY_AND_ASSIGN(ExtFileReader);
};

class ExtFileWriter {
public:
    virtual ~ExtFileWriter() {}
    // Return |data.size()| if successful, -1 otherwise.
    virtual int64_t append(const char* buf, uint32_t count) = 0;

    virtual int64_t tell() = 0;

    // Sync data of the file to disk device
    virtual bool sync() = 0;

    // Close the descriptor of this file adaptor
    virtual bool close() { return true; }

    virtual std::string file_name() { return ""; }

protected:

    ExtFileWriter() {}

private:
    DISALLOW_COPY_AND_ASSIGN(ExtFileWriter);
};

class CompactionSstCache {
public:
    static CompactionSstCache* get_instance() {
        static CompactionSstCache _instance;
        return &_instance;
    }
    virtual ~CompactionSstCache() {}

    void init(int64_t len_threshold) {
        _cache.init(len_threshold);
    }

    std::string get_info() {
        return _cache.get_info();
    }

    void add(const std::string& key, const std::string& value) {
        _cache.add(key, value);
    }

    int find(const std::string& key, std::string* value) {
        return _cache.find(key, value);
    }

    size_t size() {
        return _cache.size();
    }
private:
    CompactionSstCache() {}
    Cache<std::string, std::string> _cache;
};

#ifdef BAIDU_INTERNAL
struct AfsRWInfo {
    std::string  uri;
    std::string  absolute_path;
    afs::Reader* reader = nullptr;
    afs::Writer* writer = nullptr;
    std::shared_ptr<afs::AFSImpl> fs = nullptr;
    std::shared_ptr<AfsStatis> statis = nullptr;
    uint64_t file_size = 0; // 读文件使用
};

class AfsExtFileReader : public ExtFileReader {
class ReadCtrl {
public:
    ReadCtrl(char* buf, uint32_t count, uint32_t offset, uint64_t file_size, bool* eof, uint32_t readers_cnt) : 
        _buf(buf), _count(count), _offset(offset), _file_size(file_size), _eof(eof), _readers_cnt(readers_cnt) {}

    void set_read_result(int64_t ret, char* buf);

    // 只等待一次超时，成功返1，其他返0需要继续读
    int read_wait_once();

    // 等待一次成功或全部失败
    int64_t read_wait_onesucc_or_allfail();

private:
    char* _buf;
    const uint32_t _count;
    const uint32_t _offset;
    const uint64_t _file_size;
    bool*   _eof;
    int64_t _success_ret = 0;
    // 当_successed为true时，_buf/_eof/_success_ret才有效，并受_mutex保护
    bool _successed = false;
    const uint32_t _readers_cnt;
    uint32_t _fail_cnt = 0;
    std::condition_variable _cv;
    std::mutex _mutex;
};

struct ReadInfo {
    ReadInfo(uint32_t count, uint32_t offset, const std::shared_ptr<ReadCtrl>& ctrl, AfsRWInfo* rw_info, AfsExtFileReader* reader) : 
        count(count), offset(offset), ctrl(ctrl), rw_info(rw_info), reader(reader) {
        buf = new char[count];
    }
    ~ReadInfo() {
        if (buf != nullptr) {
            delete[] buf;
            buf = nullptr;
        }
    }
    TimeCost time;
    char* buf = nullptr;
    uint32_t count = 0;
    uint32_t offset = 0;
    std::shared_ptr<ReadCtrl> ctrl = nullptr;
    AfsRWInfo* rw_info = nullptr;
    AfsExtFileReader* reader = nullptr;
};

public:
    explicit AfsExtFileReader(const std::vector<AfsRWInfo>& infos) : _afs_rw_infos(infos) {
        for (auto& info : _afs_rw_infos) {
            if (info.statis == nullptr) {
                info.statis = std::make_shared<AfsStatis>("common");
            }
            info.statis->reader_open_count << 1;
        }
    }

    explicit AfsExtFileReader(int infos_size) {
        // 提前分配空间，防止通过pushback添加触发扩容导致 get_avaliable_reader_infos 返回的指针失效
        _afs_rw_infos.reserve(infos_size);
    }

    virtual ~AfsExtFileReader() {
        close();
        for (auto& info : _afs_rw_infos) {
            info.statis->reader_open_count << -1;
        }
    }

    void add_reader(std::shared_ptr<AfsRWInfo> info);

    // https://ku.baidu-int.com/knowledge/HFVrC7hq1Q/LdvLKBn9Q5/sS6yXnWTJe/wh4NbomjxeQAOD#anchor-9abe82c0-ab51-11ec-9814-896cc24fcda6
    virtual int64_t read(char* buf, uint32_t count, uint32_t offset, bool* eof) override;

    virtual bool close() override;
    virtual std::string file_name() { return _afs_rw_infos[0].absolute_path; }
private:
    static void pread_callback(int64_t ret, void* ptr);
    std::vector<AfsRWInfo*> get_avaliable_reader_infos();

    std::atomic<int> _read_count = {0}; // 记录异步读请求数个数
    std::vector<AfsRWInfo> _afs_rw_infos;
    std::mutex _mtx;
};

class AfsExtFileWriter : public ExtFileWriter {
public:
    explicit AfsExtFileWriter(const std::vector<AfsRWInfo>& infos) : _afs_rw_infos(infos) {
        for (auto& info : _afs_rw_infos) {
            if (info.statis == nullptr) {
                info.statis = std::make_shared<AfsStatis>("common");
            }
        }
    }
    virtual ~AfsExtFileWriter() {
        close();
    }

    virtual int64_t append(const char* buf, uint32_t count) override;

    virtual int64_t tell() override;

    virtual bool sync() override;

    virtual bool close() override;
    virtual std::string file_name() { return _afs_rw_infos[0].absolute_path; }
private:
    std::vector<AfsRWInfo> _afs_rw_infos;
};

#endif

class CompactionExtFileReader : public ExtFileReader {
public:
    explicit CompactionExtFileReader(const std::string& file_name, const std::string& server_address, const std::string& remote_compaction_id)
        : _file_name(file_name), 
        _server_address(server_address),
        _remote_compaction_id(remote_compaction_id) {
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_remote_compaction_request_file_timeout;
        channel_opt.connect_timeout_ms = FLAGS_remote_compaction_connect_timeout;

        if (_channel.Init(server_address.c_str(), &channel_opt) != 0) {
            DB_FATAL("Failed to initialize channel to %s", server_address.c_str());
            _is_open = false;
        } else {
            _is_open = true;
        }
        cache = false;
    }
    virtual ~CompactionExtFileReader() {
        // DB_WARNING("【COMPACTION_DEBUG】close remote_compaction_id: %s, file_name %s", 
        //         _remote_compaction_id.c_str(), _file_name.c_str());
        if (_is_open) {
            close();
        }
    }
    virtual int64_t read(char* buf, uint32_t count, uint32_t offset, bool* eof) override;
    virtual int64_t skip(uint32_t n, bool* eof) override;
    virtual bool close() override;
    std::string file_name() { return _file_name; }
private:
    std::string _file_name;
    brpc::Channel _channel;
    bool _is_open;
    std::string _server_address;
    std::string _remote_compaction_id; // TODO 新建赋值
    int64_t total_time = 0;
    bool cache;
};

class CompactionExtFileWriter : public ExtFileWriter {
public:
    explicit CompactionExtFileWriter(const std::string& file_name, const std::string& server_address, const std::string& remote_compaction_id)
        : _file_name(file_name), 
        _server_address(server_address),
        _remote_compaction_id(remote_compaction_id),
        _offset(0) {
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_remote_compaction_request_file_timeout;
        channel_opt.connect_timeout_ms = FLAGS_remote_compaction_connect_timeout;

        if (_channel.Init(server_address.c_str(), &channel_opt) != 0) {
            DB_FATAL("Failed to initialize channel to %s", server_address.c_str());
            _is_open = false;
        } else {
            _is_open = true;
        }
    }
    virtual ~CompactionExtFileWriter() {
        if (_is_open) {
            close();
        }
    }
    virtual int64_t append(const char* buf, uint32_t count) override;
    virtual int64_t tell() override;
    virtual bool sync() override;
    virtual bool close() override;
private:
    std::string _file_name;
    brpc::Channel _channel;
    bool _is_open;
    std::string _server_address;
    std::string _remote_compaction_id;
    int64_t _offset;
};

class ExtFileSystem {
public:
    ExtFileSystem() {}
    virtual ~ExtFileSystem() {}

    virtual int init() = 0;
    // cluster : 指定集群
    // force   : 是否强制使用指定的cluster
    virtual std::string make_full_name(const std::string& cluster, bool force, const std::string& user_define_path) = 0;
    virtual int open_reader(const std::string& full_name, std::shared_ptr<ExtFileReader>* reader) = 0;
    virtual int open_writer(const std::string& full_name, std::unique_ptr<ExtFileWriter>* writer) = 0;

    // Deletes the given path, whether it's a file or a directory. If it's a directory,
    // it's perfectly happy to delete all of the directory's contents. Passing true to 
    // recursive deletes subdirectories and their contents as well.
    // Returns true if successful, false otherwise. It is considered successful
    // to attempt to delete a file that does not exist.
    virtual int delete_path(const std::string& full_name, bool recursive) = 0;

    // 创建文件，需要支持递归创建路径上不存在的目录
    virtual int create(const std::string& full_name) = 0;

    // Creates a directory. If create_parent_directories is true, parent directories
    // will be created if not exist, otherwise, the create operation will fail.
    // Returns 'true' on successful creation, or if the directory already exists. 
    virtual int create_directory(const std::string& full_name, 
                              bool create_parent_directories) {
        // create接口支持递归创建path中不存在的目录，暂时不使用create_directory
        DB_FATAL("not support");
        return -1;
    }

    // virtual int file_size(const std::string& path, int64_t* size) = 0; 

    // Returns -1:failed; 0: not exists; 1: exists
    virtual int path_exists(const std::string& full_name) = 0;

    virtual int readdir(const std::string& full_name, std::set<std::string>& sub_files) = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(ExtFileSystem);
};

#ifdef BAIDU_INTERNAL
class AfsExtFileSystem : public ExtFileSystem {
public:
struct AfsUgi {
    std::string uri;
    std::string user;
    std::string password;
    std::string cluster_name;
    std::string root_path; // "/user/baikal"
    std::shared_ptr<afs::AFSImpl> afs = nullptr;
};
#ifdef ENABLE_OPEN_AFS_ASYNC
class AfsFileCtrl {
public:
    AfsFileCtrl(uint32_t afs_cnt): _afs_cnt(afs_cnt), _success_ret(false) {}

    // 解锁，设置success_
    void action_finish(int64_t ret);

    int64_t wait_onesucc_or_allfail();

private:
    std::condition_variable _cv;
    std::mutex _mutex;
    const uint32_t _afs_cnt;
    bool _succeeded = false;
    uint32_t _fail_cnt = 0;
    int64_t _success_ret = -1;
};

struct OpenReaderInfo {
    std::shared_ptr<AfsFileCtrl> reader_ctrl;
    std::shared_ptr<AfsExtFileReader> reader;
    std::shared_ptr<AfsRWInfo> afs_rw_info;
    uint64_t stat_cost = 0;
    uint64_t ext_file_size = 0;
    TimeCost cost;

    OpenReaderInfo(std::shared_ptr<AfsExtFileReader> reader,
            const std::shared_ptr<AfsFileCtrl>& reader_ctrl, 
            std::shared_ptr<AfsRWInfo> afs_rw_info, uint64_t ext_file_size) 
                    : reader(reader), reader_ctrl(reader_ctrl), 
                      afs_rw_info(afs_rw_info), 
                      ext_file_size(ext_file_size) {}
};

    static void open_reader_callback(int64_t ret, void* ptr);
#endif
    explicit AfsExtFileSystem(const std::vector<AfsUgi>& ugi_infos) : _ugi_infos(ugi_infos) {}
    virtual ~AfsExtFileSystem();

    virtual int init() override;

    std::string make_full_name(const std::string& cluster, bool force, const std::string& user_define_path) override;
    // path:  afs://andi.afs.baidu.com:9902/user/olap/xxx/xxxx
    virtual int open_reader(const std::string& full_name, std::shared_ptr<ExtFileReader>* reader) override;
    virtual int open_writer(const std::string& full_name, std::unique_ptr<ExtFileWriter>* writer) override;
    virtual int delete_path(const std::string& full_name, bool recursive) override;
    virtual int create(const std::string& full_name) override;
    virtual int path_exists(const std::string& full_name) override;
    virtual int readdir(const std::string& full_name, std::set<std::string>& sub_files) override;

private:
    std::shared_ptr<afs::AFSImpl> init(const std::string& uri, const std::string& user, 
                                            const std::string& password, const std::string& conf_file);
    std::vector<AfsRWInfo> get_rw_infos_by_full_name(const std::string& full_name);
    std::vector<AfsRWInfo> get_rw_infos(const std::string& user_define_path);
    bthread::Mutex _lock;
    // uri: afs://master_host:master_port
    std::map<std::string, std::shared_ptr<AfsStatis>> _uri_afs_statics;
    std::vector<AfsUgi> _ugi_infos;
};

int get_afs_infos(std::vector<AfsExtFileSystem::AfsUgi>& ugi_infos);

class ExtFileSystemGC {
public:
    static int external_filesystem_gc();
    static int external_filesystem_gc_do();
    static int get_all_partitions_from_store(std::map<int64_t, std::map<std::string, std::set<std::string>>>& table_id_name_partitions);
private:
    static int table_gc(std::shared_ptr<ExtFileSystem> ext_fs, const std::map<int64_t, std::map<std::string, std::set<std::string>>>& table_id_name_partitions_map);
    static int partition_gc(std::shared_ptr<ExtFileSystem> ext_fs, const std::string& database_name, const std::string& table_name_in_store, 
            int64_t table_id, const std::set<std::string>& partitions_in_store, const std::string& start_str);
    static int column_partition_gc(std::shared_ptr<ExtFileSystem> ext_fs, const std::string& database_name, const std::string& table_name_in_store, 
            int64_t table_id, const std::string& start_str);
    static bool need_delete_partition(const std::string& partition, const std::string& start_str);
    static int check_partition(const std::string& partition, std::string* start_date, std::string* end_date);
};
#endif

class CompactionExtFileSystem : public ExtFileSystem {
public:
    explicit CompactionExtFileSystem(const std::string& address, 
                    const std::string& remote_compaction_id) 
        : _address(address),
        _remote_compaction_id(remote_compaction_id) {
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_remote_compaction_request_file_timeout;
        channel_opt.connect_timeout_ms = FLAGS_remote_compaction_connect_timeout;

        if (_channel.Init(_address.c_str(), &channel_opt) != 0) {
            DB_FATAL("Failed to initialize channel to %s", _address.c_str());
            _is_open = false;
        } else {
            _is_open = true;
        }
    }
    virtual ~CompactionExtFileSystem() {}

    virtual int init() override;
    virtual int open_reader(const std::string& full_name, std::shared_ptr<ExtFileReader>* reader) override;
    virtual int open_writer(const std::string& full_name, std::unique_ptr<ExtFileWriter>* writer) override;
    virtual int delete_path(const std::string& full_name, bool recursive) {
        DB_FATAL("CompactionExtFileSystem not implement delete_path, name: %s", full_name.c_str());
        return -1;
    }
    virtual int create(const std::string& full_name) override;
    virtual int path_exists(const std::string& full_name) override;
    virtual int readdir(const std::string& full_name, std::set<std::string>& file_list) override;
    int get_file_info_list(std::vector<pb::CompactionFileInfo>& file_info_list);
    int rename_file(const std::string& src_file_name, const std::string& dst_file_name);
    std::string make_full_name(const std::string& cluster, bool force, const std::string& user_define_path);
    int external_send_request(pb::CompactionOpType op_type, 
                                            const std::string& full_name, 
                                            bool recursive,
                                            pb::CompactionFileResponse& response);
    bool is_sst(const std::string& full_name);
    int delete_remote_copy_file_path();
private:
    std::string _address;
    brpc::Channel _channel;
    bool _is_open;
    std::string _remote_compaction_id;
};

}  // namespace baikaldb
