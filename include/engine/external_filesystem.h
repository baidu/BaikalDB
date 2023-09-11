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
#ifdef BAIDU_INTERNAL
#include "baidu/inf/afs-api/client/afs_filesystem.h"
#include "baidu/inf/afs-api/common/afs_common.h"
#endif
#include "common.h"

namespace baikaldb {
struct AfsStatis {
    explicit AfsStatis(const std::string& cluster_name) : afs_cluster(cluster_name), 
        read_time_cost(afs_cluster + "_afs_read_time_cost", 60),
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

#ifdef BAIDU_INTERNAL
class AfsFileReader : public ExtFileReader {
public:
    explicit AfsFileReader(const std::string& uri, const std::string& name, afs::Reader* reader, 
                    const std::shared_ptr<afs::AfsFileSystem>& fs, 
                    const std::shared_ptr<AfsStatis>& statis) : 
                    _uri(uri), _name(name), _reader(reader), _fs(fs), _statis(statis) {
        if (_statis == nullptr) {
            _statis = std::make_shared<AfsStatis>("common");
        }
        _statis->reader_open_count << 1;
    }
    virtual ~AfsFileReader() {
        if (_reader != nullptr) {
            int ret = _fs->CloseReader(_reader);
            if (ret != ds::kOk) {
                DB_FATAL("close failed, uri: %s, name: %s", _uri.c_str(), _name.c_str());
            }
        }
        _statis->reader_open_count << -1;
    }

    virtual int64_t read(char* buf, uint32_t count, uint32_t offset, bool* eof) override {
        TimeCost time;
        int64_t ret = _reader->PRead(buf, count, offset, eof);
        if (ret < 0) {
            _statis->read_fail_count << 1;
            DB_FATAL("read uri: %s, file: %s count: %u, offset: %u failed", _uri.c_str(), _name.c_str(), count, offset);
            return -1;
        }
        _statis->read_bytes << ret;
        _statis->read_time_cost << time.get_time();
        DB_NOTICE("read uri: %s, file: %s count: %u, offset: %u return: %ld, time: %ld", 
                _uri.c_str(), _name.c_str(), count, offset, ret, time.get_time());
        return ret;
    }

    virtual bool close() override {
        int ret = _fs->CloseReader(_reader);
        _reader = nullptr;
        if (ret != ds::kOk) {
            DB_FATAL("close failed, uri: %s, name: %s", _uri.c_str(), _name.c_str());
            return false;
        }
        
        return true;
    }
    virtual std::string file_name() { return _name; }
private:
    std::string  _uri;
    std::string  _name;
    afs::Reader* _reader;
    std::shared_ptr<afs::AfsFileSystem> _fs;
    std::shared_ptr<AfsStatis> _statis;
};

class AfsFileWriter : public ExtFileWriter {
public:
    explicit AfsFileWriter(const std::string& uri, const std::string& name, afs::Writer* writer, 
                    const std::shared_ptr<afs::AfsFileSystem>& fs,
                    const std::shared_ptr<AfsStatis>& statis) : 
                    _uri(uri), _name(name), _writer(writer), _fs(fs), _statis(statis) {
        if (_statis == nullptr) {
            _statis = std::make_shared<AfsStatis>("common");
        }
    }
    virtual ~AfsFileWriter() {
        if (_writer != nullptr) {
            int ret = _fs->CloseWriter(_writer, nullptr, nullptr);
            if (ret != ds::kOk) {
                DB_FATAL("close failed, uri: %s, name: %s", _uri.c_str(), _name.c_str());
            }
        }
    }

    virtual int64_t append(const char* buf, uint32_t count) override {
        int64_t ret = _writer->Append(buf, count, nullptr, nullptr);
        if (ret < 0) {
            _statis->write_fail_count << 1;
            DB_FATAL("append failed, uri: %s, name: %s, error: %s", _uri.c_str(), _name.c_str(), ds::Rc2Str(ret));
            return -1;
        } else if (ret != count) {
            _statis->write_fail_count << 1;
            DB_FATAL("append failed, uri: %s, name: %s, ret: %ld, count: %u", 
                _uri.c_str(), _name.c_str(), ret, count);
            return -1;
        } else {
            _statis->write_bytes << ret;
            return ret;
        }
    }

    virtual int64_t tell() {
        return _writer->Tell();
    }

    virtual bool sync() override {
        int ret = _writer->Sync(nullptr, nullptr);
        if (ret != ds::kOk) {
            DB_FATAL("sync failed, uri: %s, name: %s, error: %s", _uri.c_str(), _name.c_str(), ds::Rc2Str(ret));
            return false;
        }
        return true;
    }

    virtual bool close() override {
        int ret = _fs->CloseWriter(_writer, nullptr, nullptr);
        _writer = nullptr;
        if (ret != ds::kOk) {
            DB_FATAL("close failed, uri: %s, name: %s", _uri.c_str(), _name.c_str());
            return false;
        }
        
        return true;
    }
    virtual std::string file_name() { return _name; }
private:
    std::string  _uri;
    std::string  _name;
    afs::Writer* _writer;
    std::shared_ptr<afs::AfsFileSystem> _fs;
    std::shared_ptr<AfsStatis> _statis;
};
#endif

class ExtFileSystem {
public:
    ExtFileSystem() {}
    virtual ~ExtFileSystem() {}

    virtual int init() = 0;
    // cluster : 指定集群
    // force   : 是否强制使用指定的cluster
    virtual std::string make_full_name(const std::string& cluster, bool force, const std::string& user_define_path) = 0;
    virtual int open_reader(const std::string& path, std::unique_ptr<ExtFileReader>* reader) = 0;
    virtual int open_writer(const std::string& path, std::unique_ptr<ExtFileWriter>* writer) = 0;

    // Deletes the given path, whether it's a file or a directory. If it's a directory,
    // it's perfectly happy to delete all of the directory's contents. Passing true to 
    // recursive deletes subdirectories and their contents as well.
    // Returns true if successful, false otherwise. It is considered successful
    // to attempt to delete a file that does not exist.
    virtual int delete_path(const std::string& path, bool recursive) = 0;

    // 创建文件，需要支持递归创建路径上不存在的目录
    virtual int create(const std::string& path) = 0;

    // Creates a directory. If create_parent_directories is true, parent directories
    // will be created if not exist, otherwise, the create operation will fail.
    // Returns 'true' on successful creation, or if the directory already exists. 
    virtual int create_directory(const std::string& path, 
                              bool create_parent_directories) {
        // create接口支持递归创建path中不存在的目录，暂时不使用create_directory
        DB_FATAL("not support");
        return -1;
    }

    // virtual int file_size(const std::string& path, int64_t* size) = 0; 

    // Returns -1:failed 0: not exists 1: exists
    virtual int path_exists(const std::string& path) = 0;

    virtual int list(const std::string& cluster, const std::string& path, std::set<std::string>& files) = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(ExtFileSystem);
};

#ifdef BAIDU_INTERNAL
class AfsFileSystem : public ExtFileSystem {
public:
struct AfsUgi {
    std::string uri;
    std::string user;
    std::string password;
    std::string cluster_name;
    std::string root_path; // "/user/baikal"
    std::shared_ptr<afs::AfsFileSystem> afs = nullptr;
};

    explicit AfsFileSystem(const std::vector<AfsUgi>& ugi_infos) : _ugi_infos(ugi_infos) {}
    virtual ~AfsFileSystem();

    virtual int init() override;

    std::string make_full_name(const std::string& cluster, bool force, const std::string& user_define_path) override;
    // path:  afs://andi.afs.baidu.com:9902/user/olap/xxx/xxxx
    virtual int open_reader(const std::string& path, std::unique_ptr<ExtFileReader>* reader) override;
    virtual int open_writer(const std::string& path, std::unique_ptr<ExtFileWriter>* writer) override;
    virtual int delete_path(const std::string& path, bool recursive) override;
    virtual int create(const std::string& path) override;
    virtual int path_exists(const std::string& path) override;
    virtual int list(const std::string& cluster, const std::string& path, std::set<std::string>& files) override;

private:
    std::shared_ptr<afs::AfsFileSystem> get_fs_by_full_name(const std::string& full_name, std::string* uri, std::string* absolute_path);
    std::shared_ptr<afs::AfsFileSystem> init(const std::string& uri, const std::string& user, 
                                            const std::string& password, const std::string& conf_file);
    bthread::Mutex _lock;
    // uri: afs://master_host:master_port
    std::map<std::string, std::shared_ptr<AfsStatis>> _uri_afs_statics;
    std::vector<AfsUgi> _ugi_infos;
};
#endif


}  // namespace baikaldb
