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

#include <map>
#include <condition_variable>
#ifdef BAIDU_INTERNAL
#include <raft/file_system_adaptor.h>
#else
#include <braft/file_system_adaptor.h>
#endif
#include "rocks_wrapper.h"
#include "sst_file_writer.h"

namespace baikaldb {

const std::string SNAPSHOT_DATA_FILE = "region_data_snapshot.sst";
const std::string SNAPSHOT_META_FILE = "region_meta_snapshot.sst";
const std::string SNAPSHOT_DATA_FILE_WITH_SLASH = "/" + SNAPSHOT_DATA_FILE;
const std::string SNAPSHOT_META_FILE_WITH_SLASH = "/" + SNAPSHOT_META_FILE;

class RaftSnapshotAdaptor;

struct IteratorContext {
    bool reading = false;
    std::unique_ptr<rocksdb::Iterator> iter;
    std::string prefix;
    bool is_meta_sst = false;
    int64_t offset = 0;
    bool done = false;
};

struct SnapshotContext {
    SnapshotContext()
        : snapshot(RocksWrapper::get_instance()->get_snapshot()) {}
    ~SnapshotContext() {
        if (data_context != nullptr) {
            delete data_context;
        }
        if (meta_context != nullptr) {
            delete meta_context;
        }
        if (snapshot != nullptr) {
            RocksWrapper::get_instance()->relase_snapshot(snapshot);
        }
    }
    const rocksdb::Snapshot* snapshot = nullptr;
    IteratorContext* data_context = nullptr;
    IteratorContext* meta_context = nullptr;
};

typedef std::shared_ptr<SnapshotContext> SnapshotContextPtr;

class PosixDirReader : public braft::DirReader {
friend class RaftSnapshotAdaptor;
public:
    virtual ~PosixDirReader() {}

    // Check if the dir reader is valid
    virtual bool is_valid() const override;

    // Move to next entry in the directory
    // Return true if a entry can be found, false otherwise
    virtual bool next() override;

    // Get the name of current entry
    virtual const char* name() const override;

protected:
    PosixDirReader(const std::string& path) : _dir_reader(path.c_str()) {}

private:
    butil::DirReaderPosix _dir_reader;
};
// RocksdbSnapshotReader used to iterate rocksdb snapshot when raft leader
// need transfer snapshot to a follower
class RocksdbSnapshotReader : public braft::FileAdaptor {
friend class RaftSnapshotAdaptor;
public:
    virtual ~RocksdbSnapshotReader();

    // Read |size| k-v from the rocksdb snapshot and serialize them to IOPortal
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    // This function will be called in braft::SnapshotFileReader after
    // 'read', to judge if all data has been read
    virtual ssize_t size() override;
    
#ifdef BAIDU_INTERNAL
    // Close this adaptor
    virtual bool close() override;
#else 
    bool close();
#endif
    void open() {
        _closed = false;
    }
    // Not used, return '-1' directly
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    // Not used, return 'false' directly
    virtual bool sync() override;

protected:
    RocksdbSnapshotReader(int64_t region_id,
                        const std::string& path,
                        RaftSnapshotAdaptor* rs,
                        SnapshotContextPtr context,
                        bool is_meta_reader);

private:
    int64_t  append_to_iobuf(butil::IOPortal* portal, const rocksdb::Slice& key) {
        if (portal != nullptr) {
            portal->append((void*)&key.size_, sizeof(size_t));
            portal->append((void*)key.data_, key.size_);
        }
        return sizeof(size_t) + key.size_;
    }
private:

    int64_t _region_id;
    std::string _path;
    RaftSnapshotAdaptor* _rs = nullptr;
    SnapshotContextPtr _context = nullptr;
    bool _is_meta_reader = false;
    bool _closed;
};

class RocksdbSstWriter : public braft::FileAdaptor {
friend class RaftSnapshotAdaptor;
public:
    virtual ~RocksdbSstWriter();

    // Open an rocksdb sst file
    int open();

    // Deserialize |data| to rocksdb k-v and put to sst file
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
#ifdef BAIDU_INTERNAL
    // Close this adaptor
    virtual bool close() override;
#else
    bool close();
#endif
    // Not used
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    // Not used
    virtual ssize_t size() override;
    // Not used
    virtual bool sync() override;

protected:
    RocksdbSstWriter(int64_t region_id, const std::string& path, const rocksdb::Options& option);

private:
    int parse_from_iobuf(const butil::IOBuf& data, std::vector<std::string>& keys, std::vector<std::string>& values) {
        size_t pos = 0;
        while (pos < data.size()) {
            if ((data.size() - pos) < sizeof(size_t)) {
                DB_FATAL("read key size from iobuf fail, region_id: %ld", _region_id);
                return -1;
            }
            size_t key_size = 0;
            data.copy_to((void*)&key_size, sizeof(size_t), pos);
            
            //DB_WARNING("key size: %lu", key_size);
            pos += sizeof(size_t);
            std::string key;
            if ((data.size() - pos) < key_size) {
                DB_FATAL("read key from iobuf fail, region_id: %ld, key_size: %ld", 
                        _region_id, key_size);
                return -1;
            }
            data.copy_to(&key, key_size, pos);

            pos += key_size;
            keys.push_back(key);
            if ((data.size() - pos) < sizeof(size_t)) {
                DB_FATAL("read value size from iobuf fail, region_id: %ld", _region_id);
                return -1;
            }
            size_t value_size = 0;
            data.copy_to((void*)&value_size, sizeof(size_t), pos);

            //DB_WARNING("value size: %lu", value_size);
            pos += sizeof(size_t);
            std::string value;
            if ((data.size() - pos) < value_size) {
                DB_FATAL("read value from iobuf fail, region_id: %ld, value_size: %ld", 
                        _region_id, value_size);
                return 1;
            }
            data.copy_to(&value, value_size, pos);
            pos += value_size;
            values.push_back(value);
        }
        return 0;
    }
    int64_t _region_id;
    bool _closed;
    // rocksdb sst file path, for logging
    std::string _path;
    //record number of key-value written
    size_t _count;
    std::unique_ptr<SstFileWriter> _writer;
};

class PosixFileAdaptor : public braft::FileAdaptor {
friend class RaftSnapshotAdaptor;
public:
    virtual ~PosixFileAdaptor();
    int open(int oflag);
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    virtual ssize_t size() override;
    virtual bool sync() override;
#ifdef BAIDU_INTERNAL
    virtual bool close() override;
#else
    bool close();
#endif

protected:
    PosixFileAdaptor(const std::string& p) : _path(p), _fd(-1) {}

private:
    std::string _path;
    int _fd;
};

class RaftSnapshotAdaptor : public braft::FileSystemAdaptor {
public:
    RaftSnapshotAdaptor(int64_t region_id);
    virtual ~RaftSnapshotAdaptor();

    virtual bool delete_file(const std::string& path, bool recursive) override;
    virtual bool rename(const std::string& old_path, const std::string& new_path) override;
    virtual bool link(const std::string& old_path, const std::string& new_path) override;
    virtual bool create_directory(const std::string& path,
                                  butil::File::Error* error,
                                  bool create_parent_directories) override;
    virtual bool path_exists(const std::string& path) override;
    virtual bool directory_exists(const std::string& path) override;
    virtual braft::DirReader* directory_reader(const std::string& path) override;

    virtual braft::FileAdaptor* open(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e) override;
    // This method will be called at the very begin before read snapshot file.
    // The default implementation is return 'true' directly.
    virtual bool open_snapshot(const std::string& snapshot_path) override;
    // This method will be called after read all snapshot files or failed.
    // The default implementation is return directly.
    virtual void close_snapshot(const std::string& snapshot_path) override;

    // Close FileAdaptor for the specified path
    void close(const std::string& path);
private:
    braft::FileAdaptor* open_for_read(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e);
    braft::FileAdaptor* open_for_write(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e);

    SnapshotContextPtr get_snapshot(const std::string& path);

private:
    int64_t             _region_id;
    std::mutex          _snapshot_mutex;
    std::condition_variable _empty_cond;
    typedef std::map<std::string, std::pair<SnapshotContextPtr, int64_t>> SnapshotMap;
    SnapshotMap         _snapshots;
};

} //namespace raft

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
