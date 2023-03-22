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

#include <map>
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
const size_t SST_FILE_LENGTH = 128 * 1024 * 1024;

class RocksdbFileSystemAdaptor;
class Region;
typedef std::shared_ptr<Region> SmartRegion;
struct SnapshotContext;
struct IteratorContext {
    bool reading = false;
    bool is_meta_sst = false;
    bool done = false;
    std::unique_ptr<rocksdb::Iterator> iter;
    std::string prefix;
    std::string upper_bound;
    rocksdb::Slice upper_bound_slice;
    int64_t offset = 0;
    int64_t snapshot_index = 0;
    int64_t applied_index = 0;
    bool need_copy_data = true;
    TimeCost offset_update_time; // 更新offset时更新此时间，长时间未访问可能对端挂掉
    SnapshotContext* sc = nullptr;
};

typedef std::shared_ptr<IteratorContext> IteratorContextPtr;

struct SnapshotContext {
    SnapshotContext()
        : snapshot(RocksWrapper::get_instance()->get_snapshot()) {}
    ~SnapshotContext() {
        if (snapshot != nullptr) {
            RocksWrapper::get_instance()->relase_snapshot(snapshot);
        }
    }
    const rocksdb::Snapshot* snapshot = nullptr;
    IteratorContextPtr data_context = nullptr;
    IteratorContextPtr meta_context = nullptr;
    int64_t data_index = 0;
    int64_t binlog_check_point = 0;
};

typedef std::shared_ptr<SnapshotContext> SnapshotContextPtr;

class PosixDirReader : public braft::DirReader {
friend class RocksdbFileSystemAdaptor;
public:
    virtual ~PosixDirReader() {}

    virtual bool is_valid() const override;

    virtual bool next() override;

    virtual const char* name() const override;

protected:
    PosixDirReader(const std::string& path) : _dir_reader(path.c_str()) {}

private:
    butil::DirReaderPosix _dir_reader;
};

//从rocksdb中读取region的全量信息，包括两部分data 和 meta 信息
class RocksdbReaderAdaptor : public braft::FileAdaptor {
friend class RocksdbFileSystemAdaptor;
public:
    virtual ~RocksdbReaderAdaptor();

    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    
    virtual ssize_t size() override;
    
    virtual bool close() override;
    void open() {
        _closed = false;
    }
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    
    virtual bool sync() override;

protected:
    RocksdbReaderAdaptor(int64_t region_id,
                        const std::string& path,
                        RocksdbFileSystemAdaptor* rs,
                        IteratorContextPtr context,
                        bool is_meta_reader);

private:
    //把rocksdb的key 和 value 串行化到iobuf中，通过rpc发送到接受peer
    int64_t serialize_to_iobuf(butil::IOPortal* portal, const rocksdb::Slice& key) {
        if (portal != nullptr) {
            portal->append((void*)&key.size_, sizeof(size_t));
            portal->append((void*)key.data_, key.size_);
        }
        return sizeof(size_t) + key.size_;
    }

    bool region_shutdown();

    void context_reset();

private:

    int64_t _region_id;
    SmartRegion _region_ptr;
    std::string _path;
    RocksdbFileSystemAdaptor* _rs = nullptr;
    IteratorContextPtr _context = nullptr;
    bool _is_meta_reader = false;
    bool _closed = true;
    size_t _num_lines = 0;
    butil::IOPortal _last_package;
    off_t _last_offset = 0;
};

class SstWriterAdaptor : public braft::FileAdaptor {
friend class RocksdbFileSystemAdaptor;
public:
    virtual ~SstWriterAdaptor();

    int open();

    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    virtual bool close() override;
    
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    
    virtual ssize_t size() override;
    
    virtual bool sync() override;

protected:
    SstWriterAdaptor(int64_t region_id, const std::string& path, const rocksdb::Options& option);

    bool region_shutdown();

private:
    bool finish_sst();
    int iobuf_to_sst(butil::IOBuf data);
    int64_t _region_id;
    SmartRegion _region_ptr;
    std::string _path;
    int _sst_idx = 0;
    size_t _count = 0;
    size_t _data_size = 0;
    bool _closed = true;
    bool _is_meta = false;
    std::unique_ptr<SstFileWriter> _writer;
};

class PosixFileAdaptor : public braft::FileAdaptor {
friend class RocksdbFileSystemAdaptor;
public:
    virtual ~PosixFileAdaptor();
    int open(int oflag);
    virtual ssize_t write(const butil::IOBuf& data, off_t offset) override;
    virtual ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
    virtual ssize_t size() override;
    virtual bool sync() override;
    virtual bool close() override;

protected:
    PosixFileAdaptor(const std::string& p) : _path(p), _fd(-1) {}

private:
    std::string _path;
    int _fd;
};

class RocksdbFileSystemAdaptor : public braft::FileSystemAdaptor {
public:
    RocksdbFileSystemAdaptor(int64_t region_id);
    virtual ~RocksdbFileSystemAdaptor();

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
    virtual bool open_snapshot(const std::string& snapshot_path) override;
    virtual void close_snapshot(const std::string& snapshot_path) override;

    void close(const std::string& path);
private:
    braft::FileAdaptor* open_reader_adaptor(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e);
    braft::FileAdaptor* open_writer_adaptor(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              butil::File::Error* e);

    SnapshotContextPtr get_snapshot(const std::string& path);

private:
    struct ContextEnv {
        SnapshotContextPtr ptr;
        int64_t count = 0;
        TimeCost cost;
    };
    int64_t             _region_id;
    bthread::Mutex      _snapshot_mutex;
    bthread::Mutex      _open_reader_adaptor_mutex;
    BthreadCond         _mutil_snapshot_cond;
    typedef std::map<std::string, ContextEnv> SnapshotMap;
    SnapshotMap         _snapshots;
};

} //namespace raft

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
