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
const std::string SNAPSHOT_PARQUET_META_FILE = "parquet_meta";
const std::string SNAPSHOT_REMOTE_FILE = "remote_file";
const int64_t rocksdb_internal_del_key_magic = -123456; // 内部keymagic，放在key前8bytes，regionid不会为负数
const int64_t rocksdb_remote_file_key_magic = -123456789;

class RocksdbFileSystemAdaptor;
class Region;
typedef std::shared_ptr<Region> SmartRegion;
struct SnapshotContext;

// Like std::destroy_at but a callable type
template <typename T>
struct Destroyer {
  void operator()(T* ptr) { ptr->~T(); }
};

// Like std::unique_ptr but only placement-deletes the object (for
// objects allocated on an arena).
template <typename T>
using ScopedArenaPtr = std::unique_ptr<T, Destroyer<T>>;

class LocalIterator {
public:
    LocalIterator(const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* family, const std::vector<std::string>& remote_files, int64_t region_id) {
        if (!remote_files.empty()) {
            _read_options = options;
            _region_id = region_id;
            _is_internal_iter = true;
            _remote_files = remote_files;
            auto txn_db = RocksWrapper::get_instance()->get_db();
            _seq_number = options.snapshot->GetSequenceNumber();
            _read_options.table_filter = [](const rocksdb::TableProperties& props) -> bool {
                const auto& user_collected = props.user_collected_properties;
                if (user_collected.find("output_level") != user_collected.end()) {
                    DB_WARNING("TablePropertiesCollection[orig_file_number: %lu] [%s]", props.orig_file_number, props.ToString().c_str());
                    return false;
                }
                DB_WARNING("TablePropertiesCollection[orig_file_number: %lu]", props.orig_file_number);
                return true;
            };
            DB_WARNING("region_id: %ld, seq_number: %lu remote_files: %s", _region_id, _seq_number, remote_files[0].c_str());
            _internal_iter.reset(txn_db->NewInternalIterator(_read_options, _seq_number, family, &_arena));
        } else {
            _iter.reset(RocksWrapper::get_instance()->new_iterator(options, family));
        }
    } 

    LocalIterator(const rocksdb::ReadOptions& options, rocksdb::ColumnFamilyHandle* family) : LocalIterator(options, family, {}, 0) { }

    rocksdb::Slice key() {
        if (_is_internal_iter) {
            // assert(!_internal_keys.empty());
            auto& key = _internal_keys.front();
            if (key.size() == sizeof(int64_t)) {
                return rocksdb::Slice(key);
            } else {
                rocksdb::Slice key_slice(key);
                rocksdb::ParsedInternalKey ikey;
                rocksdb::ParseInternalKey(key_slice, &ikey, true /* log_err_key */);
                // assert(ikey.type == rocksdb::kTypeValue || ikey.type == rocksdb::kTypeDeletion /* || ikey.type == rocksdb::kTypeMerge */);
                if (ikey.type == rocksdb::kTypeDeletion) {
                    MutTableKey tmp_key;
                    tmp_key.append_i64(rocksdb_internal_del_key_magic);
                    tmp_key.append_char(ikey.user_key.data(), ikey.user_key.size());
                    _tmp_key = std::move(tmp_key);
                    return rocksdb::Slice(_tmp_key.data());
                } else {
                    return ikey.user_key;
                }
            }
        } else {
            return _iter->key();
        }
    }

    rocksdb::Slice value() {
        if (_is_internal_iter) {
            // assert(!_internal_values.empty());
            auto& value = _internal_values.front();
            return rocksdb::Slice(value);
        } else {
            return _iter->value();
        }
    }

    bool Valid() {
        if (_is_internal_iter) {
            if (_internal_keys.empty()) {
                // assert(_internal_iter_done);
                return false;
            } else {
                // assert(!_internal_iter_done);
                return true;
            }
        } else {
            return _iter->Valid();
        }
    }

    rocksdb::Status status() {
        if (_is_internal_iter) {
            return _internal_iter->status();
        } else {
            return _iter->status();
        }
    }

    void Seek(const rocksdb::Slice& target) {
        if (!_is_internal_iter) {
            _iter->Seek(target);
            return;
        }

        MutTableKey tmp_key;
        tmp_key.append_i64(rocksdb_remote_file_key_magic);
        _internal_keys.emplace_back(tmp_key.data());
        _internal_values.emplace_back(boost::join(_remote_files, "\t"));

        rocksdb::InternalKey ikey;
        ikey.SetMinPossibleForUserKey(target);
        _internal_iter->Seek(ikey.Encode());
        internal_iterator_fill_queue();
    }

    void Next() {
        if (!_is_internal_iter) {
            _iter->Next();
            return;
        }

        // assert(!_internal_keys.empty());
        // assert(!_internal_values.empty());
        // assert(_internal_keys.size() == _internal_values.size());

        _internal_keys.pop_front();
        _internal_values.pop_front();

        if (!_internal_iter_done) {
            _internal_iter->Next();
            internal_iterator_fill_queue();
        }
    }

    void internal_iterator_fill_queue() {
        // 外部已经seek或next
        while (true) {
            if (_internal_iter->Valid()) {
                MutTableKey key;
                key.append_i64(_region_id);
                if (!_internal_iter->key().starts_with(key.data())) {
                    _internal_iter_done = true;
                    break;
                }
                rocksdb::ParsedInternalKey ikey;
                rocksdb::ParseInternalKey(_internal_iter->key(), &ikey, true /* log_err_key */);
                // 需要自己过滤snapshot的key
                if (ikey.sequence > _seq_number) {
                    _internal_iter->Next();
                    continue;
                } else {
                    if (_internal_keys.empty() || _internal_keys.back().size() < 2 * sizeof(int64_t)) {
                        _internal_keys.emplace_back(_internal_iter->key().ToString());
                        _internal_values.emplace_back(_internal_iter->value().ToString());
                    } else {
                        auto& back_key = _internal_keys.back();
                        rocksdb::Slice back_slice(back_key);
                        rocksdb::ParsedInternalKey back_ikey;
                        rocksdb::ParseInternalKey(back_slice, &back_ikey, true /* log_err_key */);
                        if (ikey.user_key.compare(back_ikey.user_key) == 0) {
                            // assert(back_ikey.sequence < ikey.sequence);
                            _internal_keys.pop_back();
                            _internal_values.pop_back();
                            _internal_keys.emplace_back(_internal_iter->key().ToString());
                            _internal_values.emplace_back(_internal_iter->value().ToString());
                        } else {
                            _internal_keys.emplace_back(_internal_iter->key().ToString());
                            _internal_values.emplace_back(_internal_iter->value().ToString());
                        }
                    }
                    if (_internal_keys.size() > 5) {
                        break;
                    }
                    _internal_iter->Next();
                }
            } else {
                _internal_iter_done = true;
                break;
            }
        }
    }

    static int get_l6_remote_files(int64_t region_id, const std::vector<std::string>& peers_installing_snapshot, 
        std::vector<std::string>& remote_files);

private:
    // @param NewInternalIterator read_options Must outlive the returned iterator.
    rocksdb::ReadOptions _read_options;
    rocksdb::Arena _arena;
    int64_t _region_id = 0;
    bool _is_internal_iter = false;
    bool _internal_iter_done = false;
    std::vector<std::string> _remote_files;
    rocksdb::SequenceNumber _seq_number;
    std::unique_ptr<rocksdb::Iterator> _iter = nullptr;
    ScopedArenaPtr<rocksdb::InternalIterator> _internal_iter = nullptr;
    MutTableKey _tmp_key;
    std::list<std::string> _internal_keys;
    std::list<std::string> _internal_values;
};

struct IteratorContext {
    bool reading = false;
    bool is_meta_sst = false;
    bool done = false;
    std::unique_ptr<LocalIterator> iter;
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
            RocksWrapper::get_instance()->release_snapshot(snapshot);
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
    std::string _remote_files;
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
