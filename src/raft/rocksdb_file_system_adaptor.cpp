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

#include "rocksdb_file_system_adaptor.h"
#include "mut_table_key.h"
#include "sst_file_writer.h"
#include "meta_writer.h"
#include "store.h"
#include "log_entry_reader.h"

namespace baikaldb {

bool inline is_snapshot_data_file(const std::string& path) {
    butil::StringPiece sp(path);
    if (sp.ends_with(SNAPSHOT_DATA_FILE_WITH_SLASH)) {
        return true;
    }
    return false;
}
bool inline is_snapshot_meta_file(const std::string& path) {
    butil::StringPiece sp(path);
    if (sp.ends_with(SNAPSHOT_META_FILE_WITH_SLASH)) {
        return true;
    }
    return false;
}
bool PosixDirReader::is_valid() const {
    return _dir_reader.IsValid();
}

bool PosixDirReader::next() {
    bool rc = _dir_reader.Next();
    while (rc && (strcmp(name(), ".") == 0 || strcmp(name(), "..") == 0)) {
        rc = _dir_reader.Next();
    }
    return rc;
}

const char* PosixDirReader::name() const {
    return _dir_reader.name();
}

RocksdbReaderAdaptor::RocksdbReaderAdaptor(int64_t region_id,
                                            const std::string& path,
                                            RocksdbFileSystemAdaptor* rs,
                                            SnapshotContextPtr context,
                                            bool is_meta_reader) :
            _region_id(region_id), 
            _path(path),
            _rs(rs),
            _context(context),
            _is_meta_reader(is_meta_reader) {}

RocksdbReaderAdaptor::~RocksdbReaderAdaptor() {
    close();
}

ssize_t RocksdbReaderAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
    if (_closed) {
        DB_FATAL("rocksdb reader has been closed, region_id: %ls, offset: %ld",
                    _region_id, offset);
        return -1;
    }
    if (offset < 0) {
        DB_FATAL("region_id: %ld red error. offset: %ld", _region_id, offset);
        return -1;
    }

    TimeCost time_cost;
    IteratorContext* iter_context = _context->data_context;
    if (_is_meta_reader) {
        iter_context = _context->meta_context;
    } 
    if (offset < iter_context->offset) {
        iter_context->offset = 0;
        iter_context->iter->Seek(iter_context->prefix);
    }

    size_t count = 0;
    int64_t key_num = 0;
    std::string log_index_prefix = MetaWriter::get_instance()->log_index_key_prefix(_region_id);
    std::string txn_info_prefix = MetaWriter::get_instance()->transcation_pb_key_prefix(_region_id);
    std::string pre_commit_prefix = MetaWriter::get_instance()->pre_commit_key_prefix(_region_id);
    std::string region_info_key = MetaWriter::get_instance()->region_info_key(_region_id);
    std::string applied_index_key = MetaWriter::get_instance()->applied_index_key(_region_id);
    int64_t applied_index = 0;
    while (count < size) {
        if (!iter_context->iter->Valid()
                || !iter_context->iter->key().starts_with(iter_context->prefix)) {
            iter_context->done = true;
            DB_WARNING("region_id: %ld snapshot read over, total size: %ld", _region_id, iter_context->offset);
            break;
        }
        // debug meta region_info applied_index
        if (iter_context->is_meta_sst && iter_context->iter->key().compare(region_info_key) == 0) {
            pb::RegionInfo region_info;
            region_info.ParseFromArray(iter_context->iter->value().data_,
                    iter_context->iter->value().size_);
            DB_WARNING("region_id: %ld meta_sst region_info:%s", _region_id,
                    region_info.ShortDebugString().c_str());
        }
        if (iter_context->is_meta_sst && iter_context->iter->key().compare(applied_index_key) == 0) {
            applied_index = TableKey(iter_context->iter->value()).extract_i64(0);
            DB_WARNING("region_id: %ld meta_sst applied_index:%ld", _region_id,
                    applied_index);
        }
        //txn_info请求不发送，理论上leader上没有该类请求
        if (iter_context->is_meta_sst && iter_context->iter->key().starts_with(txn_info_prefix)) {
            iter_context->iter->Next();
            continue;
        }
        if (iter_context->is_meta_sst && iter_context->iter->key().starts_with(pre_commit_prefix)) {
            DB_FATAL("region_id: %ld should not have this message, txn_id: %lu",
                        MetaWriter::get_instance()->decode_pre_commit_key(iter_context->iter->key()));
            iter_context->iter->Next();
            continue;
        }
        //如果是prepared事务的log_index记录需要解析出store_req
        int64_t read_size = 0;
        if (iter_context->is_meta_sst && iter_context->iter->key().starts_with(log_index_prefix)) {
            int64_t log_index = MetaWriter::get_instance()->decode_log_index_value(iter_context->iter->value());
            uint64_t txn_id = MetaWriter::get_instance()->decode_log_index_key(iter_context->iter->key());
            std::map<int64_t, std::string> txn_infos;
            auto ret  = LogEntryReader::get_instance()->read_log_entry(_region_id, log_index, applied_index, txn_id, txn_infos);
            if (ret < 0) {
                iter_context->done = true;
                DB_FATAL("read txn info fail, may be has removed, region_id: %ld", _region_id);
                iter_context->iter->Next();
                continue;
                //return -1;
            }
            for (auto& txn_info : txn_infos) {
                ++key_num;
                if (iter_context->offset >= offset) {
                    read_size += serialize_to_iobuf(portal, MetaWriter::get_instance()->transcation_pb_key(_region_id, txn_id, txn_info.first));
                    read_size += serialize_to_iobuf(portal, rocksdb::Slice(txn_info.second));
                } else {
                    read_size += serialize_to_iobuf(nullptr, MetaWriter::get_instance()->transcation_pb_key(_region_id, txn_id, txn_info.first));
                    read_size += serialize_to_iobuf(nullptr, rocksdb::Slice(txn_info.second));
                }
            }
            
        } else {
            key_num++;
            if (iter_context->offset >= offset) {
                read_size += serialize_to_iobuf(portal, iter_context->iter->key());
                read_size += serialize_to_iobuf(portal, iter_context->iter->value());
            } else {
                read_size += serialize_to_iobuf(nullptr, iter_context->iter->key());
                read_size += serialize_to_iobuf(nullptr, iter_context->iter->value());
            } 
        }
        count += read_size;
        ++_num_lines;
        iter_context->offset += read_size;
        iter_context->iter->Next();
    }
    DB_WARNING("region_id: %ld read done. count: %ld, key_num: %ld, time_cost: %ld", 
                _region_id, count, key_num, time_cost.get_time());
    return count;
}

bool RocksdbReaderAdaptor::close() {
    if (_closed) {
        DB_DEBUG("file has been closed, region_id: %ld, num_lines: %ld, path: %s", 
                _region_id, _num_lines, _path.c_str());
        return true;
    }
    _rs->close(_path);
    _closed = true;
    return true;
}

ssize_t RocksdbReaderAdaptor::size() {
    IteratorContext* iter_context = _context->data_context;
    if (_is_meta_reader) {
        iter_context = _context->meta_context;
    } 
    
    if (iter_context->done) {
        return iter_context->offset;
    }
    return std::numeric_limits<ssize_t>::max();
}

ssize_t RocksdbReaderAdaptor::write(const butil::IOBuf& data, off_t offset) {
    (void)data;
    (void)offset;
    DB_FATAL("RocksdbReaderAdaptor::write not implemented");
    return -1;
}

bool RocksdbReaderAdaptor::sync() {
    DB_FATAL("RocksdbReaderAdaptor::sync not implemented");
    return false;
}

SstWriterAdaptor::SstWriterAdaptor(int64_t region_id, const std::string& path, const rocksdb::Options& option)
        : _region_id(region_id)
        , _path(path)
        , _writer(new SstFileWriter(option)) {}

int SstWriterAdaptor::open() {
    _region_ptr = Store::get_instance()->get_region(_region_id);
    if (_region_ptr == nullptr) {
        DB_FATAL("open sst file path: %s failed, region not exist", _path.c_str());
        return -1;
    }
    _is_meta = _path.find("meta") != std::string::npos;
    auto s = _writer->open(_path);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld", _path.c_str(), s.ToString().c_str());
        return -1;
    }
    _closed = false;
    DB_WARNING("rocksdb sst writer open, path: %s, region_id: %ld", _path.c_str(), _region_id);
    return 0;
}

ssize_t SstWriterAdaptor::write(const butil::IOBuf& data, off_t offset) {
    (void)offset;
    if (_closed) {
        DB_FATAL("write sst file path: %s failed, file closed: %d data len: %d, region_id: %ld",
                _path.c_str(), _closed, data.size(), _region_id);
        return -1;
    }
    if (data.size() == 0) {
        DB_WARNING("write sst file path: %s data len = 0, region_id: %ld", _path.c_str(), _region_id);
    }
    std::string region_info_key = MetaWriter::get_instance()->region_info_key(_region_id);
    std::string applied_index_key = MetaWriter::get_instance()->applied_index_key(_region_id);
    std::vector<std::string> keys;
    std::vector<std::string> values;
    auto ret = parse_from_iobuf(data, keys, values);
    if (ret < 0) {
        DB_FATAL("write sst file path: %s failed, received invalid data, data len: %d, region_id: %ld",
                _path.c_str(), data.size(), _region_id);
        return -1;
    }
    // 大region addpeer中重置time_cost，防止version=0超时删除
    _region_ptr->reset_timecost();
    for (size_t i = 0; i < keys.size(); ++i) {
        // debug meta region_info
        if (_is_meta) {
            if (keys[i] == region_info_key) {
                pb::RegionInfo region_info;
                region_info.ParseFromString(values[i]);
                DB_WARNING("region_id: %ld meta_sst recv region_info:%s", 
                        _region_id, region_info.ShortDebugString().c_str());
            }
            if (keys[i] == applied_index_key) {
                DB_WARNING("region_id: %ld meta_sst recv applied_index:%ld", _region_id,
                        TableKey(values[i]).extract_i64(0));
            }
        }
        auto s = _writer->put(rocksdb::Slice(keys[i]), rocksdb::Slice(values[i]));
        if (!s.ok()) {            
            DB_FATAL("write sst file path: %s failed, err: %s, region_id: %ld", 
                        _path.c_str(), s.ToString().c_str(), _region_id);
            return -1;
        }
        _count++;
    }
    if (_is_meta) {
        DB_WARNING("rocksdb sst write, region_id: %ld, path: %s, offset: %lu, data.size: %ld,"
                " keys size: %ld, total_count: %ld", _region_id, _path.c_str(), offset, data.size(),
                keys.size(), _count);
    }
    return data.size();
}

bool SstWriterAdaptor::close() {
    if (_closed) {
        DB_WARNING("file has been closed, path: %s", _path.c_str());
        return true;
    }
    _closed = true;
    bool ret = true;
    if (_count > 0) {
        auto s = _writer->finish();
        DB_WARNING("_writer finished, path: %s, region_id: %ld, total_count: %ld",
                _path.c_str(), _region_id, _count);
        if (!s.ok()) {
            DB_FATAL("finish sst file path: %s failed, err: %s, region_id: %ld", 
                    _path.c_str(), s.ToString().c_str(), _region_id);
            ret = false;
        }
    } else {
        ret = butil::DeleteFile(butil::FilePath(_path), false);
        DB_WARNING("count is 0, delete path: %s, region_id: %ld", _path.c_str(), _region_id);
        if (!ret) {
            DB_FATAL("delete sst file path: %s failed, region_id: %ld", 
                        _path.c_str(), _region_id);
        }
    }
    return ret;
}

SstWriterAdaptor::~SstWriterAdaptor() {
    close();
}

ssize_t SstWriterAdaptor::size() {
    DB_FATAL("SstWriterAdaptor::size not implemented, region_id: %ld", _region_id);
    return -1;
}

bool SstWriterAdaptor::sync() {
    DB_FATAL("SstWriterAdaptor::sync not implemented, region_id: %ld", _region_id);
    return false;
}

ssize_t SstWriterAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
    DB_FATAL("SstWriterAdaptor::read not implemented, region_id: %ld", _region_id);
    return -1;
}

PosixFileAdaptor::~PosixFileAdaptor() {
    close();
}

int PosixFileAdaptor::open(int oflag) {
    oflag &= (~O_CLOEXEC);
    _fd = ::open(_path.c_str(), oflag, 0644);
    if (_fd <= 0) {
        return -1;
    }
    return 0;
}

bool PosixFileAdaptor::close() {
    if (_fd > 0) {
        bool res = ::close(_fd) == 0;
        _fd = -1;
        return res;
    }
    return true;
}

ssize_t PosixFileAdaptor::write(const butil::IOBuf& data, off_t offset) {
    ssize_t ret = braft::file_pwrite(data, _fd, offset);
    return ret;
}

ssize_t PosixFileAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
    return braft::file_pread(portal, _fd, offset, size);
}

ssize_t PosixFileAdaptor::size() {
    off_t sz = lseek(_fd, 0, SEEK_END);
    return ssize_t(sz);
}

bool PosixFileAdaptor::sync() {
    return braft::raft_fsync(_fd) == 0;
}

RocksdbFileSystemAdaptor::RocksdbFileSystemAdaptor(int64_t region_id) : _region_id(region_id) {
    bthread_mutex_init(&_snapshot_mutex, NULL);
}

RocksdbFileSystemAdaptor::~RocksdbFileSystemAdaptor() {
    _mutil_snapshot_cond.wait();
    DB_WARNING("region_id: %ld all snapshot has been closed", _region_id);
    bthread_mutex_destroy(&_snapshot_mutex);
    DB_WARNING("region_id: %ld RocksdbFileSystemAdaptor released", _region_id);
}

braft::FileAdaptor* RocksdbFileSystemAdaptor::open(const std::string& path, int oflag,
                                     const ::google::protobuf::Message* file_meta,
                                     butil::File::Error* e) {
    if (!is_snapshot_data_file(path) && !is_snapshot_meta_file(path)) {
        PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
        int ret = adaptor->open(oflag);
        if (ret != 0) {
            if (e) {
                *e = butil::File::OSErrorToFileError(errno);
            }
            delete adaptor;
            return nullptr;
        }
        DB_WARNING("open file: %s, region_id: %ld", path.c_str(), _region_id);
        return adaptor;
    }

    bool for_write = (O_WRONLY & oflag);
    if (for_write) {
        return open_writer_adaptor(path, oflag, file_meta, e);
    }
    return open_reader_adaptor(path, oflag, file_meta, e);
}

braft::FileAdaptor* RocksdbFileSystemAdaptor::open_writer_adaptor(const std::string& path, int oflag,
                                     const ::google::protobuf::Message* file_meta,
                                     butil::File::Error* e) {
    (void) file_meta;

    RocksWrapper* db = RocksWrapper::get_instance();
    rocksdb::Options options;
    if (is_snapshot_data_file(path)) {
        options = db->get_options(db->get_data_handle()); 
    } else {
        options = db->get_options(db->get_meta_info_handle());
    }
    
    SstWriterAdaptor* writer = new SstWriterAdaptor(_region_id, path, options);
    int ret = writer->open();
    if (ret != 0) {
        if (e) {
            *e = butil::File::FILE_ERROR_FAILED;
        }
        delete writer;
        return nullptr;
    }
    DB_WARNING("open for write file, path: %s, region_id: %ld", path.c_str(), _region_id);
    return writer;
}

braft::FileAdaptor* RocksdbFileSystemAdaptor::open_reader_adaptor(const std::string& path, int oflag,
                                     const ::google::protobuf::Message* file_meta,
                                     butil::File::Error* e) {
    TimeCost time_cost;
    (void) file_meta;
    std::string prefix;
    std::string upper_bound;
    size_t len = path.size();
    if (is_snapshot_data_file(path)) {
        len -= SNAPSHOT_DATA_FILE.size();
        MutTableKey key;
        key.append_i64(_region_id);
        prefix = key.data();
        key.append_u64(UINT64_MAX);
        upper_bound = key.data();

    } else {
        len -= SNAPSHOT_META_FILE.size();
        prefix = MetaWriter::get_instance()->meta_info_prefix(_region_id);
    }
    const std::string snapshot_path = path.substr(0, len - 1);
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto sc = get_snapshot(snapshot_path);
    if (sc == nullptr) {
        DB_FATAL("snapshot no found, path: %s, region_id: %ld", snapshot_path.c_str(), _region_id);
        if (e != nullptr) {
            *e = butil::File::FILE_ERROR_NOT_FOUND;
        }
        return nullptr;
    }
    bool is_meta_reader = false;
    IteratorContext* iter_context = nullptr;
    if (is_snapshot_data_file(path)) {
        is_meta_reader = false;
        iter_context = sc->data_context;
        //first open snapshot file
        if (iter_context == nullptr) {
            iter_context = new IteratorContext;
            iter_context->prefix = prefix;
            iter_context->is_meta_sst = false;
            iter_context->upper_bound = upper_bound;
            iter_context->upper_bound_slice = iter_context->upper_bound;
            rocksdb::ReadOptions read_options;
            read_options.snapshot = sc->snapshot;
            read_options.total_order_seek = true;
            read_options.iterate_upper_bound = &iter_context->upper_bound_slice;
            rocksdb::ColumnFamilyHandle* column_family = RocksWrapper::get_instance()->get_data_handle();
            iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_options, column_family));
            iter_context->iter->Seek(prefix);
            sc->data_context = iter_context;
        }
    }
    if (is_snapshot_meta_file(path)) {
        is_meta_reader = true;
        iter_context = sc->meta_context;
        if (iter_context == nullptr) {
            iter_context = new IteratorContext;
            iter_context->prefix = prefix;
            iter_context->is_meta_sst = true;
            rocksdb::ReadOptions read_options;
            read_options.snapshot = sc->snapshot;
            read_options.prefix_same_as_start = true;
            read_options.total_order_seek = false;
            rocksdb::ColumnFamilyHandle* column_family = RocksWrapper::get_instance()->get_meta_info_handle();
            iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_options, column_family));
            iter_context->iter->Seek(prefix);
            sc->meta_context = iter_context;
        }
    }
    if (iter_context->reading) {
        DB_WARNING("snapshot reader is busy, path: %s, region_id: %ld", path.c_str(), _region_id);
        if (e != nullptr) {
            *e = butil::File::FILE_ERROR_IN_USE;
        }
        return nullptr;
    }
    iter_context->reading = true;
    auto reader = new RocksdbReaderAdaptor(_region_id, path, this, sc, is_meta_reader);
    reader->open();
    DB_DEBUG("region_id: %ld open reader: path: %s, time_cost: %ld", 
                _region_id, path.c_str(), time_cost.get_time());
    return reader;
}

bool RocksdbFileSystemAdaptor::delete_file(const std::string& path, bool recursive) {
    butil::FilePath file_path(path);
    return butil::DeleteFile(file_path, recursive);
}

bool RocksdbFileSystemAdaptor::rename(const std::string& old_path, const std::string& new_path) {
    return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool RocksdbFileSystemAdaptor::link(const std::string& old_path, const std::string& new_path) {
    return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

bool RocksdbFileSystemAdaptor::create_directory(const std::string& path,
                                         butil::File::Error* error,
                                         bool create_parent_directories) {
    butil::FilePath dir(path);
    return butil::CreateDirectoryAndGetError(dir, error, create_parent_directories);
}

bool RocksdbFileSystemAdaptor::path_exists(const std::string& path) {
    butil::FilePath file_path(path);
    return butil::PathExists(file_path);
}

bool RocksdbFileSystemAdaptor::directory_exists(const std::string& path) {
    butil::FilePath file_path(path);
    return butil::DirectoryExists(file_path);
}

braft::DirReader* RocksdbFileSystemAdaptor::directory_reader(const std::string& path) {
    return new PosixDirReader(path.c_str());
}

bool RocksdbFileSystemAdaptor::open_snapshot(const std::string& path) {
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshots.find(path);
    if (iter != _snapshots.end()) {
        DB_WARNING("region_id: %ld snapshot path: %s is busy", _region_id, path.c_str());
        _snapshots[path].second++;
        return false;
    }

    _mutil_snapshot_cond.increase();
    // create new Rocksdb Snapshot
    DB_WARNING("region_id: %ld try lock before open snapshot", _region_id);
    auto region = Store::get_instance()->get_region(_region_id);
    DB_WARNING("region_id: %ld get region success", _region_id);
    region->lock_commit_meta_mutex();
    DB_WARNING("region_id: %ld get lock before open snapshot", _region_id);
    _snapshots[path].first.reset(new SnapshotContext());
    region = Store::get_instance()->get_region(_region_id);
    region->unlock_commit_meta_mutex();
    DB_WARNING("region_id: %ld relase lock before open snapshot", _region_id);
    _snapshots[path].second++;
    DB_WARNING("region_id: %ld open snapshot path: %s", _region_id, path.c_str());
    return true;
}

void RocksdbFileSystemAdaptor::close_snapshot(const std::string& path) {
    DB_WARNING("region_id: %ld close snapshot path: %s", _region_id, path.c_str());
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshots.find(path);
    if (iter != _snapshots.end()) {
        _snapshots[path].second--;
        if (_snapshots[path].second == 0) {
            _snapshots.erase(iter);
            _mutil_snapshot_cond.decrease_broadcast();
            DB_WARNING("region_id: %ld close snapshot path: %s relase", _region_id, path.c_str());
        }
    }
}

SnapshotContextPtr RocksdbFileSystemAdaptor::get_snapshot(const std::string& path) {
    auto iter = _snapshots.find(path);
    if (iter != _snapshots.end()) {
        return iter->second.first;
    }
    return nullptr;
}

void RocksdbFileSystemAdaptor::close(const std::string& path) {
    size_t len = path.size();
    if (is_snapshot_data_file(path)) {
        len -= SNAPSHOT_DATA_FILE_WITH_SLASH.size();
    } else {
        len -= SNAPSHOT_META_FILE_WITH_SLASH.size();
    }
    const std::string snapshot_path = path.substr(0, len);

    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshots.find(snapshot_path);
    if (iter == _snapshots.end()) {
        DB_FATAL("no snapshot found when close reader, path:%s, region_id: %ld", 
                    path.c_str(), _region_id);
        return;
    }
    auto& snapshot_ctx = iter->second;
    if (is_snapshot_data_file(path)) {
        DB_DEBUG("read snapshot data file close, path: %s", path.c_str());
        snapshot_ctx.first->data_context->reading = false;
    } else {
        DB_WARNING("read snapshot meta data file close, path: %s", path.c_str());
        snapshot_ctx.first->meta_context->reading = false;
    }
}

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
