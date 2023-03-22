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
DEFINE_int64(snapshot_timeout_min, 10, "snapshot_timeout_min : 10min");
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
                                            IteratorContextPtr context,
                                            bool is_meta_reader) :
            _region_id(region_id), 
            _path(path),
            _rs(rs),
            _context(context),
            _is_meta_reader(is_meta_reader) {
                _region_ptr = Store::get_instance()->get_region(_region_id);
            }

RocksdbReaderAdaptor::~RocksdbReaderAdaptor() {
    close();
}

bool RocksdbReaderAdaptor::region_shutdown() {
    return _region_ptr == nullptr || _region_ptr->is_shutdown();
}

void RocksdbReaderAdaptor::context_reset() {
    IteratorContextPtr iter_context = nullptr;
    iter_context.reset(new IteratorContext);
    if (iter_context == nullptr) {
        return;
    }
    iter_context->reading = _context->reading;
    iter_context->is_meta_sst = _context->is_meta_sst;
    iter_context->prefix = _context->prefix;
    iter_context->upper_bound = _context->upper_bound;
    iter_context->upper_bound_slice = iter_context->upper_bound;
    iter_context->applied_index = _context->applied_index;
    iter_context->snapshot_index = _context->snapshot_index;
    iter_context->need_copy_data = _context->need_copy_data;
    iter_context->sc = _context->sc;
    if (!iter_context->is_meta_sst) {
        rocksdb::ReadOptions read_options;
        read_options.snapshot = iter_context->sc->snapshot;
        read_options.total_order_seek = true;
        read_options.fill_cache = false;
        read_options.iterate_upper_bound = &iter_context->upper_bound_slice;
        rocksdb::ColumnFamilyHandle* column_family = RocksWrapper::get_instance()->get_data_handle();
        iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_options, column_family));
        iter_context->iter->Seek(iter_context->prefix);
        iter_context->sc->data_context = iter_context;
    } else {        
        rocksdb::ReadOptions read_options;
        read_options.snapshot = iter_context->sc->snapshot;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        read_options.fill_cache = false;
        rocksdb::ColumnFamilyHandle* column_family = RocksWrapper::get_instance()->get_meta_info_handle();
        iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_options, column_family));
        iter_context->iter->Seek(iter_context->prefix);
        iter_context->sc->meta_context = iter_context;
    }
    _context = iter_context;
}

ssize_t RocksdbReaderAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
    if (_closed) {
        DB_FATAL("rocksdb reader has been closed, region_id: %ld, offset: %ld",
                    _region_id, offset);
        return -1;
    }
    if (offset < 0) {
        DB_FATAL("region_id: %ld read error. offset: %ld", _region_id, offset);
        return -1;
    }

    if (region_shutdown()) {
        DB_FATAL("region_id: %ld shutdown, "
                "last_off:%lu, off:%lu, ctx->off:%lu, size:%lu", 
                _region_id, _last_offset, offset, _context->offset, size);
        return -1;
    }

    TimeCost time_cost;
    if (!_is_meta_reader && !_context->need_copy_data) {
        _context->done = true;
        DB_WARNING("region_id: %ld need not copy data, time_cost: %ld", 
                _region_id, time_cost.get_time());
        return 0;
    }
    if (offset > _context->offset) {
        DB_FATAL("region_id: %ld, retry last_offset, offset biger fail "
                "time_cost: %ld, last_off:%lu, off:%lu, ctx->off:%lu, size:%lu", 
                _region_id, time_cost.get_time(), _last_offset, offset, _context->offset, size);
        return -1;
    }
    if (offset < _context->offset) {
        // 缓存上一个包，重试一次可以恢复
        if (_last_offset == offset) {
            *portal = _last_package;
            DB_FATAL("region_id: %ld, retry last_offset time_cost: %ld, "
                    "off:%lu, ctx->off:%lu, size:%lu, ret_size:%lu", 
                    _region_id, time_cost.get_time(), offset, _context->offset, size, _last_package.size());
            return _last_package.size();
        }

        // 重置 _context
        if (offset == 0 && _context->offset_update_time.get_time() > FLAGS_snapshot_timeout_min * 60 * 1000 * 1000ULL) {
            _last_offset = 0;
            _num_lines = 0;
            context_reset();
            DB_FATAL("region_id: %ld, context_reset", _region_id);
        } else {
            DB_FATAL("region_id: %ld, retry last_offset fail time_cost: %ld, "
                    "last_off:%lu, off:%lu, ctx->off:%lu, size:%lu", 
                    _region_id, time_cost.get_time(), _last_offset, offset, _context->offset, size);
            return -1;
        }
    }

    size_t count = 0;
    int64_t key_num = 0;
    std::string log_index_prefix = MetaWriter::get_instance()->log_index_key_prefix(_region_id);
    std::string txn_info_prefix = MetaWriter::get_instance()->transcation_pb_key_prefix(_region_id);
    std::string pre_commit_prefix = MetaWriter::get_instance()->pre_commit_key_prefix(_region_id);
    std::string region_info_key = MetaWriter::get_instance()->region_info_key(_region_id);
    // 大region addpeer中重置time_cost，防止version=0超时删除
    _region_ptr->reset_timecost();
    
    while (count < size) {
        if (!_context->iter->Valid()
                || !_context->iter->key().starts_with(_context->prefix)) {
            _context->done = true;
            //portal->append((void*)iter_context->offset, sizeof(size_t));
            DB_WARNING("region_id: %ld snapshot read over, total size: %ld", _region_id, _context->offset);
            auto region = Store::get_instance()->get_region(_region_id);
            if (region == nullptr) {
                DB_FATAL("region_id: %ld is null region", _region_id);
                return -1;
            }
            if (_context->is_meta_sst) {
                region->set_snapshot_meta_size(_context->offset);
            } else {
                region->set_snapshot_data_size(_context->offset);
            }
            break;
        }
        // debug meta region_info applied_index
        if (_context->is_meta_sst && _context->iter->key().compare(region_info_key) == 0) {
            pb::RegionInfo region_info;
            region_info.ParseFromArray(_context->iter->value().data_,
                    _context->iter->value().size_);
            DB_WARNING("region_id: %ld meta_sst region_info:%s", _region_id,
                    region_info.ShortDebugString().c_str());
        }
        //txn_info请求不发送，理论上leader上没有该类请求
        if (_context->is_meta_sst && _context->iter->key().starts_with(txn_info_prefix)) {
            _context->iter->Next();
            continue;
        }
        if (_context->is_meta_sst && _context->iter->key().starts_with(pre_commit_prefix)) {
            DB_FATAL("region_id: %ld should not have this message, txn_id: %lu", _region_id,
                        MetaWriter::get_instance()->decode_pre_commit_key(_context->iter->key()));
            _context->iter->Next();
            continue;
        }
        //如果是prepared事务的log_index记录需要解析出store_req
        int64_t read_size = 0;
        if (_context->is_meta_sst && _context->iter->key().starts_with(log_index_prefix)) {
            int64_t log_index = MetaWriter::get_instance()->decode_log_index_value(_context->iter->value());
            uint64_t txn_id = MetaWriter::get_instance()->decode_log_index_key(_context->iter->key());
            std::map<int64_t, std::string> txn_infos;
            auto ret  = LogEntryReader::get_instance()->read_log_entry(_region_id, log_index, _context->applied_index, txn_id, txn_infos);
            if (ret < 0) {
                _context->done = true;
                DB_FATAL("read txn info fail, may be has removed, region_id: %ld", _region_id);
                _context->iter->Next();
                continue;
            }
            for (auto& txn_info : txn_infos) {
                ++key_num;
                read_size += serialize_to_iobuf(portal, MetaWriter::get_instance()->transcation_pb_key(_region_id, txn_id, txn_info.first));
                read_size += serialize_to_iobuf(portal, rocksdb::Slice(txn_info.second));
            }
            
        } else {
            key_num++;
            read_size += serialize_to_iobuf(portal, _context->iter->key());
            read_size += serialize_to_iobuf(portal, _context->iter->value());
        }
        count += read_size;
        ++_num_lines;
        _context->offset += read_size;
        _context->offset_update_time.reset();
        _context->iter->Next();
    }
    DB_WARNING("region_id: %ld read done. count: %ld, key_num: %ld, time_cost: %ld, "
            "off:%lu, size:%lu, last_off:%lu, last_count:%lu", 
                _region_id, count, key_num, time_cost.get_time(), offset, size, 
                _last_offset, _last_package.size());
    _last_offset = offset;
    _last_package = *portal;
    return count;
}

bool RocksdbReaderAdaptor::close() {
    if (_closed) {
        DB_WARNING("file has been closed, region_id: %ld, num_lines: %ld, path: %s", 
                _region_id, _num_lines, _path.c_str());
        return true;
    }
    _rs->close(_path);
    _closed = true;
    return true;
}

ssize_t RocksdbReaderAdaptor::size() {
    if (_context->done) {
        return _context->offset;
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
    return true;
}


bool SstWriterAdaptor::region_shutdown() {
    return _region_ptr == nullptr || _region_ptr->is_shutdown();
}

SstWriterAdaptor::SstWriterAdaptor(int64_t region_id, const std::string& path, const rocksdb::Options& option)
        : _region_id(region_id)
        , _path(path)
        , _writer(new SstFileWriter(option)) {}

int SstWriterAdaptor::open() {
    _region_ptr = Store::get_instance()->get_region(_region_id);
    if (_region_ptr == nullptr) {
        DB_FATAL("open sst file path: %s failed, region_id: %ld not exist", 
                _path.c_str(), _region_id);
        return -1;
    }
    _is_meta = _path.find("meta") != std::string::npos;
    std::string path = _path;
    if (!_is_meta) {
        path += std::to_string(_sst_idx);
    }
    auto s = _writer->open(path);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld",
                path.c_str(), s.ToString().c_str(), _region_id);
        return -1;
    }
    _closed = false;
    DB_WARNING("rocksdb sst writer open, path: %s, region_id: %ld", path.c_str(), _region_id);
    return 0;
}

ssize_t SstWriterAdaptor::write(const butil::IOBuf& data, off_t offset) {
    (void)offset;
    std::string path = _path;
    if (!_is_meta) {
        path += std::to_string(_sst_idx);
    }
    if (region_shutdown()) {
        DB_FATAL("write sst file path: %s failed, region shutdown, data len: %lu, region_id: %ld",
                path.c_str(), data.size(), _region_id);
        return -1;
    }
    if (_closed) {
        DB_FATAL("write sst file path: %s failed, file closed: %d data len: %lu, region_id: %ld",
                path.c_str(), _closed, data.size(), _region_id);
        return -1;
    }
    if (data.size() == 0) {
        DB_WARNING("write sst file path: %s data len = 0, region_id: %ld", path.c_str(), _region_id);
    }
    auto ret = iobuf_to_sst(data);
    if (ret < 0) {
        DB_FATAL("write sst file path: %s failed, received invalid data, data len: %lu, region_id: %ld",
                path.c_str(), data.size(), _region_id);
        return -1;
    }
    // 大region addpeer中重置time_cost，防止version=0超时删除
    _region_ptr->reset_timecost();
    _data_size += data.size();

    if (!_is_meta && _writer->file_size() >= SST_FILE_LENGTH) {
        if (!finish_sst()) {
            return -1;
        }
        _count = 0;
        ++_sst_idx;
        path = _path + std::to_string(_sst_idx);
        auto s = _writer->open(path);
        if (!s.ok()) {
            DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld", 
                    path.c_str(), s.ToString().c_str(), _region_id);
            return -1;
        }
        DB_WARNING("rocksdb sst write, region_id: %ld, path: %s, offset: %lu, "
                "file_size:%lu, total_count: %lu, all_size:%lu",
                _region_id, path.c_str(), offset, _writer->file_size(), _count, _data_size);
    }
    //if (_is_meta) {
        DB_WARNING("rocksdb sst write, region_id: %ld, path: %s, offset: %lu, data.size: %ld,"
                " file_size: %lu, total_count: %lu", _region_id, path.c_str(), offset, data.size(),
                _writer->file_size(), _count);
    //}
    return data.size();
}

bool SstWriterAdaptor::close() {
    if (_closed) {
        DB_WARNING("file has been closed, path: %s", _path.c_str());
        return true;
    }
    _closed = true;
    if (_is_meta) {
        _region_ptr->set_snapshot_meta_size(_data_size);
    } else {
        _region_ptr->set_snapshot_data_size(_data_size);
    }
    return finish_sst();
}

bool SstWriterAdaptor::finish_sst() {
    std::string path = _path;
    if (!_is_meta) {
        path += std::to_string(_sst_idx);
    }
    if (_count > 0) {
        DB_WARNING("_writer finished, path: %s, region_id: %ld, file_size: %lu, total_count: %ld, all_size:%lu",
                path.c_str(), _region_id, _writer->file_size(), _count, _data_size);
        auto s = _writer->finish();
        if (!s.ok()) {
            DB_FATAL("finish sst file path: %s failed, err: %s, region_id: %ld", 
                    path.c_str(), s.ToString().c_str(), _region_id);
            return false;
        }
    } else {
        bool ret = butil::DeleteFile(butil::FilePath(path), false);
        DB_WARNING("count is 0, delete path: %s, region_id: %ld", path.c_str(), _region_id);
        if (!ret) {
            DB_FATAL("delete sst file path: %s failed, region_id: %ld", 
                        path.c_str(), _region_id);
        }
    }
    return true;
}
/*
inline int iobuf_to_slices(const butil::IOBuf& data, size_t size, std::vector<rocksdb::Slice>* vec) {
    for (size_t i = 0; i < data.backing_block_num(); i++) {
        auto piece = data.backing_block(i);
        if (piece.size() >= size) {
            vec->emplace_back(piece.data(), size);
            return 0;
        } else {
            vec->emplace_back(piece.data(), piece.size());
            size -= piece.size();
        }
    }
    return -1;
}
*/

int SstWriterAdaptor::iobuf_to_sst(butil::IOBuf data) {
    std::string region_info_key = MetaWriter::get_instance()->region_info_key(_region_id);
    std::string applied_index_key = MetaWriter::get_instance()->applied_index_key(_region_id);
    char key_buf[1024];
    // 10k的栈应该可以满足大部分场景
    char value_buf[10 * 1024];
    while (!data.empty()) {
        size_t key_size = 0;
        size_t nbytes = data.cutn((void*)&key_size, sizeof(size_t));
        if (nbytes < sizeof(size_t)) {
            DB_FATAL("read key size from iobuf fail, region_id: %ld", _region_id);
            return -1;
        }
        rocksdb::Slice key;
        std::unique_ptr<char[]> big_key_buf; 
        // sst_file_writer不支持SliceParts，使用fetch可以尽量0拷贝
        if (key_size <= sizeof(key_buf)) {
            key.data_ = static_cast<const char*>(data.fetch(key_buf, key_size));
        } else {
            big_key_buf.reset(new char[key_size]);
            //DB_WARNING("region_id: %ld, key_size: %ld too big", _region_id, key_size);
            key.data_ = static_cast<const char*>(data.fetch(big_key_buf.get(), key_size));
        }
        key.size_ = key_size;
        if (key.data_ == nullptr) {
            DB_FATAL("read key from iobuf fail, region_id: %ld, key_size: %ld", 
                    _region_id, key_size);
            return -1;
        }
        data.pop_front(key_size);

        size_t value_size = 0;
        nbytes = data.cutn((void*)&value_size, sizeof(size_t));
        if (nbytes < sizeof(size_t)) {
            DB_FATAL("read value size from iobuf fail, region_id: %ld", _region_id);
            return -1;
        }
        rocksdb::Slice value;
        std::unique_ptr<char[]> big_value_buf; 
        if (value_size <= sizeof(value_buf)) {
            if (value_size > 0) {
                value.data_ = static_cast<const char*>(data.fetch(value_buf, value_size));
            }
        } else {
            big_value_buf.reset(new char[value_size]);
            //DB_WARNING("region_id: %ld, value_size: %ld too big", _region_id, value_size);
            value.data_ = static_cast<const char*>(data.fetch(big_value_buf.get(), value_size));
        }
        value.size_ = value_size;
        if (value.data_ == nullptr) {
            DB_FATAL("read value from iobuf fail, region_id: %ld, value_size: %ld", 
                    _region_id, value_size);
            return -1;
        }
        data.pop_front(value_size);
        // debug meta region_info
        if (_is_meta) {
            if (key == region_info_key) {
                pb::RegionInfo region_info;
                region_info.ParseFromArray(value.data(), value.size());
                DB_WARNING("region_id: %ld meta_sst recv region_info:%s", 
                        _region_id, region_info.ShortDebugString().c_str());
            }
            if (key == applied_index_key) {
                DB_WARNING("region_id: %ld meta_sst recv applied_index:%ld", _region_id,
                        TableKey(value).extract_i64(0));
            }
        }
        _count++;
        
        auto s = _writer->put(key, value);
        if (!s.ok()) {            
            DB_FATAL("write sst file failed, err: %s, region_id: %ld", 
                        s.ToString().c_str(), _region_id);
            return -1;
        }
    }
    return 0;
}

SstWriterAdaptor::~SstWriterAdaptor() {
    close();
}

ssize_t SstWriterAdaptor::size() {
    DB_FATAL("SstWriterAdaptor::size not implemented, region_id: %ld", _region_id);
    return -1;
}

bool SstWriterAdaptor::sync() {
    //already sync in SstFileWriter::Finish
    return true;
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
}

RocksdbFileSystemAdaptor::~RocksdbFileSystemAdaptor() {
    _mutil_snapshot_cond.wait();
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
        DB_WARNING("open file: %s, region_id: %ld, %d", path.c_str(), _region_id, oflag & O_WRONLY);
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
    options.bottommost_compression = rocksdb::kLZ4Compression;
    options.bottommost_compression_opts = rocksdb::CompressionOptions();
    
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
    //find dirname
    const std::string snapshot_path = path.substr(0, path.find_last_of('/'));
    //超时raft_copy_remote_file_timeout_ms，配了300s才重试
    //所以不加锁也没问题(除非这个函数能执行300s)，为了保险换把锁保证串行
    BAIDU_SCOPED_LOCK(_open_reader_adaptor_mutex);
    auto sc = get_snapshot(snapshot_path);
    if (sc == nullptr) {
        DB_FATAL("snapshot no found, path: %s, region_id: %ld", snapshot_path.c_str(), _region_id);
        if (e != nullptr) {
            *e = butil::File::FILE_ERROR_NOT_FOUND;
        }
        return nullptr;
    }

    std::string prefix;
    std::string upper_bound;
    if (is_snapshot_data_file(path)) {
        MutTableKey key;
        key.append_i64(_region_id);
        prefix = key.data();
        key.append_u64(UINT64_MAX);
        upper_bound = key.data();
        auto region_ptr = Store::get_instance()->get_region(_region_id);
        if (region_ptr != nullptr && region_ptr->is_binlog_region()) {
            key.replace_i64(region_ptr->get_table_id(), 8);
            key.append_i64(sc->binlog_check_point);
            prefix = key.data();
        }
    } else {
        prefix = MetaWriter::get_instance()->meta_info_prefix(_region_id);
    }

    bool is_meta_reader = false;
    IteratorContextPtr iter_context = nullptr;
    if (is_snapshot_data_file(path)) {
        is_meta_reader = false;
        iter_context = sc->data_context;
        //first open snapshot file
        if (iter_context == nullptr) {
            iter_context.reset(new IteratorContext);
            iter_context->prefix = prefix;
            iter_context->is_meta_sst = false;
            iter_context->upper_bound = upper_bound;
            iter_context->upper_bound_slice = iter_context->upper_bound;
            iter_context->sc = sc.get();
            rocksdb::ReadOptions read_options;
            read_options.snapshot = sc->snapshot;
            read_options.total_order_seek = true;
            read_options.fill_cache = false;
            read_options.iterate_upper_bound = &iter_context->upper_bound_slice;
            rocksdb::ColumnFamilyHandle* column_family = RocksWrapper::get_instance()->get_data_handle();
            iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_options, column_family));
            iter_context->iter->Seek(prefix);
            braft::NodeStatus status;

            auto region = Store::get_instance()->get_region(_region_id);
            region->get_node_status(&status);
            int64_t peer_next_index = 0;
            // 通过peer状态和data_index判断是否需要复制数据
            // addpeer在unstable里，peer_next_index=0就会走复制流程
            for (auto iter : status.stable_followers) {
                auto& peer = iter.second;
                DB_WARNING("region_id: %ld %s %d %ld", _region_id, iter.first.to_string().c_str(),
                peer.installing_snapshot, peer.next_index);
                if (peer.installing_snapshot) {
                    peer_next_index = peer.next_index;
                    break;
                }
            }
            if (sc->data_index < peer_next_index) {
                iter_context->need_copy_data = false;
            }
            sc->data_context = iter_context;
            DB_WARNING("region_id: %ld open reader, data_index:%ld,peer_next_index:%ld, path: %s, time_cost: %ld", 
                    _region_id, sc->data_index, peer_next_index, path.c_str(), time_cost.get_time());
        }
    }
    int64_t applied_index = 0;
    int64_t data_index = 0;
    int64_t snapshot_index = 0;
    if (is_snapshot_meta_file(path)) {
        is_meta_reader = true;
        iter_context = sc->meta_context;
        if (iter_context == nullptr) {
            iter_context.reset(new IteratorContext);
            iter_context->prefix = prefix;
            iter_context->is_meta_sst = true;
            iter_context->sc = sc.get();
            rocksdb::ReadOptions read_options;
            read_options.snapshot = sc->snapshot;
            read_options.prefix_same_as_start = true;
            read_options.total_order_seek = false;
            read_options.fill_cache = false;
            rocksdb::ColumnFamilyHandle* column_family = RocksWrapper::get_instance()->get_meta_info_handle();
            iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_options, column_family));
            iter_context->iter->Seek(prefix);
            sc->meta_context = iter_context;
            snapshot_index = parse_snapshot_index_from_path(path, true);
            iter_context->snapshot_index = snapshot_index;
            MetaWriter::get_instance()->read_applied_index(_region_id, read_options, &applied_index, &data_index);
            iter_context->applied_index = std::max(iter_context->snapshot_index, applied_index);
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
    auto reader = new RocksdbReaderAdaptor(_region_id, path, this, iter_context, is_meta_reader);
    reader->open();
    DB_WARNING("region_id: %ld open reader: path: %s snapshot_index: %ld applied_index: %ld data_index: %ld , time_cost: %ld", 
                _region_id, path.c_str(), snapshot_index, applied_index, data_index, time_cost.get_time());
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
    DB_WARNING("region_id: %ld lock _snapshot_mutex before open snapshot", _region_id);
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshots.find(path);
    if (iter != _snapshots.end()) {
        // raft InstallSnapshot接口超时-1
        // 长时间没处理说明对方机器卡住了
        // TODO 后续如果遇到长时间不read也可以关闭
        if (iter->second.cost.get_time() > 3600 * 1000 * 1000LL
            && iter->second.count == 1
            && (iter->second.ptr->data_context == nullptr
            || iter->second.ptr->data_context->offset == 0)) {
            _snapshots.erase(iter);
            DB_WARNING("region_id: %ld snapshot path: %s is hang over 1 hour, erase", _region_id, path.c_str());
        } else {
            // leaner拉取快照会保留raft_snapshot_reader_expire_time_s
            DB_WARNING("region_id: %ld snapshot path: %s is busy", _region_id, path.c_str());
            _snapshots[path].count++;
            return false;
        }
    }

    _mutil_snapshot_cond.increase();
    // create new Rocksdb Snapshot
    auto region = Store::get_instance()->get_region(_region_id);
    region->lock_commit_meta_mutex();
    DB_WARNING("region_id: %ld lock_commit_meta_mutex before open snapshot", _region_id);
    _snapshots[path].ptr.reset(new SnapshotContext());
    region = Store::get_instance()->get_region(_region_id);
    _snapshots[path].ptr->data_index = region->get_data_index();
    _snapshots[path].ptr->binlog_check_point = region->get_binlog_check_point();
    region->unlock_commit_meta_mutex();
    _snapshots[path].count++;
    _snapshots[path].cost.reset();
    DB_WARNING("region_id: %ld, data_index:%ld, binlog_check_point:%ld, open snapshot path: %s", 
            _region_id, _snapshots[path].ptr->data_index, _snapshots[path].ptr->binlog_check_point, path.c_str());
    return true;
}

void RocksdbFileSystemAdaptor::close_snapshot(const std::string& path) {
    DB_WARNING("region_id: %ld close snapshot path: %s", _region_id, path.c_str());
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshots.find(path);
    if (iter != _snapshots.end()) {
        _snapshots[path].count--;
        if (_snapshots[path].count == 0) {
            _snapshots.erase(iter);
            _mutil_snapshot_cond.decrease_broadcast();
            DB_WARNING("region_id: %ld close snapshot path: %s relase", _region_id, path.c_str());
        }
    }
}

SnapshotContextPtr RocksdbFileSystemAdaptor::get_snapshot(const std::string& path) {
    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshots.find(path);
    if (iter != _snapshots.end()) {
        return iter->second.ptr;
    }
    return nullptr;
}

void RocksdbFileSystemAdaptor::close(const std::string& path) {
    //find dirname
    const std::string snapshot_path = path.substr(0, path.find_last_of('/'));

    BAIDU_SCOPED_LOCK(_snapshot_mutex);
    auto iter = _snapshots.find(snapshot_path);
    if (iter == _snapshots.end()) {
        DB_FATAL("no snapshot found when close reader, path:%s, region_id: %ld", 
                    path.c_str(), _region_id);
        return;
    }
    auto& snapshot_ctx = iter->second;
    if (is_snapshot_data_file(path) && snapshot_ctx.ptr->data_context != nullptr) {
        DB_WARNING("read snapshot data file close, path: %s", path.c_str());
        snapshot_ctx.ptr->data_context.reset();
    } else if (snapshot_ctx.ptr->meta_context != nullptr) {
        DB_WARNING("read snapshot meta data file close, path: %s", path.c_str());
        snapshot_ctx.ptr->meta_context.reset();
    }
}

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
