#pragma once

#include "rocks_wrapper.h"
#include <parquet/api/writer.h>
#include <parquet/arrow/writer.h>
#include <arrow/buffer.h>
#include <arrow/api.h>
#include "schema_factory.h"
#include <arrow/io/file.h>
#include "arrow_function.h"
#include <arrow/dataset/file_parquet.h>
#include <boost/filesystem.hpp>
#include "table_record.h"
#include <parquet/file_writer.h>
#include "parquet/arrow/schema.h"
#include "file_system.h"
#include "column_record.h"
#include "rocksdb_filesystem.h"
namespace baikaldb {
DECLARE_int64(parquet_cache_size_mb);

struct ReadRange {
    int64_t offset;
    int64_t length;

    friend bool operator==(const ReadRange& left, const ReadRange& right) {
        return (left.offset == right.offset && left.length == right.length);
    }
    friend bool operator!=(const ReadRange& left, const ReadRange& right) {
        return !(left == right);
    }

    bool Contains(const ReadRange& other) const {
        return (offset <= other.offset && offset + length >= other.offset + other.length);
    }
};

struct ReadContents {
    bool has_pre_buffered = false;
    bool fill_cache = false;
    std::string file_short_name;
    int64_t region_id = 0;
    int64_t start_version = 0;
    int64_t end_version = 0;
    int file_idx = 0;
    std::vector<ReadRange> ranges;
};

struct BlockContents {
    int size = 0;
    char* data = nullptr;
    BlockContents(int len, char* buf) : size(len) , data(new char[len]) {
        ColumnVars::get_instance()->parquet_cache_bytes << size;
        memcpy(data, buf, len);
    }
    ~BlockContents() {
        if (data != nullptr) {
            ColumnVars::get_instance()->parquet_cache_bytes << -size;
            delete[] data;
        }
    }
    static void delete_fn(void* value, rocksdb::MemoryAllocator* allocator) {
        BlockContents* block = static_cast<BlockContents*>(value);
        delete block;
        return;
    } 

    static rocksdb::Cache::CacheItemHelper kBasicHelper;
};

struct ReadRangeCombiner {
    ::arrow::Result<std::vector<ReadRange>> Coalesce(std::vector<ReadRange> ranges) {
        if (ranges.empty()) {
            return ranges;
        }

        // Remove zero-sized ranges
        auto end = std::remove_if(ranges.begin(), ranges.end(),
                                [](const ReadRange& range) { return range.length == 0; });
        // Sort in position order
        std::sort(ranges.begin(), end,
                [](const ReadRange& a, const ReadRange& b) { return a.offset < b.offset; });
        // Remove ranges that overlap 100%
        end = std::unique(ranges.begin(), end,
                        [](const ReadRange& left, const ReadRange& right) {
                            return right.offset >= left.offset &&
                                right.offset + right.length <= left.offset + left.length;
                        });
        ranges.resize(end - ranges.begin());

        // Skip further processing if ranges is empty after removing zero-sized ranges.
        if (ranges.empty()) {
            return ranges;
        }

        for (size_t i = 0; i < ranges.size() - 1; ++i) {
            const auto& left = ranges[i];
            const auto& right = ranges[i + 1];
            if (left.offset > right.offset) {
                return ::arrow::Status::IOError("Some read ranges left > right");
            }
            // DCHECK_LE(left.offset, right.offset);
            if (left.offset + left.length > right.offset) {
                return ::arrow::Status::IOError("Some read ranges overlap");
            }
        }

        std::vector<ReadRange> coalesced;

        auto itr = ranges.begin();
        // Ensure ranges is not empty.
        // DCHECK_LE(itr, ranges.end());
        // Start of the current coalesced range and end (exclusive) of previous range.
        // Both are initialized with the start of first range which is a placeholder value.
        int64_t coalesced_start = itr->offset;
        int64_t prev_range_end = coalesced_start;

        for (; itr < ranges.end(); ++itr) {
            const int64_t current_range_start = itr->offset;
            const int64_t current_range_end = current_range_start + itr->length;
            // We don't expect to have 0 sized ranges.
            if (itr->length <= 0) {
                return ::arrow::Status::IOError("Some read ranges are 0 sized");
            }
            // DCHECK_LT(current_range_start, current_range_end);

            // At this point, the coalesced range is [coalesced_start, prev_range_end).
            // Stop coalescing if:
            //   - coalesced range is too large, or
            //   - distance (hole/gap) between consecutive ranges is too large.
            if (current_range_end - coalesced_start > range_size_limit_ ||
                current_range_start - prev_range_end > hole_size_limit_) {
                if (coalesced_start > prev_range_end) {
                    return ::arrow::Status::IOError("Some read ranges are too large");
                }
                // DCHECK_LE(coalesced_start, prev_range_end);
                // Append the coalesced range only if coalesced range size > 0.
                if (prev_range_end > coalesced_start) {
                    coalesced.push_back({coalesced_start, prev_range_end - coalesced_start});
                }
                // Start a new coalesced range.
                coalesced_start = current_range_start;
            }

            // Update the prev_range_end with the current range.
            prev_range_end = current_range_end;
        }
        // Append the coalesced range only if coalesced range size > 0.
        if (prev_range_end > coalesced_start) {
            coalesced.push_back({coalesced_start, prev_range_end - coalesced_start});
        }

        if (coalesced.front().offset != ranges.front().offset || 
            coalesced.back().offset + coalesced.back().length != ranges.back().offset + ranges.back().length) {
            return ::arrow::Status::IOError("Some read ranges are not coalesced");
        }
        // DCHECK_EQ(coalesced.front().offset, ranges.front().offset);
        // DCHECK_EQ(coalesced.back().offset + coalesced.back().length,
        //           ranges.back().offset + ranges.back().length);
        return coalesced;
    }

    const int64_t hole_size_limit_;
    const int64_t range_size_limit_;
};

class ParquetCache {
public:
    ~ParquetCache() {
        bthread_key_delete(_bthread_local_key);
    }

    static ParquetCache* get_instance() {
        static ParquetCache instance;
        return &instance;
    }

    void set_bthread_local(ReadContents* contents) {
        ReadContents* data = get_bthread_local();
        if (data == contents) {
            return;
        }

        bthread_setspecific(_bthread_local_key, contents); 
    }

    ReadContents* get_bthread_local() {
        void* data = bthread_getspecific(_bthread_local_key);
        return static_cast<ReadContents*>(data);
    }

    std::shared_ptr<rocksdb::Cache>& get_block_cache() {
        return _block_cache;
    }

private:
    ParquetCache() : _bthread_local_key(INVALID_BTHREAD_KEY) { 
        bthread_key_create(&_bthread_local_key, nullptr);
        _block_cache = rocksdb::NewLRUCache(FLAGS_parquet_cache_size_mb * 1024 * 1024LL, 8);
    }
    bthread_key_t _bthread_local_key;
    std::shared_ptr<rocksdb::Cache> _block_cache = nullptr;

};

class ParquetArrowReader {
public:
    ParquetArrowReader(const std::string& full_path, bool is_afs_file) : _full_path(full_path), _is_afs_file(is_afs_file) { }
    ~ParquetArrowReader() { 
        close();
    }

    int init();

    int64_t read(char* buf, uint32_t count, uint32_t offset, bool* eof);

    // Close the descriptor of this file adaptor
    bool close() {
        if (_afs_reader != nullptr) {
            _afs_reader->close();
            _afs_reader.reset();
        }
        if (_posix_reader != nullptr) {
            _posix_reader.reset();
        }
        return true;
    }

    std::string file_name() { return _full_path; }

private:
    std::string _full_path;
    bool _is_afs_file;
    std::shared_ptr<ExtFileReader> _afs_reader = nullptr;
    std::shared_ptr<FileReader> _posix_reader = nullptr;
    DISALLOW_COPY_AND_ASSIGN(ParquetArrowReader);
};

class ParquetArrowReadableFile : public ::arrow::io::RandomAccessFile {
public:    
    ParquetArrowReadableFile(const std::shared_ptr<ParquetArrowReader>& reader, int64_t size, ::arrow::MemoryPool* pool = ::arrow::default_memory_pool()) :
            _file_reader(reader), _file_size(size), _pool(pool) { }

    ~ParquetArrowReadableFile() override {
        Close();
    }

    static ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> Open(const std::string& path, int64_t size, bool is_afs_file) {
        std::shared_ptr<ParquetArrowReader> reader = std::make_shared<ParquetArrowReader>(path, is_afs_file);
        int ret = reader->init();
        if (ret != 0) {
            return ::arrow::Status::IOError("open file failed");
        }

        auto file = std::shared_ptr<ParquetArrowReadableFile>(new (std::nothrow) ParquetArrowReadableFile(reader, size));
        if (BAIKALDB_UNLIKELY(file == nullptr)) {
            return ::arrow::Status::OutOfMemory("Fail to new AfsReadableFile");
        }
        return file;
    }

    ::arrow::Status Close() override {
        if (_file_reader == nullptr) {
            return ::arrow::Status::OK();
        }
        _file_reader->close();
        _file_reader.reset();
        return ::arrow::Status::OK();
    }

    bool closed() const override {
        return _file_reader == nullptr;
    }

    // 底层双afs读，无法实现Read，自己记录游标，底层使用pread
    ::arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override {
        ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(_position, nbytes, buffer));
        _position += bytes_read;
        return bytes_read;
    }

    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> Read(int64_t nbytes) override {
        std::unique_ptr<::arrow::ResizableBuffer> buffer;
        ARROW_ASSIGN_OR_RAISE(buffer, ::arrow::AllocateResizableBuffer(nbytes, _pool));
        if (BAIKALDB_UNLIKELY(buffer == nullptr)) {
            return ::arrow::Status::IOError("Arrow buffer is empty");
        }

        int64_t bytes_read;
        ARROW_ASSIGN_OR_RAISE(bytes_read, Read(nbytes, buffer->mutable_data()));
        if (bytes_read < nbytes) {
            ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read));
        }
        return std::move(buffer);
    }

    ::arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* buffer) override;

    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        std::unique_ptr<::arrow::ResizableBuffer> buffer;
        ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(nbytes, _pool));
        if (BAIKALDB_UNLIKELY(buffer == nullptr)) {
            return ::arrow::Status::IOError("Arrow buffer is empty");
        }

        int64_t bytes_read;
        ARROW_ASSIGN_OR_RAISE(bytes_read, ReadAt(position, nbytes, buffer->mutable_data()));
        if (bytes_read < nbytes) {
            ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read));
            buffer->ZeroPadding();
        }
        return std::move(buffer);
    }

    ::arrow::Status Seek(int64_t position) override {
        _position = position;
        return ::arrow::Status::OK();
    }

    ::arrow::Result<int64_t> Tell() const override {
        return _position;
    }

    ::arrow::Result<int64_t> GetSize() override {
        return _file_size;
    }

private:
    ::arrow::Result<int64_t> read_from_device(int64_t position, int64_t nbytes, void* buffer);

    int64_t _position = 0;
    std::shared_ptr<ParquetArrowReader> _file_reader;
    const int64_t _file_size;
    ::arrow::MemoryPool* _pool;
};
} // namespace baikaldb