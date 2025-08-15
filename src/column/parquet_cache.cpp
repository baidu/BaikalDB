#include "column_statistics.h"
#include "parquet_cache.h"

namespace baikaldb {
DEFINE_int64(parquet_cache_size_mb, 8 * 1024, "parquet block_cache_size_mb, default: 8G");

rocksdb::Cache::CacheItemHelper BlockContents::kBasicHelper{
    rocksdb::CacheEntryRole::kOtherBlock, &BlockContents::delete_fn
};

int ParquetArrowReader::init() {
    if (_is_afs_file) {
        auto ext_fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
        if (ext_fs == nullptr) {
            DB_WARNING("ext fs is nullptr");
            return -1;
        }
        int ret = ext_fs->open_reader(_full_path, &_afs_reader);
        if (ret != 0) {
            DB_COLUMN_FATAL("open file failed. path: %s", _full_path.c_str());
            return -1;
        }
    } else {
        _posix_reader = PosixFileSystem().open_reader(_full_path);
        if (_posix_reader == nullptr) {
            DB_COLUMN_FATAL("open file failed. path: %s", _full_path.c_str());
            return -1;
        }
    }

    return 0;
}

int64_t ParquetArrowReader::read(char* buf, uint32_t count, uint32_t offset, bool* eof) {
    int64_t size = 0;
    TimeCost cost;
    if (_is_afs_file) {
        size = _afs_reader->read(buf, count, offset, eof);
    } else {
        size = _posix_reader->read(offset, buf, count);
        *eof = (size <= count);
    }

    if (_is_afs_file) {
        ColumnVars::get_instance()->parquet_afs_read_time_cost << cost.get_time();
    } else {
        ColumnVars::get_instance()->parquet_ssd_read_time_cost << cost.get_time();
    }
    ColumnVars::get_instance()->parquet_read_bytes << size;
    return size;
}

::arrow::Result<int64_t> ParquetArrowReadableFile::ReadAt(int64_t position, int64_t nbytes, void* buffer) {
    auto parquet_cache = ParquetCache::get_instance();
    ReadContents* contents = parquet_cache->get_bthread_local();
    if (contents == nullptr) {
        ColumnVars::get_instance()->inc_cache_hit(false);
        return read_from_device(position, nbytes, buffer);
    }

    MutTableKey key;
    key.append_i64(contents->region_id).append_i64(contents->start_version).append_i64(contents->end_version).append_i32(contents->file_idx).append_i64(position);
    rocksdb::Slice key_slice(key.data());
    auto handle = parquet_cache->get_block_cache()->BasicLookup(key_slice, nullptr);
    if (handle != nullptr) {
        auto block = static_cast<BlockContents*>(parquet_cache->get_block_cache()->Value(handle));
        if (nbytes != block->size) {
            DB_COLUMN_FATAL("filename: %s, read position: %ld, nbytes: %ld size: %d", 
                contents->file_short_name.c_str(), position, nbytes, block->size);
            ColumnVars::get_instance()->inc_cache_hit(false);
            return read_from_device(position, nbytes, buffer);
        }
        memcpy(buffer, block->data, block->size);
        auto size = block->size;
        parquet_cache->get_block_cache()->Release(handle);
        ColumnVars::get_instance()->inc_cache_hit(true);
        return size;
    }

    ColumnVars::get_instance()->inc_cache_hit(false);
    auto s = read_from_device(position, nbytes, buffer);
    if (!s.ok() || !contents->fill_cache) {
        return s;
    }

    auto block2 = new BlockContents(s.ValueOrDie(), (char*)buffer);
    parquet_cache->get_block_cache()->Insert(key_slice, block2, &BlockContents::kBasicHelper, block2->size, nullptr);

    // DB_WARNING("read from device, filename: %s, offset: %ld, len: %ld", _file_reader->file_name().c_str(), position, nbytes);

    return s;
}

::arrow::Result<int64_t> ParquetArrowReadableFile::read_from_device(int64_t position, int64_t nbytes, void* buffer) {
    int64_t r = -1;
    int64_t left = nbytes;
    char* ptr = static_cast<char*>(buffer);
    bool eof = false;
    while (left > 0) {
        r = _file_reader->read(ptr, left, position, &eof);
        if (r < 0) {
            break;
        }
        ptr += r;
        position += r;
        left -= r;
        if (eof) {
            break;
        }
    }
    if (r < 0) {
        DB_COLUMN_FATAL("filename: %s, read offset: %lu, len: %ld failed", _file_reader->file_name().c_str(), position, nbytes);
        // An error: return a non-ok status
        return ::arrow::Status::IOError("While pread offset " + std::to_string(position) + " len " +
                        std::to_string(nbytes) + " " + _file_reader->file_name());
    }

    return nbytes - left;
}

}