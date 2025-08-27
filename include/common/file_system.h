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

#include "common.h"

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "parquet/arrow/reader.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#ifdef BAIDU_INTERNAL
#include <bthread.h>
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include "baidu/inf/afs-api/client/afs_filesystem.h"
#include "baidu/inf/afs-api/common/afs_common.h"
#include "baidu/inf/afs-api/client/afs_impl.h"
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

DECLARE_int32(file_buffer_size);

typedef boost::filesystem::directory_iterator dir_iter;

enum class FileMode {
    I_FILE,
    I_DIR,
    I_LINK
};

struct FileInfo {
    FileMode mode;
    size_t size = 0;
};

class FileWriter {
public:
    FileWriter() {}
    virtual ~FileWriter() {}

    virtual int64_t append(const char* buf, uint32_t count) = 0;
    virtual int64_t tell() = 0;
    virtual bool sync() = 0;
};

#ifdef BAIDU_INTERNAL
class AfsFileWriter : public FileWriter {
public:
    AfsFileWriter(afs::Writer* writer) : _writer(writer) {}
    virtual ~AfsFileWriter() {}

    virtual int64_t append(const char* buf, uint32_t count) override;
    virtual int64_t tell() override;
    virtual bool sync() override;
    afs::Writer* get_writer() { 
        return _writer;
    }
    
private:
    afs::Writer* _writer = nullptr;
};
#endif

class FileReader {
public:
    FileReader() {}
    virtual ~FileReader() {}
    virtual int64_t seek(int64_t position) = 0;
    virtual int64_t tell() = 0;
    virtual int64_t read(void* buf, size_t buf_size) = 0;
    virtual int64_t read(size_t pos, char* buf, size_t buf_size) = 0;
};

class PosixFileReader : public FileReader {
public:
    PosixFileReader(const std::string& path) : _path(path), _f(butil::FilePath{path}, butil::File::FLAG_OPEN) {
        if (!_f.IsValid()) {
            _error = true;
            DB_FATAL("file: %s open failed", path.c_str());
        }
    }
    virtual ~PosixFileReader() {}

    virtual int64_t seek(int64_t position) override {
        DB_FATAL("not implement");
        return -1; 
    }
    virtual int64_t tell() override {
        DB_FATAL("not implement");
        return -1; 
    }
    virtual int64_t read(void* buf, size_t buf_size) override {
        DB_FATAL("not implement");
        return -1; 
    }
    virtual int64_t read(size_t pos, char* buf, size_t buf_size) override;

private:
    std::string _path;
    butil::File _f;
    bool _error = false;
};

#ifdef BAIDU_INTERNAL
class AfsFileReader : public FileReader {
public:
    AfsFileReader(afs::Reader* reader) : _reader(reader) {}
    virtual ~AfsFileReader() {}

    virtual int64_t seek(int64_t position) override;
    virtual int64_t tell() override;
    virtual int64_t read(void* buf, size_t buf_size) override;
    virtual int64_t read(size_t pos, char* buf, size_t buf_size) override;
    afs::Reader* get_reader() { 
        return _reader; 
    }
    
private:
    afs::Reader* _reader = nullptr;
};
#endif

class FileSystem {
public:
    FileSystem(bool is_posix) : _is_posix(is_posix) {}
    virtual ~FileSystem() {}

    virtual int init() = 0;
    virtual std::shared_ptr<FileReader> open_reader(const std::string& path) = 0;
    virtual int close_reader(std::shared_ptr<FileReader> file_reader) = 0;
    virtual std::shared_ptr<FileWriter> open_writer(const std::string& path, bool is_create) = 0;
    virtual int close_writer(std::shared_ptr<FileWriter> file_writer) = 0;
    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys) = 0;
    virtual int get_file_info(const std::string& path, FileInfo& file_info, std::string* err_msg) = 0;
    virtual int destroy() = 0;
    virtual int delete_path(const std::string& path, bool recursive = false) = 0;

    // Parquet读取使用
    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> open_arrow_file(const std::string& path) = 0;
    virtual ::arrow::Status close_arrow_reader(std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader) = 0;

    bool is_posix() {
        return _is_posix; 
    }

private:
    bool _is_posix = false;
};

class PosixFileSystem : public FileSystem {
public:
    PosixFileSystem() : FileSystem(true) {}
    virtual ~PosixFileSystem() {}

    virtual int init() override {
        return 0; 
    }
    virtual std::shared_ptr<FileReader> open_reader(const std::string& path) override;
    virtual int close_reader(std::shared_ptr<FileReader> file_reader) override;
    virtual std::shared_ptr<FileWriter> open_writer(const std::string& path, bool is_create) override { 
        DB_FATAL("not implement");
        return nullptr; 
    }
    virtual int close_writer(std::shared_ptr<FileWriter> file_writer) override {
        DB_FATAL("not implement");
        return -1;
    }
    virtual int destroy() override {
        DB_FATAL("not implement");
        return -1;
    }
    virtual int delete_path(const std::string& path, bool recursive = false) override { 
        DB_FATAL("not implement");
        return -1; 
    }

    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys) override;
    virtual int get_file_info(const std::string& path, FileInfo& file_info, std::string* err_msg) override;

    // Parquet读取使用
    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> open_arrow_file(const std::string& path) override;
    virtual ::arrow::Status close_arrow_reader(std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader) override;
};

#ifdef BAIDU_INTERNAL
class AfsFileSystem : public FileSystem {
public:
    AfsFileSystem(const std::string& afs_uri, 
                         const std::string& afs_user, 
                         const std::string& afs_password, 
                         const std::string& afs_conf_file)
                         : FileSystem(false)
                         , _afs_uri(afs_uri)
                         , _afs_user(afs_user)
                         , _afs_password(afs_password)
                         , _afs_conf_file(afs_conf_file) {
    }

    ~AfsFileSystem() {}

    virtual int init() override;
    virtual std::shared_ptr<FileReader> open_reader(const std::string& path) override;
    virtual int close_reader(std::shared_ptr<FileReader> file_reader) override;
    virtual std::shared_ptr<FileWriter> open_writer(const std::string& path, bool is_create) override;
    virtual int close_writer(std::shared_ptr<FileWriter> file_writer) override;
    virtual int destroy() override;
    virtual int delete_path(const std::string& path, bool recursive = false) override;

    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys) override;
    virtual int get_file_info(const std::string& path, FileInfo& file_info, std::string* err_msg) override;

    // Parquet使用
    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> open_arrow_file(const std::string& path) override;
    virtual ::arrow::Status close_arrow_reader(std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader) override;

private:
    std::string _afs_uri;
    std::string _afs_user;
    std::string _afs_password;
    std::string _afs_conf_file;
    std::shared_ptr<afs::AFSImpl> _afs;
};

class AfsReadableFile : public ::arrow::io::RandomAccessFile {
public:
    static ::arrow::Result<std::shared_ptr<AfsReadableFile>> Open(
            const std::string& path, FileSystem* fs, ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());
    
    AfsReadableFile(FileSystem* fs, ::arrow::MemoryPool* pool);
    ~AfsReadableFile() override;

    ::arrow::Status Close() override;
    bool closed() const override;
    ::arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override;
    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> Read(int64_t nbytes) override;
    ::arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* buffer) override;
    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

    ::arrow::Status Seek(int64_t position) override;
    ::arrow::Result<int64_t> Tell() const override;
    ::arrow::Result<int64_t> GetSize() override;

private:
    class AfsReadableFileImpl;
    std::unique_ptr<AfsReadableFileImpl> _impl;
};
#endif

class ReadDirImpl {
public:
    ReadDirImpl(const std::string& path, FileSystem* fs) : _path(path), _fs(fs) {}

    // return -1 : fail 
    // return  0 : success; entry is valid
    // return  1 : finish;  entry is not valid
    int next_entry(std::string& entry);

private:
    size_t                   _idx = 0;
    bool                     _read_finish = false;
    std::vector<std::string> _entrys;
    std::string              _path;
    FileSystem*              _fs = nullptr;
};

extern std::shared_ptr<FileSystem> create_filesystem(const std::string& cluster_name = "", 
                                                     const std::string& user_name = "", 
                                                     const std::string& password = "", 
                                                     const std::string& conf_file = "");

extern int destroy_filesystem(std::shared_ptr<FileSystem> fs);

} // namespace baikaldb