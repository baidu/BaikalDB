#pragma once
#include <iostream>
#include <istream>
#include <streambuf>
#include <string>
#include <vector>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <baidu/rpc/channel.h>
#include <json/json.h>
#include <mutex>

#include "meta_server_interact.hpp"
#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include "baidu/inf/afs-api/client/afs_filesystem.h"
#include "baidu/inf/afs-api/common/afs_common.h"
#else
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <butil/files/file_enumerator.h>
#endif

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"

#ifdef BAIDU_INTERNAL
namespace butil = base;
#endif

namespace baikaldb {
    
DECLARE_int32(file_buffer_size);
DECLARE_int32(file_block_size);

typedef boost::filesystem::directory_iterator dir_iter;

enum FileMode {
    I_FILE,
    I_DIR,
    I_LINK
};

struct MemBuf : std::streambuf{
    MemBuf(char* begin, char* end) {
        this->setg(begin, begin, end);
    }
};

class ImporterReaderAdaptor {
public:
    virtual ~ImporterReaderAdaptor() {}

    virtual int64_t seek(int64_t position) = 0;
    virtual int64_t tell() = 0;
    virtual int64_t read(void* buf, size_t buf_size) = 0;
    virtual int64_t read(size_t pos, char* buf, size_t buf_size) = 0;
};

class PosixReaderAdaptor : public ImporterReaderAdaptor {
public:
    PosixReaderAdaptor(const std::string& path) : _path(path), _f(butil::FilePath{path}, butil::File::FLAG_OPEN) {
        if (!_f.IsValid()) { 
            _error = true; 
            DB_FATAL("file: %s open failed", path.c_str());
        }
    }
    ~PosixReaderAdaptor() {}

    virtual int64_t seek(int64_t position) { return 0; }
    virtual int64_t tell() { return 0; }
    virtual int64_t read(void* buf, size_t buf_size) { return 0; }
    virtual int64_t read(size_t pos, char* buf, size_t buf_size);

private:
    std::string _path;
    butil::File _f;
    bool        _error { false };
};

#ifdef BAIDU_INTERNAL
class AfsReaderAdaptor : public ImporterReaderAdaptor{
public:
    AfsReaderAdaptor(afs::Reader* reader) : _reader(reader) {}

    virtual int64_t seek(int64_t position);
    virtual int64_t tell();
    virtual int64_t read(void* buf, size_t buf_size);
    virtual int64_t read(size_t pos, char* buf, size_t buf_size);

    afs::Reader* get_reader() { return _reader; }
private:
    bool         _error;
    afs::Reader* _reader;
};
#endif

class AfsReadableFile;

class ImporterFileSystemAdaptor {
public:
    ImporterFileSystemAdaptor(bool is_posix) : _is_posix(is_posix) {} 

    virtual ~ImporterFileSystemAdaptor() {}

    virtual int init() = 0;

    virtual ImporterReaderAdaptor* open_reader(const std::string& path) = 0;

    virtual void close_reader(ImporterReaderAdaptor* adaptor) = 0;

    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
                                open_arrow_file(const std::string& path) = 0;

    virtual ::arrow::Status close_arrow_reader(
                                std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader) = 0;

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size) = 0;

    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys) = 0;

    virtual void destroy() = 0;

    int recurse_handle(const std::string& path, const std::function<int(const std::string&)>& fn);

    int all_block_count(std::string path, int32_t block_size = 0);

    int all_row_group_count(const std::string& path);

    bool is_posix() {
        return _is_posix; 
    }

    size_t all_file_size() const {
        return _all_file_size;
    }

    int cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
                  std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos);
    
    int cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths, 
                  std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos, int64_t& start_pos,
                  int64_t& cur_size, int64_t& cur_start_pos, int64_t& cur_end_pos, std::string& cur_file_paths);
private:
    bool   _is_posix;
    size_t _all_file_size { 0 };
};

class PosixFileSystemAdaptor : public ImporterFileSystemAdaptor {
public:
    PosixFileSystemAdaptor() : ImporterFileSystemAdaptor(true) {}

    ~PosixFileSystemAdaptor() {}

    virtual int init() { 
        return 0; 
    }

    virtual ImporterReaderAdaptor* open_reader(const std::string& path);

    virtual void close_reader(ImporterReaderAdaptor* adaptor);

    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
                                    open_arrow_file(const std::string& path);

    virtual ::arrow::Status close_arrow_reader(
                                    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader);

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size);

    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys);

    virtual void destroy() { }
};

#ifdef BAIDU_INTERNAL
class AfsFileSystemAdaptor : public ImporterFileSystemAdaptor {
public:
    AfsFileSystemAdaptor(const std::string& afs_uri, const std::string& afs_user, 
                        const std::string& afs_password, const std::string& afs_conf_file) : 
                        ImporterFileSystemAdaptor(false), 
                        _afs_uri(afs_uri), _afs_user(afs_user), _afs_password(afs_password), 
                        _afs_conf_file(afs_conf_file) {

    }

    ~AfsFileSystemAdaptor() {
        if (_afs) {
            DB_WARNING("afs destructor");
            delete _afs;
        }
    }

    virtual int init();

    virtual ImporterReaderAdaptor* open_reader(const std::string& path);

    virtual void close_reader(ImporterReaderAdaptor* adaptor);

    virtual ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
                                    open_arrow_file(const std::string& path);

    virtual ::arrow::Status close_arrow_reader(
                                    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader);

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size);

    virtual int read_dir(const std::string& path, std::vector<std::string>& direntrys);

    virtual void destroy();

private:
    std::string         _afs_uri;
    std::string         _afs_user;
    std::string         _afs_password;
    std::string         _afs_conf_file;
    afs::AfsFileSystem* _afs;
};

// AfsReadableFile
class AfsReadableFile : public ::arrow::io::RandomAccessFile {
public:
    static ::arrow::Result<std::shared_ptr<AfsReadableFile>> Open(
            const std::string& path, ImporterFileSystemAdaptor* fs, 
            ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());
    
    AfsReadableFile(ImporterFileSystemAdaptor* fs, ::arrow::MemoryPool* pool);
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
    ReadDirImpl(const std::string& path, ImporterFileSystemAdaptor* fs) :
        _path(path), _fs(fs) { }

    // return -1 : fail 
    // return  0 : success; entry is valid
    // return  1 : finish;  entry is not valid
    int next_entry(std::string& entry);

private:
    size_t                     _idx { 0 };
    bool                       _read_finish { false };
    std::vector<std::string>   _entrys;
    std::string                _path;
    ImporterFileSystemAdaptor* _fs;
};

} //baikaldb
