#pragma once
#include <iostream>
#include <istream>
#include <streambuf>
#include <string>
#include "importer_handle.h"
#include <vector>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <baidu/rpc/channel.h>
#include <json/json.h>

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
#ifdef BAIDU_INTERNAL
namespace butil = base;
#endif

namespace baikaldb {
DECLARE_int32(concurrency);
DECLARE_int32(file_concurrency);
DECLARE_uint64(insert_values_count);

typedef boost::filesystem::directory_iterator dir_iter;

typedef std::function<void(const std::string& path, const std::vector<std::string>&)> LinesFunc;
typedef std::function<void(const std::string&)> ProgressFunc;
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

    virtual size_t read(size_t pos, char* buf, size_t buf_size) = 0;
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

    virtual size_t read(size_t pos, char* buf, size_t buf_size);

private:
    std::string _path;
    butil::File _f;
    bool _error = false;
};

#ifdef BAIDU_INTERNAL
class AfsReaderAdaptor : public ImporterReaderAdaptor{
public:
    AfsReaderAdaptor(afs::Reader* reader) : _reader(reader) {}

    virtual size_t read(size_t pos, char* buf, size_t buf_size);

    afs::Reader* get_reader() { return _reader; }
private:
    bool _error;
    afs::Reader* _reader;
};
#endif

class ImporterFileSystermAdaptor {
public:
    ImporterFileSystermAdaptor(bool is_posix) : _is_posix(is_posix) {} 

    virtual ~ImporterFileSystermAdaptor() {}

    virtual ImporterReaderAdaptor* open_reader(const std::string& path) = 0;

    virtual int init() = 0;

    virtual void close_reader(ImporterReaderAdaptor* adaptor) = 0;

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size) = 0;

    virtual int  read_dir(const std::string& path, std::vector<std::string>& direntrys) = 0;

    virtual void destroy() = 0;

    int all_block_count(std::string path);

    bool is_posix() { return _is_posix; }

    size_t all_file_size() const {
        return _all_file_size;
    }
    int cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
                  std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos);
    int cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths, 
                  std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos, int64_t& start_pos,
                  int64_t& cur_size, int64_t& cur_start_pos, int64_t& cur_end_pos, std::string& cur_file_paths);
private:
    bool _is_posix;
    size_t _all_file_size = 0;
};

class PosixFileSystermAdaptor : public ImporterFileSystermAdaptor {
public:
    PosixFileSystermAdaptor() : ImporterFileSystermAdaptor(true) {}

    ~PosixFileSystermAdaptor() {}

     virtual int init() { return 0; }

    virtual ImporterReaderAdaptor* open_reader(const std::string& path);

    virtual void close_reader(ImporterReaderAdaptor* adaptor);

    virtual int  read_dir(const std::string& path, std::vector<std::string>& direntrys);

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size);

    virtual void destroy() { }
};

#ifdef BAIDU_INTERNAL
class AfsFileSystermAdaptor : public ImporterFileSystermAdaptor {
public:
    AfsFileSystermAdaptor(const std::string& afs_uri, const std::string& afs_user, 
                        const std::string& afs_password, const std::string& afs_conf_file) : 
                        ImporterFileSystermAdaptor(false), 
                        _afs_uri(afs_uri), _afs_user(afs_user), _afs_password(afs_password), 
                        _afs_conf_file(afs_conf_file) {

    }

    ~AfsFileSystermAdaptor() {
        if (_afs) {
            DB_WARNING("afs destructor");
            delete _afs;
        }
    }

    virtual int init();

    virtual ImporterReaderAdaptor* open_reader(const std::string& path);

    virtual void close_reader(ImporterReaderAdaptor* adaptor);

    virtual int file_mode(const std::string& path, FileMode* mode, size_t* file_size);
    
    virtual int  read_dir(const std::string& path, std::vector<std::string>& direntrys);

    virtual void destroy();

private:
    std::string _afs_uri;
    std::string _afs_user;
    std::string _afs_password;
    std::string _afs_conf_file;
    afs::AfsFileSystem* _afs;
};
#endif

class ReadDirImpl {
public:
    ReadDirImpl(const std::string& path, ImporterFileSystermAdaptor* fs) :
        _path(path), _fs(fs) { }

    // return -1 : fail 
    // return  0 : success; entry is valid
    // return  1 : finish;  entry is not valid
    int next_entry(std::string& entry);

private:
    size_t  _idx        = 0;
    bool _read_finish = false;
    std::vector<std::string> _entrys;
    std::string _path;
    ImporterFileSystermAdaptor* _fs;
};

class BlockImpl {
public:
    BlockImpl(const std::string& path, size_t start_pos, size_t end_pos, size_t file_size, 
            ImporterFileSystermAdaptor* fs, const LinesFunc& fn, bool has_header, int32_t insert_values_count) :
            _path(path), _cur_pos(start_pos), _start_pos(start_pos), _end_pos(end_pos), _file_size(file_size), _fs(fs), 
            _concurrency_cond(-FLAGS_concurrency), _lines_manager(fn, _concurrency_cond, path, insert_values_count) { 
        if (_start_pos == 0 && !has_header) {
            _escape_first_line = false;
        }
        // std::vector<std::string> split_vec;
        // boost::split(split_vec, _path, boost::is_any_of("/"));
        // std::string out_path = split_vec.back();
        // _out.open("./outdata/" + out_path, std::ofstream::out | std::ofstream::app);
        DB_WARNING("path: %s, start_pos: %ld, end_pos: %ld, file_size: %ld", _path.c_str(), _start_pos, _end_pos, _file_size);

    }

    ~BlockImpl() {
        _lines_manager.join();

        _concurrency_cond.wait(-FLAGS_concurrency);

        if (_buf != nullptr) {
            free(_buf);
        }

        if (_file) {
            _fs->close_reader(_file);
        }
        // _out.close();
    }

    int init();

    int buf_resize(int64_t size);

    bool block_finish();

    // 循环调用直到读取结束
    int read_and_exec();

    void write(const std::string& line) {
        std::string line_ = line + "\n";
        _out << line_;
    }

private:
    class LinesManager {
    public:
        LinesManager(const LinesFunc& fn, BthreadCond& cond, const std::string& path, int32_t insert_values_count) : 
                        _func(fn), _cond(cond), _path(path), _insert_values_count(insert_values_count) {
            _lines.reserve(_insert_values_count);
        }

        void handle_line(std::string& line);

        void join();

    private:
        const LinesFunc&   _func;
        BthreadCond& _cond;
        const std::string& _path;
        std::vector<std::string> _lines;
        const int32_t _insert_values_count;
    };

    ImporterReaderAdaptor* _file = nullptr;
    std::string _path;
    bool  _escape_first_line = true;        // 是否跳过首行
    int64_t _cur_pos;                       // 当前位置在整个文件中的pos, 该pos不会分割行，肯定是行首
    const int64_t _start_pos;
    const int64_t _end_pos;
    const int64_t _file_size;               // 文件大小
    ImporterFileSystermAdaptor* _fs;
    char*  _buf = nullptr;
    int64_t _read_buffer_size = 0;
    std::ofstream _out;

    BthreadCond  _concurrency_cond;          // 文件块内并发发送个数
    LinesManager _lines_manager;
};

//并发控制
class ImporterImpl {
public:
    ImporterImpl(const std::string& path, ImporterFileSystermAdaptor* fs,
            const LinesFunc& fn, int64_t file_concurrency, int32_t insert_values_count,
            int64_t start_pos, int64_t end_pos, bool has_header = false) : 
            _path(path), _fs(fs), _lines_func(fn), 
            _file_concurrency(file_concurrency == 0 ? FLAGS_file_concurrency : file_concurrency),
            _file_concurrency_cond(-_file_concurrency), _start_pos(start_pos), _end_pos(end_pos),
            _insert_values_count(insert_values_count == 0 ? FLAGS_insert_values_count : insert_values_count) {
        _all_block_count = _fs->all_block_count(_path);
        _handled_block_count = 0;
        _has_header = has_header;
    }

    ~ImporterImpl() { 
        
    }

    int run(const ProgressFunc& func) {
        int ret = recurse_handle(_path, func);
        _file_concurrency_cond.wait(-_file_concurrency);
        if (ret < 0 || _result < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    int recurse_handle(const std::string& path, const ProgressFunc& func);
   
    std::string get_result() {
        return _import_ret.str();
    }
private:
    std::string _path;
    ImporterFileSystermAdaptor* _fs;
    const LinesFunc&  _lines_func;
    int32_t _file_concurrency;
    int32_t _insert_values_count;
    BthreadCond _file_concurrency_cond;   // 并发读取文件块个数
    const int64_t _start_pos;
    const int64_t _end_pos;
    int64_t _all_block_count = 0;
    std::atomic<int64_t> _handled_block_count;
    bool _has_header = false;
    int  _result = 0;
    std::ostringstream _import_ret;
};

} //baikaldb
