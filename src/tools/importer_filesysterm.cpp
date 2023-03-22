#include "parquet/arrow/reader.h"
#include "importer_filesysterm.h"
#include "importer_macros.h"

namespace baikaldb {
DEFINE_int32(file_buffer_size, 1, "read file buf size (MBytes)");
DEFINE_int32(file_block_size, 100, "split file to block to handle(MBytes)");

int64_t PosixReaderAdaptor::read(size_t pos, char* buf, size_t buf_size) {
    if (_error) {
        DB_WARNING("file: %s read failed", _path.c_str());
        return -1;
    }
    int64_t size = _f.Read(pos, buf, buf_size);
    if (size < 0) {
        DB_WARNING("file: %s read failed", _path.c_str());
    }

    DB_WARNING("read file :%s, pos: %ld, read_size: %ld", _path.c_str(), pos, size);

    return size;
}

#ifdef BAIDU_INTERNAL
int64_t AfsReaderAdaptor::seek(int64_t position) {
    int64_t afs_res = _reader->Seek(position);
    if (afs_res == ds::kOutOfRange) {
        DB_WARNING("AfsReaderAdaptor out of range pos: %lu", position);
        return 0;
    } else if (afs_res != ds::kOk) {
        DB_FATAL("AfsReaderAdaptor seek pos: %lu failed, errno:%ld, errmsg:%s", position, afs_res, ds::Rc2Str(afs_res));
        return -1;
    }
    return 0;
}

int64_t AfsReaderAdaptor::tell() {
    int64_t afs_res = _reader->Tell();
    if (afs_res < 0) {
        DB_WARNING("AfsReaderAdaptor fail to tell, ret_code: %ld", afs_res);
    }
    return afs_res;
}

int64_t AfsReaderAdaptor::read(void* buf, size_t buf_size) {
    int64_t afs_res = _reader->Read(buf, buf_size);
    if (afs_res >= 0) {
        DB_WARNING("AfsReaderAdaptor read return size: %ld", afs_res);
        return afs_res;
    } else {
        DB_FATAL("AfsReaderAdaptor read errno:%ld, errmsg:%s", afs_res, ds::Rc2Str(afs_res));
        return -1;
    }
}

int64_t AfsReaderAdaptor::read(size_t pos, char* buf, size_t buf_size) {
    int64_t afs_res = _reader->Seek(pos);
    if (afs_res == ds::kOutOfRange) {
        DB_WARNING("out of range pos: %lu", pos);
        return 0;
    } else if (afs_res != ds::kOk) {
        DB_FATAL("seek pos: %lu failed, errno:%ld, errmsg:%s", pos, afs_res, ds::Rc2Str(afs_res));
        return -1;
    }

    afs_res = _reader->Read((void*)buf, buf_size);
    if (afs_res >= 0) {
        DB_WARNING("read pos: %lu, return size: %ld", pos, afs_res);
        return afs_res;
    } else {
        DB_FATAL("read pos: %lu failed, errno:%ld, errmsg:%s", pos, afs_res, ds::Rc2Str(afs_res));
        return -1;
    }
}
#endif

int ImporterFileSystemAdaptor::cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
                                          std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos) {
    std::string cur_file_paths = "";
    int64_t cur_size = 0;
    int64_t cur_start_pos = 0;
    int64_t cur_end_pos = 0;
    int64_t start_pos = 0;
    int ret = cut_files(path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
            cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
    if (ret < 0) {
        return ret;
    }
    if (!cur_file_paths.empty()) {
        if (cur_file_paths.back() == ';') {
            cur_file_paths.pop_back();
        }
        file_paths.emplace_back(cur_file_paths);
        file_start_pos.emplace_back(start_pos);
        file_end_pos.emplace_back(cur_end_pos);
        DB_WARNING("path:%s start_pos: %ld, end_pos: %ld", cur_file_paths.c_str(), cur_start_pos, cur_end_pos);
    }
    return 0;
}

/*
 * 按照block_size将导入源文件切分多个任务
 * 可能将一个大文件，切割多个范围段生成多个子任务，也可能将多个小文件聚集到一个子任务执行
 * start_pos: 子任务的第一个文件的起始地址
 * cur_start_pos: 当前切割文件的起始地址
 * cur_end_pos: 当前切割文件的结束地址
 * cur_size: 子任务累计文件大小
 * cur_file_paths: 子任务文件列表，分号分隔
 */
int ImporterFileSystemAdaptor::cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
                                std::vector<int64_t>& file_start_pos, std::vector<int64_t>& file_end_pos, int64_t& start_pos,
                                int64_t& cur_size, int64_t& cur_start_pos, int64_t& cur_end_pos, std::string& cur_file_paths) {
    FileMode mode;
    size_t file_size = 0;
    int ret = file_mode(path, &mode, &file_size);
    if (ret < 0) {
        DB_FATAL("get file mode failed, file: %s", path.c_str());
        return -1;
    }

    if (I_DIR == mode) {
        ReadDirImpl dir_iter(path, this);
        DB_WARNING("path:%s is dir", path.c_str());
        while (1) {
            std::string child_path;
            int ret = dir_iter.next_entry(child_path);
            if (ret < 0) {
                return -1;
            } else if (ret == 1) {
                break;
            }
            ret = cut_files(path + "/" + child_path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
                    cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
            if (ret < 0) {
                return -1;
            }
        }

        return 0; 
    } else if (I_LINK == mode && _is_posix) {
        char buf[2000] = {0};
        readlink(path.c_str(), buf, sizeof(buf));
        boost::filesystem::path symlink(path);
        boost::filesystem::path symlink_dir = symlink.parent_path();
        std::string real_path;
        if (buf[0] == '/') {
            real_path = buf;
        } else {
            real_path = symlink_dir.string() + "/" + buf;
        }
        DB_WARNING("path:%s is symlink, real path:%s", path.c_str(), real_path.c_str());
        return cut_files(real_path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
                         cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
    } else if (I_FILE == mode) {
        DB_WARNING("path:%s file_size: %lu, cur_size: %ld, start_pos: %ld, end_pos: %ld", path.c_str(), file_size, cur_size, cur_start_pos, cur_end_pos);
        if (file_size == 0) {
            return 0;
        }
        if (cur_size + file_size < block_size) {
            cur_file_paths += path + ";";
            cur_size += file_size;
            cur_end_pos = file_size;
            cur_start_pos = 0;
        } else {
            cur_file_paths += path;
            cur_end_pos = cur_start_pos + block_size - cur_size;
            file_paths.emplace_back(cur_file_paths);
            file_start_pos.emplace_back(start_pos);
            file_end_pos.emplace_back(cur_end_pos);
            DB_WARNING("path:%s start_pos: %ld, end_pos: %ld", cur_file_paths.c_str(), cur_start_pos, cur_end_pos);
            cur_file_paths = "";
            cur_start_pos = cur_end_pos;
            start_pos = cur_end_pos;
            cur_size = 0;

            if (cur_start_pos < file_size && cur_start_pos + block_size >= file_size) {
                cur_file_paths += path + ";";
                cur_size += (file_size - cur_start_pos);
                cur_end_pos = file_size;
                cur_start_pos = 0;
                return 0;
            }

            if (cur_end_pos < file_size) {
                return cut_files(path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
                                 cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
            }
        }
    }
    return 0;
}

int ImporterFileSystemAdaptor::recurse_handle(
    const std::string& path, const std::function<int(const std::string&)>& fn) {
    
    FileMode mode;
    size_t file_size = 0;
    int ret = file_mode(path, &mode, &file_size);
    if (ret < 0) {
        DB_FATAL("get file mode failed, file: %s", path.c_str());
        return 0;
    }
    
    if (I_DIR == mode) {
        ReadDirImpl dir_iter(path, this);
        DB_TRACE("path:%s is dir", path.c_str());
        int count = 0;
        while (1) {
            std::string child_path;
            int ret = dir_iter.next_entry(child_path);
            if (ret < 0) {
                return 0;
            } else if (ret == 1) {
                break;
            }

            count += recurse_handle(path + "/" + child_path, fn);
        }

        return count;   
    }

    if (I_LINK == mode && _is_posix) {
        char buf[2000] = {0};
        readlink(path.c_str(), buf, sizeof(buf));
        boost::filesystem::path symlink(path);
        boost::filesystem::path symlink_dir = symlink.parent_path();
        std::string real_path;
        if (buf[0] == '/') {
            real_path = buf;
        } else {
            real_path = symlink_dir.string() + "/" + buf;
        }
        DB_WARNING("path:%s is symlink, real path:%s", path.c_str(), real_path.c_str());
        return recurse_handle(real_path, fn);
    }

    if (I_FILE != mode) {
        return 0;
    }

    return fn(path);
}

int ImporterFileSystemAdaptor::all_block_count(std::string path, int32_t block_size_mb) {
    auto fn = [this, block_size_mb] (const std::string& path) -> int {
        FileMode mode;
        size_t file_size = 0;
        int ret = file_mode(path, &mode, &file_size);
        if (ret < 0) {
            DB_FATAL("get file mode failed, file: %s", path.c_str());
            return 0;
        }
        size_t file_block_size = block_size_mb * 1024 * 1024ULL;
        _all_file_size += file_size;
        if (file_block_size <= 0) {
            DB_FATAL("file_block_size: %ld <= 0", file_block_size);
            return 0;
        }
        size_t blocks = file_size / file_block_size + 1;
        DB_TRACE("path:%s is file, size:%lu, blocks:%lu", path.c_str(), file_size, blocks);
        return blocks;
    };
    _all_file_size = 0;
    return recurse_handle(path, fn);
}

int ImporterFileSystemAdaptor::all_row_group_count(const std::string& path) {
    auto fn = [this] (const std::string& path) -> int {
        FileMode mode;
        size_t file_size = 0;
        int ret = file_mode(path, &mode, &file_size);
        if (ret < 0) {
            DB_FATAL("get file mode failed, file: %s", path.c_str());
            return 0;
        }

        _all_file_size += file_size;

        auto res = open_arrow_file(path);
        if (!res.ok()) {
            DB_WARNING("Fail to open ParquetReader, reason: %s", res.status().message().c_str());
            return 0;
        }
        auto infile = std::move(res).ValueOrDie();

        ScopeGuard arrow_file_guard(
            [this, infile] () {
                if (infile != nullptr && !infile->closed()) {
                    close_arrow_reader(infile);
                }
            } 
        );

        ::parquet::arrow::FileReaderBuilder builder;
        auto status = builder.Open(infile);
        if (!status.ok()) {
            DB_WARNING("FileBuilder fail to open file, file_path: %s, reason: %s", 
                        path.c_str(), status.message().c_str());
            return 0; 
        }
        std::unique_ptr<::parquet::arrow::FileReader> reader;
        status = builder.Build(&reader);
        if (!status.ok()) {
            DB_WARNING("FileBuilder fail to build reader, file_path: %s, reason: %s", 
                        path.c_str(), status.message().c_str());
            return 0;
        }
        if (BAIKALDM_UNLIKELY(reader == nullptr)) {
            DB_WARNING("FileReader is nullptr, file_path: %s", path.c_str());
            return 0;
        }
        if (BAIKALDM_UNLIKELY(reader->parquet_reader() == nullptr)) {
            DB_WARNING("ParquetReader is nullptr, file_path: %s", path.c_str());
            return 0;
        }
        auto file_metadata = reader->parquet_reader()->metadata();
        if (BAIKALDM_UNLIKELY(file_metadata == nullptr)) {
            DB_WARNING("FileMetaData is nullptr, file_path: %s", path.c_str());
            return 0;
        }

        int num_row_groups = file_metadata->num_row_groups();

        return num_row_groups;
    };

    return recurse_handle(path, fn);
}

ImporterReaderAdaptor* PosixFileSystemAdaptor::open_reader(const std::string& path) {

    DB_WARNING("open reader: %s", path.c_str());

    return new PosixReaderAdaptor(path);
}

void PosixFileSystemAdaptor::close_reader(ImporterReaderAdaptor* adaptor) {
    if (adaptor) {
        delete adaptor;
    }
}

::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
        PosixFileSystemAdaptor::open_arrow_file(const std::string& path) {
    return ::arrow::io::ReadableFile::Open(path);
}

::arrow::Status PosixFileSystemAdaptor::close_arrow_reader(
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader)  {
    if (arrow_reader != nullptr && !arrow_reader->closed()) {
        return arrow_reader->Close();
    }
    return ::arrow::Status::OK();
}

int PosixFileSystemAdaptor::file_mode(const std::string& path, FileMode* mode, size_t* file_size) {
    if (boost::filesystem::is_directory(path)) {
        *mode = I_DIR;
        return 0;
    }
    if (boost::filesystem::is_symlink(path)) {
        *mode = I_LINK;
        return 0;
    }
    if (boost::filesystem::is_regular_file(path)) {
        *mode = I_FILE;
        *file_size = boost::filesystem::file_size(path);
        return 0;
    }
    DB_FATAL("link file not exist ");
    return -1;
}

int PosixFileSystemAdaptor::read_dir(const std::string& path, std::vector<std::string>& direntrys) {
    dir_iter iter(path);
    dir_iter end;
    for (; iter != end; ++iter) {
        std::string child_path = iter->path().c_str();
        std::vector<std::string> split_vec;
        boost::split(split_vec, child_path, boost::is_any_of("/"));
        std::string out_path = split_vec.back();
        direntrys.emplace_back(out_path);
    }

    return 1;
}

#ifdef BAIDU_INTERNAL
int AfsFileSystemAdaptor::init() {
    // 创建一个AfsFileSystem实例
    _afs = new afs::AfsFileSystem(_afs_uri.c_str(), _afs_user.c_str(), _afs_password.c_str(), _afs_conf_file.c_str());
    if (_afs == NULL) {
        DB_FATAL("fail to new AfsFileSystem, please check your memory");
        return -1;
    }

    // 注意Init()函数的第二个参数，它是is_load_comlog的意思，如果你的程序自己会初始化comlog，这里填false，
    // 如果你不想，而是让afs-api自动顺便初始化comlog，则填true。
    // 这里还需要注意的是，comlog不接受多次初始化，从第二次开始的初始化会报错，
    // 因此，如果程序莫名其妙Init()失败，先检查一下是不是这里传了true导致comlog多次初始化了。
    int afs_res = _afs->Init(true, false);
    if (afs_res != ds::kOk) {
        DB_FATAL("fail to init afs, errno:%d, errmsg:%s", afs_res, afs::Rc2Str(afs_res));
        return afs_res;
    }
    
    // 连接集群后端                                              
    afs_res = _afs->Connect();
    if (afs_res != ds::kOk) {
        DB_FATAL("fail to connect nfs, errno:%d, errmsg:%s", afs_res, ds::Rc2Str(afs_res));
        return -1;
    }

    return 0;
}

ImporterReaderAdaptor* AfsFileSystemAdaptor::open_reader(const std::string& path) {
    int afs_res = _afs->Exist(path.c_str());
    if (afs_res < 0) {
        DB_FATAL("file: %s, not exist, errno:%d, errmsg:%s", path.c_str(), afs_res, ds::Rc2Str(afs_res));
        return nullptr;
    }

    afs::ReaderOptions r_options;                                                                 
    r_options.buffer_size = FLAGS_file_buffer_size * 1024 * 1024;    

    afs::Reader* reader = _afs->OpenReader(path.c_str(), r_options);
    if (reader == nullptr) {
        DB_FATAL("fail to open reader for file %s, errno:%d, errmsg:%s", path.c_str(), afs::GetRc(), afs::Rc2Str(afs::GetRc()));
        return nullptr;
    }

    auto reader_adaptor = new(std::nothrow) AfsReaderAdaptor(reader);
    if (reader_adaptor == nullptr) {
        DB_FATAL("new AfsReaderAdaptor failed, file: %s", path.c_str());
        return nullptr;
    }

    return reader_adaptor;
}

void AfsFileSystemAdaptor::close_reader(ImporterReaderAdaptor* adaptor) {
    afs::Reader* reader = ((AfsReaderAdaptor*)adaptor)->get_reader();
    int afs_res = _afs->CloseReader(reader);
    if (afs_res != ds::kOk) {
        DB_FATAL("fail to close reader, reader:%p, errno:%d, errmsg:%s\n", reader, afs_res, afs::Rc2Str(afs_res));
    }

    delete adaptor;
}

::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
        AfsFileSystemAdaptor::open_arrow_file(const std::string& path) {
    return AfsReadableFile::Open(path, this);
}

::arrow::Status AfsFileSystemAdaptor::close_arrow_reader(
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader)  {
    if (arrow_reader != nullptr && !arrow_reader->closed()) {
        return arrow_reader->Close();
    }
    return ::arrow::Status::OK();
}

int AfsFileSystemAdaptor::file_mode(const std::string& path, FileMode* mode, size_t* file_size) {
    int afs_res = _afs->Exist(path.c_str());
    if (afs_res < 0) {
        DB_WARNING("file: %s, not exist, errno:%d, errmsg:%s", path.c_str(), afs_res, ds::Rc2Str(afs_res));
        return -1;
    }

    struct stat stat_buf;
    afs_res = _afs->Stat(path.c_str(), &stat_buf);
    if (afs_res < 0) {
        DB_FATAL("file: %s, fail to get stat, errno:%d, errmsg:%s\n", path.c_str(), afs_res, afs::Rc2Str(afs_res));
        return -1;
    }

    if (stat_buf.st_mode & S_IFREG) {
        *mode = I_FILE;
        *file_size = stat_buf.st_size;
        DB_WARNING("file:%s, size:%lu", path.c_str(), stat_buf.st_size);
    } else if (stat_buf.st_mode & S_IFLNK) {
        *mode = I_LINK;
    } else if (stat_buf.st_mode & S_IFDIR) {
        *mode = I_DIR;
    } else {
        DB_FATAL("path:%s is not file dir link", path.c_str());
        return -1;
    }

    return 0;
}

int  AfsFileSystemAdaptor::read_dir(const std::string& path, std::vector<std::string>& direntrys) {
    std::vector<afs::DirEntry> afs_entrys;
    int afs_res = _afs->Readdir(path.c_str(), &afs_entrys);
    if (afs_res < 0){
        DB_FATAL("fail to readdir %s, errno:%d, errmsg:%s\n", path.c_str(), afs_res, afs::Rc2Str(afs_res));
        return -1;
    }
 
    // stat files 
    for (size_t i = 0; i < afs_entrys.size(); i++) {
        DB_WARNING("path: %s, child_path: %s", path.c_str(), afs_entrys[i].name.c_str());
        direntrys.emplace_back(afs_entrys[i].name);
    }
    return 1;
}

void AfsFileSystemAdaptor::destroy() {
    int afs_res = _afs->DisConnect();
    if (afs_res < 0) {
        DB_FATAL("disconnect failed, errno:%d, errmsg:%s", afs_res, ds::Rc2Str(afs_res));
    } 
}

// AfsReadableFileImpl
class AfsReadableFile::AfsReadableFileImpl {
public:
    explicit AfsReadableFileImpl(ImporterFileSystemAdaptor* fs, ::arrow::MemoryPool* pool) : _fs(fs), _pool(pool) {}
    ~AfsReadableFileImpl() {}

    ::arrow::Status Open(const std::string& path) {
        if (BAIKALDM_UNLIKELY(_fs == nullptr)) {
            return ::arrow::Status::IOError("ImporterFileSystemAdaptor is nullptr");
        }
        _path = path;
        _reader = _fs->open_reader(_path);
        if (BAIKALDM_UNLIKELY(_reader == nullptr)) {
            return ::arrow::Status::IOError("AfsReaderAdaptor is nullptr");
        }
        _is_opened = true;
        return ::arrow::Status::OK();
    }

    ::arrow::Status Close() {
        if (!_is_opened) {
            return ::arrow::Status::OK();
        }
        _is_opened = false;
        _fs->close_reader(_reader);
        return ::arrow::Status::OK();
    }

    bool closed() const {
        return !_is_opened;
    }

    ::arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) {
        int64_t total_bytes = 0;
        while (total_bytes < nbytes) {
            int64_t ret = _reader->read(buffer + total_bytes, nbytes - total_bytes);
            if (ret == 0) {
                break;
            }
            if (ret < 0) {
                return ::arrow::Status::IOError("ImporterReaderAdaptor fail to read");
            }
            total_bytes += ret;
        }
        return total_bytes;
    }

    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> Read(int64_t nbytes) {
        std::unique_ptr<::arrow::ResizableBuffer> buffer;
        BAIKALDM_ARROW_ASSIGN_OR_RAISE(buffer, ::arrow::AllocateResizableBuffer(nbytes, _pool));
        if (BAIKALDM_UNLIKELY(buffer == nullptr)) {
            return ::arrow::Status::IOError("Arrow buffer is empty");
        }

        int64_t bytes_read;
        BAIKALDM_ARROW_ASSIGN_OR_RAISE(bytes_read, Read(nbytes, buffer->mutable_data()));
        if (bytes_read < nbytes) {
            BAIKALDM_ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read));
        }
        return std::move(buffer);
    }

    // TODO - PRead
    ::arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, uint8_t* buffer) {
        std::lock_guard<std::mutex> lck(_mtx);
        BAIKALDM_ARROW_RETURN_NOT_OK(Seek(position));
        return Read(nbytes, buffer);
    }

    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) {
        std::unique_ptr<::arrow::ResizableBuffer> buffer;
        BAIKALDM_ARROW_ASSIGN_OR_RAISE(buffer, ::arrow::AllocateResizableBuffer(nbytes, _pool));
        if (BAIKALDM_UNLIKELY(buffer == nullptr)) {
            return ::arrow::Status::IOError("Arrow buffer is empty");
        }

        int64_t bytes_read;
        BAIKALDM_ARROW_ASSIGN_OR_RAISE(bytes_read, ReadAt(position, nbytes, buffer->mutable_data()));
        if (bytes_read < nbytes) {
            BAIKALDM_ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read));
            buffer->ZeroPadding();
        }
        return std::move(buffer);
    }

    ::arrow::Result<int64_t> GetSize() {
        size_t file_size;
        FileMode mode;
        auto afs_res = _fs->file_mode(_path, &mode, &file_size);
        if (afs_res < 0) {
            return ::arrow::Status::IOError("AfsFileSystemAdaptor fail to get file_mode");
        }
        if (mode != I_FILE) {
            return ::arrow::Status::IOError("Path: " + _path + " is not file");
        }
        return file_size;
    }

    ::arrow::Status Seek(int64_t position) {
        int64_t ret = _reader->seek(position);
        if (ret < 0) {
            return ::arrow::Status::IOError(
                        "AfsReaderAdaptor fail to seek, path: " + _path + ", position: " + std::to_string(position));
        }
        return ::arrow::Status::OK();
    }

    ::arrow::Result<int64_t> Tell() const {
        int64_t ret = _reader->tell();
        if (ret < 0) {
            return ::arrow::Status::IOError("AfsReaderAdaptor fail to tell, path: " + _path);
        }
        return ret;
    }

private:
    std::mutex                 _mtx;
    bool                       _is_opened { false };
    ::arrow::MemoryPool*       _pool;
    std::string                _path;
    ImporterReaderAdaptor*     _reader;
    ImporterFileSystemAdaptor* _fs;
};

// AfsReadableFile
::arrow::Result<std::shared_ptr<AfsReadableFile>> AfsReadableFile::Open(
            const std::string& path, ImporterFileSystemAdaptor* fs, ::arrow::MemoryPool* pool) {
    auto file = std::shared_ptr<AfsReadableFile>(new (std::nothrow) AfsReadableFile(fs, pool));
    if (BAIKALDM_UNLIKELY(file == nullptr || file->_impl == nullptr)) {
        return ::arrow::Status::OutOfMemory("Fail to new AfsReadableFile");
    }
    BAIKALDM_ARROW_RETURN_NOT_OK(file->_impl->Open(path));
    return file;
}

AfsReadableFile::AfsReadableFile(ImporterFileSystemAdaptor* fs, ::arrow::MemoryPool* pool) { 
    _impl.reset(new AfsReadableFileImpl(fs, pool)); 
} 

AfsReadableFile::~AfsReadableFile() { 
    _impl->Close(); 
}

::arrow::Status AfsReadableFile::Close() {
    return _impl->Close();
}

bool AfsReadableFile::closed() const {
    return _impl->closed();
}

::arrow::Result<int64_t> AfsReadableFile::Read(int64_t nbytes, void* buffer) {
    return _impl->Read(nbytes, buffer);
}

::arrow::Result<std::shared_ptr<::arrow::Buffer>> AfsReadableFile::Read(int64_t nbytes) {
    return _impl->Read(nbytes);
}

::arrow::Result<int64_t> AfsReadableFile::ReadAt(int64_t position, int64_t nbytes, void* buffer) {
    return _impl->ReadAt(position, nbytes, reinterpret_cast<uint8_t*>(buffer));
}

::arrow::Result<std::shared_ptr<::arrow::Buffer>> AfsReadableFile::ReadAt(int64_t position, int64_t nbytes) {
    return _impl->ReadAt(position, nbytes);
}

::arrow::Status AfsReadableFile::Seek(int64_t position) {
    return _impl->Seek(position);
}

::arrow::Result<int64_t> AfsReadableFile::Tell() const {
    return _impl->Tell();
}

::arrow::Result<int64_t> AfsReadableFile::GetSize() {
    return _impl->GetSize();
}

#endif

// return -1 : fail 
// return  0 : success; entry is valid
// return  1 : finish;  entry is not valid
int ReadDirImpl::next_entry(std::string& entry){
    if (_idx >= _entrys.size() && _read_finish) {
        return 1; 
    }

    if (_idx < _entrys.size()) {
        entry = _entrys[_idx++];
        DB_WARNING("readdir: %s, get next entry: %s", _path.c_str(), entry.c_str());
        return 0;
    }

    _idx = 0;
    _entrys.clear();
    int ret = _fs->read_dir(_path, _entrys);
    if (ret < 0) {
        DB_WARNING("readdir: %s failed", _path.c_str());
        return -1;
    } else if (ret == 1) {
        _read_finish = true;
    }

    if (_entrys.empty()) {
        return 1;
    }

    entry = _entrys[_idx++];
    DB_WARNING("readdir: %s, get next entry: %s", _path.c_str(), entry.c_str());
    return 0;
}

} //baikaldb
