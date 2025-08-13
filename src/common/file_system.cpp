#include "file_system.h"

namespace baikaldb {

DEFINE_int32(file_buffer_size, 1, "read file buf size (MBytes)");

#ifdef BAIDU_INTERNAL
// AfsFileWriter
int64_t AfsFileWriter::append(const char* buf, uint32_t count) {
    if (_writer == nullptr) {
        DB_WARNING("_writer is nullptr");
        return -1;
    }
    int64_t ret = _writer->Append(buf, count, nullptr, nullptr);
    if (ret < 0) {
        DB_WARNING("append failed, error: %s", ds::Rc2Str(ret));
        return -1;
    } else if (ret != count) {
        DB_WARNING("append failed, ret: %ld, count: %u", ret, count);
        return -1;
    } else {
        return ret;
    }
}

int64_t AfsFileWriter::tell() {
    if (_writer == nullptr) {
        DB_WARNING("_writer is nullptr");
        return -1;
    }
    return _writer->Tell();
}

bool AfsFileWriter::sync() {
    if (_writer == nullptr) {
        DB_WARNING("_writer is nullptr");
        return false;
    }
    int ret = _writer->Sync(nullptr, nullptr);
    if (ret != ds::kOk) {
        DB_WARNING("sync failed error: %s", ds::Rc2Str(ret));
        return false;
    }
    return true;
}
#endif

// PosixFileReader
int64_t PosixFileReader::read(size_t pos, char* buf, size_t buf_size) {
    if (_error) {
        DB_WARNING("file: %s read failed", _path.c_str());
        return -1;
    }
    int64_t size = _f.Read(pos, buf, buf_size);
    if (size < 0) {
        DB_WARNING("file: %s read failed", _path.c_str());
    }
    DB_DEBUG("read file :%s, pos: %ld, read_size: %ld", _path.c_str(), pos, size);
    return size;
}

#ifdef BAIDU_INTERNAL
// AfsFileReader
int64_t AfsFileReader::seek(int64_t position) {
    if (_reader == nullptr) {
        DB_WARNING("_reader is nullptr");
        return -1;
    }
    int64_t afs_res = _reader->Seek(position);
    if (afs_res == ds::kOutOfRange) {
        DB_WARNING("AfsFileReader out of range pos: %lu", position);
        return 0; // ds::kOk
    } else if (afs_res != ds::kOk) {
        DB_WARNING("AfsFileReader seek pos: %lu failed, errno:%ld, errmsg:%s", position, afs_res, ds::Rc2Str(afs_res));
    }
    return afs_res;
}

int64_t AfsFileReader::tell() {
    if (_reader == nullptr) {
        DB_WARNING("_reader is nullptr");
        return -1;
    }
    int64_t afs_res = _reader->Tell();
    if (afs_res < 0) {
        DB_WARNING("AfsFileReader fail to tell, ret_code: %ld", afs_res);
    }
    return afs_res;
}

int64_t AfsFileReader::read(void* buf, size_t buf_size) {
    if (_reader == nullptr) {
        DB_WARNING("_reader is nullptr");
        return -1;
    }
    int64_t afs_res = _reader->Read(buf, buf_size);
    if (afs_res < 0) {
        DB_WARNING("AfsFileReader read errno:%ld, errmsg:%s", afs_res, ds::Rc2Str(afs_res));
    }
    DB_DEBUG("AfsFileReader read return size: %ld", afs_res);
    return afs_res;
}

int64_t AfsFileReader::read(size_t pos, char* buf, size_t buf_size) {
    if (_reader == nullptr) {
        DB_WARNING("_reader is nullptr");
        return -1;
    }
    int64_t afs_res = _reader->Seek(pos);
    if (afs_res == ds::kOutOfRange) {
        DB_WARNING("out of range pos: %lu", pos);
        return 0;
    } else if (afs_res != ds::kOk) {
        DB_WARNING("seek pos: %lu failed, errno:%ld, errmsg:%s", pos, afs_res, ds::Rc2Str(afs_res));
        return afs_res;
    }
    afs_res = _reader->Read((void*)buf, buf_size);
    if (afs_res < 0) {
        DB_WARNING("read pos: %lu failed, errno:%ld, errmsg:%s", pos, afs_res, ds::Rc2Str(afs_res));
    }
    return afs_res;
}
#endif

// PosixFileSystem
std::shared_ptr<FileReader> PosixFileSystem::open_reader(const std::string& path) {
    std::shared_ptr<FileReader> file_reader(new PosixFileReader(path));
    return file_reader;
}

int PosixFileSystem::close_reader(std::shared_ptr<FileReader> file_reader) {
    return 0;
}

::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> 
        PosixFileSystem::open_arrow_file(const std::string& path) {
    return ::arrow::io::ReadableFile::Open(path);
}

::arrow::Status PosixFileSystem::close_arrow_reader(
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader)  {
    if (arrow_reader != nullptr && !arrow_reader->closed()) {
        return arrow_reader->Close();
    }
    return ::arrow::Status::OK();
}

int PosixFileSystem::read_dir(const std::string& path, std::vector<std::string>& direntrys) {
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

int PosixFileSystem::get_file_info(const std::string& path, FileInfo& file_info, std::string* err_msg) {
    if (boost::filesystem::is_directory(path)) {
        file_info.mode = FileMode::I_DIR;
    } else if (boost::filesystem::is_symlink(path)) {
        file_info.mode = FileMode::I_LINK;
    } else if (boost::filesystem::is_regular_file(path)) {
        file_info.mode = FileMode::I_FILE;
        file_info.size = boost::filesystem::file_size(path);
    } else {
        DB_WARNING("path:%s is not dir/file/link", path.c_str());
        return -1;
    }
    return 0;
}

#ifdef BAIDU_INTERNAL
// AfsFileSystem
int AfsFileSystem::init() {
    // 创建一个AfsFileSystem实例
    _afs.reset(new (std::nothrow) afs::AFSImpl(
                        _afs_uri.c_str(), _afs_user.c_str(), _afs_password.c_str(), _afs_conf_file.c_str()));
    if (_afs == nullptr) {
        DB_WARNING("fail to new AfsFileSystem, please check your memory");
        return -1;
    }
    // 注意Init()函数的第二个参数，它是is_load_comlog的意思，如果你的程序自己会初始化comlog，这里填false，
    // 如果你不想，而是让afs-api自动顺便初始化comlog，则填true。
    // 这里还需要注意的是，comlog不接受多次初始化，从第二次开始的初始化会报错，
    // 因此，如果程序莫名其妙Init()失败，先检查一下是不是这里传了true导致comlog多次初始化了。
    int afs_res = _afs->Init(true, false);
    if (afs_res != ds::kOk) {
        DB_WARNING("fail to init afs, errno:%d, errmsg:%s", afs_res, afs::Rc2Str(afs_res));
        return afs_res;
    }
    // 连接集群后端
    bool ok = _afs->Start();
    if (!ok) {
        DB_WARNING("fail to start afs, errno:%d, errmsg:%s", ds::kFail, afs::Rc2Str(ds::kFail));
        return -1;
    }
    afs_res = _afs->Connect();
    if (afs_res != ds::kOk) {
        DB_WARNING("fail to connect nfs, errno:%d, errmsg:%s", afs_res, ds::Rc2Str(afs_res));
        return -1;
    }
    return 0;
}

std::shared_ptr<FileReader> AfsFileSystem::open_reader(const std::string& path) {
    int afs_res = _afs->Exist(path.c_str());
    if (afs_res < 0) {
        DB_WARNING("file: %s, not exist, errno:%d, errmsg:%s", path.c_str(), afs_res, ds::Rc2Str(afs_res));
        return nullptr;
    }
    afs::ReaderOptions r_options;                                                                 
    r_options.buffer_size = FLAGS_file_buffer_size * 1024 * 1024ULL;
    afs::Reader* reader = _afs->OpenReader(path.c_str(), r_options, false);
    if (reader == nullptr) {
        DB_WARNING("fail to open reader for file %s, errno:%d, errmsg:%s", path.c_str(), afs::GetRc(), afs::Rc2Str(afs::GetRc()));
        return nullptr;
    }
    std::shared_ptr<FileReader> file_reader(new(std::nothrow) AfsFileReader(reader));
    if (file_reader == nullptr) {
        DB_WARNING("file_reader is nullptr, file: %s", path.c_str());
        return nullptr;
    }
    return file_reader;
}

int AfsFileSystem::close_reader(std::shared_ptr<FileReader> file_reader) {
    if (file_reader == nullptr) {
        DB_WARNING("file_reader is nullptr");
        return -1;
    }
    afs::Reader* reader = static_cast<AfsFileReader*>(file_reader.get())->get_reader();
    int afs_res = _afs->CloseReader(reader);
    if (afs_res != ds::kOk) {
        DB_WARNING("fail to close reader, reader:%p, errno:%d, errmsg:%s\n", reader, afs_res, afs::Rc2Str(afs_res));
        return -1;
    }
    return 0;
}

::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> AfsFileSystem::open_arrow_file(const std::string& path) {
    return AfsReadableFile::Open(path, this);
}

::arrow::Status AfsFileSystem::close_arrow_reader(std::shared_ptr<::arrow::io::RandomAccessFile> arrow_reader)  {
    if (arrow_reader != nullptr && !arrow_reader->closed()) {
        return arrow_reader->Close();
    }
    return ::arrow::Status::OK();
}

std::shared_ptr<FileWriter> AfsFileSystem::open_writer(const std::string& path, bool is_create) {
    if (is_create) {
        int ret = _afs->Exist(path.c_str());
        if (ret == ds::kOk) {
            ret = _afs->Delete(path.c_str(), false);
            if (ret != ds::kOk) {
                DB_WARNING("delete failed, path: %s, error: %s", path.c_str(), ds::Rc2Str(ret));
                return nullptr;
            }
        } else if (ret != ds::kNoEntry) {
            DB_WARNING("exist failed, path: %s, error: %s", path.c_str(), ds::Rc2Str(ret));
            return nullptr;
        }
        ret = _afs->Create(path.c_str(), afs::CreateOptions());
        if (ret != ds::kOk) {
            DB_WARNING("create failed, path: %s, error: %s", path.c_str(), ds::Rc2Str(ret));
            return nullptr;
        }
    }
    afs::Writer* writer = _afs->OpenWriter(path.c_str(), afs::WriterOptions(), false);
    if (writer == nullptr) {
        DB_WARNING("open writer failed, path: %s, error: %d", path.c_str(), errno);
        return nullptr;
    }
    std::shared_ptr<FileWriter> file_writer(new(std::nothrow) AfsFileWriter(writer));
    if (file_writer == nullptr) {
        DB_WARNING("file_writer is nullptr");
        return nullptr;
    }
    return file_writer;
}

int AfsFileSystem::close_writer(std::shared_ptr<FileWriter> file_writer) {
    if (file_writer == nullptr) {
        DB_WARNING("file_writer is nullptr");
        return -1;
    }
    afs::Writer* writer = static_cast<AfsFileWriter*>(file_writer.get())->get_writer();
    int afs_res = _afs->CloseWriter(writer, NULL, NULL);
    if (afs_res != ds::kOk) {
        DB_FATAL("fail to close reader, reader:%p, errno:%d, errmsg:%s", writer, afs_res, afs::Rc2Str(afs_res));
        return -1;
    }
    return 0;
}

int AfsFileSystem::read_dir(const std::string& path, std::vector<std::string>& direntrys) {
    std::vector<afs::DirEntry> afs_entrys;
    int afs_res = _afs->Readdir(path.c_str(), &afs_entrys);
    if (afs_res < 0){
        DB_WARNING("fail to readdir %s, errno:%d, errmsg:%s\n", path.c_str(), afs_res, afs::Rc2Str(afs_res));
        return -1;
    }
    for (size_t i = 0; i < afs_entrys.size(); i++) {
        direntrys.emplace_back(afs_entrys[i].name);
    }
    return 1;
}

int AfsFileSystem::get_file_info(const std::string& path, FileInfo& file_info, std::string* err_msg) {
    int afs_res = _afs->Exist(path.c_str());
    if (afs_res < 0) {
        DB_WARNING("file: %s, not exist, errno:%d, errmsg:%s", path.c_str(), afs_res, ds::Rc2Str(afs_res));
        if (err_msg != nullptr) {
            *err_msg = ds::Rc2Str(afs_res);
        }
        return -1;
    }
    struct stat stat_buf;
    afs_res = _afs->Stat(path.c_str(), &stat_buf);
    if (afs_res < 0) {
        DB_WARNING("file: %s, fail to get stat, errno:%d, errmsg:%s\n", path.c_str(), afs_res, afs::Rc2Str(afs_res));
        if (err_msg != nullptr) {
            *err_msg = ds::Rc2Str(afs_res);
        }
        return -1;
    }
    if (stat_buf.st_mode & S_IFREG) {
        file_info.mode = FileMode::I_FILE;
        file_info.size = stat_buf.st_size;
    } else if (stat_buf.st_mode & S_IFLNK) {
        file_info.mode = FileMode::I_LINK;
    } else if (stat_buf.st_mode & S_IFDIR) {
        file_info.mode = FileMode::I_DIR;
    } else {
        DB_WARNING("path:%s is not file/link/dir", path.c_str());
        return -1;
    }
    return 0;
}

int AfsFileSystem::destroy() {
    int afs_res = _afs->DisConnect();
    if (afs_res < 0) {
        DB_WARNING("disconnect failed, errno:%d, errmsg:%s", afs_res, ds::Rc2Str(afs_res));
        return -1;
    }
    return 0;
}

int AfsFileSystem::delete_path(const std::string& path, bool recursive) {
    int ret = _afs->Delete(path.c_str(), recursive);
    if (ret != ds::kOk) {
        DB_WARNING("delete failed, path: %s, error: %s", path.c_str(), ds::Rc2Str(ret));
        return -1;
    }
    return 0;
}

// AfsReadableFileImpl
class AfsReadableFile::AfsReadableFileImpl {
public:
    explicit AfsReadableFileImpl(FileSystem* fs, ::arrow::MemoryPool* pool) : _fs(fs), _pool(pool) {}
    ~AfsReadableFileImpl() {}

    ::arrow::Status Open(const std::string& path) {
        if (BAIKALDB_UNLIKELY(_fs == nullptr)) {
            return ::arrow::Status::IOError("_fs is nullptr");
        }
        _path = path;
        _reader = _fs->open_reader(_path);
        if (BAIKALDB_UNLIKELY(_reader == nullptr)) {
            return ::arrow::Status::IOError("_reader is nullptr");
        }
        _opened = true;
        return ::arrow::Status::OK();
    }

    ::arrow::Status Close() {
        if (!_opened) {
            return ::arrow::Status::OK();
        }
        _opened = false;
        int ret = _fs->close_reader(_reader);
        if (ret != 0) {
            return ::arrow::Status::IOError("fail to close reader");
        }
        return ::arrow::Status::OK();
    }

    bool closed() {
        return !_opened;
    }

    ::arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) {
        int64_t total_bytes = 0;
        while (total_bytes < nbytes) {
            int64_t ret = _reader->read(buffer + total_bytes, nbytes - total_bytes);
            if (ret == 0) {
                break;
            }
            if (ret < 0) {
                return ::arrow::Status::IOError("fail to read");
            }
            total_bytes += ret;
        }
        return total_bytes;
    }

    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> Read(int64_t nbytes) {
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

    // TODO - PRead
    ::arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, uint8_t* buffer) {
        std::lock_guard<std::mutex> lck(_mtx);
        ARROW_RETURN_NOT_OK(Seek(position));
        return Read(nbytes, buffer);
    }

    ::arrow::Result<std::shared_ptr<::arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) {
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

    ::arrow::Result<int64_t> GetSize() {
        FileInfo file_info;
        auto afs_res = _fs->get_file_info(_path, file_info, nullptr);
        if (afs_res < 0) {
            return ::arrow::Status::IOError("fail to get file_mode");
        }
        if (file_info.mode != FileMode::I_FILE) {
            return ::arrow::Status::IOError("Path: " + _path + " is not file");
        }
        return file_info.size;
    }

    ::arrow::Status Seek(int64_t position) {
        int64_t ret = _reader->seek(position);
        if (ret < 0) {
            return ::arrow::Status::IOError("fail to seek, path: " + _path + ", position: " + std::to_string(position));
        }
        return ::arrow::Status::OK();
    }

    ::arrow::Result<int64_t> Tell() const {
        int64_t ret = _reader->tell();
        if (ret < 0) {
            return ::arrow::Status::IOError("fail to tell, path: " + _path);
        }
        return ret;
    }

private:
    std::mutex _mtx;
    bool _opened = false;
    ::arrow::MemoryPool* _pool = nullptr;
    std::string _path;
    FileSystem* _fs = nullptr;
    std::shared_ptr<FileReader> _reader;
};

// AfsReadableFile
::arrow::Result<std::shared_ptr<AfsReadableFile>> AfsReadableFile::Open(
            const std::string& path, FileSystem* fs, ::arrow::MemoryPool* pool) {
    auto file = std::shared_ptr<AfsReadableFile>(new (std::nothrow) AfsReadableFile(fs, pool));
    if (BAIKALDB_UNLIKELY(file == nullptr || file->_impl == nullptr)) {
        return ::arrow::Status::OutOfMemory("Fail to new AfsReadableFile");
    }
    ARROW_RETURN_NOT_OK(file->_impl->Open(path));
    return file;
}

AfsReadableFile::AfsReadableFile(FileSystem* fs, ::arrow::MemoryPool* pool) { 
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

// ReadDirImpl
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

std::shared_ptr<FileSystem> create_filesystem(const std::string& cluster_name, 
                                              const std::string& user_name, 
                                              const std::string& password, 
                                              const std::string& conf_file) {
    std::shared_ptr<FileSystem> fs;
    if (cluster_name.find("afs") != std::string::npos) {
#ifdef BAIDU_INTERNAL
        fs.reset(new (std::nothrow) AfsFileSystem(cluster_name, user_name, password, conf_file));
#endif
    } else {
        fs.reset(new (std::nothrow) PosixFileSystem());
    }
    if (fs == nullptr) {
        DB_FATAL("fs is nullptr");
        return nullptr;
    }
    if (fs->init() != 0) {
        DB_FATAL("filesystem init failed, cluster_name: %s", cluster_name.c_str());
        return nullptr;
    }
    return fs;
}

int destroy_filesystem(std::shared_ptr<FileSystem> fs) {
    if (fs != nullptr) {
        int ret = fs->destroy();
        if (ret != 0) {
            DB_WARNING("Fail to destroy filesystem");
            return -1;
        }
    }
    return 0;
}

} // namespace baikaldb
