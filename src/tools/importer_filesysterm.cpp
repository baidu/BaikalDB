
#include "importer_filesysterm.h"

namespace baikaldb {
DEFINE_int32(file_buffer_size, 1, "read file buf size (MBytes)");
DEFINE_int32(file_block_size, 100, "split file to block to handle(MBytes)");
DEFINE_int32(concurrency, 5, "concurrency");
DEFINE_int32(file_concurrency, 30, "file_concurrency");
DEFINE_uint64(insert_values_count, 100, "insert_values_count");

size_t PosixReaderAdaptor::read(size_t pos, char* buf, size_t buf_size) {
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

size_t AfsReaderAdaptor::read(size_t pos, char* buf, size_t buf_size) {
    int afs_res = _reader->Seek(pos);
    if (afs_res == ds::kOutOfRange) {
        DB_WARNING("out of range pos: %lu", pos);
        return 0;
    } else if (afs_res != ds::kOk) {
        DB_FATAL("seek pos: %lu failed, errno:%d, errmsg:%s", pos, afs_res, ds::Rc2Str(afs_res));
        return -1;
    }

    afs_res = _reader->Read((void*)buf, buf_size);
    if (afs_res >= 0) {
        DB_WARNING("read pos: %lu, return size: %d", pos, afs_res);
        return afs_res;
    } else {
        DB_FATAL("read pos: %lu failed, errno:%d, errmsg:%s", pos, afs_res, ds::Rc2Str(afs_res));
        return -1;
    }
}

int ImporterFileSystermAdaptor::cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
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
int ImporterFileSystermAdaptor::cut_files(const std::string& path, const int64_t block_size, std::vector<std::string>& file_paths,
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
            if (cur_end_pos < file_size) {
                return cut_files(path, block_size, file_paths, file_start_pos, file_end_pos, start_pos,
                                 cur_size, cur_start_pos, cur_end_pos, cur_file_paths);
            }
        }
    }
    return 0;
}

int ImporterFileSystermAdaptor::all_block_count(std::string path) {
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

            count += all_block_count(path + "/" + child_path);
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
        return all_block_count(real_path);
    }

    if (I_FILE != mode) {
        return 0;
    }

    size_t file_block_size = FLAGS_file_block_size * 1024 * 1024ULL;
    _all_file_size += file_size;
    if (file_block_size == 0) {
        return 0;
    }
    size_t blocks = file_size / file_block_size + 1;
    DB_TRACE("path:%s is file, size:%lu, blocks:%lu", 
            path.c_str(), file_size, blocks);
    return blocks;
}

ImporterReaderAdaptor* PosixFileSystermAdaptor::open_reader(const std::string& path) {

    DB_WARNING("open reader: %s", path.c_str());

    return new PosixReaderAdaptor(path);
}

void PosixFileSystermAdaptor::close_reader(ImporterReaderAdaptor* adaptor) {
    if (adaptor) {
        delete adaptor;
    }
}

int  PosixFileSystermAdaptor::read_dir(const std::string& path, std::vector<std::string>& direntrys) {
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

int PosixFileSystermAdaptor::file_mode(const std::string& path, FileMode* mode, size_t* file_size) {
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

#ifdef BAIDU_INTERNAL
int AfsFileSystermAdaptor::init() {
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

ImporterReaderAdaptor* AfsFileSystermAdaptor::open_reader(const std::string& path) {
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

void AfsFileSystermAdaptor::close_reader(ImporterReaderAdaptor* adaptor) {
    afs::Reader* reader = ((AfsReaderAdaptor*)adaptor)->get_reader();
    int afs_res = _afs->CloseReader(reader);
    if (afs_res != ds::kOk) {
        DB_FATAL("fail to close reader, reader:%p, errno:%d, errmsg:%s\n", reader, afs_res, afs::Rc2Str(afs_res));
    }

    delete adaptor;
}

int AfsFileSystermAdaptor::file_mode(const std::string& path, FileMode* mode, size_t* file_size) {
    int afs_res = _afs->Exist(path.c_str());
    if (afs_res < 0) {
        DB_FATAL("file: %s, not exist, errno:%d, errmsg:%s", path.c_str(), afs_res, ds::Rc2Str(afs_res));
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

int  AfsFileSystermAdaptor::read_dir(const std::string& path, std::vector<std::string>& direntrys) {
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

void AfsFileSystermAdaptor::destroy() {
    int afs_res = _afs->DisConnect();
    if (afs_res < 0) {
        DB_FATAL("disconnect failed, errno:%d, errmsg:%s", afs_res, ds::Rc2Str(afs_res));
    } 
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


int BlockImpl::init() {
    _file = _fs->open_reader(_path);
    if (_file == nullptr) {
        DB_FATAL("open reader failed, file: %s", _path.c_str());
        return -1;
    }
    _read_buffer_size = FLAGS_file_buffer_size * 1024 * 1024ULL;
    _buf = (char*)malloc(_read_buffer_size);
    if (_buf == nullptr) {
        return -1;
    }

    return 0;
}

int BlockImpl::buf_resize(int64_t size) {
    if (_buf != nullptr) {
        free(_buf);
        _buf = nullptr;
    }

    _read_buffer_size = size;
    _buf = (char*)malloc(_read_buffer_size);
    if (_buf == nullptr) {
        return -1;
    }

    return 0;
}

bool BlockImpl::block_finish() {
    // 只有_cur_pos > _end_pos才能判断完成，相等说明_end_pos正好在行首，需要多读一行，因为下一个block跳过了首行
    if (_cur_pos > _end_pos) {
        return true;
    }

    if (_cur_pos == _end_pos && _end_pos == _file_size) {
        return true;
    }

    if (_start_pos == _end_pos) {
        return true;
    }

    return false;
}

// 循环调用直到读取结束
int BlockImpl::read_and_exec() {
    if (block_finish()) {
        return 1;
    }
    const int64_t file_pos = _cur_pos;
    int64_t buf_pos = 0;
    int64_t buf_size = _file->read(file_pos, _buf, _read_buffer_size);
    if (buf_size < 0) {
        DB_FATAL("read filed, file: %s, pos: %lu", _path.c_str(), file_pos);
        return -1;
    } else if (buf_size == 0) {
        return 1;
    }

    MemBuf sbuf(_buf, _buf + buf_size);
    std::istream f(&sbuf);

    // 跳过首行
    if (_escape_first_line) {
        std::string line;
        std::getline(f, line);
        // 首行没读完整说明line size > _buf size 暂时不支持
        if (f.eof()) {
            DB_FATAL("path: %s, line_size: %lu > buf_size: %ld", _path.c_str(), line.size(), buf_size);
            // 增大buffer重试
            if (buf_resize(_read_buffer_size * 2) < 0) {
                return -1;
            }
            return 0;
        }

        buf_pos += line.size() + 1;
        _escape_first_line = false;
    }

    _cur_pos = file_pos + buf_pos;
    if (block_finish()) {
        return 1;
    }

    bool  has_get_line = false;
    while (!f.eof()) {
        std::string line;
        std::getline(f, line);

        // eof直接退出不更新 _cur_pos, 下次从_cur_pos继续读
        if (f.eof()) {
            buf_pos += line.size();

            // 最后一块特殊处理，不需要跳过
            if (file_pos + buf_pos == _end_pos && _end_pos == _file_size) {
                if (!line.empty()) {
                    _lines_manager.handle_line(line);
                }
                DB_FATAL("EOF");
                return 1;
            }

            if (_cur_pos <= _end_pos && !has_get_line) {
                // 增大buffer重试
                if (buf_resize(_read_buffer_size * 2) < 0) {
                    return -1;
                }

                return 0;
            }

            DB_WARNING("path: %s, eof, pos: %ld", _path.c_str(), buf_pos + file_pos);
            return 0;
        }
        has_get_line = true;
        buf_pos += line.size() + 1;
        _cur_pos = file_pos + buf_pos;
        _lines_manager.handle_line(line);
        //write(line);
        if (block_finish()) {
            return 1;
        }
    }
    return 0;
}

void BlockImpl::LinesManager::handle_line(std::string& line) {
    _lines.emplace_back(line);
    if (_lines.size() < _insert_values_count) {
        return;
    }
    std::vector<std::string> tmp_lines;
    tmp_lines.reserve(_insert_values_count);
    _lines.swap(tmp_lines);
    auto calc = [this, tmp_lines]() {
        _func(_path, tmp_lines);
        // 如果_func(数据列数不符合要求)执行太快，会卡在push_rq
        bthread_usleep(1000);
        _cond.decrease_signal();
    };

    _cond.increase_wait();
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run(calc);
    _lines.clear();
}

void BlockImpl::LinesManager::join() {
    if (_lines.size() <= 0) {
        return;
    }
    std::vector<std::string> tmp_lines;
    tmp_lines.reserve(_insert_values_count);
    _lines.swap(tmp_lines);
    auto calc = [this, tmp_lines]() {
        _func(_path, tmp_lines);
        bthread_usleep(1000);
        _cond.decrease_signal();
    };

    _cond.increase_wait();
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run(calc);
    _lines.clear();
}

int ImporterImpl::recurse_handle(const std::string& path, const ProgressFunc& progress_func) {
    FileMode mode;
    size_t file_size = 0;
    int ret = _fs->file_mode(path, &mode, &file_size);
    if (ret < 0) {
        DB_FATAL("get file mode failed, file: %s", path.c_str());
        _import_ret << "get file mode failed, file: " << path;
        return -1;
    }
    
    if (I_DIR == mode) {
        ReadDirImpl dir_iter(path, _fs);
        while (1) {
            std::string child_path;
            int ret = dir_iter.next_entry(child_path);
            if (ret < 0) {
                DB_FATAL("fail next_entry, path:%s child_path:%s", path.c_str(), child_path.c_str());
                return -1;
            } else if (ret == 1) {
                break;
            }

            ret = recurse_handle(path + "/" + child_path, progress_func);
            if (ret < 0) {
                DB_FATAL("fail handle, path:%s, child_path:%s", path.c_str(), child_path.c_str());
                return -1;
            }
        }

        return 0;   
    }

    if (I_LINK == mode && _fs->is_posix()) {
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
        return recurse_handle(real_path, progress_func);
    }

    if (I_FILE != mode) {
        return 0;
    }
    
    size_t file_block_size = FLAGS_file_block_size * 1024 * 1024ULL;
    if (file_block_size == 0) {
        DB_FATAL("file_block_size = 0");
        return -1;
    }

    size_t file_start_pos = 0;
    size_t file_end_pos = file_size;
    if (_start_pos != 0 && _start_pos < file_size) {
        file_start_pos = _start_pos;
    }
    if (_end_pos != 0 && _end_pos < file_size) {
        file_end_pos = _end_pos;
    }
    if (file_start_pos > file_end_pos) {
        DB_FATAL("path: %s, file_start_pos(%ld) > file_end_pos(%ld)", path.c_str(), file_start_pos, file_end_pos);
        return -1;
    }
    size_t blocks =  (file_end_pos - file_start_pos) / file_block_size + 1;
    for (size_t i = 0; i < blocks; i++) {
        size_t start_pos = i * file_block_size + file_start_pos; 
        size_t end_pos   = (i + 1) * file_block_size + file_start_pos;
        end_pos = end_pos > file_end_pos ? file_end_pos : end_pos;

        auto calc = [this, path, start_pos, end_pos, file_size]() {
            TimeCost cost;
            std::shared_ptr<BthreadCond> auto_decrease(&_file_concurrency_cond,
                            [](BthreadCond* cond) {cond->decrease_signal();});
            {
                BlockImpl block(path, start_pos, end_pos, file_size, _fs, _lines_func, _has_header, _insert_values_count);
                int ret = block.init();
                if (ret < 0) {
                    _result = -1;
                    DB_FATAL("blockImpl init failed, file: %s", path.c_str());
                    _import_ret << "blockImpl init failed, file: " << path;
                    return;
                }
                DB_WARNING("block init success, path: %s", path.c_str());
                while (1) {
                    ret = block.read_and_exec();
                    if (ret < 0) {
                        _result = -1;
                        _import_ret << "blockImpl run failed, file: " << path;
                        return;
                    } else if (ret == 1) {
                        break;
                    } 
                }
            }
            _handled_block_count++;
            DB_TRACE("handle block path:%s, start_pos:%ld, rate of progress:%ld/%ld cost:%ld",
                            path.c_str(), start_pos, _handled_block_count.load(), _all_block_count, cost.get_time());
        };
        
        _file_concurrency_cond.increase_wait();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(calc);
        // 显示进度
        std::string progress_str = "rate of progress: " + std::to_string(_handled_block_count.load()) + "/" + std::to_string(_all_block_count);
        progress_func(progress_str);
    }
    return 0;

}

} //baikaldb
