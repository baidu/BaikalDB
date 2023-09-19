// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once
#include "rocksdb/file_system.h"
#include "rocksdb/utilities/options_type.h"
#include "schema_factory.h"
#include "mut_table_key.h"
#include "table_key.h"
#include "table_record.h"
#include "external_filesystem.h"

namespace baikaldb {
#define FS_LOG(level, _fmt_, args...) \
    do {\
        DB_##level("filesystem: %s; " _fmt_, _name.c_str(), ##args); \
    } while (0);

class ExtRandomAccessFile : public rocksdb::FSRandomAccessFile {
public:
    explicit ExtRandomAccessFile(std::unique_ptr<ExtFileReader> file_reader) { 
        _file_reader = std::move(file_reader);
    }
    virtual ~ExtRandomAccessFile() { 
        _file_reader->close();
        _file_reader.reset();
    }

    virtual rocksdb::IOStatus Read(uint64_t offset, size_t n, const rocksdb::IOOptions& options,
                        rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext* dbg) const override;
    virtual rocksdb::IOStatus MultiRead(rocksdb::FSReadRequest* reqs, size_t num_reqs,
                             const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override {
        DB_WARNING("MultiRead: %ld", num_reqs);
        for (size_t i = 0; i < num_reqs; ++i) {
            rocksdb::FSReadRequest& req = reqs[i];
            req.status =
                Read(req.offset, req.len, options, &req.result, req.scratch, dbg);
        }
        return rocksdb::IOStatus::OK();
  }

    // 后续实现并发MultiRead TODO
    // virtual IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
    //                 const IOOptions& options, IODebugContext* dbg);
private:
    std::unique_ptr<ExtFileReader> _file_reader;
};

class SstExtLinker {
public:
static const std::string SST_EXT_MAP_FILE_PREFIX;
struct ExtFileInfo {
    std::string full_name;
    uint64_t size = 0;
    uint64_t modify_time = 0;
};
    static SstExtLinker* get_instance() {
        static SstExtLinker instance;
        return &instance;
    }

    int init(const std::shared_ptr<ExtFileSystem>& ext_fs, const std::string& rocksdb_path);

    int sst_size(const std::string& short_name, uint64_t* size);

    int extsst_size(const std::string& ext_file, uint64_t* size);

    int sst_modify_time(const std::string& short_name, uint64_t* modify_time);

    int list_external_full_name(const std::set<std::string>& sst_relative_filename, 
                                std::set<std::string>& external_files);
    int sst_files(std::vector<std::string>& files);

    void sst_ext_map(std::map<std::string, ExtFileInfo>& sst_ext_map) {
        std::lock_guard<bthread::Mutex> l(_lock);
        sst_ext_map = _sst_ext_map;
    }

    // -1：失败；0：不存在；1：存在
    int sst_exists(const std::string& short_name);

    int sst_delete(const std::string& short_name);

    int sst_rename(const std::string& src_short_name, const std::string& dst_short_name);

    int sst_link(const std::string& ext_src_name, const std::string& dst_short_name);

    // GetIngestedFileInfo中会调用到
    int new_ext_random_access_file(const std::string& ext_file_name,
                          std::unique_ptr<rocksdb::FSRandomAccessFile>* result);

    int new_sst_random_access_file(const std::string& short_name, 
                                std::unique_ptr<rocksdb::FSRandomAccessFile>* result);

    // 需要外部加锁
    int append_file(const std::string& record);

    int deal_line(const std::string& line);

    int load_file();

    int rewrite_file();
    
    int do_rewrite_file();

    std::shared_ptr<ExtFileSystem> get_exteranl_filesystem() { return _ext_fs; }

    void TESTclear() {
        std::lock_guard<bthread::Mutex> l(_lock);
        _sst_ext_map.clear();
        _link_num = 0;
        _delete_num = 0;
        _rename_num = 0;
    }

private:
    SstExtLinker() {}
    std::shared_ptr<ExtFileSystem> _ext_fs;
    bthread::Mutex _lock;
    std::string _rocksdb_path;
    int _link_num = 0;
    int _delete_num = 0;
    int _rename_num = 0;
    std::map<std::string, ExtFileInfo> _sst_ext_map;
    DISALLOW_COPY_AND_ASSIGN(SstExtLinker);
};

class RocksdbFileSystemWrapper : public rocksdb::FileSystemWrapper {
 public:
    explicit RocksdbFileSystemWrapper(bool use_ext_fs) 
            : rocksdb::FileSystemWrapper(rocksdb::FileSystem::Default()), _use_ext_fs(use_ext_fs) { 
        if (_use_ext_fs) {
            _name = "Ext" + std::string(kClassName());
        } else {
            _name = "Local" + std::string(kClassName());
        }
        _linker = SstExtLinker::get_instance();
    }
    ~RocksdbFileSystemWrapper() override {}

    static const char* kClassName() { 
        return "RocksdbFileSystemWrapper"; 
    }
    const char* Name() const override { 
        return _name.c_str();
    }

    bool is_sst(const std::string& full_name, std::string& short_name) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, full_name, boost::is_any_of("."));
        if (split_vec.size() > 0 && split_vec.back() == "sst") {
            std::vector<std::string> path_vec;
            boost::split(path_vec, full_name, boost::is_any_of("/"));
            if (path_vec.empty()) {
                FS_LOG(FATAL, "Invalid file name: %s", full_name.c_str());
                return false;
            }
            short_name = path_vec.back();
            return true;
        } else {
            return false;
        }
    }

    bool is_sst(const std::string& full_name) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, full_name, boost::is_any_of("."));
        if (split_vec.size() > 0 && split_vec.back() == "sst") {
            return true;
        } else {
            return false;
        }
    }

    bool is_extsst(const std::string& full_name) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, full_name, boost::is_any_of("."));
        if (split_vec.size() > 0 
                && (split_vec.back() == "extsst" 
                        || split_vec.back() == "binlogsst" 
                        || split_vec.back() == "datasst")) {
            return true;
        } else {
            return false;
        }
    }


// enum class IOType : uint8_t {
//     kData,     // 0
//     kFilter, // 1
//     kIndex,     // 2
//     kMetadata, // 3
//     kWAL,            // 4
//     kManifest, // 5
//     kLog,            // 6
//     kUnknown,    // 7
//     kInvalid,     // 8
// };
    // The following text is boilerplate that forwards all methods to target()
    rocksdb::IOStatus NewSequentialFile(const std::string& f,
                                    const rocksdb::FileOptions& file_opts,
                                    std::unique_ptr<rocksdb::FSSequentialFile>* r,
                                    rocksdb::IODebugContext* dbg) override {
        // 使用外部文件系统时，不允许创建sst
        if (_use_ext_fs && is_sst(f)) {
            FS_LOG(FATAL, "NotSupported file: %s, iotype: %d", f.c_str(), (int)file_opts.io_options.type);
            return rocksdb::IOStatus::NotSupported("NewSequentialFile");
        }

        auto s = target_->NewSequentialFile(f, file_opts, r, dbg);
        FS_LOG(WARNING, "file: %s, iotype: %d, status: %s", f.c_str(), (int)file_opts.io_options.type, s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus NewRandomAccessFile(const std::string& f,
                                        const rocksdb::FileOptions& file_opts,
                                        std::unique_ptr<rocksdb::FSRandomAccessFile>* r,
                                        rocksdb::IODebugContext* dbg) override {
        FS_LOG(WARNING, "file: %s", f.c_str());
        int ret = 0;
        if (_use_ext_fs) {
            bool is_ext_file = true;
            std::string short_name;
            if (is_sst(f, short_name)) {
                ret = _linker->new_sst_random_access_file(short_name, r);
            } else if (is_extsst(f)) {
                ret = _linker->new_ext_random_access_file(f, r);
            } else {
                is_ext_file = false;
            }
            if (is_ext_file) {
                if (ret < 0) {
                    FS_LOG(FATAL, "new random access file: %s failed", f.c_str());
                    return rocksdb::IOStatus::IOError("NewRandomAccessFile failed");
                }
                FS_LOG(WARNING, "file: %s, iotype: %d", f.c_str(), (int)file_opts.io_options.type);
                return rocksdb::IOStatus::OK();
            }
        }
        auto s = target_->NewRandomAccessFile(f, file_opts, r, dbg);
        FS_LOG(WARNING, "file: %s, iotype: %d, status: %s", f.c_str(), (int)file_opts.io_options.type, s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus NewWritableFile(const std::string& f, const rocksdb::FileOptions& file_opts,
                                    std::unique_ptr<rocksdb::FSWritableFile>* r,
                                    rocksdb::IODebugContext* dbg) override {
        // 使用外部文件系统时，不允许创建sst
        if (_use_ext_fs && is_sst(f)) {
            FS_LOG(FATAL, "NotSupported file: %s, iotype: %d", f.c_str(), (int)file_opts.io_options.type);
            return rocksdb::IOStatus::NotSupported("NewWritableFile");
        }
        auto s = target_->NewWritableFile(f, file_opts, r, dbg);
        FS_LOG(WARNING, "file: %s, iotype: %d, status: %s", f.c_str(), (int)file_opts.io_options.type, s.ToString().c_str());
        return s;
    }
    // link之后会调用进行sync文件，返回not supported不支持
    rocksdb::IOStatus ReopenWritableFile(const std::string& fname,
                                        const rocksdb::FileOptions& file_opts,
                                        std::unique_ptr<rocksdb::FSWritableFile>* result,
                                        rocksdb::IODebugContext* dbg) override {
        if (_use_ext_fs && is_sst(fname)) {
            FS_LOG(WARNING, "NotSupported file: %s, iotype: %d", fname.c_str(), (int)file_opts.io_options.type);
            return rocksdb::IOStatus::NotSupported("ReopenWritableFile");
        }
        auto s = target_->ReopenWritableFile(fname, file_opts, result, dbg);
        FS_LOG(WARNING, "file: %s, iotype: %d, status: %s", fname.c_str(), (int)file_opts.io_options.type, s.ToString().c_str());
        return s;
    }

    // db_impl_open.cc:1730
    rocksdb::IOStatus ReuseWritableFile(const std::string& fname,
                                        const std::string& old_fname,
                                        const rocksdb::FileOptions& file_opts,
                                        std::unique_ptr<rocksdb::FSWritableFile>* r,
                                        rocksdb::IODebugContext* dbg) override {
        if (_use_ext_fs && is_sst(fname)) {
            FS_LOG(FATAL, "NotSupported file: %s, old_file: %s, iotype: %d", 
                    fname.c_str(), old_fname.c_str(), (int)file_opts.io_options.type);
            return rocksdb::IOStatus::NotSupported("ReuseWritableFile");
        }
        auto s = target_->ReuseWritableFile(fname, old_fname, file_opts, r, dbg);
        FS_LOG(WARNING, "file: %s, old_file: %s, iotype: %d, status: %s", 
                fname.c_str(), old_fname.c_str(), (int)file_opts.io_options.type, s.ToString().c_str());
        return s;
    }

    // external_sst_file_ingestion_job.cc:895
    rocksdb::IOStatus NewRandomRWFile(const std::string& fname,
                                    const rocksdb::FileOptions& file_opts,
                                    std::unique_ptr<rocksdb::FSRandomRWFile>* result,
                                    rocksdb::IODebugContext* dbg) override {
        if (_use_ext_fs && is_sst(fname)) {
            FS_LOG(FATAL, "NotSupported file: %s, iotype: %d", fname.c_str(), (int)file_opts.io_options.type);
            return rocksdb::IOStatus::NotSupported("NewRandomRWFile");
        }
        auto s = target_->NewRandomRWFile(fname, file_opts, result, dbg);
        FS_LOG(WARNING, "file: %s, iotype: %d, status: %s", fname.c_str(), (int)file_opts.io_options.type, s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus NewMemoryMappedFileBuffer(
                                const std::string& fname,
                                std::unique_ptr<rocksdb::MemoryMappedFileBuffer>* result) override {
        if (_use_ext_fs && is_sst(fname)) {
            FS_LOG(FATAL, "NotSupported file: %s", fname.c_str());
            return rocksdb::IOStatus::NotSupported("NewMemoryMappedFileBuffer");
        }
        auto s = target_->NewMemoryMappedFileBuffer(fname, result);
        FS_LOG(WARNING, "file: %s, status: %s", fname.c_str(), s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus NewDirectory(const std::string& name, const rocksdb::IOOptions& io_opts,
                                std::unique_ptr<rocksdb::FSDirectory>* result,
                                rocksdb::IODebugContext* dbg) override {
        auto s = target_->NewDirectory(name, io_opts, result, dbg);
        FS_LOG(WARNING, "file: %s, iotype: %d, status: %s", name.c_str(), (int)io_opts.type, s.ToString().c_str());
        return s;
    }

    //  Returns OK if the named file exists.
    //         NotFound if the named file does not exist,
    //             the calling process does not have permission to determine
    //             whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    rocksdb::IOStatus FileExists(const std::string& f, const rocksdb::IOOptions& io_opts,
                                rocksdb::IODebugContext* dbg) override {
        std::string short_name;
        if (_use_ext_fs && is_sst(f, short_name)) {
            int ret = _linker->sst_exists(short_name);
            if (ret < 0) {
                FS_LOG(FATAL, "FileExists ext file: %s short_name: %s failed", f.c_str(), short_name.c_str());
                return rocksdb::IOStatus::IOError("FileExists failed");
            } else if (ret == 0) {
                FS_LOG(FATAL, "NotFound ext file: %s short_name: %s", f.c_str(), short_name.c_str());
                return rocksdb::IOStatus::NotFound();
            } else {
                FS_LOG(WARNING, "Found ext file: %s short_name: %s, iotype: %d", f.c_str(), short_name.c_str(), (int)io_opts.type);
                return rocksdb::IOStatus::OK();
            }
        }

        auto s = target_->FileExists(f, io_opts, dbg);
        FS_LOG(WARNING, "FileExists local file: %s status: %s", f.c_str(), s.ToString().c_str());
        return s;

    }

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    // Returns OK if "dir" exists and "*result" contains its children.
    //                 NotFound if "dir" does not exist, the calling process does not have
    //                                    permission to access "dir", or if "dir" is invalid.
    //                 IOError if an IO Error was encountered
    rocksdb::IOStatus GetChildren(const std::string& dir, const rocksdb::IOOptions& io_opts,
                                std::vector<std::string>* r,
                                rocksdb::IODebugContext* dbg) override {
        r->clear();
        std::vector<std::string> result;
        auto s = target_->GetChildren(dir, io_opts, &result, dbg);
        if (!s.ok()) {
            FS_LOG(FATAL, "dir: %s, error: %s", dir.c_str(), s.ToString().c_str());
            return s;
        }

        if (!_use_ext_fs) {
            r->swap(result);
            std::ostringstream os;
            for (const std::string& c : *r) {
                os << c << " ";
            }
            std::string str = os.str();
            FS_LOG(WARNING, "dir: %s, local file count: %ld, child name: %s", dir.c_str(), r->size(), str.c_str());
            return rocksdb::IOStatus::OK();
        }

        std::vector<std::string> ext_result;
        int ret = _linker->sst_files(ext_result);
        if (ret < 0) {
            FS_LOG(FATAL, "GetChildren failed dir: %s", dir.c_str());
            return rocksdb::IOStatus::IOError("GetChildren failed");
        }

        for (const std::string& c : result) {
            // 剔除映射文件
            if (c.find(SstExtLinker::SST_EXT_MAP_FILE_PREFIX) == c.npos) {
                r->emplace_back(c);
            }
        }
        r->insert(r->end(), ext_result.begin(), ext_result.end());

        std::ostringstream os;
        for (const std::string& c : *r) {
            os << c << " ";
        }
        std::string str = os.str();

        FS_LOG(WARNING, "dir: %s, local file count: %ld, ext file count: %ld, child name: %s", 
                dir.c_str(), result.size(), ext_result.size(), str.c_str());
        return rocksdb::IOStatus::OK();
    }

    // 该接口默认会调用GetChildren、GetFileSize、FileExists来实现，并且只在backup流程中使用，此处不实现

    // rocksdb::IOStatus GetChildrenFileAttributes(const std::string& dir,
    //                                            const rocksdb::IOOptions& options,
    //                                            std::vector<rocksdb::FileAttributes>* result,
    //                                            rocksdb::IODebugContext* dbg) override {}

    rocksdb::IOStatus DeleteFile(const std::string& f, const rocksdb::IOOptions& options,
                                rocksdb::IODebugContext* dbg) override {

        int ret = 0;
        if (_use_ext_fs) {
            bool is_ext_file = true;
            std::string short_name;
            if (is_sst(f, short_name)) {
                ret = _linker->sst_delete(short_name);
            } else if (is_extsst(f)) {
                FS_LOG(WARNING, "delete external file: %s", f.c_str());
                return rocksdb::IOStatus::OK();
            } else {
                is_ext_file = false;
            }
            if (is_ext_file) {
                if (ret < 0) {
                    FS_LOG(FATAL, "delete ext file: %s failed", f.c_str());
                    return rocksdb::IOStatus::IOError("DeleteFile failed");
                }
                FS_LOG(WARNING, "delete ext file: %s", f.c_str());
                return rocksdb::IOStatus::OK();
            }
        }

        auto s = target_->DeleteFile(f, options, dbg);
        FS_LOG(WARNING, "delete local file: %s, status: %s", f.c_str(), s.ToString().c_str());
        return s;
    }

    // writable_file_writer.cc:259
    // delete_scheduler.cc:320
    rocksdb::IOStatus Truncate(const std::string& fname, size_t size,
                            const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override {
        if (_use_ext_fs && is_sst(fname)) {
            FS_LOG(FATAL, "NotSupported Truncate file: %s", fname.c_str());
            return rocksdb::IOStatus::NotSupported("Truncate");
        }
        auto s = target_->Truncate(fname, size, options, dbg);
        FS_LOG(WARNING, "file: %s, size: %ld, iotype: %d, status: %s", fname.c_str(), size, (int)options.type, s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus CreateDir(const std::string& d, const rocksdb::IOOptions& options,
                                rocksdb::IODebugContext* dbg) override {
        auto s = target_->CreateDir(d, options, dbg);
        FS_LOG(WARNING, "dir: %s, status: %s", d.c_str(), s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus CreateDirIfMissing(const std::string& d, const rocksdb::IOOptions& options,
                                        rocksdb::IODebugContext* dbg) override {
        auto s = target_->CreateDirIfMissing(d, options, dbg);
        FS_LOG(WARNING, "dir: %s, status: %s", d.c_str(), s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus DeleteDir(const std::string& d, const rocksdb::IOOptions& options,
                            rocksdb::IODebugContext* dbg) override {
        auto s = target_->DeleteDir(d, options, dbg);
        FS_LOG(FATAL, "dir: %s, status: %s", d.c_str(), s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus GetFileSize(const std::string& f, const rocksdb::IOOptions& options,
                                uint64_t* s, rocksdb::IODebugContext* dbg) override {
        if (_use_ext_fs) {
            int ret = 0;
            bool is_ext_file = true;
            std::string short_name;
            if (is_sst(f, short_name)) {
                ret = _linker->sst_size(short_name, s);
            } else if (is_extsst(f)) {
                ret = _linker->extsst_size(f, s);
            } else {
                is_ext_file = false;
            }
            if (is_ext_file) {
                if (ret < 0) {
                    FS_LOG(FATAL, "GetFileSize file: %s short_name: %s failed", f.c_str(), short_name.c_str());
                    return rocksdb::IOStatus::IOError("GetFileSize failed");
                }
                FS_LOG(WARNING, "ext file: %s short_name: %s size: %lu", f.c_str(), short_name.c_str(), *s);
                return rocksdb::IOStatus::OK();
            }
        }

        auto status = target_->GetFileSize(f, options, s, dbg);
        FS_LOG(WARNING, "local file: %s, size: %lu, status: %s", f.c_str(), *s, status.ToString().c_str());
        return status;
    }

    rocksdb::IOStatus GetFileModificationTime(const std::string& fname,
                                            const rocksdb::IOOptions& options,
                                            uint64_t* file_mtime,
                                            rocksdb::IODebugContext* dbg) override {
        std::string short_name;
        if (_use_ext_fs && is_sst(fname, short_name)) {
            int ret = _linker->sst_modify_time(short_name, file_mtime);
            if (ret < 0) {
                FS_LOG(FATAL, "GetFileModificationTime file: %s short_name: %s failed", fname.c_str(), short_name.c_str());
                return rocksdb::IOStatus::IOError("GetFileModificationTime failed");
            }
            FS_LOG(WARNING, "ext file: %s modify_time: %lu", fname.c_str(), *file_mtime);
            return rocksdb::IOStatus::OK();
        }

        auto s = target_->GetFileModificationTime(fname, options, file_mtime, dbg);
        FS_LOG(WARNING, "local file: %s, modify_time: %lu, status: %s", fname.c_str(), *file_mtime, s.ToString().c_str());
        return s;
    }

    // auto_roll_logger.cc:44
    // auto_roll_logger.cc:284
    rocksdb::IOStatus GetAbsolutePath(const std::string& db_path, const rocksdb::IOOptions& options,
                                    std::string* output_path,
                                    rocksdb::IODebugContext* dbg) override {
        auto s = target_->GetAbsolutePath(db_path, options, output_path, dbg);
        FS_LOG(WARNING, "db_path: %s, output_path: %s, status: %s", db_path.c_str(), output_path->c_str(), s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus RenameFile(const std::string& s, const std::string& t,
                                const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override {
        std::string s_short_name;
        std::string t_short_name;
        if (_use_ext_fs && is_sst(s, s_short_name) && is_sst(t, t_short_name)) {
            int ret = _linker->sst_rename(s_short_name, t_short_name);
            if (ret < 0) {
                FS_LOG(FATAL, "RenameFile file: %s => %s, short_name: %s => %s failed", 
                    s.c_str(), s_short_name.c_str(), t.c_str(), t_short_name.c_str());
                return rocksdb::IOStatus::IOError("RenameFile failed");
            }
            FS_LOG(WARNING, "RenameFile ext file: %s => %s", s.c_str(), t.c_str());
            return rocksdb::IOStatus::OK();
        }

        auto status = target_->RenameFile(s, t, options, dbg);
        FS_LOG(WARNING, "RenameFile local file: %s => %s, status: %s", s.c_str(), t.c_str(), status.ToString().c_str());
        return status;
    }

    rocksdb::IOStatus LinkFile(const std::string& s, const std::string& t,
                            const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override {
        std::string t_short_name;
        if (_use_ext_fs && is_sst(t, t_short_name)) {
            int ret = _linker->sst_link(s, t_short_name);
            if (ret < 0) {
                FS_LOG(FATAL, "LinkFile file: %s => %s, t_short_name: %s failed", s.c_str(), t.c_str(), t_short_name.c_str());
                return rocksdb::IOStatus::IOError("LinkFile failed");
            }
            FS_LOG(WARNING, "LinkFile ext file: %s => %s, t_short_name: %s", s.c_str(), t.c_str(), t_short_name.c_str());
            return rocksdb::IOStatus::OK();
        }

        auto status = target_->LinkFile(s, t, options, dbg);
        FS_LOG(WARNING, "s: %s, t: %s, status: %s", s.c_str(), t.c_str(), status.ToString().c_str());
        return status;
    }

    // delete_scheduler.cc:312
    rocksdb::IOStatus NumFileLinks(const std::string& fname, const rocksdb::IOOptions& options,
                                uint64_t* count, rocksdb::IODebugContext* dbg) override {
        if (_use_ext_fs && is_sst(fname)) {
            FS_LOG(FATAL, "NotSupported file: %s", fname.c_str());
            return rocksdb::IOStatus::NotSupported("NumFileLinks");
        }
        auto s = target_->NumFileLinks(fname, options, count, dbg);
        FS_LOG(WARNING, "fname: %s, count: %lu, status: %s", fname.c_str(), *count, s.ToString().c_str());
        return s;
    }

    // db_options.cc:944
    // blob_db_impl.cc:232
    // 应该不会调用
    rocksdb::IOStatus AreFilesSame(const std::string& first, const std::string& second,
                                const rocksdb::IOOptions& options, bool* res,
                                rocksdb::IODebugContext* dbg) override {
        auto s = target_->AreFilesSame(first, second, options, res, dbg);
        FS_LOG(FATAL, "first: %s, second: %s, res: %d, status: %s", first.c_str(), second.c_str(), *res, s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus LockFile(const std::string& f, const rocksdb::IOOptions& options,
                            rocksdb::FileLock** l, rocksdb::IODebugContext* dbg) override {
        auto s = target_->LockFile(f, options, l, dbg);
        FS_LOG(WARNING, "file: %s, status: %s", f.c_str(), s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus UnlockFile(rocksdb::FileLock* l, const rocksdb::IOOptions& options,
                                rocksdb::IODebugContext* dbg) override {
        return target_->UnlockFile(l, options, dbg);
    }

    rocksdb::IOStatus GetTestDirectory(const rocksdb::IOOptions& options, std::string* path,
                                    rocksdb::IODebugContext* dbg) override {
        auto s = target_->GetTestDirectory(options, path, dbg);
        FS_LOG(FATAL, "file: %s, status: %s", path->c_str(), s.ToString().c_str());
        return s;
    }
    rocksdb::IOStatus NewLogger(const std::string& fname, const rocksdb::IOOptions& options,
                            std::shared_ptr<rocksdb::Logger>* result,
                            rocksdb::IODebugContext* dbg) override {
        auto s = target_->NewLogger(fname, options, result, dbg);
        FS_LOG(WARNING, "file: %s, status: %s", fname.c_str(), s.ToString().c_str());
        return s;
    }

    rocksdb::IOStatus IsDirectory(const std::string& path, const rocksdb::IOOptions& options,
                                bool* is_dir, rocksdb::IODebugContext* dbg) override {
        auto s = target_->IsDirectory(path, options, is_dir, dbg);
        FS_LOG(WARNING, "path: %s, status: %s", path.c_str(), s.ToString().c_str());
        return s;                             
    }
private:
    const bool _use_ext_fs = false; // 使用外部的Filesystem不支持write
    std::string _name;
    SstExtLinker* _linker = nullptr;
};

}    // namespace baikaldb
