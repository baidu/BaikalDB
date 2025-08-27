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
#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#else
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <butil/files/file_enumerator.h>
#endif
#include "rocksdb_filesystem.h"
#include <openssl/md5.h>
#include <iomanip>
#include <cstring>
namespace baikaldb {
static bvar::Adder<int64_t> g_external_sst_count("external_afs_sst_count");
static bvar::Adder<int64_t> g_external_sst_size_bytes("external_sst_size_bytes");
const std::string SstExtLinker::SST_EXT_MAP_FILE_PREFIX = "sst_ext_map";
 
rocksdb::IOStatus ExtRandomAccessFile::Read(uint64_t offset, size_t n, const rocksdb::IOOptions& options,
                    rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext* dbg) const {
    rocksdb::IOStatus s;
    ssize_t r = -1;
    size_t left = n;
    char* ptr = scratch;
    bool eof = false;
    while (left > 0) {
        r = _file_reader->read(ptr, left, static_cast<off_t>(offset), &eof);
        if (r < 0) {
            break;
        }
        ptr += r;
        offset += r;
        left -= r;
        if (eof) {
            break;
        }
    }
    if (r < 0) {
        DB_FATAL("filename: %s, read offset: %lu, len: %ld failed", _file_reader->file_name().c_str(), offset, n);
        // An error: return a non-ok status
        s = rocksdb::IOStatus::IOError("While pread offset " + std::to_string(offset) + " len " +
                        std::to_string(n) + " " + _file_reader->file_name());
    }

    *result = rocksdb::Slice(scratch, (r < 0) ? 0 : n - left);
    return s;
}


rocksdb::IOStatus ExtSequentialFile::Read(size_t n, const rocksdb::IOOptions& options,
                    rocksdb::Slice* result, char* scratch, rocksdb::IODebugContext* dbg) {
    rocksdb::IOStatus s;
    ssize_t r = -1;
    size_t left = n;
    char* ptr = scratch;
    bool eof = false;
    while (left > 0) {
        r = _file_reader->read(ptr, left, static_cast<off_t>(current_offset_), &eof);
        if (r < 0) {
            break;
        }
        ptr += r;
        current_offset_ += r;
        left -= r;
        if (eof) {
            break;
        }
    }
    if (r < 0) {
        DB_FATAL("filename: %s, read current_offset: %lu, len: %ld failed",
            _file_reader->file_name().c_str(), current_offset_, n);
        // An error: return a non-ok status
        s = rocksdb::IOStatus::IOError("While pread offset " + std::to_string(current_offset_) + " len " +
                        std::to_string(n) + " " + _file_reader->file_name());
    }

    *result = rocksdb::Slice(scratch, (r < 0) ? 0 : n - left);
    return s;
}

rocksdb::IOStatus ExtSequentialFile::Skip(uint64_t n) {
    // 将当前偏移量移动 n 个字节
    rocksdb::IOStatus status;
    current_offset_ += n;

    // 检查跳过操作是否超过文件末尾
    bool eof = false;
    ssize_t skipped_bytes = _file_reader->skip(n, &eof);
    if (skipped_bytes < 0) {
        // 如果发生错误，返回 IOError 状态
        status = rocksdb::IOStatus::IOError("Failed to skip " + std::to_string(n) + " bytes");
    } else if (eof) {
        // 如果到达文件末尾，调整偏移量并返回 OK
        current_offset_ -= (n - skipped_bytes);
        status = rocksdb::IOStatus::OK();
    } else {
        // 成功跳过所需的字节数
        status = rocksdb::IOStatus::OK();
    }

    return status;
}

int SstExtLinker::init(const std::shared_ptr<ExtFileSystem>& ext_fs, const std::string& rocksdb_path) {
    _ext_fs = ext_fs; 
    _rocksdb_path = rocksdb_path;
    int ret = load_file();
    if (ret < 0) {
        DB_FATAL("load file: %s failed", SST_EXT_MAP_FILE_PREFIX.c_str());
        return -1;
    }
    return 0;
}

int SstExtLinker::sst_size(const std::string& short_name, uint64_t* size) {
    std::lock_guard<bthread::Mutex> l(_lock);
    auto iter = _sst_ext_map.find(short_name);
    if (iter != _sst_ext_map.end()) {
        *size = iter->second.size;
        return 0;
    }

    DB_FATAL("cant find sst: %s", short_name.c_str());
    return -1;
}

int SstExtLinker::extsst_size(const std::string& ext_file, uint64_t* size) {
    int ret = get_size_by_external_file_name(size, nullptr, ext_file);
    if (ret != 0) {
        DB_FATAL("get file: %s size failed", ext_file.c_str());
        return -1;
    }

    DB_NOTICE("ext file: %s, size: %lu", ext_file.c_str(), *size);
    return 0;
}

int SstExtLinker::sst_modify_time(const std::string& short_name, uint64_t* modify_time) {
    std::lock_guard<bthread::Mutex> l(_lock);
    auto iter = _sst_ext_map.find(short_name);
    if (iter != _sst_ext_map.end()) {
        *modify_time = iter->second.modify_time;
        return 0;
    }
    DB_FATAL("cant find sst: %s", short_name.c_str());
    return -1;
}

int SstExtLinker::list_external_full_name(const std::set<std::string>& sst_relative_filename, 
                                    std::set<std::string>& external_files) {
    external_files.clear();
    std::lock_guard<bthread::Mutex> l(_lock);
    for (const std::string& sst : sst_relative_filename) {
        auto iter = _sst_ext_map.find(sst);
        if (iter == _sst_ext_map.end()) {
            DB_FATAL("cant find short_name: %s", sst.c_str());
            return -1;
        }
        DB_NOTICE("map %s vs %s", sst.c_str(), iter->second.full_name.c_str());
        external_files.insert(iter->second.full_name);
    }
    return 0;
}

int SstExtLinker::sst_files(std::vector<std::string>& files) {
    files.clear();
    std::lock_guard<bthread::Mutex> l(_lock);
    for (const auto& info : _sst_ext_map) {
        files.emplace_back(info.first);
    }

    return 0;
}

// -1：失败；0：不存在；1：存在
int SstExtLinker::sst_exists(const std::string& short_name) {
    std::lock_guard<bthread::Mutex> l(_lock);
    auto iter = _sst_ext_map.find(short_name);
    if (iter != _sst_ext_map.end()) {
        return 1;
    }
    return 0;
}

int SstExtLinker::sst_delete(const std::string& short_name) {
    std::lock_guard<bthread::Mutex> l(_lock);
    auto iter = _sst_ext_map.find(short_name);
    if (iter == _sst_ext_map.end()) {
        DB_FATAL("cant find short_name: %s", short_name.c_str());
        return 0;
    }

    ExtFileInfo delete_info = iter->second;
    _sst_ext_map.erase(short_name);
    std::string record = "delete\t" + short_name + "\n";
    int ret = append_file(record);
    if (ret < 0) {
        // map回滚
        _sst_ext_map[short_name] = delete_info;
        DB_FATAL("append file failed");
        return -1;
    }
    ++_delete_num;
    g_external_sst_count << -1;
    g_external_sst_size_bytes << -delete_info.size;
    DB_NOTICE("delete sst: %s, ext file: %s, size: %lu, modify_time: %lu", 
        short_name.c_str(), delete_info.full_name.c_str(), 
        delete_info.size, delete_info.modify_time);
    return 0;
}

int SstExtLinker::sst_rename(const std::string& src_short_name, const std::string& dst_short_name) {
    std::lock_guard<bthread::Mutex> l(_lock);
    auto src_iter = _sst_ext_map.find(src_short_name);
    auto dst_iter = _sst_ext_map.find(dst_short_name);
    if (src_iter == _sst_ext_map.end() || dst_iter != _sst_ext_map.end()) {
        DB_FATAL("sst rename failed, src_short_name: %s, dst_short_name: %s", src_short_name.c_str(), dst_short_name.c_str());
        return -1;
    }

    ExtFileInfo src_info = src_iter->second;
    ExtFileInfo dst_info = src_info;
    dst_info.modify_time = (uint64_t)time(nullptr);
    _sst_ext_map.erase(src_short_name);
    _sst_ext_map[dst_short_name] = dst_info;
    std::string record = "rename\t" + dst_short_name + "\t" + src_short_name + "\t" + std::to_string(dst_info.modify_time) + "\n";
    int ret = append_file(record);
    if (ret < 0) {
        // map回滚
        _sst_ext_map.erase(dst_short_name);
        _sst_ext_map[src_short_name] = src_info;
        DB_FATAL("append file failed");
        return -1;
    }
    ++_rename_num;

    DB_NOTICE("rename sst: %s => %s, ext file: %s, size: %lu, modify_time: %lu", 
        src_short_name.c_str(), dst_short_name.c_str(), src_info.full_name.c_str(), 
        src_info.size, src_info.modify_time);

    return 0;
}

int SstExtLinker::sst_link(const std::string& ext_src_name, const std::string& dst_short_name) {
    uint64_t ext_file_size = 0;
    int ret = get_size_by_external_file_name(&ext_file_size, nullptr, ext_src_name);
    if (ret != 0) {
        DB_FATAL("get file: %s size failed", ext_src_name.c_str());
        return -1;
    }
    std::lock_guard<bthread::Mutex> l(_lock);
    ExtFileInfo info;
    info.full_name = ext_src_name;
    info.size = ext_file_size;
    info.modify_time = (uint64_t)time(nullptr);

    auto iter = _sst_ext_map.find(dst_short_name);
    if (iter != _sst_ext_map.end()) {
        DB_FATAL("dst sst: %s exists", dst_short_name.c_str());
        return -1; 
    }

    _sst_ext_map[dst_short_name] = info;
    std::string record = "link\t" + dst_short_name + "\t" + ext_src_name + "\t" + std::to_string(info.size) + "\t" 
                        + std::to_string(info.modify_time) + "\n";
    ret = append_file(record);
    if (ret < 0) {
        // 回滚map
        _sst_ext_map.erase(dst_short_name);
        DB_FATAL("append file failed");
        return -1;
    }
    ++_link_num;
    g_external_sst_count << 1;
    g_external_sst_size_bytes << ext_file_size;
    DB_NOTICE("link sst: %s => %s, ext file: %s, size: %lu, modify_time: %lu", 
        dst_short_name.c_str(), ext_src_name.c_str(), info.full_name.c_str(), 
        info.size, info.modify_time);

    return 0;
}

int SstExtLinker::new_ext_random_access_file(const std::string& ext_file_name,
                        std::unique_ptr<rocksdb::FSRandomAccessFile>* result) {
    result->reset();
    std::shared_ptr<ExtFileReader> file_reader;
    int ret = _ext_fs->open_reader(ext_file_name, &file_reader);
    if (ret != 0) {
        DB_FATAL("open ext file: %s failed", ext_file_name.c_str());
        return -1;
    }

    result->reset(new ExtRandomAccessFile(std::move(file_reader)));
    // DB_NOTICE("ext file: %s", ext_file_name.c_str());
    return 0;
}

int SstExtLinker::new_sst_random_access_file(const std::string& short_name, 
                            std::unique_ptr<rocksdb::FSRandomAccessFile>* result) {
    std::string ext_file_name;
    {
        std::lock_guard<bthread::Mutex> l(_lock);
        auto iter = _sst_ext_map.find(short_name);
        if (iter == _sst_ext_map.end()) {
            DB_FATAL("cant find short_name: %s", short_name.c_str());
            return -1;
        }

        ext_file_name = iter->second.full_name;
    }

    int ret = new_ext_random_access_file(ext_file_name, result);
    if (ret < 0) {
        DB_FATAL("new_ext_random_access_file: %s, short_name: %s failed", ext_file_name.c_str(), short_name.c_str());
        return -1;
    }

    DB_NOTICE("sst file: %s, ext file: %s", short_name.c_str(), ext_file_name.c_str());
    return 0;
}

/*
    record格式\t分割
    link    sstname extname size    modify_time\n
    rename  newsstname  oldsstname  modify_time\n
    delete  sstname\n
*/

int SstExtLinker::append_file(const std::string& record) {
    if (record.empty()) {
        return 0;
    }

    std::string file_name = _rocksdb_path + "/" + SST_EXT_MAP_FILE_PREFIX;
    try {
        std::ofstream ofs(file_name, std::ofstream::app);
        if (!ofs.is_open()) {
            DB_FATAL("open file %s failed", file_name.c_str());
            return -1;
        }

        ofs << record;
        ofs.close();
    } catch (const std::exception& e) {
        DB_FATAL("append file: %s failed", file_name.c_str());
        return -1;
    }

    return 0;
}

/*
    record格式\t分割
    link    sstname extname size    modify_time\n
    rename  newsstname  oldsstname  modify_time\n
    delete  sstname\n
*/
int SstExtLinker::deal_line(const std::string& line) {
    std::vector<std::string> split_vec;
    boost::split(split_vec, line, boost::is_any_of("\t"));
    if (split_vec.empty()) {
        DB_FATAL("split %s failed", line.c_str());
        return -1;
    }

    if (split_vec[0] == "link") {
        if (split_vec.size() != 5) {
            DB_FATAL("link line: %s", line.c_str());
            return -1;
        }
        ExtFileInfo info;
        info.full_name = split_vec[2];
        info.size = boost::lexical_cast<uint64_t>(split_vec[3]);
        info.modify_time = boost::lexical_cast<uint64_t>(split_vec[4]);
        _sst_ext_map[split_vec[1]] = info;
        ++_link_num;
        g_external_sst_count << 1;
        g_external_sst_size_bytes << info.size;
    } else if (split_vec[0] == "delete") {
        if (split_vec.size() != 2) {
            DB_FATAL("delete line: %s", line.c_str());
            return -1;
        }
        auto iter = _sst_ext_map.find(split_vec[1]);
        if (iter == _sst_ext_map.end()) {
            DB_FATAL("delete not exists sst: %s", split_vec[1].c_str());
            return 0;
        }
        _sst_ext_map.erase(split_vec[1]);
        ++_delete_num;
        g_external_sst_count << -1;
        g_external_sst_size_bytes << -iter->second.size;
    } else if (split_vec[0] == "rename") {
        if (split_vec.size() != 4) {
            DB_FATAL("line: %s", line.c_str());
            return -1;
        }

        auto old_iter = _sst_ext_map.find(split_vec[2]);
        auto new_iter = _sst_ext_map.find(split_vec[1]);
        if (old_iter == _sst_ext_map.end() || new_iter != _sst_ext_map.end()) {
            DB_FATAL("sst rename failed, old sst: %s, new sst: %s", split_vec[2].c_str(), split_vec[1].c_str());
            return -1;
        }

        ExtFileInfo info;
        info.full_name = old_iter->second.full_name;
        info.size = old_iter->second.size;
        info.modify_time = boost::lexical_cast<uint64_t>(split_vec[3]);
        _sst_ext_map[split_vec[1]] = info;
        _sst_ext_map.erase(split_vec[2]);
        ++_rename_num;
    } else {
        DB_FATAL("not support %s", split_vec[0].c_str());
        return -1;
    }

    return 0;
}

int SstExtLinker::load_file() {
    std::string file_name = _rocksdb_path + "/" + SST_EXT_MAP_FILE_PREFIX;
    butil::FilePath file_path(file_name);
    if (!butil::PathExists(file_path)) {
        DB_WARNING("file_name: %s not exist", file_name.c_str());
        return 0;
    }

    try {
        std::ifstream ifs(file_name);
        if (!ifs.is_open()) {
            DB_FATAL("open %s failed", file_name.c_str());
            return -1;
        }

        TimeCost cost;
        int num = 0;
        std::string line;
        while (std::getline(ifs, line)) {
            if (deal_line(line) != 0) {
                DB_FATAL("deal line: %s failed", line.c_str());
                return -1;
            }
            DB_NOTICE("load file line: %s", line.c_str());
            ++num;
        }

        if (ifs.eof()) {
            ifs.close(); // 先close再判断是否需要rewrite
            DB_NOTICE("load file success, total num: %d, link num: %d, delete num: %d, rename num: %d, cost: %ld", 
                num, _link_num, _delete_num, _rename_num, cost.get_time());
            return rewrite_file();
        } else {
            DB_FATAL("get line failed");
            return -1;
        }
    } catch (const std::exception& e) {
        DB_FATAL("load %s failed", file_name.c_str());
        return -1;
    }

    return 0;
}

int SstExtLinker::rewrite_file() {
    if ((_delete_num + _rename_num) > 10240) {
        return do_rewrite_file();
    }

    return 0;
}

int SstExtLinker::do_rewrite_file() {
    std::string file_name = _rocksdb_path + "/" + SST_EXT_MAP_FILE_PREFIX;
    std::string tmp_file = file_name + "_tmp";
    try {
        std::ofstream ofs(tmp_file, std::ofstream::out | std::ofstream::app);
        if (!ofs.is_open()) {
            DB_FATAL("open file %s failed", file_name.c_str());
            return -1;
        }
        int link_num = 0;
        for (const auto& info : _sst_ext_map) {
            std::string record = "link\t" + info.first + "\t" + info.second.full_name + "\t" + std::to_string(info.second.size) + "\t" 
                    + std::to_string(info.second.modify_time) + "\n";
            ofs << record;
            DB_NOTICE("");
            if (!ofs.good()) {
                DB_FATAL("rewrite file: %s failed", tmp_file.c_str());
                return -1;
            }
            ++link_num;
        }

        ofs.close();

        // rename move失败怎么回滚，是否会导致文件丢失 OLAPTODO
        butil::FilePath to_path(file_name);
        butil::FilePath from_path(tmp_file);
        butil::FilePath tmp_path(file_name + ".bak");
        if (!butil::Move(to_path, tmp_path)) {
            DB_FATAL("move file1 failed");
            return -1;
        }
        if (!butil::Move(from_path, to_path)) {
            DB_FATAL("move file2 failed");
            return -1;
        }
        // 删除中间文件
        butil::DeleteFile(tmp_path, false);
        _link_num = link_num;
        _delete_num = 0;
        _rename_num = 0;
    } catch (const std::exception& e) {
        DB_FATAL("append file: %s failed", file_name.c_str());
        return -1;
    }
    
    return 0;
}

int CompactionSstExtLinker::register_job(const std::string& remote_compaction_id,
                        const std::shared_ptr<CompactionExtFileSystem>& ext_fs,
                        const std::vector<std::string>& input_files) {
    std::shared_ptr<CompactionSstExtLinkerData> linker_data = std::make_shared<CompactionSstExtLinkerData>();                   
    linker_data->ext_fs = ext_fs;
    std::vector<pb::CompactionFileInfo> file_info_list;
    if (0 != ext_fs->get_file_info_list(file_info_list)) {
        DB_FATAL("remote_compaction_id: %s get_file_info_list failed", remote_compaction_id.c_str());
        return -1;
    }
    for (const auto& file_info : file_info_list) {
        linker_data->file_size_map[file_info.file_path()] = file_info.file_size();
    }
    for (const auto& file_name : input_files) {
        linker_data->input_file_set.insert("/" + file_name);
    }
    BAIDU_SCOPED_LOCK(_mutex);
    _linker_data_map[remote_compaction_id] = linker_data;
    return 0;
}

int CompactionSstExtLinker::finish_job(const std::string& remote_compaction_id) {
    BAIDU_SCOPED_LOCK(_mutex);                      
    _linker_data_map.erase(remote_compaction_id);
    return 0;
}

int CompactionSstExtLinker::new_ext_random_access_file(const std::string& remote_compaction_id,
                        const std::string& ext_file_name,
                        std::unique_ptr<rocksdb::FSRandomAccessFile>* result) {
    result->reset();
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (get_linker_data(remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    std::shared_ptr<CompactionExtFileSystem> ext_fs = linker_data->ext_fs;
    if (ext_fs == nullptr) {
        DB_FATAL("remote compaction id: %s ext_fs not found", remote_compaction_id.c_str());
        return -1;
    }
    std::shared_ptr<ExtFileReader> file_reader;
    int ret = ext_fs->open_reader(ext_file_name, &file_reader);
    if (ret != 0) {
        DB_FATAL("open ext file: %s failed", ext_file_name.c_str());
        return -1;
    }

    result->reset(new ExtRandomAccessFile(std::move(file_reader)));
    // DB_NOTICE("remote_compaction_id: %s, ext file: %s", remote_compaction_id.c_str(), ext_file_name.c_str());
    return 0;
}

int CompactionSstExtLinker::new_ext_sequential_file(const std::string& remote_compaction_id,
                        const std::string& ext_file_name,
                        std::unique_ptr<rocksdb::FSSequentialFile>* result) {
    result->reset();
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (get_linker_data(remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    std::shared_ptr<CompactionExtFileSystem> ext_fs = linker_data->ext_fs;
    if (ext_fs == nullptr) {
        DB_FATAL("remote compaction id: %s ext_fs not found", remote_compaction_id.c_str());
        return -1;
    }
    std::shared_ptr<ExtFileReader> file_reader;
    int ret = ext_fs->open_reader(ext_file_name, &file_reader);
    if (ret != 0) {
        DB_FATAL("open ext file: %s failed", ext_file_name.c_str());
        return -1;
    }

    result->reset(new ExtSequentialFile(std::move(file_reader)));
    return 0;
}

int CompactionSstExtLinker::get_dir_children(const std::string& remote_compaction_id,
                        const std::string& dir, 
                        std::set<std::string>& file_list) {
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (get_linker_data(remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    const std::map<std::string, uint64_t>& file_size_map = linker_data->file_size_map;
    for (const auto& file_info : file_size_map) {
        file_list.insert(file_info.first);
    }
    DB_DEBUG("remote_compaction_id: %s, dir: %s", remote_compaction_id.c_str(), dir.c_str());
    return file_list.size();
}

int CompactionSstExtLinker::file_exists(const std::string& remote_compaction_id,
                        const std::string& ext_file_name) {
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (get_linker_data(remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    std::map<std::string, uint64_t>& file_size_map = linker_data->file_size_map;
    std::size_t pos = ext_file_name.rfind('/');
    if (pos == std::string::npos) {
        DB_WARNING("file: %s not have /", ext_file_name.c_str());
        return -1;
    }
    const std::string& file_name = ext_file_name.substr(pos + 1);
    if (file_size_map.find(file_name) == file_size_map.end()) {
        DB_WARNING("file: %s not found, file_size_map size: %lu", file_name.c_str(), file_size_map.size());
        return -1;
    }
    return file_size_map[file_name];
}

int CompactionSstExtLinker::create_dir(const std::string& remote_compaction_id,
                            const std::string& dir) {
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (get_linker_data(remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    std::shared_ptr<CompactionExtFileSystem> ext_fs = linker_data->ext_fs;
    if (ext_fs == nullptr) {
        DB_FATAL("remote compaction id: %s ext_fs not found", remote_compaction_id.c_str());
        return -1;
    }
    return ext_fs->create(dir);
}

int CompactionSstExtLinker::get_file_size(const std::string& remote_compaction_id,
                            const std::string& ext_file_name, 
                            uint64_t* size) {
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (get_linker_data(remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    std::map<std::string, uint64_t>& file_size_map = linker_data->file_size_map;
    std::size_t pos = ext_file_name.rfind('/');
    if (pos == std::string::npos) {
        DB_WARNING("file: %s not have /", ext_file_name.c_str());
        return -1;
    }
    const std::string& file_name = ext_file_name.substr(pos + 1);
    if (file_size_map.find(file_name) == file_size_map.end()) {
        DB_DEBUG("file: %s not found", file_name.c_str());
        return -1;
    }
    *size = file_size_map[file_name];
    return 0;
}

int CompactionSstExtLinker::rename_file(const std::string& remote_compaction_id,
                            const std::string& src_file_name, 
                            const std::string& dst_file_name) {
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (get_linker_data(remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    std::shared_ptr<CompactionExtFileSystem> ext_fs = linker_data->ext_fs;
    if (ext_fs == nullptr) {
        DB_FATAL("remote compaction id: %s ext_fs not found", remote_compaction_id.c_str());
        return -1;
    }
    return ext_fs->rename_file(src_file_name, dst_file_name);
}

int CompactionSstExtLinker::get_linker_data(const std::string& remote_compaction_id, 
                            std::shared_ptr<CompactionSstExtLinkerData>& linker_data) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (_linker_data_map.find(remote_compaction_id) == _linker_data_map.end()) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id.c_str());
        return -1;
    }
    linker_data = _linker_data_map[remote_compaction_id];
    return 0;
}

} // namespace baikaldbs