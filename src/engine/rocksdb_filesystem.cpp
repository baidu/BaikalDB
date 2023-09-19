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

namespace baikaldb {
static bvar::Adder<int64_t> g_external_sst_count("external_afs_sst_count");
static bvar::Adder<int64_t> g_external_sst_size_bytes("external_sst_size_bytes");
const std::string SstExtLinker::SST_EXT_MAP_FILE_PREFIX = "sst_ext_map";
int get_size_by_external_file_name(uint64_t* size, const std::string& external_file) {
    std::vector<std::string> split_vec;
    boost::split(split_vec, external_file, boost::is_any_of("/"));
    if (split_vec.empty()) {
        DB_FATAL("split %s failed", external_file.c_str());
        return -1;
    }

    std::vector<std::string> vec;
    vec.reserve(4);
    boost::split(vec, split_vec.back(), boost::is_any_of("._"));
    if (vec.empty()) {
        DB_FATAL("split %s failed", external_file.c_str());
        return -1;
    }
    if (vec.back() == "extsst") {
        // olap sst: regionID_lines_size_time.extsst
        if (vec.size() != 5) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }
        *size = boost::lexical_cast<uint64_t>(vec[2]);
    } else if (vec.back() == "binlogsst" || vec.back() == "datasst") {
        // backup binlog sst: regionID_startTS_endTS_idx_now()_size_lines.(binlogsst/datasst)
        if (vec.size() != 8) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }
        *size = boost::lexical_cast<uint64_t>(vec[5]);
    } else {
        DB_FATAL("external file: %s with abnormal file type: %s", external_file.c_str(), vec.back().c_str());
        return -1;
    }
    return 0;
}

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
    int ret = get_size_by_external_file_name(size, ext_file);
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
    int ret = get_size_by_external_file_name(&ext_file_size, ext_src_name);
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
    std::unique_ptr<ExtFileReader> file_reader;
    int ret = _ext_fs->open_reader(ext_file_name, &file_reader);
    if (ret != 0) {
        DB_FATAL("open ext file: %s failed", ext_file_name.c_str());
        return -1;
    }

    result->reset(new ExtRandomAccessFile(std::move(file_reader)));
    DB_NOTICE("ext file: %s", ext_file_name.c_str());
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

} // namespace baikaldbs