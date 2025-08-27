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
// #include <string>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "store_interact.hpp"
#include "schema_factory.h"
#include "external_filesystem.h"
#include "rocksdb_filesystem.h"

namespace baikaldb {
DECLARE_string(meta_server_bns);
DECLARE_string(cold_rocksdb_afs_infos);
DEFINE_int64(afs_double_read_interval_us, 1000 * 1000LL, "afs_double_read_interval_us");
DEFINE_bool(afs_open_reader_async_switch, true, "afs_open_reader_async_switch");
DEFINE_int64(afs_gc_interval_s, 24 * 3600LL, "default 1 day");
DEFINE_int64(afs_gc_count, 10, "afs_gc_count");
DEFINE_int64(afs_gc_delay_days, 30, "afs_gc_delay_days");
DEFINE_int64(afs_gc_allow_dead_store_count, 3, "afs_gc_allow_dead_store_count");
DEFINE_bool(afs_gc_enable, false, "afs_gc_enable");
DEFINE_bool(need_ext_fs_gc, false, "need_ext_fs_gc");
DEFINE_int64(compaction_sst_cache_max_block, 8192, "compaction_sst_cache_max_block");

int get_size_by_external_file_name(uint64_t* size, uint64_t* lines, const std::string& external_file) {
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
        if (size != nullptr) {
            *size = boost::lexical_cast<uint64_t>(vec[2]);
        }
        if (lines != nullptr) {
            *lines = boost::lexical_cast<uint64_t>(vec[1]);
        }
    } else if (vec.back() == "binlogsst" || vec.back() == "datasst") {
        // backup binlog sst: regionID_startTS_endTS_idx_now()_size_lines.(binlogsst/datasst)
        if (vec.size() != 8) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }
        if (size != nullptr) {
            *size = boost::lexical_cast<uint64_t>(vec[5]);
        }
        if (lines != nullptr) {
            *lines = boost::lexical_cast<uint64_t>(vec[6]);
        }
    } else if (vec.back() == "parquet") {
        // parquet: regionID_tableID_startIndex_endIndex_Idx_size_lines.parquet
        if (vec.size() != 8) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }
        if (size != nullptr) {
            *size = boost::lexical_cast<uint64_t>(vec[5]);
        }
        if (lines != nullptr) {
            *lines = boost::lexical_cast<uint64_t>(vec[6]);
        }
    } else {
        DB_FATAL("external file: %s with abnormal file type: %s", external_file.c_str(), vec.back().c_str());
        return -1;
    }
    return 0;
}
#ifdef BAIDU_INTERNAL

// uri,user,password,conf_file,root_path     多组afs ugi使用英文分号分割用户名密码等信息使用英文逗号分割
int get_afs_infos(std::vector<AfsExtFileSystem::AfsUgi>& ugi_infos) {
    ugi_infos.clear();
    if (FLAGS_cold_rocksdb_afs_infos.empty()) {
        return 0;
    }
    std::vector<std::string> split_vec;
    boost::split(split_vec, boost::trim_copy(FLAGS_cold_rocksdb_afs_infos), boost::is_any_of(";"));
    if (split_vec.empty()) {
        DB_FATAL("not afs info: %s", FLAGS_cold_rocksdb_afs_infos.c_str());
        return -1;
    }

    for (const std::string& info : split_vec) {
        AfsExtFileSystem::AfsUgi ugi;
        std::vector<std::string> vec;
        boost::split(vec, boost::trim_copy(info), boost::is_any_of(","));
        if (vec.size() != 5) {
            DB_FATAL("not afs ugi info: %s", info.c_str());
            return -1;
        }
        ugi.uri       = vec[0];
        ugi.user      = vec[1];
        ugi.password  = vec[2];
        ugi.cluster_name = vec[3];
        ugi.root_path = vec[4];
        // check slash需满足格式"/user/baikal"
        if (ugi.root_path.empty() || ugi.root_path.front() != '/') {
            DB_FATAL("check slash failed %s", ugi.root_path.c_str());
            return -1;
        }
        while (ugi.root_path.back() == '/') {
            ugi.root_path.pop_back();
        }
        ugi_infos.emplace_back(ugi);
    }
    return 0;
}


void AfsExtFileReader::ReadCtrl::set_read_result(int64_t ret, char* buf) {
    {
        std::unique_lock<std::mutex> lck(_mutex);
        if (ret < 0) {
            _fail_cnt++;
        } else {
            if (!_successed) {
                _successed = true;
                _success_ret = ret;
                memcpy(_buf, buf, ret);
                if (ret < _count || (ret + _offset) == _file_size) {
                    *_eof = true;
                } else {
                    *_eof = false;
                }
            }
        }
    }
    // 唤醒主线程
    _cv.notify_one();
}

int AfsExtFileReader::ReadCtrl::read_wait_once() {
    std::unique_lock<std::mutex> lck(_mutex);
    if (FLAGS_afs_double_read_interval_us > 0) {
        _cv.wait_for(lck, std::chrono::microseconds(FLAGS_afs_double_read_interval_us));
    }
    return _successed ? 1 : 0;
}

int64_t AfsExtFileReader::ReadCtrl::read_wait_onesucc_or_allfail() {
    std::unique_lock<std::mutex> lck(_mutex);
    while (true) {
        if (_successed) {
            return _success_ret;
        }

        if (_fail_cnt == _readers_cnt) {
            // 全部失败
            return -1;
        }

        // 等待异步唤醒
        _cv.wait(lck);
    }
    return -1;
}

int64_t AfsExtFileReader::read(char* buf, uint32_t count, uint32_t offset, bool* eof) {
    TimeCost time;
    std::vector<AfsRWInfo*> rw_infos = get_avaliable_reader_infos();
    // 随机打乱reader
    std::random_shuffle(rw_infos.begin(), rw_infos.end());

    static bvar::Adder<int64_t>  afs_read_fail_count("afs_read_fail_count");
    static bvar::LatencyRecorder afs_read_time_cost("afs_read_time_cost");
    uint64_t file_size = _afs_rw_infos[0].file_size;
    std::shared_ptr<ReadCtrl> read_ctrl = std::make_shared<ReadCtrl>(buf, count, offset, file_size, eof, rw_infos.size());
    for (AfsRWInfo* rw_info : rw_infos) {
        _read_count++;
        ReadInfo* read_done = new ReadInfo(count, offset, read_ctrl, rw_info, this);
        uint32_t real_count = (count + offset) >= file_size ? file_size - offset : count;
        int64_t ret = rw_info->reader->PRead(read_done->buf, real_count, offset, nullptr, pread_callback, read_done);
        if (ret != ds::kOk) {
            DB_FATAL("read uri: %s, file: %s count: %u, real_count: %u offset: %u time: %ld failed, ret: %s", 
                rw_info->uri.c_str(), rw_info->absolute_path.c_str(), count, real_count, offset, time.get_time(), ds::Rc2Str(ret));
            read_ctrl->set_read_result(-1, nullptr);
            delete read_done;
            _read_count--;
            continue;
        }

        // 进入异步执行，等待回调
        // 只等待一次，如果已经成功则跳出，否则继续实现请求多发
        if (1 == read_ctrl->read_wait_once()) {
            break;
        }
    }

    int64_t ret = read_ctrl->read_wait_onesucc_or_allfail();
    if (ret < 0) {
        afs_read_fail_count << 1;
        DB_FATAL("file: %s count: %u, offset: %u time: %ld failed", _afs_rw_infos[0].absolute_path.c_str(), count, offset, time.get_time());
        return -1;
    }

    afs_read_time_cost << time.get_time();
    return ret;
}

bool AfsExtFileReader::close() {
    // 等待所有异步请求结束
    TimeCost cost;
    while (_read_count.load() > 0) {
        bthread_usleep(100 * 1000);
        if (cost.get_time() > 10 * 1000 * 1000) {
            cost.reset();
            DB_FATAL("close file: %s, read count: %u", _afs_rw_infos[0].absolute_path.c_str(), _read_count.load());
        }
    }
    for (auto& info : _afs_rw_infos) {
        if (info.reader != nullptr) {
            TimeCost time;
            int ret = info.fs->CloseReader(info.reader);
            info.reader = nullptr;
            if (ret != ds::kOk) {
                DB_FATAL("close failed, uri: %s, absolute_path: %s", info.uri.c_str(), info.absolute_path.c_str());
            }
            info.statis->close_time_cost << time.get_time();
        }
    }
    return true;
}

void AfsExtFileReader::pread_callback(int64_t ret, void* ptr) {
    ReadInfo* read_info = static_cast<ReadInfo*>(ptr);
    AfsRWInfo* rw_info = read_info->rw_info;
    if (ret < 0) {
        rw_info->statis->read_fail_count << 1;
        rw_info->statis->read_time_cost << read_info->time.get_time();
        DB_WARNING("read uri: %s, file: %s count: %u, offset: %u time: %ld failed, ret: %s", 
            rw_info->uri.c_str(), rw_info->absolute_path.c_str(), read_info->count, read_info->offset, read_info->time.get_time(), ds::Rc2Str(ret));
    } else {
        rw_info->statis->read_bytes << ret;
        rw_info->statis->read_time_cost << read_info->time.get_time();
        DB_NOTICE("read uri: %s, file: %s count: %u, offset: %u time: %ld, ret: %ld", 
            rw_info->uri.c_str(), rw_info->absolute_path.c_str(), read_info->count, read_info->offset, read_info->time.get_time(), ret);
    }
    read_info->ctrl->set_read_result(ret, read_info->buf);
    read_info->reader->_read_count--;
    delete read_info;
}

std::vector<AfsRWInfo*> AfsExtFileReader::get_avaliable_reader_infos() {
    std::unique_lock<std::mutex> lck(_mtx);
    std::vector<AfsRWInfo*> avaliable_readers;
    for (int i = 0; i < _afs_rw_infos.size(); ++i) {
        if (_afs_rw_infos[i].reader != nullptr) {
            avaliable_readers.emplace_back(&(_afs_rw_infos[i]));
        }
    }
    return avaliable_readers;
}

void AfsExtFileReader::add_reader(std::shared_ptr<AfsRWInfo> info) {
    std::unique_lock<std::mutex> lck(_mtx);
    _afs_rw_infos.emplace_back(*info);
    if (_afs_rw_infos.back().statis == nullptr) {
        _afs_rw_infos.back().statis = std::make_shared<AfsStatis>("common");
    }
    _afs_rw_infos.back().statis->reader_open_count << 1;
}

int64_t AfsExtFileWriter::append(const char* buf, uint32_t count) {
    // 双afs写不要求性能，写串行即可
    for (auto& info : _afs_rw_infos) {
        int64_t ret = info.writer->Append(buf, count, nullptr, nullptr);
        if (ret < 0) {
            info.statis->write_fail_count << 1;
            DB_FATAL("append failed, uri: %s, absolute_path: %s, error: %s", info.uri.c_str(), info.absolute_path.c_str(), ds::Rc2Str(ret));
            return -1;
        } else if (ret != count) {
            info.statis->write_fail_count << 1;
            DB_FATAL("append failed, uri: %s, absolute_path: %s, ret: %ld, count: %u", 
                info.uri.c_str(), info.absolute_path.c_str(), ret, count);
            return -1;
        } else {
            info.statis->write_bytes << ret;
        }
    }

    return count;
}

int64_t AfsExtFileWriter::tell() {
    int64_t len = _afs_rw_infos[0].writer->Tell();
    for (int i = 1; i < _afs_rw_infos.size(); i++) {
        int64_t tmp_len = _afs_rw_infos[i].writer->Tell();
        if (len != tmp_len) {
            DB_FATAL("tell failed, uri: %s, absolute_path: %s, len: %ld, tmp_len: %ld", 
                _afs_rw_infos[i].uri.c_str(), _afs_rw_infos[i].absolute_path.c_str(), len, tmp_len);
            return -1;
        }
    }
    return len;
}

bool AfsExtFileWriter::sync() {
    bool all_succ = true;
    for (auto& info : _afs_rw_infos) {
        int ret = info.writer->Sync(nullptr, nullptr);
        if (ret != ds::kOk) {
            all_succ = false;
            DB_FATAL("sync failed, uri: %s, absolute_path: %s, error: %s", 
                info.uri.c_str(), info.absolute_path.c_str(), ds::Rc2Str(ret));
        }
    }

    return all_succ ? true : false;
}

bool AfsExtFileWriter::close() {
    bool all_succ = true;
    for (auto& info : _afs_rw_infos) {
        if (info.writer != nullptr) {
            int ret = info.fs->CloseWriter(info.writer, nullptr, nullptr);
            info.writer = nullptr;
            if (ret != ds::kOk) {
                all_succ = false;
                DB_FATAL("close failed, uri: %s, absolute_path: %s", info.uri.c_str(), info.absolute_path.c_str());
            }
        }
    }
    
    return all_succ ? true : false;
}

#endif

int64_t CompactionExtFileReader::read(char* buf, uint32_t count, uint32_t offset, bool* eof) {
    if (!_is_open) {
        DB_FATAL("File: %s is not open for reading", _file_name.c_str());
        return -1;
    }
    TimeCost time;
    if (_file_name.find(".log") != std::string::npos) {
        *eof = true;
        return 0;
    }
    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (CompactionSstExtLinker::get_instance()->get_linker_data(_remote_compaction_id, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", _remote_compaction_id.c_str());
        return -1;
    }

    std::string cache_key = _server_address + "_" + _file_name + "_" + std::to_string(offset) + "_" + std::to_string(count);
    std::string cache_value;
    if (_file_name.find(".sst") != std::string::npos && count < FLAGS_compaction_sst_cache_max_block 
            && CompactionSstCache::get_instance()->find(cache_key, &cache_value) == 0) {
        memcpy(buf, cache_value.c_str(), cache_value.size());
        if (count != cache_value.size()) {
            *eof = true;
        }
        linker_data->cache_size++;
        linker_data->read_file_time += time.get_time();
        // cache = true;
        // DB_WARNING("cache remote_compaction_id: %s, cache_key:%s, time:%ld",
        //         _remote_compaction_id.c_str(), cache_key.c_str(), time.get_time());
        return cache_value.size();
    }
    linker_data->not_cache_size++;
    // Prepare the request
    pb::CompactionFileRequest request;
    request.set_remote_compaction_id(_remote_compaction_id);
    request.set_op_type(pb::OP_READ);
    request.set_file_name(_file_name);
    request.set_offset(offset);
    request.set_count(count);

    // Prepare the response and controller
    pb::CompactionFileResponse response;
    brpc::Controller cntl;

    // Call the remote procedure
    pb::StoreService_Stub stub(&_channel);
    stub.query_file_system(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        DB_FATAL("connect with server: %s fail, error:%s, remote_compaction_id:%s, file_name: %s",
                        _server_address.c_str(), cntl.ErrorText().c_str(), _remote_compaction_id.c_str(),
                        _file_name.c_str());
        return -1;
    }

    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("send read request to %s fail, error:%s, remote_compaction_id:%s, file_name: %s",
                        _server_address.c_str(), pb::ErrCode_Name(response.errcode()).c_str(), _remote_compaction_id.c_str(),
                        _file_name.c_str());
        *eof = true;
        return 0;
    }
    
    if (_file_name.find(".sst") != std::string::npos 
            && linker_data->input_file_set.count(_file_name) == 0
            && count < FLAGS_compaction_sst_cache_max_block) {
        CompactionSstCache::get_instance()->add(cache_key, response.data());
    }
    // Copy the data to the buffer
    memcpy(buf, response.data().c_str(), response.data().size());
    if (count != response.data().size()) {
        *eof = true;
    }
    linker_data->read_file_time += time.get_time();
    // if (linker_data->input_file_set.count(_file_name) == 0) {
    //     DB_WARNING("cache_not remote_compaction_id: %s, cache_key:%s, time:%ld",
    //                 _remote_compaction_id.c_str(), cache_key.c_str(), time.get_time());
    // }
    return response.data().size();
}

int64_t CompactionExtFileReader::skip(uint32_t n, bool* eof) {
    // TODO 实现skip功能
    return n;
}

bool CompactionExtFileReader::close() {
    _is_open = false;
    return true;
}

int64_t CompactionExtFileWriter::append(const char* buf, uint32_t count) {
    if (!_is_open) {
        DB_FATAL("File: %s is not open for writing", _file_name.c_str());
        return -1;
    }
    // Prepare the request
    pb::CompactionFileRequest request;
    request.set_op_type(pb::OP_WRITE);
    request.set_file_name(_file_name);
    request.set_data(std::string(buf, count));
    request.set_remote_compaction_id(_remote_compaction_id);
    request.set_offset(_offset);

    // Prepare the response and controller
    pb::CompactionFileResponse response;
    brpc::Controller cntl;

    // Call the remote procedure
    pb::StoreService_Stub stub(&_channel);
    stub.query_file_system(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        DB_FATAL("connect with server: %s fail, error:%s, remote_compaction_id:%s",
                        _server_address.c_str(), cntl.ErrorText().c_str(), _remote_compaction_id.c_str());
        return -1;
    }
    if (response.errcode() != pb::SUCCESS) {
        DB_FATAL("send read request to %s fail, error:%s, remote_compaction_id:%s",
                        _server_address.c_str(), pb::ErrCode_Name(response.errcode()).c_str(), _remote_compaction_id.c_str());
        return -1;
    }
    _offset += count;
    return count;
}

int64_t CompactionExtFileWriter::tell() {
    // Assuming tell functionality is handled remotely
    return _offset;
}

bool CompactionExtFileWriter::sync() {
    return true;
}

bool CompactionExtFileWriter::close() {
    _is_open = false;
    return true;
}

#ifdef BAIDU_INTERNAL

AfsExtFileSystem::~AfsExtFileSystem() {
    for (auto& info : _ugi_infos) {
        if (info.afs != nullptr) {
            info.afs->DisConnect();
        }
    }
}

std::shared_ptr<afs::AFSImpl> AfsExtFileSystem::init(const std::string& uri, const std::string& user, 
                                            const std::string& password, const std::string& conf_file) {
    TimeCost cost;
    std::shared_ptr<afs::AFSImpl> fs(new afs::AFSImpl(uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str()));
    if (fs == nullptr) {
        DB_FATAL("new afs filesystem failed, uri: %s, user: %s, password: %s, conf_file: %s", 
            uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str());
        return nullptr;
    }

    int afs_res = fs->Init(true, false);
    if (afs_res != ds::kOk) {
        DB_FATAL("Init afs filesystem failed, uri: %s, user: %s, password: %s, conf_file: %s, error: %s", 
            uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str(), ds::Rc2Str(afs_res));
        return nullptr;
    }
    // 连接集群后端
    if (!fs->Start()) {
        DB_WARNING("fail to start afs, errno:%d, errmsg:%s", ds::kFail, afs::Rc2Str(ds::kFail));
        return nullptr;
    }
    afs_res = fs->Connect();
    if (afs_res != ds::kOk) {
        DB_FATAL("Connect afs filesystem failed, uri: %s, user: %s, password: %s, conf_file: %s, error: %s", 
            uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str(), ds::Rc2Str(afs_res));
        return nullptr;
    }

    DB_NOTICE("init afs filesystem succ, uri: %s, user: %s, password: %s, conf_file: %s, cost: %ld", 
        uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str(), cost.get_time());  
    return fs;
}

int AfsExtFileSystem::init() {
    // 初始化如果失败直接跳过，后续使用afs时再次初始化，避免afs影响store重启
    for (AfsUgi& ugi_info : _ugi_infos) {
        // afs://yinglong.afs.baidu.com:9902中提取yinglong
        if (_uri_afs_statics.find(ugi_info.uri) == _uri_afs_statics.end()) {
            _uri_afs_statics[ugi_info.uri] = std::make_shared<AfsStatis>(ugi_info.uri.substr(6, ugi_info.uri.find_first_of(".") - 6));
        }
        auto fs = init(ugi_info.uri, ugi_info.user, ugi_info.password, "./conf/client.conf");
        if (fs == nullptr) {
            continue;
        }
        ugi_info.afs = fs;
    }

    return 0;
}

std::vector<AfsRWInfo> AfsExtFileSystem::get_rw_infos_by_full_name(const std::string& full_name) {
    std::string uri;
    std::string user_define_path;
    for (const AfsUgi& ugi_info : _ugi_infos) {
        auto uri_pos  = full_name.find(ugi_info.uri);
        auto path_pos = full_name.find(ugi_info.root_path + "/");
        if (uri_pos != std::string::npos && path_pos != std::string::npos) {
            uri = ugi_info.uri;
            user_define_path = full_name.substr(path_pos + ugi_info.root_path.size() + 1);
        }
    }

    return get_rw_infos(user_define_path);
}

std::vector<AfsRWInfo> AfsExtFileSystem::get_rw_infos(const std::string& user_define_path) {
    std::lock_guard<bthread::Mutex> l(_lock);
    std::vector<AfsRWInfo> afs_infos;
    if (user_define_path.empty()) {
        return afs_infos;
    }

    for (const AfsUgi& ugi_info : _ugi_infos) {
        if (ugi_info.afs == nullptr) {
            DB_FATAL("afs is null, uri: %s", ugi_info.uri.c_str());
            continue;
        }

        AfsRWInfo info;
        info.uri = ugi_info.uri;
        info.absolute_path = ugi_info.root_path + "/" + user_define_path;
        info.fs = ugi_info.afs;
        info.statis = _uri_afs_statics[ugi_info.uri];
        afs_infos.emplace_back(info);
    }
    return afs_infos;
}

// 进程启动时不要求全部afs初始化成功，make_full_name时如果未初始化则再次初始化
std::string AfsExtFileSystem::make_full_name(const std::string& cluster, bool force, const std::string& user_define_path) {
    if (_ugi_infos.empty()) {
        return "";
    }
    int pos = 0;
    if (!cluster.empty()) {
        bool find = false;
        for (const auto& info : _ugi_infos) {
            if (info.cluster_name == cluster) {
                find = true;
                break;
            }
            pos++;
        }

        if (!find) {
            if (force) {
                DB_FATAL("cant find cluster: %s", cluster.c_str());
                return "";
            } else {
                pos = 0;
            }
        }
    } 

    std::string full_name;
    {
        std::lock_guard<bthread::Mutex> l(_lock);
        if (_ugi_infos[pos].afs != nullptr) {
            full_name = _ugi_infos[pos].uri + _ugi_infos[pos].root_path + "/" + user_define_path;
        } else {
            // 放在锁外初始化？OLAPTODO
            auto fs = init(_ugi_infos[pos].uri, _ugi_infos[pos].user, _ugi_infos[pos].password, "./conf/client.conf");
            if (fs == nullptr) {
                DB_FATAL("init afs: %s failed", _ugi_infos[pos].uri.c_str());
                return "";
            }
            _ugi_infos[pos].afs = fs;
            full_name = _ugi_infos[pos].uri + _ugi_infos[pos].root_path + "/" + user_define_path;
        }
    }
    DB_NOTICE("cluster: %s, force: %d, user_define_path: %s, full_name: %s", cluster.c_str(), force, user_define_path.c_str(), full_name.c_str());
    return full_name;
}
#ifdef ENABLE_OPEN_AFS_ASYNC
void AfsExtFileSystem::AfsFileCtrl::action_finish(int64_t ret) {
    {
        std::unique_lock<std::mutex> lck(_mutex);
        if (ret < 0) {
            _fail_cnt++;
        } else {
            if (!_succeeded) {
                _succeeded = true;
                _success_ret = ret;
            }
        }
    }
    // 唤醒主线程
    _cv.notify_one();
}

int64_t AfsExtFileSystem::AfsFileCtrl::wait_onesucc_or_allfail() {
    std::unique_lock<std::mutex> lck(_mutex);
    while (true) {
        if (_succeeded) {
            return _success_ret;
        }

        if (_fail_cnt == _afs_cnt) {
            // 全部失败
            return -1;
        }

        // 等待异步唤醒
        _cv.wait(lck);
    }
    return -1;
}

void AfsExtFileSystem::open_reader_callback(int64_t ret, void* ptr) {
    OpenReaderInfo* open_reader_info = reinterpret_cast<OpenReaderInfo*>(ptr);
    std::shared_ptr<AfsFileCtrl> reader_ctrl = open_reader_info->reader_ctrl;
    ScopeGuard info_guard([open_reader_info](){delete open_reader_info;});
    ScopeGuard ctrl_guard(
        [reader_ctrl, &ret](){
            reader_ctrl->action_finish(ret);
        });
    std::shared_ptr<AfsRWInfo> afs_rw_info = open_reader_info->afs_rw_info;
    if (ret < 0) {
        if (ret == ds::kNoEntry) {
            DB_WARNING("uri: %s, absolute_path: %s, afs open reader failed, errno:%ld, errmsg:%s", 
                        afs_rw_info->uri.c_str(), afs_rw_info->absolute_path.c_str(), ret, ds::Rc2Str(ret));
        } else {
            DB_FATAL("uri: %s, absolute_path: %s, afs open reader failed, errno:%ld, errmsg:%s", 
                        afs_rw_info->uri.c_str(), afs_rw_info->absolute_path.c_str(), ret, ds::Rc2Str(ret));
        }
    } else {
        // 校验文件长度
        ret = -1;
        afs::Reader* reader = afs_rw_info->reader;
        if (nullptr == reader) {
            DB_NOTICE("open afs file failed, file %s may not exist.", afs_rw_info->absolute_path.c_str());
            return;
        }
        int64_t file_size = reader->FileSize();
        if (file_size != open_reader_info->ext_file_size) {
            DB_FATAL("uri: %s, absolute_path: %s, size:%lu not equal to %lu", 
                    afs_rw_info->uri.c_str(), afs_rw_info->absolute_path.c_str(), 
                    file_size, open_reader_info->ext_file_size);
            return;
        }

        afs_rw_info->file_size = open_reader_info->ext_file_size;
        open_reader_info->reader->add_reader(afs_rw_info);
        DB_NOTICE("open uri: %s, absolute_path: %s, total_cost: %ld", 
                afs_rw_info->uri.c_str(), afs_rw_info->absolute_path.c_str(), 
                open_reader_info->cost.get_time());
        afs_rw_info->statis->open_time_cost << open_reader_info->cost.get_time();
        ret = 0;
    }
}
#endif

int AfsExtFileSystem::open_reader(const std::string& full_name, std::shared_ptr<ExtFileReader>* reader) {
    reader->reset();
    uint64_t ext_file_size = 0;
    int ret = get_size_by_external_file_name(&ext_file_size, nullptr, full_name);
    if (ret != 0) {
        DB_FATAL("get size by external file name failed, full_name: %s", full_name.c_str());
        return -1;
    }

    std::vector<AfsRWInfo> tmp_afs_infos = get_rw_infos_by_full_name(full_name);
    std::vector<AfsRWInfo> afs_infos;

#ifdef ENABLE_OPEN_AFS_ASYNC
    std::shared_ptr<AfsFileCtrl> reader_ctrl = std::make_shared<AfsFileCtrl>(tmp_afs_infos.size());
    std::shared_ptr<AfsExtFileReader> tmp_reader = std::make_shared<AfsExtFileReader>(tmp_afs_infos.size());
    bool enable_async = FLAGS_afs_open_reader_async_switch;
#endif


    for (auto& info : tmp_afs_infos) {
#ifdef ENABLE_OPEN_AFS_ASYNC
        if (enable_async) {
            std::shared_ptr<AfsRWInfo> info_ptr = std::make_shared<AfsRWInfo>(info);
            std::unique_ptr<TimeCost> cost = std::make_unique<TimeCost>();
            OpenReaderInfo* open_reader_info = 
                    new OpenReaderInfo(tmp_reader, reader_ctrl, 
                                    info_ptr, ext_file_size);
            afs::ReaderOptions options;
            options.buffer_size = 0; // 关闭预读
            int ret = info_ptr->fs->OpenReaderAsync(info_ptr->absolute_path.c_str(), &(info_ptr->reader), 
                    options, true, open_reader_callback, open_reader_info);
            if (ret != ds::kOk) {
                DB_FATAL("uri: %s, absolute_path: %s, afs open reader failed", 
                    info.uri.c_str(), info.absolute_path.c_str());
                delete open_reader_info;
                continue;
            }
        } else {
#endif
            TimeCost cost;
            int64_t stat_cost = 0;
            struct stat stat_buf;
            int ret = info.fs->Stat(info.absolute_path.c_str(), &stat_buf);
            if (ret != ds::kOk) {
                DB_WARNING("uri: %s, absolute_path: %s not exist, error: %s", info.uri.c_str(), info.absolute_path.c_str(), ds::Rc2Str(ret));
                continue;
            }
            stat_cost = cost.get_time();
            // 校验文件长度
            if (stat_buf.st_size != ext_file_size) {
                DB_FATAL("uri: %s, absolute_path: %s, size:%lu not equal to %lu", info.uri.c_str(), info.absolute_path.c_str(), stat_buf.st_size, ext_file_size);
                continue;
            }

            afs::ReaderOptions options;
            options.buffer_size = 0; // 关闭预读
            info.reader = info.fs->OpenReader(info.absolute_path.c_str(), options, true);
            if (info.reader == nullptr) {
                DB_FATAL("open reader failed, uri: %s, absolute_path: %s, error: %d", info.uri.c_str(), info.absolute_path.c_str(), errno);
                continue;
            }
            info.file_size = ext_file_size;
            afs_infos.emplace_back(info);
            DB_NOTICE("uri: %s, absolute_path: %s, stat cost: %ld, total_cost: %ld", info.uri.c_str(), info.absolute_path.c_str(), stat_cost, cost.get_time());
            info.statis->open_time_cost << cost.get_time();
#ifdef ENABLE_OPEN_AFS_ASYNC
        }
#endif
    }

#ifdef ENABLE_OPEN_AFS_ASYNC
    if (enable_async) {
        if (reader_ctrl->wait_onesucc_or_allfail() != 0) {
            DB_FATAL("open reader failed, path: %s", full_name.c_str());
            return -1;
        }
        *reader = std::move(tmp_reader);
    } else {
#endif
        if (afs_infos.empty()) {
            DB_FATAL("open reader failed, path: %s", full_name.c_str());
            return -1;
        }
        reader->reset(new AfsExtFileReader(afs_infos));
#ifdef ENABLE_OPEN_AFS_ASYNC
    }
#endif
    return 0;
}

int AfsExtFileSystem::open_writer(const std::string& full_name, std::unique_ptr<ExtFileWriter>* writer) {
    writer->reset();
    std::vector<AfsRWInfo> tmp_afs_infos = get_rw_infos_by_full_name(full_name);
    std::vector<AfsRWInfo> afs_infos;

    for (auto& info : tmp_afs_infos) {
        int ret = info.fs->Exist(info.absolute_path.c_str());
        if (ret != ds::kOk) {
            continue;
        }
        info.writer = info.fs->OpenWriter(info.absolute_path.c_str(), afs::WriterOptions(), false);
        if (info.writer == nullptr) {
            DB_FATAL("open writer failed, uri: %s, absolute_path: %s, error: %d", info.uri.c_str(), info.absolute_path.c_str(), errno);
            continue;
        }
        afs_infos.emplace_back(info);
    }

    if (afs_infos.empty()) {
        DB_FATAL("open writer failed, path: %s", full_name.c_str());
        return -1;
    }

    writer->reset(new AfsExtFileWriter(afs_infos));
    return 0;
}

int AfsExtFileSystem::delete_path(const std::string& full_name, bool recursive) {
    std::vector<AfsRWInfo> afs_infos = get_rw_infos_by_full_name(full_name);
    if (afs_infos.empty()) {
        DB_FATAL("get_rw_infos failed, path: %s", full_name.c_str());
        return -1;
    }
    bool fail = false;
    for (auto& info : afs_infos) {
        int ret = info.fs->Exist(info.absolute_path.c_str());
        if (ret == ds::kNoEntry) {
            DB_WARNING("uri: %s, absolute_path: %s not exist, error: %d", info.uri.c_str(), info.absolute_path.c_str(), errno);
            continue;
        } else if (ret != ds::kOk) {
            fail = true;
            DB_FATAL("exist failed, uri: %s, absolute_path: %s, error: %d", info.uri.c_str(), info.absolute_path.c_str(), errno);
            continue;
        }
        ret = info.fs->Delete(info.absolute_path.c_str(), recursive);
        if (ret != ds::kOk) {
            fail = true;
            DB_FATAL("delete failed, uri: %s, absolute_path: %s, error: %d", info.uri.c_str(), info.absolute_path.c_str(), errno);
            continue;
        }
    }

    return fail ? -1 : 0;
}

int AfsExtFileSystem::create(const std::string& full_name) {
    std::vector<AfsRWInfo> afs_infos = get_rw_infos_by_full_name(full_name);
    int fail_cnt = 0;
    for (auto& info : afs_infos) {
        afs::CreateOptions create_opts;
        create_opts.block_size_type = afs::BlockSizeType::BST_64M;
        int ret = info.fs->Create(info.absolute_path.c_str(), create_opts);
        if (ret != ds::kOk) {
            fail_cnt++;
            DB_FATAL("create failed, full_name: %s, error: %s", full_name.c_str(), ds::Rc2Str(ret));
            continue;
        }
    }
    if (afs_infos.size() == fail_cnt) {
        DB_FATAL("create failed, full_name: %s", full_name.c_str());
        return -1;
    }

    return 0;
}

int AfsExtFileSystem::path_exists(const std::string& full_name) {
    std::vector<AfsRWInfo> afs_infos = get_rw_infos_by_full_name(full_name);
    if (afs_infos.empty()) {
        DB_FATAL("get_rw_infos failed, path: %s", full_name.c_str());
        return -1;
    }

    // 一个fs存在则认为存在
    for (auto& info : afs_infos) {
        int ret = info.fs->Exist(info.absolute_path.c_str());
        if (ret == ds::kOk) {
            return 1;
        } else {
            continue;
        }
    }

    return 0;
}

int AfsExtFileSystem::readdir(const std::string& full_name, std::set<std::string>& sub_files) {
    std::vector<AfsRWInfo> afs_infos = get_rw_infos_by_full_name(full_name);
    for (auto& info : afs_infos) {
        int ret = info.fs->Exist(info.absolute_path.c_str());
        if (ret == ds::kNoEntry) {
            continue;
        } else if (ret != ds::kOk) {
            DB_FATAL("exist failed, uri: %s, absolute_path: %s, error: %s", info.uri.c_str(), info.absolute_path.c_str(), ds::Rc2Str(ret));
            continue;
        }
        std::vector<afs::DirEntry> dirents;
        ret = info.fs->Readdir(info.absolute_path.c_str(), &dirents);
        if (ret < 0) {
            DB_FATAL("afs readdir failed: path: %s, error: %s", info.absolute_path.c_str(), ds::Rc2Str(ret));
            continue;
        }
        for (int i = 0; i < dirents.size(); i++) {
            sub_files.insert(dirents[i].name);
        }

        return 0;
    }

    return -1;
}


int ExtFileSystemGC::external_filesystem_gc() {
    if (!FLAGS_need_ext_fs_gc) {
        return 0;
    }

    static TimeCost cost;
    if (cost.get_time() > FLAGS_afs_gc_interval_s * 1000 * 1000LL) {
        cost.reset();
        external_filesystem_gc_do();
    }
    return 0;
}

int ExtFileSystemGC::external_filesystem_gc_do() {
    std::vector<AfsExtFileSystem::AfsUgi> ugi_infos;
    int ret = get_afs_infos(ugi_infos);
    if (ret < 0 || ugi_infos.empty()) {
        DB_FATAL("get afs infos failed");
        return -1;
    }
    static int64_t idx = 0;    
    AfsExtFileSystem::AfsUgi& ugi = ugi_infos[idx++ % ugi_infos.size()];
    std::shared_ptr<AfsExtFileSystem> ext_fs(new AfsExtFileSystem({ ugi }));

    ret = ext_fs->init();
    if (ret < 0) {
        DB_FATAL("init external filesystem failed");
        return -1;
    }
    TimeCost cost;
    std::map<int64_t, std::map<std::string, std::set<std::string>>> table_id_name_partitions;
    ret = get_all_partitions_from_store(table_id_name_partitions);
    if (ret < 0) {
        DB_FATAL("get all partitions from store failed");
        return -1;
    }

    DB_NOTICE("get all partitions from store success, count: %ld, cost: %ld", table_id_name_partitions.size(), cost.get_time());

    return table_gc(ext_fs, table_id_name_partitions);
}

int ExtFileSystemGC::get_all_partitions_from_store(std::map<int64_t, std::map<std::string, std::set<std::string>>>& table_id_name_partitions) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    std::unordered_map<std::string, InstanceDBStatus> instance_info_map;
    factory->get_all_instance_status(&instance_info_map);

    ConcurrencyBthread bth(10);
    bthread::Mutex lock; 
    int dead_count = 0;
    std::atomic<bool> success = {true};
    for (const auto& pair : instance_info_map) {
        std::string store_addr = pair.first;
        if (pair.second.meta_id != 0) {
            // 不处理外挂link表的store
            continue;
        }
        if (pair.second.status == pb::DEAD) {
            DB_WARNING("store %s is dead", store_addr.c_str());
            if (++dead_count >= FLAGS_afs_gc_allow_dead_store_count) {
                // dead超过三个可能store有问题，暂停gc
                success = false;
                break;
            }
            continue;
        }

        auto func = [store_addr, &lock, &success, &table_id_name_partitions]() {
            if (!success.load()) {
                return;
            }
            pb::RegionIds req;
            pb::StoreRes res;
            req.set_query_all_afs_file(true);
            StoreInteract interact(store_addr);
            int ret = interact.send_request("query_region", req, res);
            if (ret < 0) {
                DB_FATAL("send request to store %s failed", store_addr.c_str());
                success = false;
                return;
            }
            if (!res.extra_res().get_afs_path_succ()) {
                DB_FATAL("query region from store %s failed", store_addr.c_str());
                success = false;
                return;
            }

            for (const auto& full_name : res.extra_res().afs_full_names()) {
                std::vector<std::string> split_vec;
                boost::split(split_vec, full_name, boost::is_any_of("."));
                if (split_vec.empty()) {
                    DB_FATAL("split %s failed", full_name.c_str());
                    success = false;
                    return;
                }

                if (split_vec.back() != "extsst") {
                    continue;
                }

                //                                                            partition     regionid_lines_size_timestamp.extsst
                //  baikal_olap/meta_bns/database_name/table_name/table_id/2023-03-01_2023-03-31/26783_1024_1234.extsst
                std::vector<std::string> vec;
                vec.reserve(4);
                boost::split(vec, full_name, boost::is_any_of("/"));
                if (vec.size() < 7) {
                    DB_FATAL("split %s failed", full_name.c_str());
                    success = false;
                    return;
                }

                std::string table_name = vec[vec.size() - 4];
                int64_t table_id = boost::lexical_cast<int64_t>(vec[vec.size() - 3]);
                std::string partition = vec[vec.size() - 2];
                if (check_partition(partition, nullptr, nullptr) < 0) {
                    success = false;
                    return;
                }

                std::lock_guard<bthread::Mutex> l(lock);
                table_id_name_partitions[table_id][table_name].insert(partition);
            }

        };
        bth.run(func);
    }
    bth.join();

    if (!success.load()) {
        table_id_name_partitions.clear();
        return -1;
    }
    return 0;
}

int ExtFileSystemGC::table_gc(std::shared_ptr<ExtFileSystem> ext_fs, const std::map<int64_t, std::map<std::string, std::set<std::string>>>& table_id_name_partitions_map) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    for (const auto& id_name_partitions_pair : table_id_name_partitions_map) {
        int64_t table_id = id_name_partitions_pair.first;
        const auto& name_partitions_map = id_name_partitions_pair.second;
        SmartTable table = factory->get_table_info_ptr(table_id);
        if (table == nullptr) {
            DB_FATAL("table_id %ld not exist", table_id);
            continue;
        }

        std::string start_str;
        if (table->is_range_partition && table->partition_info.has_dynamic_partition_attr()) {
            const auto& dynamic_partition_attr = table->partition_info.dynamic_partition_attr();
            if (!dynamic_partition_attr.has_enable() || dynamic_partition_attr.enable() == false) {
                continue;
            }

            if (!dynamic_partition_attr.has_start() || dynamic_partition_attr.start() >= 0) {
                continue;
            }

            const std::string& time_unit = dynamic_partition_attr.time_unit();
            int32_t start = dynamic_partition_attr.start();
            const pb::PrimitiveType& partition_col_type = table->partition_info.field_info().mysql_type();
            TimeUnit unit;
            if (boost::algorithm::iequals(time_unit, "DAY")) {
                unit = TimeUnit::DAY;
                if (FLAGS_afs_gc_delay_days > 0) {
                    start -= FLAGS_afs_gc_delay_days;
                }
            } else if (boost::algorithm::iequals(time_unit, "MONTH")) {
                unit = TimeUnit::MONTH;
                if (FLAGS_afs_gc_delay_days > 0) {
                    start -= (FLAGS_afs_gc_delay_days/31 + 1);
                }
            } else {
                continue;
            }
            time_t current_ts = ::time(NULL);
            time_t start_ts = current_ts;
            date_add_interval(start_ts, start, unit);
            if (partition_col_type == pb::DATE) {
                timestamp_to_format_str(start_ts, "%Y-%m-%d", start_str);
            } else {
                continue;
            }

        } else {
            continue;
        }

        if (start_str.empty()) {
            continue;
        }

        for (const auto& name_partitions_pair : name_partitions_map) {
            std::string table_name = name_partitions_pair.first;
            const auto& partitions = name_partitions_pair.second;
            if (partitions.empty()) {
                continue;
            }
            std::vector<std::string> vec;
            boost::split(vec, table->name, boost::is_any_of("."));
            if (vec.size() != 2) {
                DB_FATAL("invaild table name: %s", table->name.c_str());
                continue;
            }
            partition_gc(ext_fs, vec[0], table_name, table_id, partitions, start_str);
            column_partition_gc(ext_fs, vec[0], table_name, table_id, start_str);
        }
    }

    return 0;
}

int ExtFileSystemGC::partition_gc(std::shared_ptr<ExtFileSystem> ext_fs, const std::string& database_name, const std::string& table_name_in_store, 
            int64_t table_id, const std::set<std::string>& partitions_in_store, const std::string& start_str) {
    std::string table_path = "baikal_olap/" + FLAGS_meta_server_bns + "/" + database_name + "/" + table_name_in_store + "/" + std::to_string(table_id);
    std::string full_name = ext_fs->make_full_name("", false, table_path);
    if (full_name.empty()) {
        DB_FATAL("local_file: %s make full path failed", table_path.c_str());
        return -1;
    }
    if (ext_fs->path_exists(full_name) != 1) {
        DB_WARNING("table path %s not exist", full_name.c_str());
        return 0;
    }

    std::set<std::string> partitions_in_fs;
    int ret = ext_fs->readdir(full_name, partitions_in_fs);
    if (ret < 0) {
        DB_FATAL("table path %s readdir failed", full_name.c_str());
        return -1;
    }

    int delete_count = 0;
    for (const std::string& p : partitions_in_fs) {
        if (!need_delete_partition(p, start_str)) {
            // 不需要删除的分区后续分区都不需要删除，直接跳出
            break;
        }
        if (partitions_in_store.find(p) == partitions_in_store.end()) {
            std::string partition_full_path = full_name + "/" + p;
            if (delete_count++ < FLAGS_afs_gc_count && FLAGS_afs_gc_enable) {
                // 每个table单次最多删除10个分区
                ext_fs->delete_path(partition_full_path, true);
                DB_NOTICE("table path %s gc", partition_full_path.c_str());
            } else {
                // 仅打日志
                DB_NOTICE("table path %s need gc", partition_full_path.c_str());
            }
        }
    }

    return 0;
}

int ExtFileSystemGC::column_partition_gc(std::shared_ptr<ExtFileSystem> ext_fs, const std::string& database_name, const std::string& table_name_in_store, 
            int64_t table_id, const std::string& start_str) {
    std::string table_path = "baikal_column/" + FLAGS_meta_server_bns + "/" + database_name + "/" + table_name_in_store + "/" + std::to_string(table_id);
    std::string full_name = ext_fs->make_full_name("", false, table_path);
    if (full_name.empty()) {
        DB_FATAL("local_file: %s make full path failed", table_path.c_str());
        return -1;
    }
    if (ext_fs->path_exists(full_name) != 1) {
        DB_WARNING("table path %s not exist", full_name.c_str());
        return 0;
    }

    std::set<std::string> partitions_in_fs;
    int ret = ext_fs->readdir(full_name, partitions_in_fs);
    if (ret < 0) {
        DB_FATAL("table path %s readdir failed", full_name.c_str());
        return -1;
    }

    int delete_count = 0;
    for (const std::string& p : partitions_in_fs) {
        if (!need_delete_partition(p, start_str)) {
            // 不需要删除的分区后续分区都不需要删除，直接跳出
            break;
        }
        std::string partition_full_path = full_name + "/" + p;
        ext_fs->delete_path(partition_full_path, true);
        DB_NOTICE("table path %s gc", partition_full_path.c_str());
    }

    return 0;
}

bool ExtFileSystemGC::need_delete_partition(const std::string& partition, const std::string& start_str) {
    std::string partition_start_date;
    std::string partition_end_date;
    int ret = check_partition(partition, &partition_start_date, &partition_end_date);
    if (ret < 0) {
        return false;
    }

    ExprValue start_expr(pb::DATE, start_str);
    ExprValue partition_end_expr(pb::DATE, partition_end_date);
    if (partition_end_expr.compare(start_expr) < 0) {
        return true;
    }

    return false;
}

int ExtFileSystemGC::check_partition(const std::string& partition, std::string* start_date, std::string* end_date) {
    std::vector<std::string> split_vec;
    boost::split(split_vec, partition, boost::is_any_of("_"));
    if (split_vec.size() != 2) {
        return -1;
    }

    // xxxx-xx-xx_xxxx-xx-xx
    if (partition.size() != 21 || split_vec[0].size() != 10 || split_vec[1].size() != 10) {
        return -1;
    } 

    if (start_date != nullptr) {
        *start_date = split_vec[0];
    }
    if (end_date != nullptr) {
        *end_date = split_vec[1];
    }
    return 0;
}
#endif

int CompactionExtFileSystem::init() {
    return 0;
}

int CompactionExtFileSystem::open_reader(const std::string& full_name, std::shared_ptr<ExtFileReader>* reader) {
    reader->reset(new CompactionExtFileReader(full_name, _address, _remote_compaction_id));
    return 0;
}

int CompactionExtFileSystem::open_writer(const std::string& full_name, std::unique_ptr<ExtFileWriter>* writer) {
    writer->reset(new CompactionExtFileWriter(full_name, _address, _remote_compaction_id));
    return 0;
}

// -1：失败；0：不存在；大于0：存在
int CompactionExtFileSystem::path_exists(const std::string& full_name) {
    pb::CompactionFileResponse response;
    if (0 != external_send_request(pb::OP_PATH_EXISTS, full_name, false, response)) {
        return -1;
    }
    return response.file_info_size();
}

int CompactionExtFileSystem::create(const std::string& full_name) {
    pb::CompactionFileResponse response;
    if (0 != external_send_request(pb::OP_CREATE_DIR, full_name, true, response)) {
        return -1;
    }
    return 0;
}

int CompactionExtFileSystem::get_file_info_list(std::vector<pb::CompactionFileInfo>& file_info_list) {
    pb::CompactionFileResponse response;
    if (0 != external_send_request(pb::OP_GET_FILE_INFO_LIST, "", false, response)) {
        return -1;
    }
    for (const auto& file : response.file_info()) {
        file_info_list.push_back(file);
    }
    return 0;
}

int CompactionExtFileSystem::readdir(const std::string& full_name, std::set<std::string>& file_list) {
    pb::CompactionFileResponse response;
    if (0 != external_send_request(pb::OP_READ_DIR, full_name, false, response)) {
        return -1;
    }
    for (auto& file : response.file_info()) {
        file_list.insert(file.file_path());
    }
    return file_list.size();
}

int CompactionExtFileSystem::delete_remote_copy_file_path() {
    pb::CompactionFileResponse response;
    if (0 != external_send_request(pb::OP_DELETE_PATH, "", true, response)) {
        return -1;
    }
    return 0;
}

int CompactionExtFileSystem::external_send_request(pb::CompactionOpType op_type, 
                                            const std::string& full_name, 
                                            bool recursive,
                                            pb::CompactionFileResponse& response) {
    pb::CompactionFileRequest request;
    request.set_op_type(op_type);
    request.set_remote_compaction_id(_remote_compaction_id);
    request.set_file_name(full_name);
    request.set_recursive(recursive);

    brpc::Controller cntl;
    pb::StoreService_Stub stub(&_channel);
    stub.query_file_system(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        DB_FATAL("send store fail, error:%s, remote_compaction_id:%s, path:%s",
                        cntl.ErrorText().c_str(), _remote_compaction_id.c_str(), full_name.c_str());
        return -1;
    }
    if (response.errcode() == pb::COMPACTION_FILE_NOT_EXIST) {
        // DB_WARNING("file not exist, remote_compaction_id:%s, path:%s",
        //                 _remote_compaction_id.c_str(), full_name.c_str());
        return -1;
    } else if (response.errcode() != pb::SUCCESS) {
        DB_FATAL("send store fail, error:%s, remote_compaction_id:%s, path:%s",
                        pb::ErrCode_Name(response.errcode()).c_str(), _remote_compaction_id.c_str(), full_name.c_str());
        return -1;
    }
    return 0;
} 

std::string CompactionExtFileSystem::make_full_name(const std::string& cluster, bool force, const std::string& user_define_path) {
    return "";
} 

int CompactionExtFileSystem::rename_file(const std::string& src_file_name, const std::string& dst_file_name) {
    DB_FATAL("rename_file not support src:%s, dst:%s", src_file_name.c_str(), dst_file_name.c_str());
    return -1;
}

} // namespace baikaldb
