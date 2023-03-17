#include "backup_tool.h"

#include <vector>
#include <string>
#include <atomic>
#include <fstream>
#include <unordered_set>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <baidu/rpc/channel.h>
#include <json/json.h>

#include "meta_server_interact.hpp"
#include "backup_import.h"
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
#ifdef BAIDU_INTERNAL
namespace butil = base;
#endif

DEFINE_string(backup_dir, "./data/", "backup directory file");
DEFINE_string(table_file, "", "backup_table_id");
DEFINE_bool(is_get_info_from_meta, false, "get meta info from meta server.");
DEFINE_bool(is_backup_all_tables, false, "backup all tables");
DEFINE_string(json_file, "./json_file", "upload region ids");
DEFINE_string(upload_date, "", "upload date directory");
DEFINE_int32(table_id, 1, "region id");
DEFINE_int32(store_concurrent, 1, "threads per store");
DEFINE_int32(cluster_concurrent, 10, "the number of store concurrent run");
DEFINE_string(region_file, "", "upload region ids");
DEFINE_int32(ingest_store_latest_sst, 0, "0 for not ingest; 1 for ingest.");

DEFINE_string(d, "", "POST this data to the http server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, -1, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_string(protocol, "http", "http or h2c");
DEFINE_bool(is_backup, false, "is request sst");
DEFINE_bool(is_send, false, "is send sst");
DEFINE_int64(streaming_max_buf_size, 60 * 1024 * 1024LL, "streaming max buf size : 60M");
DEFINE_int64(streaming_idle_timeout_ms, 1800 * 1000LL, "streaming idle time : 1800s");

namespace brpc = baidu::rpc;
namespace bfs = boost::filesystem;
namespace baikaldb {

std::unordered_map<int64_t, TableRegionFiles> get_table_region_files(
    std::unordered_set<int64_t> tids, std::string meta_server_bns, std::string upload_date) {
    std::unordered_map<int64_t, TableRegionFiles> table_ret_map;
    for (auto tid : tids) {

        auto table_path = FLAGS_backup_dir + "/" + meta_server_bns + \
            "/" + std::to_string(tid) + "/";

        bfs::path tpath {table_path};
        TableRegionFiles& region_ret_map = table_ret_map[tid];
        for (auto i = bfs::directory_iterator(tpath); i!=bfs::directory_iterator(); ++i) {
            if (!bfs::is_directory(i->path())) {
                auto filename = i->path().filename().string();
                std::vector<std::string> seps;
                boost::split(seps, filename, boost::is_any_of("_"));
                if (seps.size() == 3) {                        

                    if (upload_date != "" && seps[1] > upload_date) {
                        DB_NOTICE("date[%s] > upload_date[%s] skip.", seps[1].c_str(), upload_date.c_str());
                        continue; 
                    }

                    int64_t index = 0;
                    int64_t region_id = 0;
                    try {
                        index = std::stol(seps[2]);
                        region_id = std::stol(seps[0]);
                    } catch (std::exception& exp) {
                        DB_FATAL("can't convert[%s] to long.", seps[2].c_str());
                        continue;
                    }

                    auto& ret = region_ret_map[region_id];

                    ret.push_back(
                        RegionFile {seps[0], seps[1], index, filename}
                    );
                }
            }
        }
        for (auto& region_file_pair : region_ret_map) {
            auto& region_file_vec = region_file_pair.second;
            std::sort(region_file_vec.begin(), region_file_vec.end(), [](const RegionFile& l, const RegionFile& r){
                if (l.date != r.date) {
                    return l.date > r.date;
                } else {
                    return l.log_index > r.log_index;
                }
            });
        }
    }
    return table_ret_map;
}

std::vector<RegionFile> get_region_files(std::string path, int64_t region_id, std::string upload_date) {
    bfs::path tpath {path};
    std::vector<RegionFile> ret;
    for (auto i = bfs::directory_iterator(tpath); i!=bfs::directory_iterator(); ++i) {
        if (!bfs::is_directory(i->path())) {
            auto filename = i->path().filename().string();
            std::vector<std::string> seps;
            boost::split(seps, filename, boost::is_any_of("_"));
            if (seps.size() == 3 && seps[0] == std::to_string(region_id)) {

                if (upload_date != "" && seps[1] > upload_date) {
                    DB_NOTICE("date[%s] > upload_date[%s] skip.", seps[1].c_str(), upload_date.c_str());
                    continue; 
                }

                int64_t index = 0;
                try {
                    index = std::stol(seps[2]);
                } catch (std::exception& exp) {
                    DB_FATAL("can't convert[%s] to long.", seps[2].c_str());
                    continue;
                }
                ret.push_back(
                    RegionFile {seps[0], seps[1], index, filename}
                );
            }
        }
    }
    std::sort(ret.begin(), ret.end(), [](const RegionFile& l, const RegionFile& r){
        if (l.date != r.date) {
            return l.date > r.date;
        } else {
            return l.log_index > r.log_index;
        }
    });

    return ret;
}

std::string get_current_time(int day_delta = 0) {
    typedef std::chrono::system_clock Clock;
    auto now = Clock::now() + std::chrono::hours(day_delta * 24);
    std::time_t now_c = Clock::to_time_t(now);
    struct tm parts;
    localtime_r(&now_c, &parts);
    std::string mon = std::to_string(parts.tm_mon + 1);
    std::string day = std::to_string(parts.tm_mday);

    if (mon.size() == 1) {
        mon = std::string("0") + mon;
    }
    if (day.size() == 1) {
        day = std::string("0") + day;
    }
    return std::to_string(parts.tm_year + 1900) + mon + day;
}
}
namespace baikaldb {

bool BackUp::_shutdown = false;

int BackUp::get_meta_info(const pb::QueryRequest& request, pb::QueryResponse& response) {
    MetaServerInteract msi;
    msi.init_internal(_meta_server_bns);
    int ret = msi.send_request("query", request, response);
    if (ret != 0) {
        DB_WARNING("send_request to meta error.");
        return -1;
    }
    return 0;
}

void BackUp::init() {}

std::unordered_set<int64_t> get_ids(std::string filename) {
    std::unordered_set<int64_t> ret;
    std::ifstream infile(filename);
    std::string line;
    if (infile.is_open()) {
        while (std::getline(infile, line)) {
            try {
                auto id = std::stol(line);
                DB_NOTICE("add id[%ld]\n", id);
                ret.insert(id);
            } catch (std::exception& exp) {
                DB_WARNING("parse id exp[%s]", exp.what());
                continue;
            }
        }
    }
    
    return ret;
}

Status BackUp::request_sst(std::string url, int64_t table_id, int64_t region_id, 
    std::vector<RegionFile>& rfs, std::string table_path) {

    int64_t log_index = 0;
    Status status;
    baidu::rpc::Channel channel;
    baidu::rpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.timeout_ms = FLAGS_timeout_ms;
    options.connect_timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    //解析url
    if (channel.Init(url.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        DB_WARNING("Fail to initialize channel");
        return Status::Error;
    }

    baidu::rpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    DB_NOTICE("table_%ld region_%ld url_%s rsst_begin_send.", table_id, region_id, url.c_str());
    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        DB_WARNING("%s", cntl.ErrorText().c_str());
        return Status::Error;
    }

    auto status_code = cntl.http_response().status_code();

    DB_NOTICE("table_%ld region_%ld url_%s rsst_get_result.", table_id, region_id, url.c_str());
    switch (status_code) {
    case brpc::HTTP_STATUS_OK: {
        if (!cntl.response_attachment().empty()) {
            auto& att = cntl.response_attachment();
            if (att.copy_to(&log_index, sizeof(int64_t)) != sizeof(int64_t)) {
                status = Status::Error;
                DB_WARNING("not enough space for logindex table[%ld] region[%ld].", table_id, region_id);
            } else {
                DB_NOTICE("get log_index[%ld] all_size[%ld]", log_index, att.size());

                auto outfile_name = table_path  + std::to_string(region_id) + "_" 
                    + get_current_time() + "_" + std::to_string(log_index) + ".sst";
                std::ofstream os(outfile_name, std::ios::out | std::ios::binary);
                os << att;

                DB_NOTICE("save_rsst table[%ld] region[%ld] path[%s]", table_id, region_id, outfile_name.c_str());
                status = Status::Succss;
            }
        } else {
            status = Status::Error;
            DB_WARNING("request sst error. no sst file.");
        }
        break;
    }
    case brpc::HTTP_STATUS_NO_CONTENT: {
        status = Status::SameLogIndex;
        break;
    }
    case brpc::HTTP_STATUS_PARTIAL_CONTENT: {
        //无数据
        DB_NOTICE("table[%ld] region[%ld] not data in store.", 
            table_id, region_id);
        status = Status::NoData;
        break;
    }
    case brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR: {
        status = Status::Error;
        DB_FATAL("table[%ld] region[%ld] url[%s] store internal error.",
            table_id, region_id, url.c_str());
        break;
    }
    default:
        DB_FATAL("table[%ld] region[%ld] url[%s] request sst error.", 
            table_id, region_id, url.c_str());
        status = Status::Error;
        break;
    }

    return status;
}

void BackUp::send_sst(std::string url, std::string infile, int64_t table_id, int64_t region_id) {
    DB_NOTICE("send sst table[%ld] region[%ld] url[%s], file[%s]", 
        table_id, region_id, url.c_str(), infile.c_str());
    baidu::rpc::Channel channel;
    baidu::rpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.timeout_ms = FLAGS_timeout_ms;
    options.connect_timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    if (channel.Init(url.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        DB_FATAL("send sst table[%ld] region[%ld] Fail to initialize channel", table_id, region_id);
        return;
    }

    baidu::rpc::Controller cntl;

    cntl.http_request().uri() = url.c_str();
    cntl.http_request().set_method(baidu::rpc::HTTP_METHOD_POST);
    std::ifstream file(infile.c_str(), std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        DB_WARNING("open file[%s] failed.", infile.c_str());
        return;
    }
    //std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::string data = std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());

    if (data.size() > 0) {
        cntl.request_attachment().append(data);
    } else {
        DB_FATAL("send sst table[%ld] region[%ld] file has no content.,%s", 
            table_id, region_id, infile.c_str());
        return;
    }

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        DB_WARNING("%s", cntl.ErrorText().c_str());
        return;
    }

    auto status_code = cntl.http_response().status_code();

    switch (status_code) {
    case brpc::HTTP_STATUS_OK: {
        DB_NOTICE("upload table_%ld region_%ld url[%s] success", 
            table_id, region_id, url.c_str());
        break;
    }
    case brpc::HTTP_STATUS_PARTIAL_CONTENT: {
        DB_NOTICE("table_%ld region_%ld url[%s] ingest latest sst error.", 
            table_id, region_id, url.c_str());
        break;
    }
    default: {
        DB_FATAL("table_%ld region_%ld url[%s] error.", table_id, region_id, url.c_str());
    }
    }
}

int BackUp::backup_region_streaming_with_retry(int64_t table_id, int64_t region_id, 
    const std::string& target_peer, std::vector<RegionFile>& region_files) {
    TimeCost tc;
    int retry_time = 1;
    std::string peer = target_peer;
    const int64_t max_time = 3600 * 1000 * 1000LL;
    int ret = 0;
    do {
        DB_NOTICE("get table_id[%ld] region_id[%ld] retry_time[%d] peer[%s]", 
            table_id, region_id, retry_time, peer.c_str());
        ret = backup_region_streaming(table_id, region_id, peer, region_files);
        if (ret == 0) {
            return 0;
        }
        int64_t time = tc.get_time();
        if (time > max_time) {
            DB_NOTICE("table_id[%ld] region_id[%ld] retry_time[%ld]", table_id, region_id, time);
            break;
        }

        bthread_usleep(300 * 1000 * 1000LL);
        pb::QueryResponse response;
        pb::QueryRequest request;
        request.set_op_type(pb::QUERY_REGION);
        request.add_region_ids(region_id);

        if (get_meta_info(request, response) != 0) {
            if (response.errcode() == pb::REGION_NOT_EXIST) {
                // 被merge了
                DB_WARNING("region: %ld not exist, maybe merged", region_id);
                return 0;
            }
            DB_WARNING("get meta info error. response: %s", response.ShortDebugString().c_str());
        } else {
            for (const auto& region_info : response.region_infos()) {
                auto tid = region_info.table_id();
                auto rid = region_info.region_id();
                std::vector<std::string> tmp_peers;
                tmp_peers.reserve(response.region_infos_size());
                if (table_id == tid && rid == region_id) {
                    for (auto& tmp_peer : region_info.peers()) {
                        if (tmp_peer != peer) {
                            tmp_peers.emplace_back(tmp_peer);
                        }
                    }
                    // 随机访问一个peer
                    if (tmp_peers.size() > 0) {
                        size_t idx = butil::fast_rand() % tmp_peers.size();
                        if (idx < tmp_peers.size()) {
                            peer = tmp_peers[idx];
                        }
                    }
                    break;
                }
            }
        }
        retry_time++;
    } while (true);
    return ret;
}

int BackUp::backup_region_streaming(int64_t table_id, int64_t region_id, const std::string& target_peer, std::vector<RegionFile>& region_files) {

    DB_NOTICE("backup table_id[%ld] region_id[%ld]", table_id, region_id);

    auto table_path = FLAGS_backup_dir + "/" + _meta_server_bns + \
        "/" + std::to_string(table_id) + "/";

    if (!butil::DirectoryExists(butil::FilePath{table_path})) {
        DB_WARNING("directory %s not exist.", table_path.c_str());
        return -1;
    }
    int64_t current_log_index = region_files.size() > 0 ? region_files[0].log_index : 0;
    DB_NOTICE("send data request target[%s]", target_peer.c_str());
    Status data_ret;
    request_sst_streaming(target_peer, table_id, region_id, region_files, table_path, current_log_index, data_ret);

    if (data_ret == Status::SameLogIndex) {
        DB_NOTICE("same log index table[%ld] region[%ld]", table_id, region_id);
        return 0;
    } else if (data_ret == Status::Succss) {
        DB_NOTICE("backup success table[%ld] region[%ld]", table_id, region_id);
    } else {
        DB_FATAL("download sst error table[%ld] region[%ld] target peer[%s]", table_id, region_id, target_peer.c_str());
        return -1;
    }
    for (auto idx = _backup_times - 1; idx < region_files.size(); ++idx) {
        std::string file_name = table_path + region_files[idx].filename;
        butil::DeleteFile(butil::FilePath(file_name), false); 
        DB_WARNING("table_id: %ld, region_id: %ld, delete file %s", table_id, region_id, file_name.c_str());
    }
    return 0;
}

int BackUp::run_backup_region(int64_t table_id, int64_t region_id, const std::string& target_peer) {

    DB_NOTICE("backup table_id[%ld] region_id[%ld]", table_id, region_id);

    auto table_path = FLAGS_backup_dir + "/" + _meta_server_bns + \
        "/" + std::to_string(table_id) + "/";

    if (!butil::DirectoryExists(butil::FilePath{table_path})) {
        DB_WARNING("directory %s not exist.", table_path.c_str());
        return -1;
    }
    auto region_files = get_region_files(table_path, region_id, "");
    std::string current_log_index = region_files.size() > 0 ? std::to_string(region_files[0].log_index) : "";
    std::string data_url = std::string{"http://"} + target_peer + "/StoreService/backup_region/download/" \
        + std::to_string(region_id) + "/data/" + current_log_index; 

    DB_NOTICE("send data request data_url[%s]  target[%s]", data_url.c_str(), 
        target_peer.c_str());

    auto data_ret = request_sst(data_url, table_id, region_id, region_files, table_path);
    if (data_ret == Status::SameLogIndex) {
        DB_NOTICE("same log index table[%ld] region[%ld]", table_id, region_id);
    } else if (data_ret == Status::Succss) {
        auto new_region_files = get_region_files(table_path, region_id, "");
        for (auto idx = _backup_times; idx < new_region_files.size(); ++idx) {
            std::string file_path = table_path + new_region_files[idx].filename;
            butil::DeleteFile(butil::FilePath(file_path), false); 
            DB_WARNING("delete region file, table[%ld] region[%ld], file[%s]", table_id, region_id, file_path.c_str());
        }
    } else {
        DB_FATAL("download sst error table[%ld] region[%ld] target peer[%s]", table_id, region_id, target_peer.c_str());
        return -1;
    }
    return 0;
}

void BackUp::gen_backup_task(std::unordered_map<std::string, std::vector<TaskInfo>>& tasks, 
    std::unordered_set<int64_t>& table_ids, 
    const pb::QueryResponse& response, 
    std::unordered_map<int64_t, std::set<int64_t>>& regions_from_meta) {

    for (const auto& region_info : response.region_infos()) {
        auto table_id = region_info.table_id();
        auto region_id = region_info.region_id();
        if (table_ids.count(table_id) == 1) {
            std::string valid_peer = region_info.leader();
            if (!_pefered_peer_resource_tag.empty()) {
                auto iter = _resource_tag_to_instance.find(_pefered_peer_resource_tag);
                if (iter != _resource_tag_to_instance.end()) {
                    for (const auto& peer : region_info.peers()) {
                        if (iter->second.find(peer) != iter->second.end()) {
                            valid_peer = peer;
                            break;
                        }
                    }
                }
            }
            tasks[valid_peer].emplace_back(table_id, region_id);
            regions_from_meta[table_id].insert(region_id);
        }
    }
}

int BackUp::run_backup(std::unordered_set<int64_t>& table_ids, 
    const std::function<int(int64_t, int64_t, const std::string&, std::vector<RegionFile>&)>& backup_proc) {
    std::atomic<int> failed_num {0};
    std::unordered_map<std::string, std::vector<TaskInfo>> tasks;

    if (!_pefered_peer_resource_tag.empty()) {
        // 设置了优先访问的集群
        pb::QueryRequest request;
        pb::QueryResponse response;
        request.set_op_type(pb::QUERY_INSTANCE_FLATTEN);
        request.set_resource_tag(_pefered_peer_resource_tag);

        if (get_meta_info(request, response) != 0) {
            DB_WARNING("get resource_tag instances info error: %s", _pefered_peer_resource_tag.c_str());
            return -1;
        }
        for (const auto& ins : response.flatten_instances()) {
            _resource_tag_to_instance[ins.resource_tag()].insert(ins.address());
        }
    }

    std::unordered_map<int64_t, std::set<int64_t>> regions_from_meta;
    pb::QueryResponse response;
    for (auto table_id : table_ids) {
        pb::QueryRequest request;
        request.set_op_type(pb::QUERY_REGION);
        request.set_table_id(table_id);

        if (get_meta_info(request, response) != 0) {
            DB_WARNING("get meta info error.");
            return -1;
        }
        gen_backup_task(tasks, table_ids, response, regions_from_meta);
    }
    
    DB_NOTICE("backup store size[%lu]", tasks.size());

    for (auto tid : table_ids) {
        auto table_path = FLAGS_backup_dir + "/" + _meta_server_bns + \
            "/" + std::to_string(tid) + "/";
        butil::File::Error err;
        if (!butil::DirectoryExists(butil::FilePath{table_path}) && 
            !butil::CreateDirectoryAndGetError(butil::FilePath{table_path}, &err)) {
            DB_FATAL("create directory %s error[%d].", table_path.c_str(), err);
            return -1;
        }
    }

    auto table_region_map = get_table_region_files(table_ids, _meta_server_bns, "");
    BthreadCond store_cond {-FLAGS_cluster_concurrent};
    for (auto& pair : tasks) {
        DB_NOTICE("peer_%s store_thread in.", pair.first.c_str());
        store_cond.increase();
        store_cond.wait();
        auto store_thread = [&store_cond, this, &pair, &failed_num, &backup_proc, &table_region_map]() {
            ON_SCOPE_EXIT(([&store_cond, &pair]() {
                store_cond.decrease_signal();
                DB_NOTICE("peer_%s store_thread out.", pair.first.c_str());
            }));
            BthreadCond cond{-FLAGS_store_concurrent};
            for (auto& task : pair.second) {
                cond.increase();
                cond.wait();
                auto req_thread = [&task, &pair, &cond, this, &failed_num, &backup_proc, &table_region_map]() {
                    DB_NOTICE("start_run table_%ld region_%ld peer_%s", 
                        task.table_id, task.region_id, pair.first.c_str());
                    int ret = backup_proc(task.table_id, task.region_id, pair.first, 
                        table_region_map[task.table_id][task.region_id]);
                    if (ret != 0) {
                        failed_num++;
                        DB_FATAL("backup region error table_%ld region_%ld peer_%s", 
                            task.table_id, task.region_id, pair.first.c_str());
                    } else {
                        DB_NOTICE("backup region end table_%ld region_%ld peer_%s", 
                            task.table_id, task.region_id, pair.first.c_str());
                    }
                    DB_NOTICE("end_run table_%ld region_%ld peer_%s", 
                        task.table_id, task.region_id, pair.first.c_str());
                    cond.decrease_signal();
                };

                if (!_shutdown) {
                    Bthread bth {&BTHREAD_ATTR_NORMAL};
                    bth.run(req_thread);
                }
            }
            cond.wait(-FLAGS_store_concurrent);
        };

        if (!_shutdown) {
            Bthread bth(&BTHREAD_ATTR_NORMAL);
            bth.run(store_thread);
        }
        
    }
    store_cond.wait(-FLAGS_cluster_concurrent);

    for (const auto& table : table_region_map) {
        int64_t table_id = table.first;
        for (const auto& region_file : table.second) {
            int64_t region_id = region_file.first;
            if (regions_from_meta[table_id].find(region_id) == regions_from_meta[table_id].end() 
                && region_file.second.size() > 0) {
                // 被merge了
                std::string max_file_date_s = region_file.second[0].date;
                int max_file_date = strtoll(max_file_date_s.c_str(), NULL, 10);
                int64_t expire_date = get_ndays_date(get_today_date(), -_interval_days * _backup_times);
                if (max_file_date < expire_date) {
                    // 保证在最旧的备份前被merge才能把region对应的数据删除掉
                    auto table_path = FLAGS_backup_dir + "/" + _meta_server_bns + \
                                "/" + std::to_string(table_id) + "/";
                    for (auto f : region_file.second) {
                        std::string file_path = table_path + f.filename;
                        butil::DeleteFile(butil::FilePath(file_path), false); 
                        DB_WARNING("delete region file, table[%ld] region[%ld], file[%s]", table_id, region_id, file_path.c_str());
                    }
                }
            }
        }
    }
    return failed_num;
}

void BackUp::run_upload_per_region(const std::string& meta_server, int64_t table_id, 
    int64_t region_id, std::string upload_date, const google::protobuf::RepeatedPtrField< ::std::string>& peers) {

    std::string table_upload_path = FLAGS_backup_dir + "/" + meta_server + "/" + std::to_string(table_id) + "/";
    
    auto region_files = get_region_files(table_upload_path, region_id, upload_date);
    if (region_files.size() == 0) {
        DB_FATAL("no table[%ld] region[%ld] to upload.", table_id, region_id);
        return;
    }

    auto region_data_sst = table_upload_path + "/" + region_files[0].filename;

    for (const auto& peer : peers) {
        std::string data_url = std::string{"http://"} + peer + "/StoreService/backup_region/upload/" \
            + std::to_string(region_id) + "/data/" + std::to_string(FLAGS_ingest_store_latest_sst);

        DB_NOTICE("upload table_id[%ld], region_id[%ld] to peer[%s] data_url[%s]", 
            table_id, region_id, peer.c_str(), data_url.c_str());

        send_sst(data_url, region_data_sst, table_id, region_id);
    }
}

void BackUp::run_upload_from_region_id_files(const std::string& meta_server, int64_t table_id, std::unordered_set<int64_t> region_ids) {
    pb::QueryResponse response;
    pb::QueryRequest request;
    request.set_op_type(pb::QUERY_REGION);
    request.set_table_id(table_id);
    if (get_meta_info(request, response) != 0) {
        DB_WARNING("get meta info error.");
        return;
    }
    UploadTask task;
    for (auto region_id : region_ids) {
        gen_upload_task(task, response, [region_id, table_id](const pb::RegionInfo& ri) -> bool {
            return ri.region_id() == region_id && ri.table_id() == table_id;
        });
    }
    DB_NOTICE("task size[%zu] store size[%zu]", 
        task.all_tasks.size(), task.store_task_info.size());

    std::unordered_set<int64_t> tids;
    tids.insert(table_id);
    auto table_region_map = get_table_region_files(tids, _meta_server_bns, _upload_date);
    run_upload_task(meta_server, task, table_region_map);
}

void BackUp::run_upload_task(const std::string& meta_server, UploadTask& task, std::unordered_map<int64_t, TableRegionFiles>& table_region_files) {

    std::atomic<int32_t> failed_nums {0};
    ConcurrencyBthread cluster_threads {FLAGS_cluster_concurrent, &BTHREAD_ATTR_NORMAL};
    DB_NOTICE("task size[%zu] store size[%zu]", task.all_tasks.size(), task.store_task_info.size());

    while (task.all_tasks.size() > 0) {
        auto iter = task.all_tasks.begin();
        for (; iter != task.all_tasks.end();) {
            auto task_ptr = *iter;
            auto table_id = task_ptr->table_id;
            auto region_id = task_ptr->region_id;
            auto is_selected = true;
            for (const auto& peer : task_ptr->peers) {
                if (task.store_task_info[peer] > FLAGS_store_concurrent) {
                    //DB_NOTICE("task skip table_%ld region_%ld", table_id, region_id);
                    is_selected = false; 
                    break;
                }
            }

            if (is_selected) {
                DB_NOTICE("task run table_%ld region_%ld", table_id, region_id);
                for (const auto& peer : task_ptr->peers) {
                    task.store_task_info[peer]++;
                }
                auto peers = task_ptr->peers;
                auto store_thread = [this, &task, table_id, region_id, meta_server, peers, &failed_nums, &table_region_files]() {
                    auto ret = run_upload_per_region_streaming(meta_server, table_id, region_id,
                        peers, table_region_files[table_id][region_id]);         
                    if (ret != 0) {
                        ++failed_nums;
                    }
                    for (const auto& peer : peers) {
                        task.store_task_info[peer]--;
                    }
                };
                cluster_threads.run(store_thread);
                iter = task.all_tasks.erase(iter);
            } else {
                ++iter;
            }
        }
        bthread_usleep(10 * 1000);
    }
    cluster_threads.join();
    DB_NOTICE("run_upload_task fail nums[%d]", failed_nums.load());
}
void BackUp::run_upload_from_meta_info(const std::string& meta_server, int64_t table_id) {
    pb::QueryResponse response;
    pb::QueryRequest request;
    request.set_op_type(pb::QUERY_REGION);
    request.set_table_id(table_id);
    if (get_meta_info(request, response) != 0) {
        DB_WARNING("get meta info error.");
        return;
    }

    UploadTask task;
    gen_upload_task(task, response, [table_id](const pb::RegionInfo& ri) -> bool {
        return ri.table_id() == table_id;
    });

    std::unordered_set<int64_t> tids;
    tids.insert(table_id);
    auto table_region_map = get_table_region_files(tids, _meta_server_bns, _upload_date);

    run_upload_task(meta_server, task, table_region_map);
}

void BackUp::run_upload_from_meta_json_file(std::string& meta_server, std::string meta_json_file) {

    std::ifstream ifs(meta_json_file);
    std::string fs_content(
        (std::istreambuf_iterator<char>(ifs)),
        std::istreambuf_iterator<char>());
    Json::Reader reader;
    Json::Value recover_json;
    bool ret = reader.parse(fs_content, recover_json);
    if (!ret) {
        DB_FATAL("fail parse %d", ret);
        return;
    }
    if (!recover_json.isMember("recover_response")) {
        DB_WARNING("no member recover_response.");
        return;
    }

    UploadTask task;
    pb::QueryResponse response;
    pb::QueryRequest request;
    std::unordered_set<int64_t> region_ids;
    request.set_op_type(pb::QUERY_REGION);
    const auto& recover_response = recover_json["recover_response"];
    if (recover_response.isMember("inited_regions") && recover_response["inited_regions"].isArray()) {
        std::unordered_set<int64_t> tids;
        for (const auto& init_region : recover_response["inited_regions"]) {
            if (init_region.isMember("peer_status") && init_region["peer_status"].isString() &&
                init_region["peer_status"].asString() == "STATUS_INITED") {
                    auto table_id = init_region["table_id"].asInt64();
                    tids.insert(table_id);
                    auto region_id = init_region["region_id"].asInt64();
                    request.add_region_ids(region_id);
                    region_ids.insert(region_id);
            } else {
                DB_WARNING("check peer error.");
            }
        }
        if (get_meta_info(request, response) != 0) {
            DB_WARNING("get meta info error.");
            return;
        }
        gen_upload_task(task, response, [&region_ids](const pb::RegionInfo& ri) -> bool {
            return region_ids.count(ri.region_id()) == 1;
        });

        auto table_region_map = get_table_region_files(tids, _meta_server_bns, _upload_date);
        run_upload_task(meta_server, task, table_region_map);

    } else {
        DB_WARNING("check inited_regions error.");
    }
}



void BackUp::request_sst_streaming(std::string url, int64_t table_id, int64_t region_id, 
    std::vector<RegionFile>& rfs, std::string table_path, int64_t log_index, Status& status) {

    DB_NOTICE("request sst streaming region_id[%ld] table_id[%ld] begin", region_id, table_id);
    status = Status::Succss;
    baidu::rpc::Channel channel;
    baidu::rpc::ChannelOptions options;
    options.protocol = baidu::rpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = FLAGS_timeout_ms;
    options.connect_timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    if (channel.Init(url.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        DB_WARNING("Fail to initialize channel");
        status = Status::Error;
        return;
    }
    std::shared_ptr<BackupStreamReceiver> receiver_handle(new BackupStreamReceiver);
    baidu::rpc::Controller cntl;
    baidu::rpc::StreamId stream;
    baidu::rpc::StreamOptions stream_options;
    stream_options.handler = receiver_handle.get();
    stream_options.max_buf_size = FLAGS_streaming_max_buf_size;
    stream_options.idle_timeout_ms = FLAGS_streaming_idle_timeout_ms;
    if (baidu::rpc::StreamCreate(&stream, cntl, &stream_options) != 0) {
        DB_WARNING("Fail to create stream region_id[%ld] table_id[%ld]", region_id, table_id);
        status = Status::Error;
        return;
    }
    std::string outfile_name;
    ON_SCOPE_EXIT(([&status, receiver_handle, region_id, table_id, stream, &outfile_name](){
        DB_NOTICE("request sst streaming region_id[%ld] table_id[%ld] end", region_id, table_id);
        brpc::StreamClose(stream);
        receiver_handle->wait();
        if (receiver_handle->get_status() == pb::StreamState::SS_FAIL ||
            receiver_handle->get_status() == pb::StreamState::SS_PROCESSING) {
            DB_WARNING("sst streaming receiver region_id[%ld] table_id[%ld] stream[%lu] status[%d]", 
                region_id, table_id, stream, int(receiver_handle->get_status()));
            status = Status::Error;
        }
        if (receiver_handle->get_status() != pb::StreamState::SS_SUCCESS 
            && !outfile_name.empty()){
            butil::DeleteFile(butil::FilePath(outfile_name), false); 
            DB_WARNING("table_id: %ld, region_id: %ld, delete file: %s", table_id, region_id, outfile_name.c_str());
        }
        Bthread b;
        b.run([receiver_handle](){
            DB_NOTICE("waiting recive handle.");
            bthread_usleep(240 * 1000 * 1000LL);
        });
    }));

    pb::StoreService_Stub stub(&channel);
    pb::BackupRequest request;
    pb::BackupResponse response;
    request.set_region_id(region_id);
    request.set_log_index(log_index);
    request.set_backup_op(pb::BACKUP_DOWNLOAD);
    
    stub.backup(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        auto errcode = response.errcode();
        if (errcode == pb::BACKUP_SAME_LOG_INDEX) {
            DB_NOTICE("same index region_id[%ld] table_id[%ld] stream[%lu]", 
                region_id, table_id, stream);
            status = Status::SameLogIndex;
            return;
        } else {
            DB_WARNING("Fail to connect stream, %s", cntl.ErrorText().c_str());
            status = Status::Error;
            return;
        }
    }
    auto response_log_index = response.log_index();
    outfile_name = table_path  + std::to_string(region_id) + "_" 
        + get_current_time() + "_" + std::to_string(response_log_index) + ".sst";

    if (log_index == response_log_index) {
        status = Status::SameLogIndex;
        DB_NOTICE("same index region_id[%ld] table_id[%ld] stream[%lu]", 
                region_id, table_id, stream);
        brpc::StreamClose(stream);
        return;
    } 
    if (!receiver_handle->init(outfile_name.c_str())) {
        DB_WARNING("open file %s error stream[%lu] region_%ld", outfile_name.c_str(), stream, region_id);
        status = Status::Error;
        brpc::StreamClose(stream);
        return;
    }
    
    DB_NOTICE("request sst streaming region_id[%ld] table_id[%ld] start process data", region_id, table_id);
    TimeCost tc;
    const int64_t max_time = 1000 * 1000 * 60 * 60LL;
    while (receiver_handle->get_status() == pb::StreamState::SS_INIT ||
        receiver_handle->get_status() == pb::StreamState::SS_PROCESSING) {
        if (_shutdown) {
            DB_NOTICE("shutdown...");
            butil::DeleteFile(butil::FilePath(outfile_name), false); 
            break;
        }
        DB_NOTICE("waiting for reciver handle status[%d] stream[%lu].", 
                (int)receiver_handle->get_status(), stream);
        int64_t time = tc.get_time();
        if (time > max_time || receiver_handle->is_closed()) {
            DB_FATAL("wait too long stream[%lu] time[%ld] or streaming is closed.", stream, time);
            break;
        }
        bthread_usleep(10 * 1000);
    }
}

int BackUp::run_upload_per_region_streaming(const std::string& meta_server, int64_t table_id, 
    int64_t region_id, const google::protobuf::RepeatedPtrField< ::std::string>& peers, 
    const std::vector<RegionFile>& region_files) {

    std::atomic<int32_t> failed_nums {0};

    std::string table_upload_path = FLAGS_backup_dir + "/" + meta_server + "/" + std::to_string(table_id) + "/";
    if (region_files.size() == 0) {
        DB_FATAL("no table[%ld] region[%ld] to upload.", table_id, region_id);
        return -1;
    }

    auto region_data_sst = table_upload_path + "/" + region_files[0].filename;
    ConcurrencyBthread send_sst_work{peers.size(), &BTHREAD_ATTR_NORMAL};
    for (const auto& peer : peers) {
        DB_NOTICE("upload table_id[%ld], region_id[%ld] to peer[%s]", 
            table_id, region_id, peer.c_str());
        send_sst_work.run([this, &peer, &region_data_sst, table_id, region_id, &failed_nums]() {
            while (true) {
                auto ret = send_sst_streaming(peer, region_data_sst, table_id, region_id);
                if (ret == Status::RetryLater) {
                    continue;
                } 
                if (ret != Status::Succss) {
                    ++failed_nums;
                } 
                break;
            }
        });
    }
    send_sst_work.join();
    return failed_nums;
}

Status BackUp::send_sst_streaming(std::string peer, std::string infile, int64_t table_id, int64_t region_id,
                                  bool only_data_sst, int64_t row_size) {
    baidu::rpc::Channel channel;
    baidu::rpc::ChannelOptions options;
    options.protocol = baidu::rpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = FLAGS_timeout_ms;
    options.connect_timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    // https://github.com/apache/incubator-brpc/issues/392
    // streaming rpc和普通rpc混用有问题。
    options.connection_type = "pooled";

    if (channel.Init(peer.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        DB_WARNING("Fail to initialize channel region_id[%ld], table_id[%ld]", region_id, table_id);
        return Status::Error;
    }
    std::shared_ptr<CommonStreamReceiver> receiver_handle{ new CommonStreamReceiver };
    baidu::rpc::Controller cntl;
    baidu::rpc::StreamId stream;
    baidu::rpc::StreamOptions stream_options;
    stream_options.handler = receiver_handle.get();
    stream_options.max_buf_size = FLAGS_streaming_max_buf_size;
    stream_options.idle_timeout_ms = FLAGS_streaming_idle_timeout_ms;
    if (receiver_handle == nullptr) {
        return Status::Error;
    }
    if (baidu::rpc::StreamCreate(&stream, cntl, &stream_options) != 0) {
        DB_WARNING("Fail to create stream");
        return Status::Error;
    }
    bool streaming_has_wait_timeout = false;
    ON_SCOPE_EXIT(([receiver_handle, region_id, table_id, stream, &streaming_has_wait_timeout](){
        if (!streaming_has_wait_timeout) {
            receiver_handle->timed_wait(10 * 60 * 1000 * 1000LL);
        }
        // 当store网络异常，没收到sst，需要在这里close流，不然on_idle_timeout会导致DM core
        brpc::StreamClose(stream);
        Bthread b;
        b.run([receiver_handle](){
            bthread_usleep(240 * 1000 * 1000LL);
        });
    }));

    DB_NOTICE("Created Stream=%ld peer[%s] region_%ld", stream, peer.c_str(), region_id);
    pb::StoreService_Stub stub(&channel);
    pb::BackupRequest request;
    pb::BackupResponse response;
    request.set_region_id(region_id);
    request.set_backup_op(pb::BACKUP_UPLOAD);
    request.set_ingest_store_latest_sst(FLAGS_ingest_store_latest_sst);
    if (only_data_sst) {
        request.set_ingest_store_latest_sst(false);
        size_t file_size = boost::filesystem::file_size(infile);
        request.set_data_sst_to_process_size(file_size);
        request.set_row_size(row_size);
        DB_WARNING("table_id: %ld, region_id: %ld, infile: %s, file_size: %ld, peer: %s, row_size: %lu",
            table_id, region_id, infile.c_str(), file_size, peer.c_str(), row_size);
    }

    stub.backup(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        if (response.errcode() == pb::RETRY_LATER) {
            // retry later的时候, store没有accept stream, 返回[E1003]The server didn't accept the stream
            bthread_usleep(1 * 1000 * 1000);
            DB_WARNING("store reach limit, retry later, region_id: %ld, store: %s", region_id, peer.c_str());
        } else {
            DB_WARNING("Fail to connect stream, region_id: %ld, store: %s, err: %s", 
                region_id, peer.c_str(), cntl.ErrorText().c_str());
        }
        return Status::Error;
    }

    butil::File f(butil::FilePath{infile}, butil::File::FLAG_OPEN);
    if (!f.IsValid()) {
        DB_WARNING("file[%s] is not valid.", infile.c_str());
        return Status::Error;
    }
    int64_t read_ret = 0;
    int64_t read_size = 0;
    const static int BUF_SIZE {2 * 1024 * 1024LL};
    std::unique_ptr<char[]> buf(new char[BUF_SIZE]);
    do {
        read_ret = f.Read(read_size, buf.get(), BUF_SIZE);
        if (read_ret == -1) {
            DB_WARNING("read file[%s] error.", infile.c_str());
            return Status::Error;
        } 
        if (read_ret != 0) {
            DB_DEBUG("region_%ld read: %ld", region_id, read_ret);
            base::IOBuf msg;
            msg.append(buf.get(), read_ret);
            int err = brpc::StreamWrite(stream, msg);
            while (err == EAGAIN) {
                bthread_usleep(10 * 1000);
                err = brpc::StreamWrite(stream, msg);
            }
            if (err != 0) {
                DB_WARNING("write to stream[%lu] error region_%ld", stream, region_id);
                return Status::Error;
            }
        }
        read_size += read_ret;
    } while (read_ret == BUF_SIZE);
    // 等store close流，网络故障情况下，会一直等不到close，需要加超时
    int ret = receiver_handle->timed_wait(10 * 60 * 1000 * 1000LL);
    if (ret < 0) {
        streaming_has_wait_timeout = true;
        return Status::Error;
    }
    // check store真的ingest了sst
    TimeCost wait_time;
    while (wait_time.get_time() < 10 * 60 * 1000 * 1000LL) {
        pb::StreamState state = get_streaming_result_from_store(peer, response.streaming_id(), region_id);
        if (state == pb::StreamState::SS_SUCCESS) {
            return Status::Succss;
        } else if (state == pb::StreamState::SS_FAIL) {
            return Status::Error;
        }
        bthread_usleep(1000 * 1000);
    }
    DB_NOTICE("region_%ld write size: %ld stream %ld", region_id, read_size, stream);
    return Status::Error;
}

Status BackUp::get_region_peers_from_leader(const std::string& peer, int64_t region_id, 
                            std::set<std::string>& peers, std::set<std::string>& unstable_followers) {
    int retry_times = 0;
    TimeCost time_cost;
    std::string leader = peer;

    do {
        baidu::rpc::Channel channel;
        baidu::rpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = 5 * 1000;
        channel_opt.connect_timeout_ms = 1000;
        if (channel.Init(leader.c_str(), &channel_opt) != 0) {
            DB_WARNING("channel init failed, region_id: %ld, addr: %s",
                      region_id, leader.c_str());
            ++retry_times;
            continue;
        }

        baidu::rpc::Controller cntl;
        pb::BackupRequest request;
        pb::BackupResponse response;
        request.set_region_id(region_id);
        request.set_backup_op(pb::BACKUP_QUERY_PEERS);
        pb::StoreService_Stub(&channel).backup(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_WARNING("cntl failed, region_id: %ld, addr: %s, err: %s", 
                region_id, leader.c_str(), cntl.ErrorText().c_str());
            ++retry_times;
            continue;
        }

        if (response.errcode() == pb::SUCCESS) {
            for (const auto& peer : response.peers()) {
                if (peer != "0.0.0.0:0") {
                    peers.insert(peer);
                }
            }

            for (const auto& peer : response.unstable_followers()) {
                unstable_followers.insert(peer);
            }

            return Status::Succss;
        }

        if (response.errcode() == pb::NOT_LEADER) {
            if (response.has_leader() && response.leader() != "0.0.0.0:0") {
                DB_WARNING("region_id: %ld, addr: %s not leader, new_leader: %s", 
                    region_id, leader.c_str(), response.leader().c_str());
                leader = response.leader();
                ++retry_times;
                continue;
            } else {
                DB_WARNING("region_id: %ld, addr: %s not leader", 
                    region_id, leader.c_str());
                return Status::Error;
            }
        } else {
            DB_WARNING("region_id: %ld, addr: %s, errcode: %s", 
                region_id, leader.c_str(), pb::ErrCode_Name(response.errcode()).c_str());
            return Status::Error;
        }

    } while (retry_times < 3);
    
    if (retry_times >= 3) {
        return Status::Error;
    }

    return Status::Succss;
}

pb::StreamState BackUp::get_streaming_result_from_store(const std::string& peer, brpc::StreamId server_id, int64_t region_id) {
    int retry_times = 0;
    TimeCost time_cost;
    std::string leader = peer;

    do {
        baidu::rpc::Channel channel;
        baidu::rpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = 5 * 1000;
        channel_opt.connect_timeout_ms = 1000;
        if (channel.Init(leader.c_str(), &channel_opt) != 0) {
            DB_WARNING("channel init failed, region_id: %ld, addr: %s",
                       region_id, leader.c_str());
            ++retry_times;
            continue;
        }

        baidu::rpc::Controller cntl;
        pb::BackupRequest request;
        pb::BackupResponse response;
        request.set_region_id(region_id);
        request.set_streaming_id(server_id);
        request.set_backup_op(pb::BACKUP_QUERY_STREAMING);
        pb::StoreService_Stub(&channel).backup(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_WARNING("cntl failed, region_id: %ld, addr: %s, err: %s",
                       region_id, leader.c_str(), cntl.ErrorText().c_str());
            ++retry_times;
            continue;
        }
        if (response.errcode() == pb::SUCCESS) {
            return response.streaming_state();
        } else {
            DB_WARNING("region_id: %ld, addr: %s, stream_id: %lu, errcode: %s",
                       region_id, leader.c_str(), server_id, pb::ErrCode_Name(response.errcode()).c_str());
            return pb::StreamState::SS_FAIL;
        }

    } while (retry_times < 3);

    if (retry_times >= 3) {
        return pb::StreamState::SS_INIT;
    }
    return pb::StreamState::SS_INIT;
}


void BackUp::run() {

    DB_NOTICE("backup begin.");

    if (FLAGS_is_backup) {
        DB_NOTICE("run backup.");
        if (FLAGS_table_file == "") {

            if (FLAGS_is_backup_all_tables) {
                //备份所有table_id
                std::unordered_set<int64_t> table_ids;
                pb::QueryResponse response;
                pb::QueryRequest request;
                request.set_op_type(pb::QUERY_REGION);
                if (get_meta_info(request, response) != 0) {
                    DB_WARNING("get meta info error.");
                    return;
                }

                for (const auto& region_info : response.region_infos()) {
                    table_ids.insert(region_info.table_id());
                }
                int ret = run_backup(table_ids, std::bind(&baikaldb::BackUp::backup_region_streaming_with_retry, this, 
                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));
                if (ret != 0) {
                    DB_FATAL("backup %d region failed.", ret);
                }
            } else {
                DB_FATAL("not backup table_id file.");
            }
        } else {
            //根据table_id_file里指定的table_id进行备份。
            auto table_ids = get_ids(FLAGS_table_file);
            int ret = run_backup(table_ids, std::bind(&baikaldb::BackUp::backup_region_streaming_with_retry, this, 
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));
            if (ret != 0) {
                DB_FATAL("backup %d region failed.", ret);
            }
        }
    }

    if (FLAGS_is_send) {
        //上传FLAGS_upload_date之前（包含FLAGS_upload_date）的备份。
        //如果不指定，上传最新的。
        _upload_date = FLAGS_upload_date;
        
        DB_NOTICE("send table_id[%d] meta[%s]", FLAGS_table_id, _meta_server_bns.c_str());
        if (FLAGS_is_get_info_from_meta) {
            //根据table_id、region_file进行上传。
            if (FLAGS_region_file != "") {
                DB_NOTICE("send file from region_id_file [%s]", FLAGS_region_file.c_str());
                run_upload_from_region_id_files(_meta_server_bns, FLAGS_table_id, get_ids(FLAGS_region_file));
            } else {
                run_upload_from_meta_info(_meta_server_bns, FLAGS_table_id);
            }
        } else {
            //根据meta_json提供的table、region上传。
            if (FLAGS_json_file != "") {
                DB_NOTICE("send file meta_json_file[%s]", FLAGS_json_file.c_str());
                run_upload_from_meta_json_file(_meta_server_bns, FLAGS_json_file);
            }
        }
    }
    DB_NOTICE("backup end.");
}

}
