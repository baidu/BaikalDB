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

#include "common.h"
#include "meta_server_interact.hpp"
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
DEFINE_string(ingest_store_latest_sst, "0", "0 for not ingest; 1 for ingest.");

DEFINE_string(d, "", "POST this data to the http server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, -1, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_string(protocol, "http", "http or h2c");
DEFINE_bool(is_backup, false, "is request sst");
DEFINE_bool(is_send, false, "is send sst");

namespace brpc = baidu::rpc;
namespace bfs = boost::filesystem;
namespace baikaldb {

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
    struct tm *parts = std::localtime(&now_c);
    std::string mon = std::to_string(parts->tm_mon + 1);
    std::string day = std::to_string(parts->tm_mday);

    if (mon.size() == 1) {
        mon = std::string("0") + mon;
    }
    if (day.size() == 1) {
        day = std::string("0") + day;
    }
    return std::to_string(parts->tm_year + 1900) + mon + day;
}
}
namespace baikaldb {

DECLARE_string(meta_server_bns);

int BackUp::get_meta_info(pb::QueryResponse& response) {
    MetaServerInteract msi;
    msi.init_internal(_meta_server_bns);
    pb::QueryRequest request;
    request.set_op_type(pb::QUERY_REGION);

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
                DB_NOTICE("add id[%lld]\n", id);
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
    bool download_flag = false;
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

    DB_NOTICE("table_%lld region_%lld url_%s rsst_begin_send.", table_id, region_id, url.c_str());
    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        DB_WARNING("%s", cntl.ErrorText().c_str());
        return Status::Error;
    }

    auto status_code = cntl.http_response().status_code();

    DB_NOTICE("table_%lld region_%lld url_%s rsst_get_result.", table_id, region_id, url.c_str());
    switch (status_code) {
    case brpc::HTTP_STATUS_OK: {
        if (!cntl.response_attachment().empty()) {
            auto& att = cntl.response_attachment();
            att.cutn(&log_index, sizeof(int64_t));
            DB_NOTICE("get log_index[%lld] all_size[%lld]", log_index, att.size());

            auto outfile_name = table_path  + std::to_string(region_id) + "_" 
                + get_current_time() + "_" + std::to_string(log_index) + ".sst";
            std::ofstream os(outfile_name, std::ios::out | std::ios::binary);
            os << att;
            download_flag = true;

            DB_NOTICE("save_rsst table[%lld] region[%lld] path[%s]", table_id, region_id, outfile_name.c_str());
            status = Status::Succss;
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
        DB_NOTICE("table[%lld] region[%lld] not data in store.", 
            table_id, region_id);
        status = Status::NoData;
        break;
    }
    case brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR: {
        status = Status::Error;
        DB_FATAL("table[%lld] region[%lld] url[%s] store internal error.",
            table_id, region_id, url.c_str());
        break;
    }
    default:
        DB_FATAL("table[%lld] region[%lld] url[%s] request sst error.", 
            table_id, region_id, url.c_str());
        status = Status::Error;
        break;
    }

    return status;
}

void BackUp::send_sst(std::string url, std::string infile, int64_t table_id, int64_t region_id) {
    DB_NOTICE("send sst table[%lld] region[%lld] url[%s], file[%s]", 
        table_id, region_id, url.c_str(), infile.c_str());
    baidu::rpc::Channel channel;
    baidu::rpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.timeout_ms = FLAGS_timeout_ms;
    options.connect_timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    if (channel.Init(url.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        DB_FATAL("send sst table[%lld] region[%lld] Fail to initialize channel", table_id, region_id);
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
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::string data = std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());

    if (data.size() > 0) {
        cntl.request_attachment().append(data);
    } else {
        DB_FATAL("send sst table[%lld] region[%lld] file has no content.", 
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
        DB_NOTICE("upload table_%lld region_%lld url[%s] success", 
            table_id, region_id, url.c_str());
        break;
    }
    case brpc::HTTP_STATUS_PARTIAL_CONTENT: {
        DB_NOTICE("table_%lld region_%lld url[%s] ingest latest sst error.", 
            table_id, region_id, url.c_str());
        break;
    }
    default: {
        DB_FATAL("table_%lld region_%lld url[%s] error.", table_id, region_id, url.c_str());
    }
    }
}

int BackUp::run_backup_region(int64_t table_id, int64_t region_id, const std::string& target_peer) {

    DB_NOTICE("backup table_id[%lld] region_id[%lld]", table_id, region_id);

    auto table_path = FLAGS_backup_dir + "/" + _meta_server_bns + \
        "/" + std::to_string(table_id) + "/";

    butil::File::Error err;
    if (!butil::DirectoryExists(butil::FilePath{table_path}) && 
        !butil::CreateDirectoryAndGetError(butil::FilePath{table_path}, &err)) {
        DB_WARNING("create directory %s error[%d].", table_path.c_str(), err);
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
        DB_NOTICE("same log index table[%lld] region[%lld]", table_id, region_id);
    } else if (data_ret == Status::Succss) {
        auto new_region_files = get_region_files(table_path, region_id, "");
        if (new_region_files.size() > 3) {
            std::remove((table_path + new_region_files[3].filename).c_str());
        }
    } else {
        DB_FATAL("download sst error table[%lld] region[%lld] target peer[%s]", table_id, region_id, target_peer.c_str());
        return -1;
    }
    return 0;
}

void BackUp::gen_backup_task(std::unordered_map<std::string, std::vector<TaskInfo>>& tasks, 
    std::unordered_set<int64_t>& table_ids, 
    const pb::QueryResponse& response) {

    for (const auto& region_info : response.region_infos()) {
        auto table_id = region_info.table_id();
        auto region_id = region_info.region_id();
        if (table_ids.count(table_id) == 1) {
            tasks[region_info.leader()].emplace_back(table_id, region_id);
        }
    }
}
int BackUp::run_backup(std::unordered_set<int64_t>& table_ids) {
    
    pb::QueryResponse response;
    std::atomic<int> failed_num {0};
    if (get_meta_info(response) != 0) {
        DB_WARNING("get meta info error.");
        return -1;
    }
    std::unordered_map<std::string, std::vector<TaskInfo>> tasks;
    gen_backup_task(tasks, table_ids, response);
    DB_NOTICE("backup store size[%d]", tasks.size());

    BthreadCond store_cond {-FLAGS_cluster_concurrent};
    for (auto& pair : tasks) {
        DB_NOTICE("peer_%s store_thread in.", pair.first.c_str());
        store_cond.increase();
        store_cond.wait();
        auto store_thread = [&store_cond, this, &pair, &failed_num]() {
            ON_SCOPE_EXIT(([&store_cond, &pair]() {
                store_cond.decrease_signal();
                DB_NOTICE("peer_%s store_thread out.", pair.first.c_str());
            }));
            BthreadCond cond{-FLAGS_store_concurrent};
            for (auto& task : pair.second) {
                cond.increase();
                cond.wait();
                auto req_thread = [&task, &pair, &cond, this, &failed_num]() {
                    DB_NOTICE("start_run table_%lld region_%lld peer_%s", 
                        task.table_id, task.region_id, pair.first.c_str());
                    int ret = run_backup_region(task.table_id, task.region_id, pair.first);
                    if (ret != 0) {
                        failed_num++;
                        DB_FATAL("backup region error table_%lld region_%lld peer_%s", 
                            task.table_id, task.region_id, pair.first.c_str());
                    } else {
                        DB_NOTICE("backup region end table_%lld region_%lld peer_%s", 
                            task.table_id, task.region_id, pair.first.c_str());
                    }
                    cond.decrease_signal();
                };
                Bthread bth;
                bth.run(req_thread);
            }
            cond.wait(-FLAGS_store_concurrent);
        };

        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(store_thread);
    }
    store_cond.wait(-FLAGS_cluster_concurrent);
    return failed_num;
}

void BackUp::run_upload_per_region(const std::string& meta_server, int64_t table_id, 
    int64_t region_id, std::string upload_date, const google::protobuf::RepeatedPtrField< ::std::string>& peers) {

    std::string table_upload_path = FLAGS_backup_dir + "/" + meta_server + "/" + std::to_string(table_id) + "/";
    
    auto region_files = get_region_files(table_upload_path, region_id, upload_date);
    if (region_files.size() == 0) {
        DB_FATAL("no table[%lld] region[%lld] to upload.", table_id, region_id);
        return;
    }

    auto region_data_sst = table_upload_path + "/" + region_files[0].filename;

    for (const auto& peer : peers) {
        std::string data_url = std::string{"http://"} + peer + "/StoreService/backup_region/upload/" \
            + std::to_string(region_id) + "/data/" + FLAGS_ingest_store_latest_sst;

        DB_NOTICE("upload table_id[%lld], region_id[%lld] to peer[%s] data_url[%s]", 
            table_id, region_id, peer.c_str(), data_url.c_str());

        send_sst(data_url, region_data_sst, table_id, region_id);
    }
}

void BackUp::run_upload_from_region_id_files(const std::string& meta_server, int64_t table_id, std::unordered_set<int64_t> region_ids) {
    pb::QueryResponse response;
    if (get_meta_info(response) != 0) {
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

    run_upload_task(meta_server, task);
}


void BackUp::run_upload_task(const std::string& meta_server, UploadTask& task) {
    ConcurrencyBthread cluster_threads {FLAGS_cluster_concurrent};
        
    DB_NOTICE("task size[%zu] store size[%zu]", 
        task.all_tasks.size(), task.store_task_info.size());

    while (task.all_tasks.size() > 0) {
        auto iter = task.all_tasks.begin();
        for (; iter != task.all_tasks.end();) {
            auto task_ptr = *iter;
            auto table_id = task_ptr->table_id;
            auto region_id = task_ptr->region_id;
            auto is_selected = true;
            for (const auto& peer : task_ptr->peers) {
                if (task.store_task_info[peer] > FLAGS_store_concurrent) {
                    //DB_NOTICE("task skip table_%lld region_%lld", table_id, region_id);
                    is_selected = false; 
                    break;
                }
            }

            if (is_selected) {
                DB_NOTICE("task run table_%lld region_%lld", table_id, region_id);
                for (const auto& peer : task_ptr->peers) {
                    task.store_task_info[peer]++;
                }
                auto peers = task_ptr->peers;
                auto store_thread = [this, &task, table_id, region_id, meta_server, peers](){
                    run_upload_per_region(meta_server, table_id, region_id, _upload_date, peers);         
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
}
void BackUp::run_upload_from_meta_info(const std::string& meta_server, int64_t table_id) {
    pb::QueryResponse response;
    if (get_meta_info(response) != 0) {
        DB_WARNING("get meta info error.");
        return;
    }

    UploadTask task;
    gen_upload_task(task, response, [table_id](const pb::RegionInfo& ri) -> bool {
        return ri.table_id() == table_id;
    });
    run_upload_task(meta_server, task);
}

void BackUp::run_load_from_meta_json_file(std::string& meta_server, std::string meta_json_file) {
    pb::QueryResponse response;
    if (get_meta_info(response) != 0) {
        DB_WARNING("get meta info error.");
        return;
    }
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
    const auto& recover_response = recover_json["recover_response"];
    if (recover_response.isMember("inited_regions") && recover_response["inited_regions"].isArray()) {
        for (const auto& init_region : recover_response["inited_regions"]) {
            if (init_region.isMember("peer_status") && init_region["peer_status"].isString() &&
                init_region["peer_status"].asString() == "STATUS_INITED") {
                    auto table_id = init_region["table_id"].asInt64();
                    auto region_id = init_region["region_id"].asInt64();
                    gen_upload_task(task, response, [table_id, region_id](const pb::RegionInfo& ri) -> bool {
                        return ri.table_id() == table_id && ri.region_id() == region_id;
                    });
            } else {
                DB_WARNING("check peer error.");
            }
        }

        run_upload_task(meta_server, task);

    } else {
        DB_WARNING("check inited_regions error.");
    }
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
                if (get_meta_info(response) != 0) {
                    DB_WARNING("get meta info error.");
                    return;
                }

                for (const auto& region_info : response.region_infos()) {
                    table_ids.insert(region_info.table_id());
                }
                int ret = run_backup(table_ids);
                if (ret != 0) {
                    DB_FATAL("backup %d region failed.", ret);
                }
            } else {
                DB_FATAL("not backup table_id file.");
            }
        } else {
            //根据table_id_file里指定的table_id进行备份。
            auto table_ids = get_ids(FLAGS_table_file);
            int ret = run_backup(table_ids);
            if (ret != 0) {
                DB_FATAL("backup %d region failed.", ret);
            }
        }
    }

    if (FLAGS_is_send) {
        //上传FLAGS_upload_date之前（包含FLAGS_upload_date）的备份。
        //如果不指定，上传最新的。
        _upload_date = FLAGS_upload_date;
        
        DB_NOTICE("send table_id[%lld] meta[%s]", FLAGS_table_id, _meta_server_bns.c_str());
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
                run_load_from_meta_json_file(_meta_server_bns, FLAGS_json_file);
            }
        }
    }
    DB_NOTICE("backup end.");
}

}
