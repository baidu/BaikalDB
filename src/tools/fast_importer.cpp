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

#include "fetcher_tool.h"
#include <signal.h>
#include "baidu/inf/afs-api/client/afs_filesystem.h"
#include <base/arena.h>
#include "backup_import.h"
#include "proto/fc.pb.h"
#include "time.h"
#include "importer_handle.h"
#include "fast_importer.h"
#include "table_record.h"
#include "dm_rocks_wrapper.h"
#include "expr.h"

using namespace afs;
namespace baikaldb {
DEFINE_string(fast_importer_db,                   "HDFS_TASK_DB",           "param database");
DEFINE_string(fast_importer_tbl,                  "FAST_IMPORTER_SUBTASK",  "param table");
DEFINE_int64(fast_importer_block_size_g,          100,                      "default 100G");
DEFINE_int64(fast_importer_check_subtask_s,       30,                       "default 30s");
DEFINE_int64(fast_importer_send_sst_concurrency,  5,                        "default 5");
DECLARE_string(hdfs_mnt_wutai);
DECLARE_string(hdfs_mnt_mulan);
DECLARE_string(hdfs_mnt_khan);
DECLARE_string(param_tbl);
DECLARE_int32(query_retry_times);
DECLARE_bool(null_as_string);

bool FastImporterCtrl::_shutdown = false;

int FastImporterCtrl::parse_done() {
    Json::Reader json_reader;
    Json::Value done_root;
    bool ret = json_reader.parse(_task.done_json, done_root);
    if (!ret) {
        DB_FATAL("fail parse %d, json: %s", ret, _task.done_json.c_str());
        _result << "fail parse json";
        return -1;
    }
    try {
        for (auto& name_type : {"replace", "update"}) {
            if (done_root.isMember(name_type)) {
                for (auto& node : done_root[name_type]) {
                    ImporterHandle handle(REP, nullptr);
                    int ret = handle.init(node, "", _task);
                    if (ret < 0) {
                        DB_FATAL("fail parse %d, json: %s", ret, _task.done_json.c_str());
                        _result << "fail parse json";
                        return -1;
                    }
                    _task.is_replace = (name_type == "replace");
                }
            }
        }
    } catch (Json::LogicError& e) {
        DB_FATAL("fail parse what:%s", e.what());
        return -1;
    }
    return 0;
}

int FastImporterCtrl::init(const FastImportTaskDesc& task_desc) {
    if (FLAGS_hostname == "hostname") {
        DB_FATAL("hostname is default");
        return -1;
    }
    _task = task_desc;
    parse_done();
    if (_task.db.empty() || _task.table.empty()) {
        _result << "db or table is empty";
        return -1;
    }
    return 0;
}

int FastImporterCtrl::query(const std::string& sql) {
    if (_user_baikaldb == nullptr) {
        DB_FATAL("baikaldb sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        return -1;
    }
    int ret = 0;
    int retry = 0;
    int affected_rows = 0;
    do {
        baikal::client::ResultSet result_set;
        ret = _user_baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            affected_rows = result_set.get_affected_rows();
            break;
        }
        bthread_usleep(1000000);
    } while (++retry < FLAGS_query_retry_times);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        return -1;
    }
    return affected_rows;
}

int FastImporterCtrl::rename_table(std::string old_name, std::string new_name) {
    std::string sql = "alter table " + _task.db + "." + old_name
                      + " rename to " + _task.db + "." + new_name;
    // rename 不允许失败，无限重试
    do {
        int ret = query(sql);
        if (ret == 0) {
            break;
        } else {
            DB_FATAL("rename table fail, %s", sql.c_str());
        }
        bthread_usleep(10 * 1000 * 1000L);
    } while (true);
    return 0;
}

int FastImporterCtrl::query_task_baikaldb(std::string sql, baikal::client::ResultSet &result_set) {
    int ret = 0;
    int retry = 0;
    if (_task_baikaldb == nullptr) {
        DB_FATAL("_baikaldb is null");
        return -1;
    }
    do {
        ret = _task_baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000LL);
    }while (++retry < 20);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        return ret;
    }
    DB_TRACE("query %d times, sql:%s affected row:%lu", retry, sql.c_str(), result_set.get_affected_rows());
    return 0;
}

// 创建文件系统
ImporterFileSystemAdaptor* create_filesystem(const std::string& cluster_name, const std::string& user_name, const std::string& password) {
    ImporterFileSystemAdaptor* fs = nullptr;
    if (cluster_name.find("afs") != cluster_name.npos) {
#ifdef BAIDU_INTERNAL
        fs = new AfsFileSystemAdaptor(cluster_name, user_name, password, "./conf/client.conf");
#endif
    } else {
        fs = new PosixFileSystemAdaptor();
    }

    if (fs == nullptr) {
        return nullptr;
    }

    int ret = fs->init();
    if (ret < 0) {
        DB_FATAL("filesystem init failed, cluster_name: %s", cluster_name.c_str());
        return nullptr;
    }
    return fs;
}

void destroy_filesystem(ImporterFileSystemAdaptor* fs) {
    if (fs) {
        fs->destroy();
        delete fs;
    }
}

int FastImporterCtrl::select_new_task(baikal::client::ResultSet& result_set) {
    // fetch子任务，主任务由fetcher_tool领取
    int affected_rows = 0;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    select_vec.emplace_back("*");
    where_map["status"] = "'idle'";
    where_map["resource_tag"] = "'" + FLAGS_resource_tag + "'";
    std::string sql = gen_select_sql(FLAGS_fast_importer_tbl, select_vec, where_map);

    int ret = query_task_baikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str()); 
        return -1;
    }
    
    affected_rows = result_set.get_row_count();
    if (affected_rows > 0) {
        DB_WARNING("select new task row number:%d sql:%s", affected_rows, sql.c_str());
    }
    return affected_rows; 
}

int FastImporterCtrl::update_task_doing() {
    int affected_rows = 0;
    baikal::client::ResultSet result_set;

    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;
    set_map["status"]   = "'doing'";
    set_map["hostname"]   = "'" + FLAGS_hostname + "'";
    set_map["start_time"]   = "now()";
    set_map["import_line"]   = "0";
    set_map["import_diff_line"]   = "0";
    where_map["status"] = "'idle'";
    where_map["id"] = std::to_string(_task.id);
    where_map["table_info"]   = "'" + _task.table_info + "'";

    std::string sql = gen_update_sql(FLAGS_fast_importer_tbl, set_map, where_map);

    int ret = query_task_baikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb failed, sql:%s", sql.c_str()); 
        return -1;
    }

    affected_rows = result_set.get_affected_rows();
    if (affected_rows != 1 ) {
        DB_WARNING("update taskid doing affected row:%d, sql:%s", affected_rows, sql.c_str());
    }
    DB_TRACE("update taskid doing affected row:%d, sql:%s", affected_rows, sql.c_str());
    return affected_rows; 
}


// 只获取子任务
int FastImporterCtrl::fetch_task() {
    int ret = 0;
    bool fetch = false;
    baikal::client::ResultSet result_set;
    
    ret = select_new_task(result_set);
    if (ret <= 0) {
        //等于0则表示没有获取到
        return -2;
    }

    while (result_set.next()) {
        reset();
        result_set.get_string("file_path", &_task.file_path);
        result_set.get_string("meta_bns", &_task.meta_bns);
        result_set.get_string("cluster_name", &_task.cluster_name);
        result_set.get_string("user_name", &_task.user_name);
        result_set.get_string("password", &_task.password);
        result_set.get_string("table_info", &_task.table_info);
        result_set.get_int64("id", &_task.id);
        result_set.get_int64("start_pos", &_task.start_pos);
        result_set.get_int64("end_pos", &_task.end_pos);
        result_set.get_string("local_done_json", &_task.done_json);
        result_set.get_string("baikaldb_resource", &_task.baikaldb_resource);
        // 如果设置 need_iconv，说明文件编码与 charset字段确定不一样，importer将不进行验证，直接将文件内容编码转成 charset 设置值。
        int32_t need_iconv = 0;
        result_set.get_int32("need_iconv", &need_iconv);
        result_set.get_string("charset", &_task.charset);
        result_set.get_int64("old_version", &_task.old_version);
        result_set.get_int64("new_version", &_task.new_version);
        result_set.get_string("table_namespace", &_task.table_namespace);
        result_set.get_string("config", &_task.conf);
        _task.need_iconv = need_iconv != 0;
        auto iter = _baikaldb_map.find(_task.baikaldb_resource);
        if (iter == _baikaldb_map.end()) {
            DB_FATAL("table_info:%s, has not baikaldb_rsource:%s , fail",
                     _task.table_info.c_str(), _task.baikaldb_resource.c_str());
            continue;
        }
        _user_baikaldb = iter->second;
        ret = update_task_doing();
        if (ret != 1) {
            continue;
        }

        if (_task.cluster_name == "wutai") {
            _file_path_prefix = FLAGS_hdfs_mnt_wutai;
        } else if (_task.cluster_name == "mulan") {
            _file_path_prefix = FLAGS_hdfs_mnt_mulan;
        } else if (_task.cluster_name == "khan") {
            _file_path_prefix = FLAGS_hdfs_mnt_khan;
        } else {
            // do nothing
        }
        std::vector<std::string> sub_paths;
        boost::split(sub_paths, _task.file_path, boost::is_any_of(";"));
        for (auto& sub_path : sub_paths) {
            _sub_file_paths.emplace_back(_file_path_prefix + sub_path);
        }

        fetch = true;
        break;
    }

    if (!fetch) {
        return -2;
    }

    return 0;
}

int FastImporterCtrl::update_local_task(const std::map<std::string, std::string>& set_map) {
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> where_map;
    where_map["id"] = std::to_string(_task.id);
    where_map["table_info"]   = "'" + _task.table_info + "'";
    return query_task_baikaldb(gen_update_sql(_is_main_task ? FLAGS_param_tbl : FLAGS_fast_importer_tbl, set_map, where_map), result_set);
}

int FastImporterCtrl::gen_subtasks() {
    auto fs = create_filesystem(_task.cluster_name, _task.user_name, _task.password);
    if (fs == nullptr) {
        DB_FATAL("create_filesystem failed, cluster_name: %s, user_name: %s, password: %s",
             _task.cluster_name.c_str(), _task.user_name.c_str(), _task.password.c_str());
        _result << "create fs fail";
        return -1;
    }

    int64_t fast_importer_subtask_size_g = FLAGS_fast_importer_block_size_g;
    if (!_task.conf.empty()) {
        Json::Reader json_reader;
        Json::Value done_root;
        bool ret1 = json_reader.parse(_task.conf, done_root);
        if (ret1) {
            try {
                if (done_root.isMember("fast_importer_block_size_g")) {
                    fast_importer_subtask_size_g = done_root["fast_importer_block_size_g"].asInt64();
                }
            } catch (Json::LogicError& e) {
                DB_FATAL("fail parse what:%s ", e.what());
            }
        }
    }
    DB_WARNING("cut file by %ldg", fast_importer_subtask_size_g);
    std::vector<std::string> file_paths;
    std::vector<int64_t> file_start_pos;
    std::vector<int64_t> file_end_pos;
    int ret = fs->cut_files(_task.file_path, fast_importer_subtask_size_g * 1024 * 1024 * 1024LL, file_paths, file_start_pos, file_end_pos);
    destroy_filesystem(fs);
    if (ret < 0) {
        DB_WARNING("cut file failed, file_path: %s", _task.file_path.c_str());
        _result << "cut file fail";
        return -1;
    }

    if (file_paths.empty() || file_paths.size() != file_start_pos.size() || file_start_pos.size() != file_end_pos.size()) {
        return -2;
    }

    std::string sql = "INSERT INTO " + FLAGS_fast_importer_db + "." + FLAGS_fast_importer_tbl + 
                    " (table_info, main_id, meta_bns, file_path, start_pos, end_pos, "
                    " status, cluster_name, user_name, password, resource_tag, need_iconv, charset, "
                    " local_done_json, old_version, new_version, table_namespace,baikaldb_resource,config) VALUES ";
    std::string done_json = boost::replace_all_copy(_task.done_json, "\\", "\\\\");  // delim里面有\，需要转成\\生成子任务
    done_json = boost::replace_all_copy(done_json, "'", "\\'");
    std::string config = boost::replace_all_copy(_task.conf, "\\", "\\\\"); 
    config = boost::replace_all_copy(config, "'", "\\'");
    for (int i = 0; i < file_paths.size(); i++) {
        std::string path = file_paths[i].substr(_file_path_prefix.size(), file_paths[i].size() - _file_path_prefix.size());
        DB_WARNING("_file_path_prefix: %s file_paths[i]: %s", _file_path_prefix.c_str(), file_paths[i].c_str());
        //生成子任务
        sql += "(";
        sql += "'" + _task.table_info + "',";
        sql += std::to_string(_task.main_id) + ",";
        sql += "'" + _task.meta_bns + "',";
        sql += "'" + path + "',";
        sql += std::to_string(file_start_pos[i]) + ",";
        sql += std::to_string(file_end_pos[i]) + ",";
        sql += "'idle',";
        sql += "'" + _task.cluster_name + "',";
        sql += "'" + _task.user_name + "',";
        sql += "'" + _task.password + "',";
        sql += "'" + FLAGS_resource_tag + "',";
        sql += std::to_string(_task.need_iconv ? 1 : 0) + ",";
        sql += "'" + _task.charset + "',";
        sql += "'" + done_json + "',";
        sql += std::to_string(_task.old_version) + ",";
        sql += std::to_string(_task.new_version) + ",";
        sql += "'" + _task.table_namespace + "',";
        sql += "'" + _task.baikaldb_resource + "',";
        sql += "'" + config + "'";
        sql += "),";
    }

    sql.pop_back();

    baikal::client::ResultSet result_set;
    ret = query_task_baikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb failed, sql:%s", sql.c_str()); 
        _result << "query baikaldb fail";
        return -1;
    }

    int affected_rows = result_set.get_affected_rows();
    DB_WARNING("update taskid doing affected row:%d, sql:%s", affected_rows, sql.c_str());
    return 0; 
}

int FastImporterCtrl::has_gen_subtasks(bool& has_gened) {
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;
    baikal::client::ResultSet result_set;
    select_vec.emplace_back("id");
    where_map["main_id"] = std::to_string(_task.main_id);
    where_map["resource_tag"] = "'" + FLAGS_resource_tag + "'";
    where_map["table_info"] = "'" + _task.table_info + "'";
    where_map["old_version"] = std::to_string(_task.old_version);
    where_map["new_version"] = std::to_string(_task.new_version);
    std::string sql = gen_select_sql(FLAGS_fast_importer_tbl, select_vec, where_map);

    int ret = query_task_baikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str());
        return -1;
    }
    has_gened = result_set.get_row_count() > 0;
    return 0;
}

int FastImporterCtrl::check_subtasks(int& todo_cnt, int& doing_cnt, int& success_cnt, int& fail_cnt) {
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;
    baikal::client::ResultSet result_set;

    select_vec.emplace_back("status");
    select_vec.emplace_back("import_line");
    select_vec.emplace_back("import_diff_line");
    where_map["main_id"] = std::to_string(_task.main_id);
    where_map["resource_tag"] = "'" + FLAGS_resource_tag + "'";
    where_map["table_info"] = "'" + _task.table_info + "'";
    where_map["old_version"] = std::to_string(_task.old_version);
    where_map["new_version"] = std::to_string(_task.new_version);
    std::string sql = gen_select_sql(FLAGS_fast_importer_tbl, select_vec, where_map);

    int ret = query_task_baikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str()); 
        return -1;
    }
    _import_line = 0;
    _import_diff_line = 0;
    todo_cnt = 0;
    doing_cnt = 0;
    success_cnt = 0;
    fail_cnt = 0;
    while(result_set.next()) {
        std::string status;
        int64_t import_line = 0;
        int64_t import_diff_line = 0;
        result_set.get_string("status", &status);
        if (status == "doing") {
            doing_cnt++;
        } else if (status == "idle") {
            todo_cnt++;
        } else if (status == "success") {
            success_cnt++;
        } else if (status == "fail") {
            fail_cnt++;
        }
        result_set.get_int64("import_line", &import_line);
        _import_line += import_line;
        result_set.get_int64("import_diff_line", &import_diff_line);
        _import_diff_line += import_diff_line;
    }

    return 0; 
}

// 查找本节点做过的快速导入子任务，可能由于进程crash导致任务中断
// return -1:fail; 0:没有遗留任务; 1:有遗留任务
int FastImporterCtrl::handle_restart() {
    int ret = 0;
    int affected_rows = 0;
    _is_main_task = false;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    std::string table_name;
    select_vec.emplace_back("*");
    where_map["hostname"] = "'" + FLAGS_hostname + "'";
    where_map["status"] = "'doing'";
    std::string sql = gen_select_sql(FLAGS_fast_importer_tbl, select_vec, where_map);

    ret = query_task_baikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb failed, sql:%s", sql.c_str());
        return -1;
    }

    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return 0;
    }
    
    while (result_set.next()) {
        reset();
        result_set.get_string("table_info", &_task.table_info);
        result_set.get_int64("id", &_task.id);
        result_set.get_int64("main_id", &_task.main_id);

        // 更新重试次数,状态重置idle
        std::map<std::string, std::string> set_map;
        set_map["retry_times"]   = "retry_times + 1";
        set_map["status"]   = "'idle'";
        update_local_task(set_map);
    }

    return 0;
}

int FastImporterCtrl::get_table_schema() {
    // 获取table信息
    std::string table_name = _task.table;
    if (_task.is_replace) {
        table_name += "_tmp";
    }
    int ret = _meta_interact.init_internal(_task.meta_bns);
    if (ret < 0) {
        DB_FATAL("init meta_interact fail, meta_bns: %s", _task.meta_bns.c_str());
        _result << "init meta_interact fail";
        return -1;
    }
    pb::QueryRequest req;
    req.set_op_type(pb::QUERY_SCHEMA);
    req.set_namespace_name(_task.table_namespace);
    req.set_database(_task.db);
    req.set_table_name(table_name);
    pb::QueryResponse res;
    ret = _meta_interact.send_request("query", req, res);
    if (ret < 0 || res.schema_infos_size() == 0 || res.region_infos_size() == 0) {
        DB_FATAL("get_table_and_regions fail, db: %s, table: %s", _task.db.c_str(), _task.table.c_str());
        _result << "get table schema fail";
        return -1;
    }

    // 更新主表schema和region信息
    _schema_info = res.schema_infos(0);
    _region_infos = res.region_infos();

    // 检测是否不支持快速导入
    for (const auto& idx : _schema_info.indexs()) {
        if (idx.index_type() == pb::I_FULLTEXT) {
            DB_FATAL("not support fulltext db: %s, table: %s", _task.db.c_str(), _task.table.c_str());
            _result << "not support fulltext";
            return -1;
        }
    }

    // 拿到所有的store
    _stores.clear();
    for (auto& region : _region_infos) {
        for (auto& peer : region.peers()) {
            _stores.insert(peer);
        }
    }

    // 随机选store获取rocksdb的配置
    for (auto& peer : res.region_infos(0).peers()) {
        uint64_t level0 = 0;
        uint64_t compaction_size = 0;
        ret = get_store_rocks_statistic(peer, level0, compaction_size,
                                        _slowdown_write_sst_cnt, _stop_write_sst_cnt,
                                        _rocks_soft_pending_compaction_g, _rocks_hard_pending_compaction_g);
        if (ret == 0) {
            break;
        }
    }
    if (ret < 0) {
        DB_FATAL("get rocksdb options fail, db: %s, table: %s", _task.db.c_str(), _task.table.c_str());
    }
    return 0;
}

int FastImporterCtrl::get_store_rocks_statistic(const std::string& store,
                                                uint64_t& level0_sst,
                                                uint64_t& compaction_size,
                                                uint64_t& slowdown_write_sst_cnt,
                                                uint64_t& stop_write_sst_cnt,
                                                uint64_t& rocks_soft_pending_compaction_g,
                                                uint64_t& rocks_hard_pending_compaction_g) {
    int retry_times = 0;
    TimeCost time_cost;

    do {
        baidu::rpc::Channel channel;
        baidu::rpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = 5 * 1000;
        channel_opt.connect_timeout_ms = 1000;
        if (channel.Init(store.c_str(), &channel_opt) != 0) {
            DB_WARNING("channel init failed, addr: %s", store.c_str());
            ++retry_times;
            continue;
        }

        baidu::rpc::Controller cntl;
        pb::RocksStatisticReq request;
        pb::RocksStatisticRes response;
        request.add_keys("slowdown_write_sst_cnt");
        request.add_keys("stop_write_sst_cnt");
        request.add_keys("rocks_soft_pending_compaction_g");
        request.add_keys("rocks_hard_pending_compaction_g");
        pb::StoreService_Stub(&channel).get_rocks_statistic(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            DB_WARNING("cntl failed, addr: %s, err: %s", store.c_str(), cntl.ErrorText().c_str());
            ++retry_times;
            continue;
        }

        if (response.errcode() == pb::SUCCESS) {
            level0_sst = response.level0_sst_num();
            compaction_size = response.compaction_data_size();
            for(int i = 0; i < response.key_size(); ++i) {
                if (response.key(i) == "slowdown_write_sst_cnt") {
                    slowdown_write_sst_cnt = strtoull(response.value(i).c_str(), NULL, 10);
                    if (slowdown_write_sst_cnt > 10000) {
                        slowdown_write_sst_cnt /= 10000;
                    }
                } else if (response.key(i) == "stop_write_sst_cnt") {
                    stop_write_sst_cnt = strtoull(response.value(i).c_str(), NULL, 10);
                    if (stop_write_sst_cnt > 10000) {
                        stop_write_sst_cnt /= 10000;
                    }
                } else if (response.key(i) == "rocks_soft_pending_compaction_g") {
                    rocks_soft_pending_compaction_g = strtoull(response.value(i).c_str(), NULL, 10);
                    if (rocks_soft_pending_compaction_g > 10000) {
                        rocks_soft_pending_compaction_g /= 10000;
                    }
                } else if (response.key(i) == "rocks_hard_pending_compaction_g") {
                    rocks_hard_pending_compaction_g = strtoull(response.value(i).c_str(), NULL, 10);
                    if (rocks_hard_pending_compaction_g > 10000) {
                        rocks_hard_pending_compaction_g /= 10000;
                    }
                }
            }
            return 0;
        } else {
            DB_WARNING("addr: %s, errcode: %s", store.c_str(), pb::ErrCode_Name(response.errcode()).c_str());
            return -1;
        }
    } while (retry_times < 3);

    if (retry_times >= 3) {
        return -1;
    }
    return -1;
}

int FastImporterCtrl::wait_cluster_rocksdb_normal() {
    TimeCost wait_normal_cost;
    while (true) {
        if (wait_normal_cost.get_time() > 10 * 60 * 1000 * 1000LL) {
            // 10分钟更新一次store列表，防止有store挂了或者迁移，一直卡住给这个store发请求
            wait_normal_cost.reset();
            int ret = get_table_schema();
            if (ret < 0) {
                return -1;
            }
        }
        std::atomic<int64_t> abnormal_stores{0};
        ConcurrencyBthread send_bth(FLAGS_fast_importer_send_sst_concurrency);
        for (const auto& store : _stores) {
            auto send_fn = [this, store, &abnormal_stores]() {
                uint64_t level0_sst = 0;
                uint64_t compaction_size = 0;
                uint64_t slowdown_write_sst_cnt = 0;
                uint64_t stop_write_sst_cnt = 0;
                uint64_t rocks_soft_pending_compaction_g = 0;
                uint64_t rocks_hard_pending_compaction_g = 0;
                int ret = get_store_rocks_statistic(store, level0_sst, compaction_size,
                                                slowdown_write_sst_cnt, stop_write_sst_cnt,
                                                rocks_soft_pending_compaction_g, rocks_hard_pending_compaction_g);
                if (ret < 0
                    || level0_sst > slowdown_write_sst_cnt
                    || compaction_size / 1073741824ull > rocks_soft_pending_compaction_g) {
                    abnormal_stores++;
                    DB_WARNING("wait store: %s rocksdb abnormal", store.c_str());
                }
            };
            if (abnormal_stores.load() == 0) {
                send_bth.run(send_fn);
            }
        }
        send_bth.join();
        if (abnormal_stores.load() == 0) {
            return 0;
        }
        bthread_usleep(60 * 1000 * 1000);
    }
    return 0;
}

/*
 * 主任务开始执行的时候：
 * 1. 将table设置为in_fast_importer状态
 *      meta维护了resource_tag -> table in_fast_importer cnt的计数
 *      meta调度，如果集群有处于改状态的表，不进行peer_load_balance / migrate
 *      store检测分裂，region所在表出于该状态，不会进行分裂（即屏蔽了split_lines和use_approximate_size_to_split）
 * 2. 将要集群store rocksdb的四个参数，直接调整为原来的1万倍，避免stall
 *      meta 计数0->1，则将参数（* 10000）下发到集群所有store
 *
 * 主任务结束的时候：
 * 1. 检测集群所有store rocksdb状态
 * 2. table取消in_fast_importer状态
 * 3. 将要集群store rocksdb的四个参数，恢复原来的值
 *      meta 计数1->0，则将参数（/ 10000）下发到集群所有store
 */
int FastImporterCtrl::update_cluster_params(bool load_finish) {
    std::string table = _task.table;
    if (_task.is_replace) {
        table += "_tmp";
    }
    if (!load_finish) {
        DB_WARNING("SCHEMA: %s", _schema_info.ShortDebugString().c_str());
        std::unordered_map<std::string, std::string> kvs = {
                {"slowdown_write_sst_cnt", std::to_string(_slowdown_write_sst_cnt * 10000)},
                {"stop_write_sst_cnt", std::to_string(_stop_write_sst_cnt * 10000)},
                {"rocks_soft_pending_compaction_g", std::to_string(_rocks_soft_pending_compaction_g * 10000)},
                {"rocks_hard_pending_compaction_g", std::to_string(_rocks_hard_pending_compaction_g * 10000)},
        };
        pb::MetaManagerRequest request;
        pb::MetaManagerResponse response;
        request.set_op_type(pb::OP_UPDATE_SCHEMA_CONF);
        auto table_info = request.mutable_table_info();
        table_info->set_table_name(table);
        table_info->set_database(_task.db);
        table_info->set_namespace_name(_task.table_namespace);
        table_info->set_resource_tag(_schema_info.resource_tag());
        table_info->set_table_id(_schema_info.table_id());
        auto schema_conf = table_info->mutable_schema_conf();
        schema_conf->set_in_fast_import(true);
        auto instance_params = request.mutable_instance_params();
        auto instance_param = instance_params->Add();
        instance_param->set_resource_tag_or_address(_schema_info.resource_tag());
        for (auto& kv : kvs) {
            auto params = instance_param->mutable_params()->Add();
            params->set_key(kv.first);
            params->set_value(kv.second);
        }
        int ret = _meta_interact.send_request("meta_manager", request, response);
        if (ret < 0) {
            DB_FATAL("update cluster params fail before load data");
            return -1;
        }
    } else {
        // check rocksdb
        wait_cluster_rocksdb_normal();
        // 导数据结束后恢复各参数
        std::unordered_map<std::string, std::string> kvs = {
                {"slowdown_write_sst_cnt", std::to_string(_slowdown_write_sst_cnt)},
                {"stop_write_sst_cnt", std::to_string(_stop_write_sst_cnt)},
                {"rocks_soft_pending_compaction_g", std::to_string(_rocks_soft_pending_compaction_g)},
                {"rocks_hard_pending_compaction_g", std::to_string(_rocks_hard_pending_compaction_g)},
        };
        pb::MetaManagerRequest request;
        pb::MetaManagerResponse response;
        request.set_op_type(pb::OP_UPDATE_SCHEMA_CONF);
        auto table_info = request.mutable_table_info();
        table_info->set_table_name(table);
        table_info->set_database(_task.db);
        table_info->set_namespace_name(_task.table_namespace);
        table_info->set_resource_tag(_schema_info.resource_tag());
        table_info->set_table_id(_schema_info.table_id());
        auto schema_conf = table_info->mutable_schema_conf();
        schema_conf->set_in_fast_import(false);
        auto instance_params = request.mutable_instance_params();
        auto instance_param = instance_params->Add();
        instance_param->set_resource_tag_or_address(_schema_info.resource_tag());
        for (auto& kv : kvs) {
            auto params = instance_param->mutable_params()->Add();
            params->set_key(kv.first);
            params->set_value(kv.second);
        }
        int ret = _meta_interact.send_request("meta_manager", request, response);
        if (ret < 0) {
            DB_FATAL("update cluster params fail after load data");
            return -1;
        }
    }
    return 0;
}

int FastImporterCtrl::run_main_task() {
    _is_main_task = true;
    int ret = get_table_schema();
    if (ret < 0) {
        return -1;
    }
    // 查询是否已经生成了子任务记录
    bool gen_subtask_already = false;
    ret = has_gen_subtasks(gen_subtask_already);
    if (ret < 0) {
        _result << "get subtasks fail";
        return -1;
    }
    // 生成子任务
    if (!gen_subtask_already) {
        // 生成子任务之前先让表禁止分裂，调大rocksdb参数等
        ret = update_cluster_params(false);
        if (ret < 0) {
            _result << "update_cluster_params fail before load data";
            return ret;
        }
        // replace truncate tmp table
        if (_task.is_replace) {
            std::string sql  = "truncate table " + _task.db + "." + _task.table + "_tmp";
            ret = query(sql);
            if (ret < 0) {
                _result << sql + "; truncate table fail";
                DB_FATAL("truncate table fail");
                return -1;
            }
        }
        bthread_usleep(60 * 1000 * 1000LL);
        ret = gen_subtasks();
        if (ret == -2) {
            _result << " no subtasks";
            return 0;
        } else if (ret < 0) {
            _result << " gen_subtask fail";
            return ret;
        }
    }

    int todo_cnt = 0;
    int doing_cnt = 0;
    int success_cnt = 0;
    int fail_cnt = 0;
    while(!_shutdown) {
        bthread_usleep_fast_shutdown(FLAGS_fast_importer_check_subtask_s * 1000 * 1000LL, _shutdown);
        ret = check_subtasks(todo_cnt, doing_cnt, success_cnt, fail_cnt);
        if (ret < 0) {
            DB_FATAL("check_subtasks fail");
            continue;
        }

        // 更新进度
        std::ostringstream progressing;
        progressing << "todo_cnt: " << todo_cnt << ", doing_cnt: " << doing_cnt << ", success_cnt: "
                    << success_cnt << ", fail_cnt: " << fail_cnt;
        std::map<std::string, std::string> set_map;
        set_map["progress"] = "'" + progressing.str() + "'";
        set_map["import_line"] = std::to_string(_import_line);
        update_local_task(set_map);

        if (todo_cnt == 0 && doing_cnt == 0) {
            if (fail_cnt > 0) {
                _result << " has failed subtask";
                return -1;
            }
            break;
        }
    }
    ret = update_cluster_params(true);
    if (ret < 0) {
        _result << "update table split_lines fail after load data";
        return ret;
    }
    if (_task.is_replace) {
        rename_table(_task.table, _task.table + "_tmp2");
        rename_table(_task.table + "_tmp", _task.table);
        rename_table(_task.table + "_tmp2", _task.table + "_tmp");
    }
    _result << "success_cnt: " << success_cnt << ", fail_cnt: " << fail_cnt;
    return 0;
}

int FastImporterCtrl::import_to_baikaldb(FastImporterImpl& fast_importer, 
        const FieldsFunc& fields_func, const SplitFunc& split_func, const ConvertFunc& convert_func) {
    auto fs = create_filesystem(_task.cluster_name, _task.user_name, _task.password);
    if (fs == nullptr) {
        DB_FATAL("create_filesystem failed, cluster_name: %s, user_name: %s, password: %s",
                 _task.cluster_name.c_str(), _task.user_name.c_str(), _task.password.c_str());
        _result << "create fs fail";
        fast_importer.close();
        return -1;
    }
    ON_SCOPE_EXIT(([this, &fs]() {
        destroy_filesystem(fs);
    }));
    
    size_t i = 0;
    size_t all_files = _sub_file_paths.size();

    auto progress_func = [this, &fast_importer, &i, &all_files](const std::string& progress_str) {
        std::string str = "file: " + std::to_string(i) + "/" +  std::to_string(all_files) + ", " + progress_str;
        DB_WARNING("str: %s", str.c_str());
        fast_importer.progress_dialog(str);
    };
    auto finish_block_func = [this](std::string path, int64_t start_pos, int64_t end_pos, BlockHandleResult*, bool file_finish) {
        // 快速导入不支持断点续传
        DB_WARNING("path: %s:%ld:%ld finished", path.c_str(), start_pos,end_pos);
    };
    TimeCost time;
    {
        // 需要保证ImporterImpl析构才真正结束,执行子任务之前把rocksdb清空
        auto s = DMRocksWrapper::get_instance()->clean_roscksdb();
        if (!s.ok()) {
            DB_FATAL("clean rocksdb fail, err: %s", s.ToString().c_str());
        }
        for (i = 0; i < _sub_file_paths.size(); ++i) {
            ImporterImpl Impl(_sub_file_paths[i], fs, fields_func, split_func, convert_func, _task.conf,
                              i == 0 ? _task.start_pos : 0,
                              i == _sub_file_paths.size() - 1 ? _task.end_pos : 0,
                              _task.has_header, _task.file_type);
            int ret = Impl.run(progress_func, finish_block_func, false, {});
            if (ret < 0) {
                _result << Impl.get_result();
                fast_importer.set_handle_files_failed();
                fast_importer.close();
                // 失败了，把子任务状态置为fail
                return -2;
            }
        }
    }
    DB_WARNING("HANDLE LINE END, time: %ld", time.get_time());
    fast_importer.close();

    if (fast_importer.get_diff_lines_fail()) {
        DB_FATAL("FastImporter diff lines fail");
        return -2;
    }

    return 0;
}

int FastImporterCtrl::run_sub_task() {
    _is_main_task = false;
    // 取不取得到子任务都先把rocksdb数据清了
    auto s = DMRocksWrapper::get_instance()->clean_roscksdb();
    if (!s.ok()) {
        DB_FATAL("clean rocksdb fail, err: %s", s.ToString().c_str());
    }
    int ret = fetch_task();
    if (ret < 0) {
        return 0;
    }

    ON_SCOPE_EXIT(([this, &ret]() {
        // 更新任务执行结果
        std::map<std::string, std::string> set_map;
        set_map["result"] = "'" + _result.str() + "'";
        set_map["end_time"]   = "now()";
        if (ret == 0) {
            set_map["status"] = "'success'";
        } else if (ret == -2) {
            set_map["status"] = "'fail'";
        } else {
            set_map["status"] = "'idle'";
        }

        update_local_task(set_map);
    }));

    // 子任务的done在FAST_IMPORTER_SUBTASK里
    ret = parse_done();
    if (ret < 0) {
        _result << "parse done fail";
        return -1;
    }
    ret = get_table_schema();
    if (ret < 0) {
        _result << "get table schema fail";
        return -1;
    }

    FastImporterImpl fast_importer(_task, _task_baikaldb);
    ret = fast_importer.init(_schema_info, _region_infos);
    if (ret < 0) {
        DB_FATAL("fast_importer init failed");
        _result << fast_importer.result() << " fast_importer init failed";
        return -1;
    }
    auto fields_func = 
        [this, &fast_importer] (const std::string& path, 
                                const std::vector<std::vector<std::string>>& fields_vec, 
                                BlockHandleResult*) {
            fast_importer.handle_fields(path, fields_vec);
        };

    auto split_func = 
        [this, &fast_importer] (std::string& line, std::vector<std::string>& split_vec) {
            return fast_importer.split(line, split_vec);
        };
    
    auto convert_func = 
        [this, &fast_importer] (std::string& line) {
            return fast_importer.convert(line);
        };

    ret = import_to_baikaldb(fast_importer, fields_func, split_func, convert_func);
    if (ret < 0) {
        DB_FATAL("fast_importer run failed, result: %s", _result.str().c_str());
        return -1;
    }

    // 子任务更新统计信息
    if (fast_importer.need_redo_task()) {
        fast_importer.progress_dialog("need redo task");
        ret = -1;
        return -1;
    } else {
        fast_importer.progress_dialog("");
    }
    if (fast_importer.send_failed_sst_count() > 0) {
        DB_WARNING("sub_task has failed sst send count: %ld, main_task_id: %ld, sub_task_id: %ld",
                   fast_importer.send_failed_sst_count(), _task.main_id, _task.id);
        ret = -2;
    }
    return 0;
}

int SSTsender::write_sst_file() {
    TimeCost time;
    MutTableKey key;
    key.append_i64(_region_id);
    std::string prefix = key.data();
    key.append_u64(UINT64_MAX);
    std::string upper_bound = key.data();

    rocksdb::Options options;
    options = DMRocksWrapper::get_instance()->get_option();
    options.bottommost_compression = rocksdb::kLZ4Compression;
    options.bottommost_compression_opts = rocksdb::CompressionOptions();
    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(options));

    if (_index_size == 0) {
        DB_FATAL("index_size = 0, region_id: %ld", _region_id);
        return -1;
    }

    auto s = writer->open(_path);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s, region_id: %ld",
                _path.c_str(), s.ToString().c_str(), _region_id);
        return -1;
    }

    rocksdb::ReadOptions read_options;
    read_options.total_order_seek = true;
    read_options.fill_cache = false;
    rocksdb::Slice upper_bound_slice = upper_bound;
    read_options.iterate_upper_bound = &upper_bound_slice;
    std::unique_ptr<rocksdb::Iterator> iter(DMRocksWrapper::get_instance()->new_iterator(read_options));
    iter->Seek(prefix);

    int count = 0;
    while (iter->Valid()) {
        if (iter->key().starts_with(prefix)) {
            auto s = writer->put(iter->key(), iter->value());
            if (!s.ok()) {            
                DB_FATAL("write sst file failed, err: %s, region_id: %ld", 
                            s.ToString().c_str(), _region_id);
                return -1;
            }
            count++;
        } else {
            break;
        }
        iter->Next();
    }
    if (!iter->status().ok()) {
        DB_WARNING("iter status not ok, region_id: %ld", _region_id);
    }

    if (count == 0) {
        DB_WARNING("region_id: %ld, sst is empty", _region_id);
        return -2;
    }
    s = writer->finish();
    if (!s.ok()) {
        DB_FATAL("finish sst file path: %s failed, err: %s, region_id: %ld",
                _path.c_str(), s.ToString().c_str(), _region_id);
        return -1;
    }
    _rocksdb_kv_count = count;
    _row_size = count / _index_size;
    DB_WARNING("region_id: %ld, write sst: %s, count: %d, file size: %lu, cost: %lu",
               _region_id, _path.c_str(), count, writer->file_size(), time.get_time());
    return 0;
}

void SSTsender::send_concurrency(const std::set<std::string>& peers, std::unordered_map<std::string, int>& peer_ret_map) {
    for (auto& peer : peers) {
        peer_ret_map[peer] = -1;
    }
    BthreadCond cond;
    for (const auto& peer: peers) {
        auto send_one_peer = [this, &cond, peer, &peer_ret_map]() {
            ON_SCOPE_EXIT([&cond]{cond.decrease_signal();});
            while (true) {
                auto s = BackUp::send_sst_streaming(peer, _path, _table_id, _region_id, true, _row_size);
                if (s == Status::RetryLater) {
                    DB_WARNING("region_id: %ld, send sst to store: %s need retry later", _region_id, peer.c_str());
                    continue;
                }
                if (s == Status::Succss) {
                    DB_WARNING("region_id: %ld, send sst to store: %s success", _region_id, peer.c_str());
                    peer_ret_map[peer] = 0;
                } else {
                    DB_WARNING("region_id: %ld, send sst to store: %s fail", _region_id, peer.c_str());
                }
                break;
            }
        };

        cond.increase();
        Bthread bth;
        bth.run(send_one_peer);
    }

    cond.wait();
}

int SSTsender::get_peers(std::set<std::string>& peers, std::set<std::string>& unstable_followers) {
    // 获取leader上记录的peers最准确
    bool succ = false;
    for (const auto& peer : _peers) {
        peers.clear();
        unstable_followers.clear();
        auto s = BackUp::get_region_peers_from_leader(peer, _region_id, peers, unstable_followers);
        if (s == Status::Succss) {
            succ = true;
            break;
        } else {
            continue;
        }
    }

    if (succ) {
        std::ostringstream os;
        os << "peers: ";
        for (const auto& peer : peers) {
            os << peer << ",";
        }

        os << " unstable_followers: ";
        for (const auto& peer : unstable_followers) {
            os << peer << ",";
        }

        DB_WARNING("region_id: %ld, peers: %s", _region_id, os.str().c_str());
        _peers = peers;
        return 0;
    } else {
        DB_WARNING("region_id: %ld, get peers failed", _region_id);
        return -1;
    }
}

int SSTsender::check_peers(std::set<std::string>& need_send_peers, std::set<std::string>& unstable_followers) {
    std::set<std::string> peers;
    int retry_time = 0;
    int ret = get_peers(peers, unstable_followers);
    while (ret < 0) {
        ++retry_time;
        if (retry_time < 60) {
            bthread_usleep(2 * 1000 * 1000);
            ret = get_peers(peers, unstable_followers);
            continue;
        } else {
            DB_WARNING("region_id: %ld, get_peers failed", _region_id);
            return -1;
        }
    }

    // 清理peer
    auto iter = _peer_send_status_map.begin();
    while (iter != _peer_send_status_map.end()) {
        if (peers.count(iter->first) > 0) {
            ++iter;
        } else {
            iter = _peer_send_status_map.erase(iter);
        }
    }

    // 添加peer
    for (const auto& peer : peers) {
        auto iter = _peer_send_status_map.find(peer);
        if (iter != _peer_send_status_map.end()) {
            if (iter->second.status == SendStatus::SEND_INIT) {
                need_send_peers.insert(peer);
            } else if (iter->second.status == SendStatus::SEND_FAIL) {
                DB_WARNING("region_id: %ld, peer: %s, failed", _region_id, peer.c_str());
                return -1;
            }
        } else {
            _peer_send_status_map[peer] = SendStatus();
            need_send_peers.insert(peer);
        }
    }

    return 0;
}

// retry_no_limit = true，无线重试发sst
int SSTsender::send(bool retry_no_limit) {
    int ret = write_sst_file();
    if (ret == -2) {
        // no log entry
        return 1;
    }
    else if (ret < 0) {
        DB_FATAL("region_id: %ld, write_sst_file failed", _region_id);
        return -1;
    }
    
    TimeCost wait_unstable_time;
    bool begin_wait_unstable = false;

    while (true) {
        std::set<std::string> need_send_peers;
        std::set<std::string> unstable_followers;
        ret = check_peers(need_send_peers, unstable_followers);
        if (ret < 0) {
            DB_WARNING("region_id: %ld check peer failed", _region_id);
            if (retry_no_limit) {
                continue;
            }
            return -1;
        }

        if (need_send_peers.empty()) {
            if (unstable_followers.empty()) {
                DB_WARNING("region_id: %ld, send finish", _region_id);
                return 0;
            } else {
                if (!begin_wait_unstable) {
                    begin_wait_unstable = true;
                    wait_unstable_time.reset();
                }
                // 持续时间太长需要报警
                DB_WARNING("region_id: %ld, wait unstable_followers, wait time: %ld", 
                    _region_id, wait_unstable_time.get_time());
                bthread_usleep(5 * 1000 * 1000);
                continue;
            }
        }

        std::unordered_map<std::string, int> peer_ret_map;
        send_concurrency(need_send_peers, peer_ret_map);
        for (const auto& pair : peer_ret_map) {
            auto iter = _peer_send_status_map.find(pair.first);
            if (pair.second < 0) {
                bthread_usleep(5 * 1000 * 1000);
                if (++iter->second.retry_times > 10 && !retry_no_limit) {
                    iter->second.status = SendStatus::SEND_FAIL;
                }
            } else {
                iter->second.status = SendStatus::SEND_SUCCESS;
            }
        }
    }

    return 0;
}

int FastImporterImpl::init(pb::SchemaInfo& schema_info,
                           ::google::protobuf::RepeatedPtrField< ::baikaldb::pb::RegionInfo >& region_infos) {
    if (_task.db.empty() || _task.table.empty() || _task.table_info.empty() || _task.fields.empty()) {
        DB_FATAL("init meta_interact fail, table_info : %s, db: %s, table: %s, field_size: %ld",
                 _task.table.c_str(), _task.db.c_str(), _task.table.c_str(), _task.table_info.size());
        return -1;
    }

    // 更新主表schema和region信息
    _schema_info = schema_info;
    SchemaFactory::get_instance()->update_table(_schema_info);
    SchemaFactory::get_instance()->update_regions_double_buffer_sync(region_infos);
    bool has_global_index = false;
    // 检查主表region连续，避免预分裂没结束
    int ret = SchemaFactory::get_instance()->check_region_ranges_consecutive(_schema_info.table_id());
    if (ret < 0) {
        DB_FATAL("region range not consecutive, db: %s, table: %s", _task.db.c_str(), _task.table.c_str());
        return -1;
    }
    // 检查全局索引表region连续，避免预分裂没结束
    for (const auto& idx : _schema_info.indexs()) {
        if (idx.is_global()) {
            has_global_index = true;
            ret = SchemaFactory::get_instance()->check_region_ranges_consecutive(idx.index_id());
            if (ret < 0) {
                DB_FATAL("region range not consecutive, db: %s, table: %s", _task.db.c_str(), _task.table.c_str());
                return -1;
            }
        }
    }

    DB_WARNING("region num: %d", region_infos.size());
    for (auto& region : region_infos) {
        // peer数少于半数，等至少补齐导半数后再进行导入
        if (region.peers_size() <= _schema_info.replica_num() / 2) {
            DB_FATAL("region id: %ld, peer_size: %d, replica: %ld not match",
                     region.region_id(), region.peers_size(), _schema_info.replica_num());
            return -1;
        }
        for (auto& peer : region.peers()) {
            DB_WARNING("region_id %ld, peer: %s", region.region_id(), peer.c_str());
            _region_peers_map[region.region_id()].insert(peer);
        }
        if (region.main_table_id() != region.table_id()) {
            _region_index_map[region.region_id()] = 1;
        } else {
            _region_index_map[region.region_id()] = has_global_index ? _schema_info.indexs_size() - 1
                                                                     : _schema_info.indexs_size();
        }
        DB_DEBUG("get region: %ld, table_id: %ld, main_table_id: %ld",
                   region.region_id(), region.table_id(), region.main_table_id());
    }

    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_schema_info.table_id());
    // 表所有的索引信息
    for (const auto index_id : _table_info->indices) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("no index info found with index_id: %ld", index_id);
            return -1;
        }
        if (info_ptr->type == pb::I_PRIMARY) {
            _pk_index = info_ptr;
        }
        _indexes.emplace_back(info_ptr);
    }
    if (_pk_index == nullptr) {
        DB_FATAL("get pri index info fail, db: %s, table: %s", _task.db.c_str(),  _task.table.c_str());
        return -1;
    }
    // 按源文件列顺序解析fieldInfo，为后面insert_row, 同时检查自增列
    std::unordered_map<std::string, FieldInfo*> field_map;
    std::string auto_increase_filed;
    for (auto& field : _table_info->fields) {
        field_map[field.short_name] = &field;
        if (field.auto_inc) {
            auto_increase_filed = field.short_name;
        }
    }
    for (auto i = 0; i < _task.fields.size(); ++i) {
        auto& field_name = _task.fields[i];
        if (_task.ignore_indexes.count(i) > 0) {
            _field_infos.emplace_back(nullptr);
        } else {
            if (field_map.find(field_name) == field_map.end()) {
                DB_FATAL("cant find field in schemaInfo: %s", field_name.c_str());
                return -1;
            }
            _field_infos.emplace_back(field_map[field_name]);
        }
    }
    for (auto& const_value : _task.const_map) {
        const auto& field_name = const_value.first;
        if (field_map.find(field_name) == field_map.end()) {
            DB_FATAL("cant find field in schemaInfo: %s", field_name.c_str());
            return -1;
        }
        _const_field_infos[field_name] = field_map[field_name];
    }
    // 如果表有自增列，检查数据文件包括这个自增列
    if (!auto_increase_filed.empty()) {
        bool auto_increase_filed_in_data = false;
        for (auto& filed_in_data_file : _task.fields) {
            if (filed_in_data_file == auto_increase_filed) {
                auto_increase_filed_in_data = true;
                break;
            }
        }
        if (!auto_increase_filed_in_data) {
            DB_FATAL("cant find auto_increase field in data file: %s", auto_increase_filed.c_str());
            return -1;
        }
    }
    // 判断ttl表
    _use_ttl = _schema_info.ttl_duration() > 0;
    if (_use_ttl) {
        if (_task.ttl == 0) {
            _task.ttl = butil::gettimeofday_us() + _schema_info.ttl_duration() * 1000 * 1000LL;
        } else {
            _task.ttl = butil::gettimeofday_us() + _task.ttl * 1000 * 1000LL;
        }
    }
    // 开启检测rocksdb, 一旦检测超过阈值就会禁写，开始发送sst
    start_db_statistics();
    return 0;
}

int FastImporterImpl::put_secondary(int64_t region,
                                  IndexInfo& index,
                                  SmartRecord record,
                                  std::vector<std::string>& keys,
                                  std::vector<std::string>& values) {
    if (record == nullptr) {
        DB_WARNING("record is nullptr");
        return -1;
    }
    if (index.type != pb::I_KEY && index.type != pb::I_UNIQ) {
        DB_WARNING("invalid index type, region_id: %ld, table_id: %ld, index_type:%d", region, index.id, index.type);
        return -1;
    }
    MutTableKey key;
    key.append_i64(region).append_i64(index.id);

    if(0 != key.append_index(index, record.get(), -1, false)) {
        DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", region, index.id);
        return -1;
    }
    rocksdb::Status res;
    MutTableKey pk;
    if (index.type == pb::I_KEY) {
        if (0 != record->encode_primary_key(index, key, -1)) {
            DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", region, index.pk);
            return -1;
        }
        keys.emplace_back(key.data());
        values.emplace_back("");
    } else if (index.type == pb::I_UNIQ) {
        if (0 != record->encode_primary_key(index, pk, -1)) {
            DB_FATAL("Fail to append_index, reg:%ld, tab:%ld", region, index.pk);
            return -1;
        }
        keys.emplace_back(key.data());
        values.emplace_back(pk.data());
    }
    return 0;
}

// 暂时先不考虑cstore
int FastImporterImpl::put_primary(int64_t region,
                                IndexInfo& pk_index,
                                SmartRecord record,
                                std::vector<std::string>& keys,
                                std::vector<std::string>& values) {
    if (record == nullptr) {
        DB_WARNING("record is nullptr");
        return -1;
    }
    MutTableKey key;
    int ret = -1;
    key.append_i64(region).append_i64(pk_index.id);
    if (0 != key.append_index(pk_index, record.get(), -1, true)) {
        DB_FATAL("Fail to append_index, reg=%ld, tab=%ld", region, pk_index.id);
        return -1;
    }
    std::string value;
    ret = record->encode(value);
    if (ret != 0) {
        DB_WARNING("encode record failed: reg=%ld, tab=%ld", region, pk_index.id);
        return -1;
    }
    keys.emplace_back(key.data());
    values.emplace_back(value);
    return 0;
}

void FastImporterImpl::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) {
    rocksdb::WriteBatch batch;
    int imported_lines = 0;
    std::vector<SmartRecord> records;
    std::vector<int64_t> region_ids;
    std::vector<SmartRecord> global_records;
    std::vector<int64_t> global_region_ids;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    records.reserve(FLAGS_insert_values_count);
    region_ids.reserve(FLAGS_insert_values_count);
    keys.reserve(FLAGS_insert_values_count);
    values.reserve(FLAGS_insert_values_count);
    butil::Arena arena;
    if (fields_vec.empty()) {
        return;
    }

    _compacting_cond.wait();
    _writing_cond.increase();
    ON_SCOPE_EXIT([this](){
        _writing_cond.decrease_signal();
    });
    for (auto& fields : fields_vec) {
        bool handle_line_fail = false;
        if (fields.size() != _task.fields.size()) {
            if (_import_diff_lines % 1000 == 0) {
                std::string line;
                restore_line(fields, line);
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _task.fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
            }
            _import_diff_lines++;
            continue;
        }
        // 生成record
        SmartRecord record = SchemaFactory::get_instance()->new_record(_schema_info.table_id());
        if (record == nullptr) {
            std::string line;
            restore_line(fields, line);
            DB_FATAL("alloc record fail for line: %s", line.c_str()); 
            _import_diff_lines++;
            continue;
        }
        for (auto i = 0; i < _field_infos.size() && i < fields.size(); ++i) {
            if (_task.ignore_indexes.count(i) != 0
                || (_task.empty_as_null_indexes.count(i) == 1 && fields[i] == "")
                || (!_task.null_as_string && fields[i] == "NULL")
                || _task.const_map.find(_field_infos[i]->short_name) != _task.const_map.end()) {
                continue;
            }
            std::string value_str_from_hex = ""; 
            if (_task.binary_indexes.find(i) != _task.binary_indexes.end()) {
                if (fields[i].size() < 2 
                    || fields[i][0] != '0' 
                    || (fields[i][1] != 'X' && fields[i][1] != 'x')) {
                    DB_FATAL("decode_binary_from_hex fail: %s", fields[i].data());
                    handle_line_fail = true;
                    break;
                }
                parser::LiteralExpr* literal = parser::LiteralExpr::make_hex(fields[i].data() + 2, 
                                                fields[i].size() - 2, arena);
                value_str_from_hex = literal->_u.str_val.to_string();
            } 
            ExprValue value(_field_infos[i]->type, 
                            !value_str_from_hex.empty() ? value_str_from_hex : fields[i]);
            if (0 != record->set_value(record->get_field_by_idx(_field_infos[i]->pb_idx), value)) {
                DB_FATAL("parse field fail: %s", fields[i].data());
                handle_line_fail = true;
                break;
            }
        }
        for (auto& pair : _const_field_infos) {
            ExprValue value(pair.second->type, _task.const_map[pair.first]);
            if (0 != record->set_value(record->get_field_by_idx(pair.second->pb_idx), value)) {
                DB_FATAL("parse field fail: %s, value: %s", pair.first.c_str(), _task.const_map[pair.first].c_str());
                handle_line_fail = true;
                break;
            }
        }
        if (handle_line_fail) {
            _import_diff_lines++;
            continue;
        }
        records.emplace_back(record);
    }
    // 找到主表所属的region ids
    int ret = SchemaFactory::get_instance()->get_region_ids_by_key(*_pk_index, records, region_ids);
    if (ret < 0 || region_ids.size() == 0 || region_ids.size() != records.size()) {
        DB_FATAL("get region id fail");
        _import_diff_lines += records.size();
        return;
    }
    // 找到全局索引的region ids
    for (auto& idx : _indexes) {
        if (idx != nullptr && idx->is_global) {
            ret = SchemaFactory::get_instance()->get_region_ids_by_key(*idx, records, global_region_ids);
            if (ret < 0 || region_ids.size() == 0) {
                DB_FATAL("get global region id fail");
                _import_diff_lines += records.size();
                return;
            }
        }
    }

    for (int32_t i = 0; i < records.size(); ++i) {
        bool handle_line_fail = false;
        SmartRecord record = records[i];
        keys.clear();
        values.clear();
        // 处理主键外的其他索引
        for (auto& idx : _indexes) {
            if (idx == nullptr) {
                continue;
            }
            if (idx->is_global) {
                if (put_secondary(global_region_ids[i], *idx, record, keys, values) < 0) {
                    DB_FATAL("put global index fail: idx: %s, line: %s", idx->name.c_str(), record->debug_string().c_str());
                    handle_line_fail = true;
                    break;
                }
            }
            else if (idx->type != pb::I_PRIMARY) {
                if (put_secondary(region_ids[i], *idx, record, keys, values) < 0) {
                    DB_FATAL("put secondary fail: idx: %s, line: %s", idx->name.c_str(), record->debug_string().c_str());
                    handle_line_fail = true;
                    break;
                }
            }
        }
        if (handle_line_fail) {
            _import_diff_lines++;
            continue;
        }
        // 处理主键
        ret = put_primary(region_ids[i], *_pk_index, record, keys, values);
        if (ret < 0) {
            DB_FATAL("put_primary fail: line: %s", record->debug_string().c_str());
            _import_diff_lines++;
            continue;
        }
        // 将这行产生的所有kv写到batch
        for(int i = 0; i < keys.size(); ++i) {
            rocksdb::Slice key_slice(keys[i]);
            rocksdb::Slice value_slices[2];
            rocksdb::SliceParts key_slice_parts(&key_slice, 1);
            rocksdb::SliceParts value_slice_parts;
            uint64_t ttl_storage = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_task.ttl));
            value_slices[0].data_ = reinterpret_cast<const char*>(&ttl_storage);
            value_slices[0].size_ = sizeof(uint64_t);
            value_slices[1].data_ = values[i].data();
            value_slices[1].size_ = values[i].size();
            DB_DEBUG("use_ttl:%d ttl_timestamp_us:%ld", _use_ttl, _task.ttl);
            if (_use_ttl && _task.ttl > 0) {
                value_slice_parts.parts = value_slices;
                value_slice_parts.num_parts = 2;
            } else {
                value_slice_parts.parts = value_slices + 1;
                value_slice_parts.num_parts = 1;
            }
            int retry_time = 0; 
            while (retry_time++ < 3) {
                auto s = batch.Put(key_slice_parts, value_slice_parts);
                if(s.ok()) {
                    break;
                } else {
                    DB_FATAL("write rocksdb kv fail: %s", s.ToString().c_str());
                }
            }
        }
        ++imported_lines;
    }
    if (batch.Count() == 0) {
        return;
    }
    int i = 0; 
    while (i++ < 3) {
        auto s = DMRocksWrapper::get_instance()->write(&batch);
        if(s.ok()) {
            _import_lines += imported_lines;
            return;
        } else {
            DB_FATAL("write rocksdb batch fail: %s", s.ToString().c_str());
        }
    }
    return;
}

void FastImporterImpl::progress_dialog(std::string progressing) {
    // 统计进度
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> where_map;
    std::map<std::string, std::string> set_map;
    if (progressing.empty()) {
        std::ostringstream progressing_stream;
        progressing_stream << "compact_cnt: " << _compaction_times.load()
                           << ", send_sst_lines_cnt: " << _send_sst_lines_cnt.load()
                           << ", cost: " << _total_cost.get_time() / 1000000
                           << ", compaction_cost: " << _total_compaction_cost / 1000000
                           << ", send_sst_cost: " << _total_send_sst_cost / 1000000
                           << ", send_sst_succ_cnt: " << _send_sst_cnt.load()
                           << ", send_sst_fail_cnt: " << _send_sst_fail_cnt.load();
        set_map["progress"] = "'" + progressing_stream.str() + "'";
    } else {
        set_map["progress"] = "'" + progressing + "'";
    }
    set_map["import_line"] = std::to_string(_import_lines.load());
    set_map["import_diff_line"] = std::to_string(_import_diff_lines.load());
    where_map["id"] = std::to_string(_task.id);
    where_map["hostname"]   = "'" + FLAGS_hostname + "'";
    where_map["status"]   = "'doing'";
    std::string sql = _gen_update_sql(FLAGS_fast_importer_db, FLAGS_fast_importer_tbl, set_map, where_map);
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000LL);
    }while (++retry < 20);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        return;
    }
    DB_TRACE("query %d times, sql:%s affected row:%lu", retry, sql.c_str(), result_set.get_affected_rows());
}

// 循环检测rocksdb是否达到指定的存储阈值，是的话，停止写，进行compaction，发送sst，清空rocksdb
// 中途处理文件失败了, 不需要发部分kv, 直接人为重做整个子任务
void FastImporterImpl::start_db_statistics() {
    Bthread bth(&BTHREAD_ATTR_SMALL);
    auto func = [this] () {
        _multi_thread_cond.increase();
        while (!_closed) {
            bool need_compaction = DMRocksWrapper::get_instance()->db_statistics();
            if (need_compaction && !_handle_files_fail) {
                complate();
            }
            bthread_usleep(5 * 1000 * 1000);
        }
        if (!_handle_files_fail) {
            complate();
        }
        _multi_thread_cond.decrease_signal();
    };
    TimeCost time;
    bth.run(func);
}


int FastImporterImpl::reset_region_peers_from_meta() {
    MetaServerInteract meta_interact;
    std::string table_name = _task.table;
    if (_task.is_replace) {
        table_name += "_tmp";
    }
    int ret = meta_interact.init_internal(_task.meta_bns);
    if (ret < 0) {
        DB_FATAL("init meta_interact fail, meta_bns: %s", _task.meta_bns.c_str());
        return -1;
    }
    pb::QueryRequest req;
    req.set_op_type(pb::QUERY_SCHEMA);
    req.set_namespace_name(_task.table_namespace);
    req.set_database(_task.db);
    req.set_table_name(table_name);
    pb::QueryResponse res;
    ret = meta_interact.send_request("query", req, res);
    if (ret < 0) {
        DB_FATAL("get_table_and_regions fail, db: %s, table: %s", _task.db.c_str(), _task.table.c_str());
        return -1;
    }
    if (res.schema_infos_size() == 0 || res.region_infos_size() != _region_peers_map.size()) {
        DB_FATAL("region num not match, ori: %lu, now: %d", _region_peers_map.size(), res.region_infos_size());
        return -1;
    }
    _region_peers_map.clear();
    for (auto& region : res.region_infos()) {
        for (auto& peer : region.peers()) {
            DB_WARNING("region_id %ld, peer: %s", region.region_id(), peer.c_str());
            _region_peers_map[region.region_id()].insert(peer);
        }
    }
    return 0;
}

int FastImporterImpl::complate() {
    TimeCost time_cost;
    TimeCost send_time_cost;
    int64_t compaction_cost = 0;
    int64_t send_cost = 0;
    std::atomic<int64_t> todo_cnt    { _region_peers_map.size() };
    std::atomic<int64_t> doing_cnt   { 0 };
    std::atomic<int64_t> success_cnt { 0 };
    std::atomic<int64_t> fail_cnt    { 0 };
    int64_t send_concurrey = FLAGS_fast_importer_send_sst_concurrency;
    if (!_task.conf.empty()) {
        Json::Reader json_reader;
        Json::Value done_root;
        bool ret1 = json_reader.parse(_task.conf, done_root);
        if (ret1) {
            try {
                if (done_root.isMember("fast_importer_send_sst_concurrency")) {
                    send_concurrey = done_root["fast_importer_send_sst_concurrency"].asInt64();
                }
            } catch (Json::LogicError& e) {
                DB_FATAL("fail parse what:%s ", e.what());
            }
        }
    }
    DB_WARNING("send sst concurrency: %ld", send_concurrey);
    _compacting_cond.increase();
    _writing_cond.wait();
    ON_SCOPE_EXIT([this]() {
        _compacting_cond.decrease_broadcast();
    });

    const int64_t import_lines      = _import_lines.load();
    const int64_t import_diff_lines = _import_diff_lines.load();
    const int64_t import_all_lines  = import_lines + import_diff_lines;
    if (import_diff_lines > import_all_lines * 0.5) {
        std::string err_msg = "import_diff_lines[" + std::to_string(import_diff_lines) + 
                              "] bigger than 50 percent import_all_lines[" + std::to_string(import_all_lines) + "]";
        DB_FATAL("fail to complate, err_msg: %s", err_msg.c_str());
        _diff_lines_fail = true;
        return -1;
    }

    auto upload_progress = [this, &compaction_cost, &send_cost,
                            &success_cnt, &fail_cnt, &todo_cnt, &doing_cnt]() {
        std::ostringstream progressing;
        progressing << "compaction_cnt" << _compaction_times.load()
                    << ", send_sst_lines_cnt: " << _send_sst_lines_cnt.load()
                    << ", compaction_cost: " << compaction_cost / 1000000
                    << ", send_cost: " << send_cost / 1000000
                    << ", send_success_cnt: " << success_cnt
                    << ", send_fail_cnt: " << fail_cnt
                    << ", send_todo_cnt: " << todo_cnt
                    << ", send_doing_cnt: " << doing_cnt;
        progress_dialog(progressing.str());
    };
    progress_dialog("compacting");
    auto s = DMRocksWrapper::get_instance()->compact_all();
    if (!s.ok()) {
        DB_FATAL("compaction fail, err: %s", s.ToString().c_str());
        return -1;
    }
    compaction_cost = time_cost.get_time();
    time_cost.reset();
    _compaction_times++;

    // 发sst之前拿一次schema_conf，比较region数，看是否在导入过程中split了，是的话，需要重做任务
    int ret = reset_region_peers_from_meta();
    if (ret < 0) {
        _need_redo_task = true;
        return -1;
    }

    // 开始扫rocksdb和ingest
    ConcurrencyBthread send_bth(send_concurrey);
    for (const auto& pair : _region_peers_map) {
        auto send_fn = [this, pair, &todo_cnt, &doing_cnt, &success_cnt, &fail_cnt]() {
            todo_cnt--;
            doing_cnt++;
            std::unique_ptr<SSTsender> sender(new SSTsender(_schema_info.table_id(), pair.first, pair.second, _region_index_map[pair.first]));
            int ret = sender->send(false);
            if (ret < 0) {
                DB_FATAL("region_id: %ld, send sst failed", pair.first);
                BAIDU_SCOPED_LOCK(_failed_region_ids_mutex);
                _failed_region_ids.insert(pair.first);
                fail_cnt++;
            } else if (ret == 0){
                success_cnt++;
                _send_sst_lines_cnt += sender->_rocksdb_kv_count;
            }
            doing_cnt--;
        };

        // 更新进度
        if (send_time_cost.get_time() > 60 * 1000 * 1000LL) {
            send_time_cost.reset();
            send_cost = time_cost.get_time();
            upload_progress();
        }

        send_bth.run(send_fn);
    }
    send_bth.join();

    if (!_failed_region_ids.empty()) {
        // 无限重发失败region sst之前，从meta拿最新的region peer列表
        reset_region_peers_from_meta();
    }
    // 重新做失败的任务，无限重试
    for (const auto& failed_region_id : _failed_region_ids) {
        auto send_fn = [this, failed_region_id, &todo_cnt, &doing_cnt, &success_cnt, &fail_cnt]() {
            doing_cnt++;
            std::unique_ptr<SSTsender> sender(new SSTsender(_schema_info.table_id(), failed_region_id,
                                                            _region_peers_map[failed_region_id],
                                                            _region_index_map[failed_region_id]));
            int ret = sender->send(true);
            if (ret < 0) {
                DB_FATAL("region_id: %ld, send sst failed finally, need redo subtask or remove err peer", 
                         failed_region_id);
            } else if (ret == 0) {
                success_cnt++;
                fail_cnt--;
                _send_sst_lines_cnt += sender->_rocksdb_kv_count;
            }
            doing_cnt--;
        };

        // 更新进度
        if (send_time_cost.get_time() > 30 * 1000 * 1000LL) {
            send_time_cost.reset();
            send_cost = time_cost.get_time();
            upload_progress();
        }

        send_bth.run(send_fn);
    }
    send_bth.join();
    send_cost = time_cost.get_time();

    // 清空rocksdb
    s = DMRocksWrapper::get_instance()->clean_roscksdb();
    if (!s.ok()) {
        DB_FATAL("clean rocksdb fail, err: %s", s.ToString().c_str());
    }
    DB_WARNING("compaction_cost: %ld, send_sst_cost: %ld", compaction_cost, send_cost);
    upload_progress();
    _total_compaction_cost += compaction_cost;
    _total_send_sst_cost += send_cost;
    _send_sst_fail_cnt += fail_cnt.load();
    _send_sst_cnt += success_cnt;
    return 0;
}

} // namespace baikaldb
