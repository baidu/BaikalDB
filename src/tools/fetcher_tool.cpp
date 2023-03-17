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
#include "dm_rocks_wrapper.h"
#include "fn_manager.h"
#include "rate_limiter.h"

using namespace afs;
namespace baikaldb {
DECLARE_string(hostname);

DEFINE_string(param_tbl,           "HDFS_TASK_TABLE",        "hdfs import conf table");
DEFINE_string(result_tbl,          "HDFS_TASK_RESULT_TABLE", "hdfs import result info table");
DEFINE_string(finish_blk_tbl,      "HDFS_TASK_FINISHED_BLOCK", "hdfs import result info table");
DEFINE_string(sst_backup_tbl,      "SST_BACKUP_TABLE",       "sst backup result table");
DEFINE_string(sst_backup_info_tbl, "SST_BACKUP_INFO",        "sst backup conf table");
DEFINE_string(sql_agg_tbl,         "SQL_AGG_INFO",           "sst backup conf table");
DEFINE_string(resource_tag,        "e0",                     "baikalDM resource_tag");

DEFINE_int32(tasktimeout_s,        60*60, "task time out second");
DEFINE_int32(interval_days,        7, "defult interval days");
DEFINE_string(hdfs_mnt_wutai,      "/home/work/hadoop_cluster/mnt/wutai", "wutai mnt path");
DEFINE_string(hdfs_mnt_mulan,      "/home/work/hadoop_cluster/mnt/mulan", "mulan mnt path");
DEFINE_string(hdfs_mnt_khan,       "/home/work/hadoop_cluster/mnt/khan", "khan mnt path");
DEFINE_bool(is_hdfs_import,        true, "default hdfs import open");
DEFINE_bool(is_sst_backup,   false, "default sst backup close");
DEFINE_bool(is_agg_sql,      false, "default sst backup close");
DEFINE_bool(is_fast_importer,      false, "default fast_importer_worker close");
DEFINE_string(baikaldb_resource, "baikaldb,baikaldb_gbk,baikaldb_utf8,baikaldb_trace", "default baikaldb,baikaldb_gbk,baikaldb_utf8,baikaldb_trace");
DEFINE_string(sql_agg_ignore_resource_tag, "'e0','e1','e0-nj','e0-ct','qa','qadisk'", "ignore_resource_tag");
DEFINE_string(db_path, "./rocks_db", "rocks db path");
DECLARE_string(data_path);
DECLARE_bool(is_mnt);

bool Fetcher::_shutdown = false;

int Fetcher::init() {
    if (FLAGS_hostname == "hostname") {
        DB_FATAL("hostname is default");
        return -1;
    }

    _today_version = get_today_date();
    _interval_days = FLAGS_interval_days;
    return 0;
}

Fetcher::~Fetcher() {
    destroy_filesystem();
}

int Fetcher::querybaikaldb(std::string sql, baikal::client::ResultSet& result_set) {
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 20);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        return ret;
    }
    DB_TRACE("query %d times, sql:%s affected row:%lu", retry, sql.c_str(), result_set.get_affected_rows());
    return 0;
}

//准备工作：分裂donefile的路径和名字；获取集群路径和挂载路径
void Fetcher::prepare() {
    if (_local_done_json != "") {
        _is_local_done_json = true;
    } else {
        _is_local_done_json = false;
    }
    if (_done_file.find("{DATE}") == _done_file.npos) {
        _only_one_donefile = true;
    } else {
        _only_one_donefile = false;
    }
    std::vector<std::string> split_info;
    boost::split(split_info, _table_info, boost::is_any_of("."));
    _import_db = split_info.front();
    _import_tbl = split_info.back();
    if (!_is_local_done_json) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, _done_file, boost::is_any_of("/"));
        _done_name = split_vec.back();
        std::string str = _done_file;
        _done_common_path = str.replace(str.rfind(_done_name.c_str()), _done_name.size(), "/");
    } else {
        _done_name.clear();
        _done_common_path.clear();
    }
    DB_WARNING("task only one donefile:%d, today version:%ld, done name:%s, done_common_path:%s, "
        "_import_db:%s, _import_tbl:%s local_done_json:%s", _only_one_donefile, _today_version, _done_name.c_str(), 
        _done_common_path.c_str(), _import_db.c_str(), _import_tbl.c_str(), _local_done_json.c_str());
}
// 创建文件系统
int Fetcher::create_filesystem(const std::string& cluster_name, const std::string& user_name, const std::string& password) {
    if (cluster_name.find("afs") != cluster_name.npos) {
#ifdef BAIDU_INTERNAL
        _fs = new AfsFileSystemAdaptor(cluster_name, user_name, password, "./conf/client.conf");
#endif
    } else {
        _fs = new PosixFileSystemAdaptor();
    }

    int ret = _fs->init();
    if (ret < 0) {
        DB_FATAL("filesystem init failed, cluster_name: %s", cluster_name.c_str());
        return ret;
    }
    return 0;
}

void Fetcher::destroy_filesystem() {
    if (_fs) {
        _fs->destroy();
        delete _fs;
        _fs = nullptr;
    }
}

int Fetcher::get_done_file(const std::string& done_file_name, Json::Value& done_root) {
    std::string st1;
    int size = 0;
    if (!_is_local_done_json) {
        ImporterReaderAdaptor* reader = _fs->open_reader(done_file_name);
        if (reader == nullptr) {
            DB_WARNING("open reader file: %s", done_file_name.c_str());
            return -1;
        }

        ON_SCOPE_EXIT(([this, reader]() {
            _fs->close_reader(reader);
        }));

        base::Arena arena;
        char* st = (char*)arena.allocate(1024*1024);
        if (st == nullptr) {
            return -1;
        }
        size = reader->read(0, st, 1024 * 1024);
        if (size < 0) {
            DB_FATAL("read done file failed");
            return -1;
        }

        st1 = std::string(st, st + size);
    } else {
        st1 = _local_done_json;
        size = st1.size();
    }

    DB_WARNING("doen file size: %d content: %s", size, st1.c_str());
    if (size == 0) {
        _import_ret << "failed: done file empty";
        return -1;
    }
    
    Json::Reader json_reader;
    bool ret1 = json_reader.parse(st1, done_root);
    if (!ret1) {
        _import_ret << " parse done file failed";
        DB_FATAL("fail parse %d, json:%s", ret1, st1.c_str());
        return -1;
    }
    _local_done_json = st1;
    return 0;
}

bool Fetcher::file_exist(const std::string& done_file_name) {
    FileMode mode;
    size_t file_size = 0;
    int ret = _fs->file_mode(done_file_name, &mode, &file_size);
    if (ret < 0) {
        return false;
    }

    if (mode == I_FILE) {
        return true;
    }

    return false;
}


//替换文件路径中的通配符
std::string Fetcher::replace_path_date(int64_t version) {
    std::string str = _done_common_path;
    auto pos = str.find("{DATE}");
    if (pos == std::string::npos) {
        return str;
    }
    str.replace(pos, 6, std::to_string(version));
    return str;
}

//分析只有一个done文件的情况，因为版本号在done文件内部，所以只能读取json之后分析
int Fetcher::analyse_one_donefile() {
    Json::Value done_root;
    int ret = get_done_file(_done_real_path + _done_name, done_root);
    if (ret < 0) {
        return -1;
    }

    try {
        for (auto& name_type : op_name_type) {
            if (done_root.isMember(name_type.name)) {
                for (auto& node : done_root[name_type.name]) {
                    DB_WARNING("%s", name_type.name.c_str());
                    return analyse_one_do(node);
                }
            } 
        }
    } catch (std::exception& exp) {
        DB_FATAL("parse done file error. exp : %s", exp.what());
        _import_ret << "parse done file error";
        return -1;
    }
    

    DB_FATAL("task is not update or replace or delete fail");
    //其他类型任务暂不处理
    return -1;
}
int Fetcher::analyse_one_do(const Json::Value& node) {
    if (!_only_one_donefile) {
        return 0;
    }
    if (node.isMember("version")) {
        int64_t done_version = atoll(node["version"].asString().c_str());
        // 回溯
        if (!_backtrack_done.empty()) {
            _new_version = done_version;
            DB_WARNING("_backtrack_done: %s, path has not DATE, new_version: %ld", _backtrack_done.c_str(), _new_version);
            return 0;
        }
        //初次导入
        if (_old_version == 0) {
            _new_version = done_version;
            return 0;
        }
        if (done_version == _old_version) {
            return -2;
        } else if (done_version > _old_version) {
            _new_version = done_version;
            return 0;
        } else {
            return -1;
        }
        return 0;
    } else {
        if (!_backtrack_done.empty() && _new_version != 0) {
            // 之前在_backtrack_done path里解析出来了new_version
            DB_WARNING("_backtrack_done: %s, path has DATE, new_version: %ld", _backtrack_done.c_str(), _new_version);
            return 0;
        }
        DB_FATAL("done file hasn`t version fail");
        return -1;
    }
}

//初次执行
int Fetcher::first_deal() {
    int i = 0;
    bool find = false;
    int64_t begin_version = get_ndays_date(_today_version, -_ago_days);
    for (i = 0; i < _interval_days; ++i) {
        int64_t version = get_ndays_date(begin_version, -i);
        std::string path = replace_path_date(version);
        std::string done_file = path + _done_name;
        if (file_exist(done_file)) {
            find = true;
            _new_version = version;
            _done_real_path = path;
            DB_WARNING("first deal done file:%s exist new_version:%ld, done_real_path:%s", 
                done_file.c_str(), _new_version, _done_real_path.c_str());
            break;
        } else {
            DB_TRACE("done file:%s is not exist", done_file.c_str());
            continue;
        }
    }
    if (find) {
        return 0;
    } else {
        return -2;
    }
}

//初次执行update
int Fetcher::first_deal_update() {
    int i = 0;
    bool find = false;
    int64_t begin_version = get_ndays_date(_today_version, -_ago_days);
    for (i = 0; i < _interval_days; ++i) {
        int64_t version = get_ndays_date(begin_version, -i);
        std::string path = replace_path_date(version);
        std::string done_file = path + _done_name;
        if (file_exist(done_file)) {
            find = true;
            _new_version = version;
            _done_real_path = path;
            _new_bitflag = 0xFFFFFFFF;
            DB_WARNING("table_info:%s, first deal done file:%s exist new_version:%ld, done_real_path:%s", 
                _table_info.c_str(), done_file.c_str(), _new_version, _done_real_path.c_str());
            break;
        } else {
            DB_WARNING("table_info:%s, done file:%s is not exist", _table_info.c_str(), done_file.c_str());
            continue;
        }
    }
    if (find) {
        return 0;
    } else {
        DB_WARNING("table_info:%s, done file:%s not find", _table_info.c_str(), _done_file.c_str());
        return -2;
    }
}
    
int Fetcher::deal_update_before_oldversion() {
    uint64_t bitflag = 1;
    bool find = false;
    int before_days = 0;
    if (_old_bitflag == 0 || (_old_bitflag & 1) == 0) {
        DB_FATAL("table_info:%s done_file:%s old_bitfalg is illegal",
                _table_info.c_str(), _done_file.c_str());
        return -1;
    }
    for (int i = 0; i < _interval_days; i++) {
        bitflag = bitflag << 1;
        before_days++;
        if ((_old_bitflag & bitflag) != 0) {
            int64_t version = get_ndays_date(_old_version, -before_days);
            DB_WARNING("table_info:%s, done_file:%s, %d days before %ld, date:%ld already done _old_bitflag:%lx, bitflag:%lx", 
                _table_info.c_str(), _done_file.c_str(), before_days, _old_version, version, _old_bitflag, bitflag);        
            continue;
        } else {
            int64_t version = get_ndays_date(_old_version, -before_days);
            std::string path = replace_path_date(version);
            std::string done_file = path + _done_name;
            if (file_exist(done_file)) {
                find = true;
                _new_bitflag = _old_bitflag | bitflag;
                _new_version = version;
                _done_real_path = path;
                DB_WARNING("done file:%s old_bitflag:%lx, bitflag:%lx, new_bitflag:%lx, version:%ld find path success", 
                    done_file.c_str(), _old_bitflag, bitflag, _new_bitflag, version);
                break;
            } else {
                DB_WARNING("done file:%s is not exist， old_bitflag:%lx, bitflag:%lx, version:%ld", 
                    done_file.c_str(), _old_bitflag, bitflag, version);
                continue;
            }
        }
    }
    
    if (!find) {
        DB_WARNING("table_info:%s, done file:%s not find", _table_info.c_str(), _done_file.c_str());
        return -2;
    }
    return 0;
}

int Fetcher::deal_update_after_oldversion() {
    int i = 0;
    bool find = false;
    uint64_t new_bitflag = _old_bitflag;
    while (true) {
        i++;
        new_bitflag = new_bitflag << 1;
        int64_t version = get_ndays_date(_old_version, i);
        if (version > _today_version) {
            break;
        }
        std::string path = replace_path_date(version);
        std::string done_file = path + _done_name;
        if (file_exist(done_file)) {
            find = true;
            _new_version = version;
            _new_bitflag = new_bitflag | 1;
            _done_real_path = path;
            DB_WARNING("table_info:%s, done file:%s old_bitflag:%lx, new_bitflag:%lx, version:%ld find path success", 
                _table_info.c_str(), done_file.c_str(), _old_bitflag, _new_bitflag, version);
            break;
        } else {
            DB_WARNING("table_info:%s, done_file:%s, the day %ld hasn`t done file", 
                _table_info.c_str(), _done_file.c_str(), version);
            continue;
        }
    }
    
    if (!find) {
        DB_WARNING("table_info:%s, done file:%s not find", _table_info.c_str(), _done_file.c_str());
        return -2;
    }
    return 0;
}

//非首次处理replace，version可以跳跃
int Fetcher::deal_replace() {
    int i = 0;
    bool find = false;
    int64_t begin_version = get_ndays_date(_today_version, -_ago_days);
    for (i = 0; i < _interval_days; ++i) {
        int64_t version = get_ndays_date(begin_version, -i);
        if (version <= _old_version) {
            break;
        }
        std::string path = replace_path_date(version);
        std::string done_file = path + _done_name;
        if (file_exist(done_file)) {
            find = true;
            _new_version = version;
            _done_real_path = path;
            DB_WARNING("deal replace done file:%s is exist new_version:%ld, done_real_path:%s", 
                done_file.c_str(), _new_version, _done_real_path.c_str());
            break;
        } else {
            continue; 
        }
    }
    if (find) {
        return 0; 
    } else {
        return -2;
    }
}

//分析每天一个done文件的形式，通过确认版本号确认准确的donefile路径
int Fetcher::analyse_multi_donefile_replace() {
    if (_old_version == 0) {
        return first_deal();
    } else if (_old_version == _today_version) {
        return -2;
    } else {
        return deal_replace();
    }
}
int Fetcher::analyse_multi_donefile_update() {
    int ret = 0;
    if (_old_version == 0) {
        return first_deal_update();
    } 
    ret = deal_update_before_oldversion();
    if (ret == -2) {
        //没有找到oldversion之前需要导入的目录，则查找oldversion之后需要导入的目录
        return deal_update_after_oldversion();
    } else {
        return ret;
    }
}

int Fetcher::analyse_version() {
    try {
        if (_only_one_donefile || _is_local_done_json) {
            //只有一个done文件，需要在读取json之后分析
            _done_real_path = _done_common_path;
            return analyse_one_donefile();
        }    
        if (_modle == "replace") {
            return analyse_multi_donefile_replace();
        } else {
            return analyse_multi_donefile_update();
        }
    } catch (Json::LogicError& e) {
        DB_FATAL("fail parse what:%s ", e.what());
        return -1;
    }
}

void Fetcher::get_finished_file_and_blocks(const std::string& db, const std::string& tbl) {
    reset_all_last_import_info();
    if (!_broken_point_continuing) {
        return;
    }
    
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    select_vec.emplace_back("*");
    where_map["table_info"] = "'" + _table_info + "'";
    where_map["version"] = std::to_string(_new_version);
    where_map["database_name"] = "'" + db + "'";
    where_map["table_name"] = "'" + tbl + "'";
    std::string sql = gen_select_sql(FLAGS_finish_blk_tbl, select_vec, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("get finished block fail, sql:%s", sql.c_str()); 
        return;
    }
    while (result_set.next()) {
        std::string file_path;
        std::string database_name;
        std::string table;
        int64_t start_pos = 0;
        int64_t end_pos = 0;
        int64_t import_line = 0;
        int64_t diff_line = 0;
        int64_t err_sql = 0;
        int64_t affected_row = 0;
        result_set.get_string("file_path", &file_path);
        result_set.get_string("database_name", &database_name);
        result_set.get_string("table_name", &table);
        result_set.get_int64("start_pos", &start_pos);
        result_set.get_int64("end_pos", &end_pos);
        result_set.get_int64("import_line", &import_line);
        result_set.get_int64("diff_line", &diff_line);
        result_set.get_int64("err_sql", &err_sql);
        result_set.get_int64("affected_row", &affected_row);
        DB_WARNING("%s.%s file_path: %s:%ld:%ld has been imported, "
                   "import_line: %ld, diff_line: %ld, err_sql: %ld, affected_row: %ld", 
                   database_name.c_str(), table.c_str(),
                   file_path.c_str(), start_pos, end_pos,
                   import_line, diff_line, err_sql, affected_row);
        _finish_blocks_last_time[file_path][start_pos] = end_pos;
        _finish_blocks_import_line_last_import[file_path] += import_line;
        _finish_blocks_diff_line_last_import[file_path] += diff_line;
        _finish_blocks_err_sql_last_import[file_path] += err_sql;
        _finish_blocks_affected_row_last_import[file_path] += affected_row;
        _import_line_last_import += import_line;
        _diff_line_last_import += diff_line;
        _err_sql_last_import += err_sql;
        _affected_row_last_import += affected_row;
    }
    DB_WARNING("total import_line: %ld, diff_line: %ld, err_sql: %ld, affected_row: %ld for last import",
        _import_line_last_import, _diff_line_last_import, _err_sql_last_import, _affected_row_last_import);
    return; 
}

void Fetcher::insert_finished_file_or_blocks(std::string db, 
                                             std::string tbl, 
                                             std::string path, 
                                             int64_t start_pos, 
                                             int64_t end_pos, 
                                             BlockHandleResult* result,
                                             bool file_finished) {
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> values_map;
    values_map["table_info"] = "'" + _table_info + "'";
    values_map["version"] = std::to_string(_new_version);
    values_map["hostname"] = "'" + FLAGS_hostname + "'";
    values_map["file_path"] = "'" + path + "'";
    values_map["database_name"] = "'" + db + "'";
    values_map["table_name"] = "'" + tbl + "'";
    values_map["start_pos"] = std::to_string(start_pos);
    values_map["end_pos"] = std::to_string(end_pos);
    int64_t import_line = 0;
    int64_t diff_line = 0;
    int64_t err_sql = 0;
    int64_t affected_row = 0;
    if (result != nullptr) {
        import_line += result->import_line.load();
        diff_line += result->diff_line.load();
        err_sql += result->errsql_line.load();
        affected_row += result->affected_row.load();
    }
    if (file_finished) {
        import_line += _finish_blocks_import_line_last_import[path];
        diff_line += _finish_blocks_diff_line_last_import[path];
        err_sql += _finish_blocks_err_sql_last_import[path];
        affected_row += _finish_blocks_affected_row_last_import[path];
    }
    values_map["import_line"] = std::to_string(import_line);
    values_map["diff_line"] = std::to_string(diff_line);
    values_map["err_sql"] = std::to_string(err_sql);
    values_map["affected_row"] = std::to_string(affected_row);
    std::string sql = gen_insert_sql(FLAGS_finish_blk_tbl, values_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb begin task failed, sql:%s", sql.c_str());
        return;
    }
    if (file_finished) {
        std::vector<std::string> select_vec;
        std::map<std::string, std::string> where_map;
        where_map["table_info"] = "'" + _table_info + "'";
        where_map["version"] = std::to_string(_new_version);
        where_map["file_path"] = "'" + path + "'";
        where_map["database_name"] = "'" + db + "'";
        where_map["table_name"] = "'" + tbl + "'";
        std::string sql = gen_delete_sql(FLAGS_finish_blk_tbl, where_map);
        sql += " and (start_pos != " + std::to_string(start_pos) + " or end_pos != " + std::to_string(end_pos) + ")";
        int ret = querybaikaldb(sql, result_set);
        if (ret != 0) {
            DB_WARNING("delete all finished blocks fail after importer, sql:%s", sql.c_str()); 
        }
    }
    return;
}

void Fetcher::delete_finished_blocks_after_import() {
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> where_map;

    where_map["table_info"] = "'" + _table_info + "'";
    where_map["version"] = std::to_string(_new_version);
    std::string sql = gen_delete_sql(FLAGS_finish_blk_tbl, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("delete all finished blocks fail after importer, sql:%s", sql.c_str()); 
    }
    return; 
}

bool Fetcher::is_broken_point_continuing_support_type(OpType type) {
    if (type == OpType::REP 
        || type == OpType::UP
        || type == OpType::SQL_UP
        || type == OpType::DUP_UP
        || type == OpType::DEL) {
        return true;
    }
    return false;
}

int Fetcher::importer(const Json::Value& node, OpType type, const std::string& done_path, const std::string& charset) {
    ImporterHandle* importer = ImporterHandle::new_handle(type, _baikaldb_user, done_path);
    if (importer == nullptr) {
        return -1;
    }

    importer->set_need_iconv(_need_iconv);
    importer->set_charset(charset);
    importer->set_local_json(_is_local_done_json);
    importer->set_retry_times(_retry_times);
    importer->set_task_config(_config);
    ON_SCOPE_EXIT(([this, importer]() {
        delete importer;
    }));

    if (!is_broken_point_continuing_support_type(type)) {
        _broken_point_continuing = false;
    }
    
    int ret = importer->init(node, _filesystem_path_prefix);
    if (ret < 0) {
        _import_ret << importer->handle_result();
        DB_FATAL("importer init failed");
        return -1;
    }
    // 查出所有已完成的block信息
    get_finished_file_and_blocks(importer->get_db(), importer->get_table());
    importer->set_upload_errsql_info(_table_info, _new_version, _baikaldb);
    importer->set_finished_blocks(_broken_point_continuing, _finish_blocks_last_time);

    auto progress_func = [this, importer](const std::string& progress_str) {
        _progress_info.imported_line = importer->get_import_lines() + _import_line_last_import + _import_line;
        _progress_info.affected_row = importer->get_import_affected_row() + _affected_row_last_import + _import_affected_row;
        _progress_info.diff_line = importer->get_import_diff_lines() + _diff_line_last_import + _import_diff_line;
        if (_imported_tables > 0) {
            // 多表导入
            _progress_info.progress = time_print(_cost.get_time()) 
                                    + " (" + progress_str + ")"
                                    + _import_all_tbl_ret.str()
                                    + importer->get_all_import_result(_import_line_last_import, 
                                                                      _affected_row_last_import, 
                                                                      _diff_line_last_import,
                                                                      _err_sql_last_import);
        } else {
            _progress_info.progress = time_print(_cost.get_time()) 
                                    + " (" + progress_str + "), diffline: " + std::to_string(_progress_info.diff_line);
        }
        if (!_progress_info.updated_sample_sql) {
            _progress_info.sample_sql = importer->get_sample_sqls();
        }
        if (!_progress_info.updated_sample_diff_line) {
            _progress_info.sample_diff_line = importer->get_diffline_sample();
        }
        if (!_progress_info.updated_sql_err_reason) {
            _progress_info.sql_err_reason = importer->get_sql_err_reason();
        }
        update_task_progress();
        // SELECT config
        whether_importer_config_need_update();
    };
    auto finish_block_func = [this, importer](std::string path, 
                                    int64_t start_pos, 
                                    int64_t end_pos, 
                                    BlockHandleResult* result, 
                                    bool file_finished) {
        if (_broken_point_continuing) {
            insert_finished_file_or_blocks(importer->get_db(), 
                                           importer->get_table(), 
                                           path,
                                           start_pos, 
                                           end_pos, 
                                           result, 
                                           file_finished);
        }
    };
    int64_t lines = importer->run(_fs, _config, progress_func, finish_block_func);
    _import_ret << importer->handle_result();
    _import_all_tbl_ret << importer->get_all_import_result(_import_line_last_import, 
                                                           _affected_row_last_import, 
                                                           _diff_line_last_import,
                                                           _err_sql_last_import);
    if (lines < 0) {
        DB_FATAL("importer run failed");
        return -1;
    }
    _import_diff_line += importer->get_import_diff_lines();
    _import_err_sql += importer->get_err_sql_cnt();
    _import_affected_row += importer->get_import_affected_row();
    _import_line += lines - importer->get_import_diff_lines();
    _import_diffline_sample = importer->get_diffline_sample();
    if (_broken_point_continuing) {
        _import_diff_line += _diff_line_last_import;
        _import_line += _import_line_last_import;
        _import_err_sql += _err_sql_last_import;
        _import_affected_row += _affected_row_last_import;
    }
    return 0;
}

int Fetcher::import_to_baikaldb() {
    Json::Value done_root;
    int ret = get_done_file(_done_real_path + _done_name, done_root);
    if (ret < 0) {
        return -1;
    }

    Bthread capture_worker;
    time_t now = time(NULL);
    int64_t start_ts = baikaldb::timestamp_to_ts((uint32_t)now);
    if (ImporterHandle::is_launch_capture_task(done_root)) {
        Json::Value node = ImporterHandle::get_node_json(done_root);
        if (LaunchCapture::get_instance()->truncate_old_table(_baikaldb_user, node) != 0) {
            DB_FATAL("truncate old table failed");
            return -1;
        }
        capture_worker.run([this, &done_root, start_ts]() {
            LaunchCapture::get_instance()->set_capture_initial_param(_baikaldb_user);
            Json::Value node = ImporterHandle::get_node_json(done_root);
            LaunchCapture::get_instance()->init_capture(node, start_ts);
        });
    }

    ON_SCOPE_EXIT(([this, &capture_worker]() {
        capture_worker.join();
        LaunchCapture::get_instance()->destroy();
    }));

    try {
        for (auto& name_type : op_name_type) {
            if (done_root.isMember(name_type.name)) {
                DB_WARNING("%s hdfs real path:%s, charset:%s", name_type.name.c_str(), _done_real_path.c_str(), _charset.c_str());
                for (auto& node : done_root[name_type.name]) {
                    std::string db_name;
                    std::string table_name;
                    std::string mails;
                    if (node.isMember("db") && node.isString()) {
                        db_name = node["db"].asString();
                    }
                    if (node.isMember("table") && node.isString()) {
                        table_name = node["table"].asString();
                    }
                    if (node.isMember("mail") && node.isString()) {
                        const std::string& mail = node["mail"].asString();
                        std::vector<std::string> users;
                        boost::split(users, mail, boost::is_any_of(" ;,\n\t\r"), boost::token_compress_on);
                        for (auto& user : users) {
                            mails += user;
                            if (!boost::iends_with(user, "@baidu.com")) {
                                mails += "@baidu.com";
                            }
                            mails += " ";
                        }
                    }
                    if (_is_fast_importer) {
                        // 快速导入
                        std::string tmp_path = node["path"].asString();
                        std::string path;
                        if (tmp_path.empty()) {
                            DB_WARNING("path is empty");
                            _import_ret << "path is empty";
                            return -1;
                        }
                        if (_done_real_path.empty() && !_is_local_done_json) {
                            if (tmp_path[0] == '/') {
                                std::vector<std::string> split_vec;
                                boost::split(split_vec, tmp_path, boost::is_any_of("/"));
                                tmp_path = split_vec.back();
                            }
                            if (FLAGS_is_mnt) {
                                tmp_path = "import_data";
                            }
                            path = FLAGS_data_path + "/" + tmp_path;
                            DB_WARNING("path:%s", path.c_str());
                        } else {
                            if (tmp_path[0] == '/') {
                                path = tmp_path;
                            } else {
                                path = _done_real_path + "/" + tmp_path;
                            } 
                            DB_WARNING("path:%s", path.c_str());
                        }
                        baikaldb::FastImporterCtrl fast_importer_main_worker(_baikaldb,
                                                                             _baikaldb_map,
                                                                             _baikaldb_user);
                        FastImportTaskDesc task_desc;
                        task_desc.meta_bns = _meta_bns;
                        task_desc.done_json = _local_done_json;
                        task_desc.file_path = path;
                        task_desc.table_info = _table_info;
                        task_desc.table_namespace = _table_namespace;
                        task_desc.charset = _charset;
                        task_desc.need_iconv = _need_iconv;
                        task_desc.old_version = _old_version;
                        task_desc.new_version = _new_version;
                        task_desc.main_id = _id;
                        task_desc.id = _id;
                        task_desc.baikaldb_resource = _baikaldb_resource;
                        task_desc.cluster_name = _cluster_name;
                        task_desc.user_name = _user_name;
                        task_desc.password = _password;
                        task_desc.conf = _config;
                        ret = fast_importer_main_worker.init(task_desc);
                        if (ret < 0) {
                            DB_FATAL("fast importer init main task:%s failed", _table_info.c_str());
                            return -1;
                        }
                        // 创建子任务并等待子任务完成
                        ret = fast_importer_main_worker.run_main_task();
                        _import_line = fast_importer_main_worker.get_import_line();
                        _import_diff_line = fast_importer_main_worker.get_import_diff_line();
                        _import_ret << fast_importer_main_worker.get_result();
                        _import_ret << ", import_diff_line: " << _import_diff_line;
                        if (ret < 0) {
                            DB_FATAL("fast importer run main task:%s failed", _table_info.c_str());
                            return -1;
                        }
                    } else {
                        // sql导入
                        ret = importer(node, name_type.type, _done_real_path, _charset);
                        if (ret < 0) {
                            DB_FATAL("importer :%s failed", name_type.name.c_str());
                            return -1;
                        }
                        _imported_tables += 1;
                    }
                    const int64_t import_all_line = _import_line + _import_diff_line;
                    if (_import_diff_line > import_all_line * 0.5) {
                        std::string err_msg = "import_diff_line[" + std::to_string(_import_diff_line) + 
                                              "] bigger than 50 percent import_all_line[" + std::to_string(import_all_line) + "]";
                        _import_ret << err_msg;
                        DB_FATAL("fail to import_to_baikaldb, err_msg: %s", err_msg.c_str());
                        if (mails != "") {
                            std::string cmd = "echo \"" + _import_ret.str() + "\" | mail -s 'BaikalDB import fail, " +
                                              db_name + "." + table_name + "' '" + mails + "' -- -f baikalDM@baidu.com";
                            system_cmd(cmd.c_str());
                        }
                        return -1;
                    }
                }
            }
        } 
    } catch (Json::LogicError& e) {
        _import_ret << "fail parse what: " << e.what();
        DB_FATAL("fail parse what:%s", e.what());
        return -1;
    }

    if (_imported_tables == 1) {
        _import_ret << " diff lines: " << _import_diff_line;
        if (_import_err_sql > 0) {
            _import_ret << " sql_err: " << _import_err_sql;
        }
    }
    return 0;
}

int Fetcher::select_new_task(baikal::client::ResultSet& result_set, bool is_fast_importer) {

    int affected_rows = 0;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    select_vec.push_back("*");
    where_map["status"] = "'idle'";
    where_map["resource_tag"] = "'" + FLAGS_resource_tag + "'";
    if (is_fast_importer) {
        where_map["fast_importer"] = "1";
    } else {
        where_map["fast_importer"] = "0";
    }
    std::string sql = gen_select_sql(FLAGS_param_tbl, select_vec, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str()); 
        return -1;
    }
    
    affected_rows = result_set.get_row_count();
    DB_TRACE("select new task row number:%d sql:%s", affected_rows, sql.c_str());
    return affected_rows; 
}

int Fetcher::update_task_doing(int64_t id, int64_t version) {
    int affected_rows = 0;
    int64_t nowtime_s = butil::gettimeofday_us() / (1000 * 1000);
    baikal::client::ResultSet result_set;

    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;
    set_map["status"]   = "'doing'";
    set_map["start_time"]   = std::to_string(nowtime_s);
    set_map["hostname"]   = "'" + FLAGS_hostname + "'";
    set_map["progress"]   = "''";
    where_map["status"] = "'idle'";
    where_map["id"] = std::to_string(id);
    where_map["version"] = std::to_string(version);

    std::string sql = gen_update_sql(FLAGS_param_tbl, set_map, where_map);

    int ret = querybaikaldb(sql, result_set);
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

int Fetcher::update_task_idle(int64_t id) {
    int affected_rows = 0;
    baikal::client::ResultSet result_set;

    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;
    set_map["status"]   = "'idle'";
    where_map["status"] = "'doing'";
    where_map["id"] = std::to_string(id);

    std::string sql = gen_update_sql(FLAGS_param_tbl, set_map, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb failed, sql:%s", sql.c_str()); 
        return -1;
    }

    affected_rows = result_set.get_affected_rows();
    if (affected_rows != 1 ) {
        DB_WARNING("update taskid idle affected row:%d, sql:%s", affected_rows, sql.c_str());
    }
    DB_TRACE("update taskid idle affected row:%d, sql:%s", affected_rows, sql.c_str());
    return affected_rows; 
}

int Fetcher::fetch_task(bool is_fast_importer) {
    int ret = 0;
    bool fetch = false;
    baikal::client::ResultSet result_set;
    static int64_t last_fetch_id = 0;
    
    ret = select_new_task(result_set, is_fast_importer);
    if (ret <= 0) {
        //等于0则表示没有获取到
        DB_WARNING("fetch task fail:%d", ret);
        return -2;
    }
    
    while (result_set.next()) {
        _import_ret.str("");
        _import_all_tbl_ret.str("");
        reset_all_last_import_info();
        _import_diffline_sample.clear();
        std::string status;
        std::string table_info;
        std::string done_file;
        std::string cluster_name;
        std::string user_name;
        std::string password;
        std::string modle;
        std::string user_sql;
        std::string charset;
        std::string baikaldb_resource;
        std::string config;
        std::string local_done_json;
        std::string table_namespace;
        std::string meta_bns;
        std::string backtrack_done; // 回溯某一天用

        int64_t id = 0;
        int64_t version = 0;
        int64_t ago_days = 0;
        int64_t interval_days = 0;
        int64_t bitflag = 0;
        int32_t need_iconv = 0;
        int32_t is_fast_importer = 0;
        int64_t retry_times = 0;
        int32_t broken_point_continuing = 0;
        result_set.get_string("status", &status);
        result_set.get_string("done_file", &done_file);
        result_set.get_string("cluster_name", &cluster_name);
        result_set.get_string("user_name", &user_name);
        result_set.get_string("password", &password);
        result_set.get_string("modle", &modle);
        result_set.get_string("user_sql", &user_sql);
        result_set.get_string("charset", &charset);
        result_set.get_string("table_info", &table_info);
        result_set.get_string("table_namespace", &table_namespace);
        result_set.get_string("baikaldb_resource", &baikaldb_resource);
        result_set.get_int64("id", &id);
        result_set.get_int64("version", &version);
        result_set.get_int64("ago_days", &ago_days);
        result_set.get_int64("interval_days", &interval_days);
        result_set.get_int64("bitflag", &bitflag);
        result_set.get_string("config", &config);
        result_set.get_string("meta_bns", &meta_bns);
        result_set.get_string("backtrack_done", &backtrack_done);
        result_set.get_int64("retry_times", &retry_times);
        // 如果设置 need_iconv，说明文件编码与 charset字段确定不一样，importer将不进行验证，直接将文件内容编码转成 charset 设置值。
        result_set.get_int32("need_iconv", &need_iconv);
        // 断点续传
        result_set.get_int32("broken_point_continuing", &broken_point_continuing);
        if (id <= last_fetch_id) {
            continue;
        }
        _need_iconv = need_iconv != 0;
        _broken_point_continuing = broken_point_continuing != 0;
        _config = config;
        result_set.get_string("local_done_json", &local_done_json);
        // 是否是快速导入
        result_set.get_int32("fast_importer", &is_fast_importer);
        auto iter = _baikaldb_map.find(baikaldb_resource);
        if (iter == _baikaldb_map.end()) {
            DB_FATAL("table_info:%s, has not baikaldb_resource:%s, fail", table_info.c_str(), baikaldb_resource.c_str());
            continue;
        }

        _baikaldb_user = iter->second;

        ret = update_task_doing(id, version);
        if (ret != 1) {
            continue;
        }
        _id = id;
        _old_version = version;
        _local_done_json = local_done_json;
        _new_version = 0;
        if (backtrack_done != "") {
            // backtrack_done可能是done json，也可能是done路径
            Json::Value json_root;
            Json::Reader json_reader;
            bool ret = json_reader.parse(backtrack_done, json_root);
            if (ret) {
                _local_done_json = backtrack_done;
                DB_WARNING("backtrack_done is json: %s", backtrack_done.c_str());
            } else {
                if (backtrack_done.find("{DATE}") != std::string::npos) {
                    DB_FATAL("backtrack_done has {DATE}: %s", backtrack_done.c_str());
                    update_task_idle(id);
                    continue;
                }
                auto pos = done_file.find_first_of("{DATE}");
                if (pos != std::string::npos) {
                    if (backtrack_done.size() < pos + 8) {
                        DB_FATAL("backtrack_done path wrong {DATE}: %s", backtrack_done.c_str());
                        update_task_idle(id);
                        continue;
                    }
                    std::string version_str = backtrack_done.substr(pos, 8);
                    _new_version = strtoll(version_str.c_str(), NULL, 10);
                }
                done_file = backtrack_done;
                DB_WARNING("backtrack_done is path: %s", backtrack_done.c_str());
            }
        }
        if (cluster_name == "wutai") {
            _done_file = FLAGS_hdfs_mnt_wutai + done_file;
            _filesystem_path_prefix = FLAGS_hdfs_mnt_wutai;
        } else if (cluster_name == "mulan") {
            _done_file = FLAGS_hdfs_mnt_mulan + done_file;
            _filesystem_path_prefix = FLAGS_hdfs_mnt_mulan;
        } else if (cluster_name == "khan") {
            _done_file = FLAGS_hdfs_mnt_khan + done_file;
            _filesystem_path_prefix = FLAGS_hdfs_mnt_khan;
        } else {
            _done_file = done_file;
            _filesystem_path_prefix = "/";
        }
        _modle = modle;
        _user_sql = user_sql;
        _charset = charset;
        _table_info = table_info;
        _baikaldb_resource = baikaldb_resource;
        _ago_days = ago_days;
        _old_bitflag = bitflag;
        _table_namespace = table_namespace;
        _meta_bns = meta_bns;
        if (_old_bitflag == 0) {
            _old_bitflag = 0xFFFFFFFF;
        }
        _new_bitflag = 0;
        _interval_days = interval_days;
        _is_fast_importer = is_fast_importer != 0;
        _cluster_name = cluster_name;
        _user_name = user_name;
        _password = password;
        _retry_times = retry_times;
        _backtrack_done = backtrack_done;
        prepare();
        int retry_time = 6;
        while (--retry_time > 0) {
            ret = create_filesystem(cluster_name, user_name, password);
            if (ret == 0) {
                break;
            }
            bthread_usleep(5 * 1000 * 1000);
        };
        if (ret < 0) {
            update_task_idle(id);
            begin_task("read done file fail");
            continue;
        }
        ret = analyse_version();
        if (ret < 0) {
            DB_WARNING("fetch task fail:%d, id:%d, done_file:%s, common path:%s, done name:%s, real path:%s, "
            "only one donefile:%d, old_version:%ld, new_version:%ld, today version:%ld "
            "user sql:%s, internal_days:%d, bitflag:%lx", 
            ret, _id, _done_file.c_str(), _done_common_path.c_str(), _done_name.c_str(), _done_real_path.c_str(), 
            _only_one_donefile, _old_version, _new_version, 
            _today_version, _user_sql.c_str(), _interval_days, bitflag);
            //销毁fs
            destroy_filesystem();
            update_task_idle(id);
            if (!_import_ret.str().empty()) {
                begin_task(_import_ret.str());
            }
            continue;
        }
        fetch = true;
        DB_WARNING("fetch task succ id:%d, done_file:%s, common path:%s, done name:%s, real path:%s, "
        "only one donefile:%d, old_version:%ld, new_version:%ld, today version:%ld "
        "user sql:%s, interval_days:%d, bitflag:%lx, retry_times: %ld", 
        _id, _done_file.c_str(), _done_common_path.c_str(), _done_name.c_str(), _done_real_path.c_str(), 
        _only_one_donefile, _old_version, _new_version, 
        _today_version, _user_sql.c_str(), _interval_days, bitflag, _retry_times);
        break;
    }
    if (!fetch) {
        last_fetch_id = 0;
        return -2;
    }
    last_fetch_id = _id;
    return 0;
}

std::string Fetcher::user_sql_replace(std::string &sql){
    std::string tmp_sql = sql;
    int64_t delete_day = get_ndays_date(_new_version, -14);
    std::string version_str = std::to_string(_new_version);
    if (tmp_sql.find("{TABLE}") != tmp_sql.npos) {
        tmp_sql.replace(tmp_sql.find("{TABLE}"), 7, _import_tbl);
    }
    if (tmp_sql.find("{VERSION}") != tmp_sql.npos) {
        tmp_sql.replace(tmp_sql.find("{VERSION}"), 9, version_str);
    }
    if (tmp_sql.find("{DELETE_DAY}") != tmp_sql.npos) {
        tmp_sql.replace(tmp_sql.find("{DELETE_DAY}"), 12, std::to_string(delete_day));
    }
    DB_WARNING("sql:%s, tmp_sql:%s", sql.c_str(), tmp_sql.c_str());
    return tmp_sql;
}

int Fetcher::exec_user_sql() {
    baikal::client::ResultSet result_set;
    std::string user_sql = _user_sql;
    int ret = 0;
    //执行用户sql
    std::vector<std::string> split_sql;
    boost::split(split_sql, user_sql, boost::is_any_of(";"));
    for (auto& s_sql : split_sql) {
        if (s_sql.empty()) {
            continue;
        }
        std::string exec_sql = user_sql_replace(s_sql);
        ret = _baikaldb_user->query(0, exec_sql, &result_set);
        if (ret != 0) {
            DB_WARNING("finish task succ querybaikaldb user sql fail, sql:%s", exec_sql.c_str());
            return -1;
        }
        DB_WARNING("finish task succ querybaikaldb user sql success, sql:%s", exec_sql.c_str());
    } 
    return 0;
}

// 开始任务，或者还没开始任务就失败了，比如done解析失败，insert到result_table
int Fetcher::begin_task(std::string result) {
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> values_map;

    values_map["database_name"] = "'" + _import_db + "'";
    values_map["table_name"] = "'" + _import_tbl + "'";
    values_map["hostname"] = "'" + FLAGS_hostname + "'";
    values_map["old_version"] = std::to_string(_old_version);
    values_map["new_version"] = std::to_string(_new_version);
    if (result.empty()) {
        values_map["status"] = "'doing'";
    } else {
        values_map["status"] = "'" + result + "'";
    }
    std::string sql = gen_insert_sql(FLAGS_result_tbl, values_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb begin task failed, sql:%s", sql.c_str());
        return ret;
    }

    _result_id = result_set.get_auto_insert_id();
    DB_WARNING("begin task success sql:%s, id:%lu", sql.c_str(), _result_id);
    return 0;
}

int Fetcher::finish_task(bool is_succ, std::string& result, std::string& time_cost) {
    baikal::client::ResultSet result_set;

    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;
    set_map["status"]   = "'idle'";
    if (is_succ) {
        set_map["version"]   = std::to_string(std::max(_new_version, _old_version));
        set_map["retry_times"] = std::to_string(0);
        if (_backtrack_done.empty()) {
            // 正常导入
            set_map["bitflag"]   = std::to_string(_new_bitflag);
        } else {
            // 回溯某一天
            set_map["backtrack_done"] = "''";
        }
    } else {
        if (!_backtrack_done.empty()) {
            // 回溯某一天失败了，不再重试
            set_map["backtrack_done"] = "''";
        }
        set_map["retry_times"] = std::to_string(_retry_times + 1);
    }

    where_map["status"] = "'doing'";
    where_map["id"] = std::to_string(_id);

    std::string sql = gen_update_sql(FLAGS_param_tbl, set_map, where_map);
    
    // 更新任务状态
    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
         DB_WARNING("query baikaldb update task status failed, sql:%s", sql.c_str());
         return -1;
    }

    DB_WARNING("query baikaldb update task status succ, sql:%s", sql.c_str());
    
    // 输出执行结果    
    set_map.clear();
    where_map.clear();
    set_map["exec_time"]   = "'" + time_cost + "'";
    set_map["import_line"] = std::to_string(_import_line);
    set_map["affected_row"] = std::to_string(_import_affected_row);
    if (_imported_tables <= 1) {
        // 单表导入
        set_map["status"]      = "'" + result + "'";
    } else {
        // 多表导入
        set_map["status"]      = "'" + result + ":" + _import_all_tbl_ret.str() + "'";
    }
    where_map["id"] = std::to_string(_result_id);

    sql = gen_update_sql(FLAGS_result_tbl, set_map, where_map);
    
    ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("update result table fail, sql:%s", sql.c_str());
        return -1;
    }

    DB_WARNING("update result table succ, sql:%s", sql.c_str());

    return 0;
}

void Fetcher::whether_importer_config_need_update() {
    int affected_rows = 0;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;
    baikal::client::ResultSet result_set;

    select_vec.emplace_back("config");
    where_map["id"] = std::to_string(_id);
    where_map["table_info"] = "'" + _table_info + "'";
    std::string sql = gen_select_sql(FLAGS_param_tbl, select_vec, where_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str()); 
        return;
    }
    
    affected_rows = result_set.get_row_count();
    DB_TRACE("select new task row number:%d sql:%s", affected_rows, sql.c_str());
    if (affected_rows != 1) {
        return;
    }
    while (result_set.next()) {
        std::string new_config;
        result_set.get_string("config", &new_config);
        if (new_config != _config) {
            // 目前只有tps才能动态调
            DB_WARNING("id: %d, table_info: %s, new_config: %s", _id, _table_info.c_str(), new_config.c_str());
            TaskConfig old_conf(_config);
            TaskConfig new_conf(new_config);
            if (old_conf.tps != new_conf.tps && new_conf.tps > 0) {
                GenericRateLimiter::get_instance()->set_bytes_per_second(new_conf.tps);
            }
            _config = new_config;
        }
        break;
    }
    return; 
}

int Fetcher::update_task_progress() {
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;
    set_map["exec_time"]   = "'" + _progress_info.progress + "'";
    set_map["import_line"]  = std::to_string(_progress_info.imported_line);
    set_map["affected_row"]  = std::to_string(_progress_info.affected_row);
    if (!_progress_info.updated_sample_sql && !_progress_info.sample_sql.empty()) {
        std::string sql = boost::replace_all_copy(_progress_info.sample_sql, "\\", "\\\\"); 
        sql = boost::replace_all_copy(sql, "'", "\\'");
        set_map["sample_sql"] = "'" + sql + "'";
    }
    if (!_progress_info.updated_sample_diff_line && !_progress_info.sample_diff_line.empty()) {
        set_map["diffline_sample"] = "'" + _progress_info.sample_diff_line + "'"; // mysql_escape_string过
    }
    if (!_progress_info.updated_sql_err_reason && !_progress_info.sql_err_reason.empty()) {
        std::string sql_err_reason = boost::replace_all_copy(_progress_info.sql_err_reason, "\\", "\\\\"); 
        sql_err_reason = boost::replace_all_copy(sql_err_reason, "'", "\\'");
        set_map["sql_err_reason"] = "'" + sql_err_reason + "'"; // sql失败原因
    }
    where_map["id"] = std::to_string(_result_id);

    std::string sql = gen_update_sql(FLAGS_result_tbl, set_map, where_map);
    
    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("update result table fail, sql:%s", sql.c_str());
        return -1;
    }
    DB_WARNING("update result progress succ, sql:%s", sql.c_str());
    if (!_progress_info.sample_sql.empty()) {
        _progress_info.updated_sample_sql = true;
    }
    if (!_progress_info.sample_diff_line.empty()) {
        _progress_info.updated_sample_diff_line = true;
    }
    if (!_progress_info.sql_err_reason.empty()) {
        _progress_info.updated_sql_err_reason = true;
    }
    return 0;
}

// 更新任务状态，避免由于任务执行期间挂掉导致的任务丢失
// 查找本节点做过的任务，可能由于进程crash导致任务中断
int Fetcher::update_main_task_idle(bool is_fast_importer) {
    int ret = 0;
    int affected_rows = 0;
    std::vector<int64_t> ids;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    std::string table_name;
    std::string status;
    table_name = FLAGS_param_tbl;
    status = "idle";
    if (is_fast_importer) {
        where_map["fast_importer"] = "1";
    } else {
        where_map["fast_importer"] = "0";
    }

    select_vec.emplace_back("id");
    where_map["hostname"] = "'" + FLAGS_hostname + "'";
    where_map["status"] = "'doing'";
    std::string sql = gen_select_sql(table_name, select_vec, where_map);

    ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb failed, sql:%s", sql.c_str());
        return -1;
    }

    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return 0;
    }
    
    int64_t id = 0;
    ids.reserve(5);
    while (result_set.next()) {
        result_set.get_int64("id", &id);
        ids.emplace_back(id);
    }

    for (auto& id : ids) {
        std::map<std::string, std::string> set_map;
        std::map<std::string, std::string> where_map;
        set_map["status"]   = "'" + status + "'";
        where_map["status"] = "'doing'";
        where_map["id"] = std::to_string(id);
        std::string sql = gen_update_sql(table_name, set_map, where_map);

        ret = querybaikaldb(sql, result_set);
        if (ret != 0) {
            DB_WARNING("query baikaldb failed, sql:%s", sql.c_str());
            continue;
        }

        DB_WARNING("update task query baikaldb succ, sql:%s", sql.c_str());
    }

    return 0;
}

int Fetcher::update_result_task_fail() {
    int ret = 0;
    int affected_rows = 0;
    std::vector<int64_t> ids;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    std::string table_name;
    std::string status;
    table_name = FLAGS_result_tbl;
    status = "failed!!! baikalImporter maybe exit!";

    select_vec.emplace_back("id");
    where_map["hostname"] = "'" + FLAGS_hostname + "'";
    where_map["status"] = "'doing'";
    std::string sql = gen_select_sql(table_name, select_vec, where_map);

    ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb failed, sql:%s", sql.c_str());
        return -1;
    }

    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return 0;
    }
    
    int64_t id = 0;
    ids.reserve(5);
    while (result_set.next()) {
        result_set.get_int64("id", &id);
        ids.emplace_back(id);
    }

    for (auto& id : ids) {
        std::map<std::string, std::string> set_map;
        std::map<std::string, std::string> where_map;
        set_map["status"]   = "'" + status + "'";
        where_map["status"] = "'doing'";
        where_map["id"] = std::to_string(id);
        std::string sql = gen_update_sql(table_name, set_map, where_map);

        ret = querybaikaldb(sql, result_set);
        if (ret != 0) {
            DB_WARNING("query baikaldb failed, sql:%s", sql.c_str());
            continue;
        }

        DB_WARNING("update task query baikaldb succ, sql:%s", sql.c_str());
    }

    return 0;
}

std::string Fetcher::time_print(int64_t cost) {
    std::ostringstream os;
    cost /= 1000000;
    int days = cost / (3600 * 24);
    if (days > 0) {
        os << days << "d ";
    } 
    cost = cost % (3600 * 24);
    int hours = cost / 3600;
    if (hours > 0) {
        os << hours << "h ";
    }
    cost = cost % 3600;
    int mins = cost / 60;
    if (mins > 0) {
        os << mins << "m ";
    }
    os << cost % 60 << "s";
    return os.str();
}

int Fetcher::run(bool is_fast_importer) {
    _import_ret.str("");
    _import_all_tbl_ret.str("");
    int ret = fetch_task(is_fast_importer);
    if (ret < 0) {
        DB_WARNING("fetch task fail:%d", ret);
        return -1;
    }
    // fetcher成功之后清理_import_ret，避免fetchetr失败的任务残留
    _import_ret.str("");
    _import_all_tbl_ret.str("");
    ret = begin_task();
    if (ret < 0) {
        DB_WARNING("begin task fail:%d", ret);
        return -1;
    }

    _cost.reset();
    ret = import_to_baikaldb();

    std::string time_cost(time_print(_cost.get_time()).c_str());
    if (ret == 0) {

        int ret1 = exec_user_sql();
        if (ret1 != 0) {
            std::string result = "execute user sql failed ";
            result += _import_ret.str();
            finish_task(true, result, time_cost);
        } else {
            std::string result = "success ";
            result += _import_ret.str();
            finish_task(true, result, time_cost);
            DB_NOTICE("import to baikaldb success, cost:%s", time_print(_cost.get_time()).c_str());
        }

    } else if (ret < 0) {

        std::string result = "import failed ";
        result += _import_ret.str();
        finish_task(false, result, time_cost);
        DB_NOTICE("import to baikaldb failed, cost:%s", time_print(_cost.get_time()).c_str());

    } 
    // 导完删除所有finish block, 不然同版本重新执行, 会全部跳过。
    if (!is_fast_importer && _broken_point_continuing) {
        delete_finished_blocks_after_import();
    }
    DB_NOTICE("delete all finished block log");
    return 0;
}

void handle_dm_signal() {
    DB_WARNING("recv kill signal");
    baikaldb::BackUp::shutdown();
    baikaldb::Fetcher::shutdown();
    baikaldb::FastImporterCtrl::shutdown();
}

//处理历史遗留任务，可能由于core导致进程退出
int SqlAgg::reset_legacy_task() {
    int ret = 0;
    int affected_rows = 0;
    std::vector<int64_t> ids;
    ids.reserve(5);
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;
    select_vec.emplace_back("id");
    where_map["hostname"] = "'" + FLAGS_hostname + "'";
    where_map["status"] = "'doing'";
    std::string sql = gen_select_sql(select_vec, where_map);

    ret = _baikaldb_info->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query baikaldb failed");
        return -1;
    }

    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return 0;
    }

    int64_t id = 0;
    while (result_set.next()) {
        result_set.get_int64("id", &id);
        ids.push_back(id);
    }

    for (auto& id : ids) {
        std::map<std::string, std::string> set_map;
        std::map<std::string, std::string> where_map;
        set_map["status"]   = "'idle'";
        where_map["status"] = "'doing'";
        where_map["id"] = std::to_string(id);
        std::string sql = gen_update_sql(set_map, where_map);
        ret = _baikaldb_info->query(0, sql, &result_set);
        if (ret != 0) {
            DB_FATAL("query baikaldb failed, sql:%s", sql.c_str());
            continue;
        }

        DB_WARNING("update task query baikaldb succ, sql:%s", sql.c_str());
    }

    return 0;
}

bool SqlAgg::need_to_trigger(int64_t& date) {

    struct tm local;                  
    time_t timep;

    timep = time(NULL);                         
    localtime_r(&timep, &local); //localtime把time_t类型转换成struct tm
                                               
    int64_t l_date = (local.tm_year + 1900) * 10000 + (local.tm_mon + 1) * 100 + local.tm_mday;

    int64_t ndays_date = get_ndays_date(l_date, -1);

    date = ndays_date;
    if (local.tm_hour == 0) {
        return true;
    }

    return false;
}

int SqlAgg::gen_agg_task() {

    baikal::client::ResultSet result_set;

    int64_t date = 0;
    if (!need_to_trigger(date)) {
        return -1;
    }

    std::string sql = "INSERT IGNORE " + FLAGS_param_db + "." + FLAGS_sql_agg_tbl + " (date, status) VALUES (" + std::to_string(date) + ", 'idle')";

    int ret = _baikaldb_info->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query baikaldb failed, sql:%s", sql.c_str()); 
        return -1;
    }

    DB_WARNING("insert_backup_task, sql:%s", sql.c_str());

    return 0;   
}

bool SqlAgg::agg_task_is_doing() {
    int affected_rows = 0;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    select_vec.emplace_back("*");
    where_map["status"] = "'doing'";
    std::string sql = gen_select_sql(select_vec, where_map);

    int ret = _baikaldb_info->query(0, sql, &result_set);
    if (ret != 0) {
        DB_WARNING("select doing task failed, sql:%s", sql.c_str()); 
        return false;
    }

    affected_rows = result_set.get_row_count();
    if (affected_rows > 0) {
        DB_WARNING("some task id doing");
        return true;
    }

    return false;
}

int SqlAgg::update_task_doing(int64_t id) {

    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;

    set_map["status"]    = "'doing'";
    set_map["hostname"]  = "'" + FLAGS_hostname + "'";
    set_map["start_time"] = "now()";
    where_map["id"] = std::to_string(id);
    where_map["status"]  = "'idle'";
    std::string sql = gen_update_sql(set_map, where_map);

    int ret = _baikaldb_info->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query baikaldb failed, sql:%s", sql.c_str()); 
        return -1;
    }

    int affected_rows = result_set.get_affected_rows();
    if (affected_rows != 1 ) {
        DB_FATAL("update taskid doing affected row:%d, sql:%s", affected_rows, sql.c_str());
        return -1;
    }

    DB_WARNING("update taskid doing, sql:%s", sql.c_str());

    return 0;
}

int SqlAgg::update_task_status(int64_t id, std::string status) {

    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;

    set_map["status"]   = "'" + status + "'";
    set_map["end_time"] = "now()";
    where_map["id"]     = std::to_string(id);
    where_map["status"] = "'doing'";

    std::string sql = gen_update_sql(set_map, where_map);

    int ret = _baikaldb_info->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query baikaldb failed, sql:%s", sql.c_str()); 
        return -1;
    }

    int affected_rows = result_set.get_affected_rows();
    if (affected_rows != 1 ) {
        DB_FATAL("update taskid doing affected row:%d, sql:%s", affected_rows, sql.c_str());
        return -1;
    }

    DB_WARNING("update taskid doing, sql:%s", sql.c_str());

    return 0;
}

int SqlAgg::replace_into_table(const std::vector<std::string>& insert_values) {

    if (insert_values.size() <= 0) {
        return 0;
    }

    std::string sql = "REPLACE INTO BaikalStat.baikaldb_trace_agg_by_day (q, month, date, sign, family, tbl, pv, total_cost, affected_rows, scan_rows, filter_rows, "
                " err_count, region_count) VALUES ";
    sql += insert_values[0];

    for (size_t i = 1; i < insert_values.size(); i++) {
        sql += "," + insert_values[i];
    }

    baikal::client::ResultSet result_set;

    int ret = _baikaldb_task->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query fail, sql:%s", sql.c_str());
        return -1;
    }

    return 0;
}

void SqlAgg::get_sample_sql(const std::string& sign, std::string* sample_sql) {
    std::string sql = "select `sql` as sample_sql from BaikalStat.sign_family_table_sql where sign = " + sign;
    baikal::client::ResultSet result_set;

    int ret = _baikaldb_task->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query fail, sql:%s", sql.c_str());
        *sample_sql = "ERROR";
        return;
    }

    while (result_set.next()) {
        result_set.get_string("sample_sql", sample_sql);
        break;
    }

    return;

}

int SqlAgg::send_mail(const char* to, const char *message) {
    int retval = -1;
    FILE *mailpipe = popen("/usr/lib/sendmail -t", "w");
    if (mailpipe != NULL) {
        fprintf(mailpipe, "To: %s\n", to);
        fprintf(mailpipe, "From: %s\n", "work");
        fprintf(mailpipe, "Subject: %s\n\n", "BaikalDB alarm");
        fwrite(message, 1, strlen(message), mailpipe);
        fwrite(".\n", 1, 2, mailpipe);
        pclose(mailpipe);
        retval = 0;
     } 
     return retval;
}

void SqlAgg::gen_mail_message(const std::string& date_str, const int64_t& pv, const int64_t& avg, const std::string& db, const std::string& tbl, 
    const std::map<std::string, RuleDesc>& sign_rule, const std::set<std::string>& shield_signs, 
    std::ostringstream& os) {
    std::string link = "http://showx.baidu.com/group/watt/report/54637?conditions=%7B%22dateTime"
        "Range%22%3A%22{DATE}%2000%3A00%3A00%2C{DATE}%2023%3A59%3A00%22%2C%22db%22%3A%22%22%2C%22tbl%22%3A%22%22%2C%22type%22%3A%224%22%2C%22si"
        "gn%22%3A%22{SIGN}%22%7D";

    for (auto& sub_iter : sign_rule) {
        if (shield_signs.count(sub_iter.first) != 0) {
            continue;
        }
        if (sub_iter.second.pv > pv && sub_iter.second.avg > avg) {
            os << "============ " << date_str << " ===========" << "\n";
            os << "database: " << db << "      " << "table: " << tbl << "\n";
            os << "pv: " <<  sub_iter.second.pv << "  avg: " << sub_iter.second.avg << " us" << "\n";
            os << "sign: " << sub_iter.first << "\n";
            std::string sample_sql;
            get_sample_sql(sub_iter.first, &sample_sql);
            os << "SQL: " << sample_sql << "\n";
            std::string tmp_link = link;
            tmp_link.replace(tmp_link.find("{DATE}"), 6, date_str);
            tmp_link.replace(tmp_link.find("{DATE}"), 6, date_str);
            tmp_link.replace(tmp_link.find("{SIGN}"), 6, sub_iter.first);
            os << "link: " << tmp_link << "\n\n";
        }
    }
    
}

int SqlAgg::alarm(int date, const AlarmRule& rule) {
    baikal::client::ResultSet result_set;
    std::string sql = "SELECT family, tbl, pv, avg, user_mail, sign FROM " + FLAGS_param_db + ".ALARM_CONF";
    int ret = _baikaldb_info->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query fail, sql:%s", sql.c_str());
        return -1;
    }

    std::string tmp_str   = std::to_string(date);

    if (tmp_str.length() != 8) {
        DB_FATAL("date:%d", date);
        return -1;
    }

    std::string year_str  = tmp_str.substr(0, 4);
    std::string month_str = tmp_str.substr(4, 2);
    std::string day_str   = tmp_str.substr(6, 2);
    std::string date_str  = year_str + "-" + month_str + "-" + day_str;
    
    while (result_set.next()) {
        std::set<std::string> shield_signs; //屏蔽的sign
        std::string family;
        std::string tbl;
        std::string user_mail;
        std::string sign;
        int64_t     pv     = 0;
        int64_t     avg    = 0;

        result_set.get_int64("pv", &pv);
        result_set.get_int64("avg", &avg);
        result_set.get_string("family", &family);
        result_set.get_string("tbl", &tbl);
        result_set.get_string("user_mail", &user_mail);
        result_set.get_string("sign", &sign);
        std::vector<std::string> table_split_vec;
        boost::split(table_split_vec, tbl, boost::is_any_of(","));
        std::vector<std::string> family_split_vec;
        boost::split(family_split_vec, family, boost::is_any_of(","));
        std::vector<std::string> sign_split_vec;
        boost::split(sign_split_vec, sign, boost::is_any_of(","));
        for (auto& s : sign_split_vec) {
            DB_WARNING("shield_signs:%s", s.c_str());
            shield_signs.insert(s);
        }
        std::ostringstream os;
        for (auto& db : family_split_vec) {
            for (auto& t : table_split_vec) {
                if (t == "ALL") {
                    for (auto& iter : rule) {
                        std::vector<std::string> split_vec2;
                        boost::split(split_vec2, iter.first, boost::is_any_of("."));
                        if (db != split_vec2[0]) {
                            continue;
                        }

                        gen_mail_message(date_str, pv, avg, db, split_vec2[1], iter.second, shield_signs, os);
                    }
                } else {
                    auto iter = rule.find(db + "." + t);
                    if (iter == rule.end()) {
                        continue;
                    }
                    gen_mail_message(date_str, pv, avg, db, t, iter->second, shield_signs, os);
                }
            }
        }

        if (os.str().empty()) {
            continue;
        }

        ret = send_mail(user_mail.c_str(), os.str().c_str());
        if (ret < 0) {
            DB_WARNING("send mail failed");
        }
        bthread_usleep(1000);        
    }

    return 0;

}

int SqlAgg::exec(int64_t date, AlarmRule& rules) {

    if (date <= 0) {
        return -1;
    }

    std::string tmp_str   = std::to_string(date);

    if (tmp_str.length() != 8) {
        DB_FATAL("date:%ld", date);
        return -1;
    }

    int month_int = date % 10000 / 100;

    std::string year_str  = tmp_str.substr(0, 4);
    std::string month_str = tmp_str.substr(4, 2);
    std::string day_str   = tmp_str.substr(6, 2);
    std::string year_month = year_str + "-" + month_str;
    std::string q_str      = year_str + "-Q" + std::to_string((month_int - 1) / 3 + 1);
    
    std::string begin_time = year_str + "-" + month_str + "-" + day_str + " 00:00:00:000";
    std::string end_time   = year_str + "-" + month_str + "-" + day_str + " 23:59:59:999";

    std::string sql = "SELECT family, tbl, sum(count) as pv, sum(sum) as cost, sign, sum(affected_rows) as a_rows, sum(scan_rows) as s_rows, "
                      "sum(filter_rows) as f_rows, sum(err_count) as sum_err_cnt, sum(region_count) as sum_region_cnt FROM BaikalStat.baikaldb_trace_info "
                      "WHERE time > '" + begin_time + "' AND time < '" + end_time + "' AND resource_tag not in (" + FLAGS_sql_agg_ignore_resource_tag + ") GROUP BY sign";


    TimeCost time;
    baikal::client::ResultSet result_set;

    int ret = _baikaldb_task->query(0, sql, &result_set);
    if (ret != 0) {
        DB_FATAL("query fail, sql:%s", sql.c_str());
        return -1;
    }

    DB_WARNING("exec sql:%s succ, cost:%ld", sql.c_str(), time.get_time());

    std::string date_str;
    date_str.reserve(10);
    date_str = year_str + "-" + month_str + "-" + day_str;
    std::vector<std::string> insert_values;
    insert_values.reserve(4096);
    int cnt = 0;

    while (result_set.next()) {

        std::string family;
        std::string tbl;
        std::string sign;
        int64_t     pv     = 0;
        int64_t     cost   = 0;
        int64_t     a_rows = 0;
        int64_t     s_rows = 0;
        int64_t     f_rows = 0;
        int64_t     sum_err_cnt = 0;
        int64_t     sum_region_cnt = 0;

        result_set.get_int64("pv", &pv);
        result_set.get_int64("cost", &cost);
        result_set.get_string("family", &family);
        result_set.get_string("tbl", &tbl);
        result_set.get_string("sign", &sign);
        result_set.get_int64("a_rows", &a_rows);
        result_set.get_int64("s_rows", &s_rows);
        result_set.get_int64("f_rows", &f_rows);
        result_set.get_int64("sum_err_cnt", &sum_err_cnt);
        result_set.get_int64("sum_region_cnt", &sum_region_cnt);

        //q, month, date, sign, family, tbl, pv, total_cost, affected_rows, scan_rows, filter_rows
        std::string values = "('" + q_str + "','" + year_month + "','" + date_str + "'," + sign + ",'" + family + "','" + tbl + "'," 
                    + std::to_string(pv) + "," + std::to_string(cost) + "," + std::to_string(a_rows) + "," + std::to_string(s_rows) + "," + std::to_string(f_rows) + 
                    + "," + std::to_string(sum_err_cnt) + "," + std::to_string(sum_err_cnt) + ")";

        if (pv > 0) {
            RuleDesc desc;
            desc.pv = pv;
            desc.avg = cost / pv;
            rules[family + "." + tbl][sign] = desc; 
        }
        insert_values.push_back(values);

        cnt++;

        if (insert_values.size() >= 100) {

            ret = replace_into_table(insert_values);
            if (ret != 0) {
                DB_FATAL("insert into table fail");
                return -1;
            }

            insert_values.clear();

        }

    }

    if (insert_values.size() > 0) {

        ret = replace_into_table(insert_values);
        if (ret != 0) {
            DB_FATAL("insert into table fail");
            return -1;
        }

    }

    return cnt;

}

int SqlAgg::fetch_new_task() {

    int affected_rows = 0;

    baikal::client::ResultSet          result_set;
    std::vector<std::string>           select_vec;
    std::map<std::string, std::string> where_map;

    if (agg_task_is_doing()) {
        return -2;
    }

    select_vec.emplace_back("*");
    where_map["status"] = "'idle'";
    std::string sql = gen_select_sql(select_vec, where_map);
    int ret = _baikaldb_info->query(0, sql, &result_set);
    if (ret != 0) {
        DB_WARNING("select new task failed, sql:%s", sql.c_str()); 
        return -1;
    }

    affected_rows = result_set.get_row_count();
    if (affected_rows == 0) {
        return -2;
    }

    while (result_set.next()) {

        int64_t id  = 0;
        int64_t date = 0;
        std::string status;

        result_set.get_int64("date", &date);
        result_set.get_int64("id", &id);

        if (update_task_doing(id) < 0) {
            continue;
        }
        AlarmRule rules;
        int ret = exec(date, rules);
        alarm(date, rules);
        if (ret < 0) {
            update_task_status(id, "idle");
        } else {
            update_task_status(id, "success, " + std::to_string(ret) + " sql agg");
        }

        break;
    }

    return 0;
}

void SqlAgg::run_sql_agg() {
    while (true) {

        bthread_usleep(5 * 60 * 1000000);
        DB_WARNING("run_sql_agg");
        reset_legacy_task();

        gen_agg_task();

        fetch_new_task();

    }
}


} // namespace baikaldb

// core太大，获取堆栈
void sigsegv_handler(int signum, siginfo_t* info, void* ptr) {
    void* buffer[1000];
    char** strings;
    int nptrs = backtrace(buffer, 1000);
    DB_FATAL("segment fault, backtrace() returned %d addresses", nptrs);
    strings = backtrace_symbols(buffer, nptrs);
    if (strings != NULL) {
        for (int j = 0; j < nptrs; j++) {
            DB_FATAL("%s", strings[j]);
        }
    }
    // 虚存太高，不core直接重启
    exit(0);
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, (sighandler_t)baikaldb::handle_dm_signal);
    signal(SIGTERM, (sighandler_t)baikaldb::handle_dm_signal);
    struct sigaction act;
    int sig = SIGSEGV;
    sigemptyset(&act.sa_mask);
    act.sa_sigaction = sigsegv_handler;
    act.sa_flags = SA_SIGINFO;
    if (sigaction(sig, &act, NULL) < 0) {
        DB_FATAL("sigaction fail, %m");
        exit(1);
    }
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    // brpc::StartDummyServerAt(8800);

    // Initail log
    if (baikaldb::init_log(argv[0]) != 0) {
         fprintf(stderr, "log init failed.");
         return -1;
    }

    DB_WARNING("log file load success");

    baikal::client::Manager manager;

    int ret = manager.init("conf", "baikal_client.conf");
    if (ret != 0) {
        DB_FATAL("baikal client init fail:%d", ret);
        return -1;
    }

    std::vector<std::string> split_info;
    boost::split(split_info, baikaldb::FLAGS_baikaldb_resource, boost::is_any_of(","));
    DB_WARNING("baikaldb_resource:%s", baikaldb::FLAGS_baikaldb_resource.c_str());

    std::map<std::string, baikal::client::Service*> baikaldb_map;
    for (auto& srv_name : split_info) {
        if (srv_name == "baikaldb" || srv_name == "baikaldb_trace") {
            continue;
        }
        auto tmp_baikaldb = manager.get_service(srv_name);
        if (tmp_baikaldb == NULL) {
            DB_FATAL("baikaldb srv_name:%s is null", srv_name.c_str());
            return -1;
        }
        DB_WARNING("srv_name : %s, init success", srv_name.c_str());
        baikaldb_map[srv_name] = tmp_baikaldb;
    }

    auto baikaldb = manager.get_service("baikaldb");
    if (baikaldb == NULL) {
        DB_FATAL("baikaldb is null");
        return -1;
    }

    auto baikaldb_trace = manager.get_service("baikaldb_trace");
    // 分区表需要初始化函数集
    baikaldb::FunctionManager::instance()->init();
    if (baikaldb::SchemaFactory::get_instance()->init() != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    }
    if (baikaldb::DMRocksWrapper::get_instance()->init(baikaldb::FLAGS_db_path) != 0) {
        DB_FATAL("DMRocksWrapper init failed");
        return -1;
    }
    DB_WARNING("int baikaldb service success");

    if (baikaldb::FLAGS_is_agg_sql) {
        if (baikaldb_trace == NULL) {
            DB_FATAL("baikaldb_trace init fail");
            return -1;
        }
    }
    // 重启之后的一些初始化操作
    if (baikaldb::FLAGS_is_hdfs_import || baikaldb::FLAGS_is_fast_importer) {
        baikaldb::Fetcher fetcher(baikaldb, baikaldb_map);
        // 1. 更新残留任务的result table信息
        fetcher.update_result_task_fail();
    }

    baikaldb::SqlAgg sqlagg(baikaldb, baikaldb_trace);
    if (baikaldb::FLAGS_is_agg_sql) {
        DB_WARNING("sql agg bthread run");
        sqlagg.run();
    }

    baikaldb::Bthread backup_fetch_worker {&BTHREAD_ATTR_NORMAL};
    backup_fetch_worker.run([&baikaldb](){
        baikaldb::BackUpImport backup(baikaldb);
        while (!baikaldb::Fetcher::is_shutdown()) {
            if (baikaldb::FLAGS_is_sst_backup) {
                // 产生新任务
                backup.gen_backup_task();
            }
            bthread_usleep(5 * 60 * 1000000);
        }
    });

    baikaldb::Bthread fast_main_worker {&BTHREAD_ATTR_NORMAL};
    fast_main_worker.run([&baikaldb, &baikaldb_map](){
        while (!baikaldb::Fetcher::is_shutdown()) {
            // 快速导入主任务，生成子任务，等待子任务执行完
            if (baikaldb::FLAGS_is_fast_importer) {
                baikaldb::Fetcher fetcher(baikaldb, baikaldb_map);
                int ret = fetcher.init();
                if (ret < 0) {
                    DB_WARNING("fetcher init faild:%d", ret);
                    continue;
                }
                // 将残留快速导入主任务状态都置为idle
                fetcher.update_main_task_idle(true);
                fetcher.run(true);
            }
            bthread_usleep(30 * 1000000);
        }
    });

    baikaldb::Bthread importer_worker {&BTHREAD_ATTR_NORMAL};
    importer_worker.run([&baikaldb, &baikaldb_map](){
        // 快速导入子任务和sql导入放在一个bthread里面做，避免一个DM同时做快速导入子任务和sql导入，相互影响导入性能
        while (!baikaldb::Fetcher::is_shutdown()) {
             if (baikaldb::FLAGS_is_hdfs_import) {
                baikaldb::Fetcher fetcher(baikaldb, baikaldb_map);
                // 将残留sql导入任务状态都置为idle
                fetcher.update_main_task_idle(false);
            }
            // 处理快速导入子任务
            if (baikaldb::FLAGS_is_fast_importer) {
                baikaldb::FastImporterCtrl fast_importer_sub_worker(baikaldb, baikaldb_map);
                // 更新残留的快速导入子任务状态重置为idle
                fast_importer_sub_worker.handle_restart();
                fast_importer_sub_worker.run_sub_task();
            }
            // sql导入
            if (baikaldb::FLAGS_is_hdfs_import) {
                baikaldb::Fetcher fetcher(baikaldb, baikaldb_map);
                int ret = fetcher.init();
                if (ret < 0) {
                    DB_WARNING("fetcher init faild:%d", ret);
                    return;
                }
                fetcher.run(false);
            }
            bthread_usleep(30 * 1000000);
        }
    });

    while (!baikaldb::Fetcher::is_shutdown()) {
        if (baikaldb::FLAGS_is_sst_backup) {
            baikaldb::BackUpImport backup(baikaldb);
            backup.run();
        }
        bthread_usleep(60 * 1000000);

        continue;
    }

    fast_main_worker.join();
    importer_worker.join();
    baikaldb::GenericRateLimiter::get_instance()->stop();
    baikaldb::DMRocksWrapper::get_instance()->close();

    return 0;
}
