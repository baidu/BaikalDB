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
#include "backup_import.h"
#include "proto/fc.pb.h"
#include "time.h"

namespace baikaldb {
DEFINE_string(hostname, "hostname", "host name");

DEFINE_string(param_db, "HDFS_TASK_DB", "param database");
DEFINE_string(param_tbl, "HDFS_TASK_TABLE", "hdfs import conf table");
DEFINE_string(result_tbl, "HDFS_TASK_RESULT_TABLE", "hdfs import result info table");
DEFINE_string(sst_backup_tbl, "SST_BACKUP_TABLE", "sst backup result table");
DEFINE_string(sst_backup_info_tbl, "SST_BACKUP_INFO", "sst backup conf table");

DEFINE_int32(tasktimeout_s, 60*60, "task time out second");
DEFINE_int32(interval_days, 7, "defult interval days");
DEFINE_string(hdfs_mnt_wutai, "/home/work/hadoop_cluster/mnt/wutai", "wutai mnt path");
DEFINE_string(hdfs_mnt_mulan, "/home/work/hadoop_cluster/mnt/mulan", "mulan mnt path");
DEFINE_string(hdfs_mnt_khan, "/home/work/hadoop_cluster/mnt/khan", "khan mnt path");

int Fetcher::init(baikal::client::Service* baikaldb, baikal::client::Service* baikaldb_gbk, baikal::client::Service* baikaldb_utf8) {
    _baikaldb = baikaldb;
    _baikaldb_gbk = baikaldb_gbk;
    _baikaldb_utf8 = baikaldb_utf8;

    int ret = _importer.init(_baikaldb_gbk, _baikaldb_utf8);
    if (ret != 0) {
        DB_FATAL("importer init fail:%d", ret);
        return -1;
    }

    if (FLAGS_hostname == "hostname") {
        DB_FATAL("hostname is default");
        return -1;
    }

    _today_version = get_today_date();
    _interval_days = FLAGS_interval_days;
    return 0;
}

Fetcher::~Fetcher() {

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
        DB_FATAL("sql_len:%d query fail : %s", sql.size(), sql.c_str());
        return ret;
    }
    DB_TRACE("query %d times, sql:%s affected row:%d", retry, sql.c_str(), result_set.get_affected_rows());
    return 0;
}

//准备工作：分裂donefile的路径和名字；获取集群路径和挂载路径
void Fetcher::prepare() {
    if (_done_file.find("{DATE}") == _done_file.npos) {
        _only_one_donefile = true;
    } else {
        _only_one_donefile = false;
    }
    std::vector<std::string> split_info;
    boost::split(split_info, _table_info, boost::is_any_of("."));
    _import_db = split_info.front();
    _import_tbl = split_info.back();
    std::vector<std::string> split_vec;
    boost::split(split_vec, _done_file, boost::is_any_of("/"));
    _done_name = split_vec.back();
    std::string str = _done_file;
    _done_common_path = str.replace(str.find(_done_name.c_str()), _done_name.size(), "/");
    get_cluster_info(_cluster_name);
    DB_WARNING("task only one donefile:%d, today version:%lld, done name:%s, done_common_path:%s, "
        "hdfs_mnt:%s, _import_db:%s, _import_tbl:%s", _only_one_donefile, _today_version, _done_name.c_str(), 
        _done_common_path.c_str(), _hdfs_mnt.c_str(), _import_db.c_str(), _import_tbl.c_str());
}
//从集群信息表中获取集群信息
void Fetcher::get_cluster_info(const std::string& cluster_name) {
    if (cluster_name == "wutai") {
        _hdfs_mnt = FLAGS_hdfs_mnt_wutai;
    }
    if (cluster_name == "mulan") {
        _hdfs_mnt = FLAGS_hdfs_mnt_mulan;
    }
    if (cluster_name == "khan") {
        _hdfs_mnt = FLAGS_hdfs_mnt_khan;
    }
}

//替换文件路径中的通配符
std::string Fetcher::replace_path_date(int64_t version) {
    std::string str = _done_common_path;
    str.replace(str.find("{DATE}"), 6, std::to_string(version));
    return str;
}

//分析只有一个done文件的情况，因为版本号在done文件内部，所以只能读取json之后分析
int Fetcher::analyse_one_donefile() {
    std::ifstream done_ifs(_hdfs_mnt + _done_real_path + _done_name);
    std::string done_config(
        (std::istreambuf_iterator<char>(done_ifs)),
        std::istreambuf_iterator<char>());
    Json::Reader reader;
    Json::Value done_root;
    bool ret1 = reader.parse(done_config, done_root);
    if (!ret1) {
        DB_FATAL("fail parse %d", ret1);
        return -1;
    }
    if (done_root["replace"].isArray()) {
        for (auto& node : done_root["replace"]) {
            if (_modle != "replace") {
                DB_FATAL("donefile modle:replace isn`t match param modle:%s fail", _modle.c_str());
                return -1;
            }
            return analyse_one_do(node, REP);
        }
    }
    DB_FATAL("task is not update or replace fail");
    //其他类型任务暂不处理
    return -1;
}
int Fetcher::analyse_one_do(const Json::Value& node, OpType type) {
    if (!_only_one_donefile) {
        return 0;
    }
    if (node.isMember("version")) {
        int64_t done_version = atoll(node["version"].asString().c_str());
        //初次导入
        if (_old_version == 0) {
            _new_version = done_version;
            return 0;
        }
        //replace模式
        if (type == REP) {
            if (done_version == _old_version) {
                return -2;
            } else if (done_version > _old_version) {
                _new_version = done_version;
                return 0;
            } else {
                return -1;
            }
        }
        return 0;
    } else {
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
        std::string done_file = _hdfs_mnt + path + _done_name;
        if (boost::filesystem::is_regular_file(done_file)) {
            find = true;
            _new_version = version;
            _done_real_path = path;
            DB_WARNING("first deal done file:%s exist new_version:%lld, done_real_path:%s", 
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
        std::string done_file = _hdfs_mnt + path + _done_name;
        if (boost::filesystem::is_regular_file(done_file)) {
            find = true;
            _new_version = version;
            _done_real_path = path;
            _new_bitflag = 0xFFFFFFFF;
            DB_WARNING("table_info:%s, first deal done file:%s exist new_version:%lld, done_real_path:%s", 
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
        DB_FATAL("table_inf:%s done_file:%s old_bitfalg is illegal");
        return -1;
    }
    for (int i = 0; i < _interval_days; i++) {
        bitflag = bitflag << 1;
        before_days++;
        if ((_old_bitflag & bitflag) != 0) {
            int64_t version = get_ndays_date(_old_version, -before_days);
            DB_WARNING("table_info:%s, done_file:%s, %d days before %lld, date:%lld already done _old_bitflag:%x, bitflag:%x", 
                _table_info.c_str(), _done_file.c_str(), before_days, _old_version, version, _old_bitflag, bitflag);        
            continue;
        } else {
            int64_t version = get_ndays_date(_old_version, -before_days);
            std::string path = replace_path_date(version);
            std::string done_file = _hdfs_mnt + path + _done_name;
            if (boost::filesystem::is_regular_file(done_file)) {
                find = true;
                _new_bitflag = _old_bitflag | bitflag;
                _new_version = _old_version;
                _done_real_path = path;
                DB_WARNING("done file:%s old_bitflag:%x, bitflag:%x, new_bitflag:%x, version:%lld find path success", 
                    done_file.c_str(), _old_bitflag, bitflag, _new_bitflag, version);
                break;
            } else {
                DB_WARNING("done file:%s is not exist， old_bitflag:%x, bitflag:%x, version:%lld", 
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
        std::string done_file = _hdfs_mnt + path + _done_name;
        if (boost::filesystem::is_regular_file(done_file)) {
            find = true;
            _new_version = version;
            _new_bitflag = new_bitflag | 1;
            _done_real_path = path;
            DB_WARNING("table_info:%s, done file:%s old_bitflag:%x, new_bitflag:%x, version:%lld find path success", 
                _table_info.c_str(), done_file.c_str(), _old_bitflag, _new_bitflag, version);
            break;
        } else {
            DB_WARNING("table_info:%s, done_file:%s, the day %lld hasn`t done file", 
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
    for (i = 1; i < _interval_days; ++i) {
        int64_t version = get_ndays_date(begin_version, -i);
        if (version <= _old_version) {
            break;
        }
        std::string path = replace_path_date(version);
        std::string done_file = _hdfs_mnt + path + _done_name;
        if (boost::filesystem::is_regular_file(done_file)) {
            find = true;
            _new_version = version;
            _done_real_path = path;
            DB_WARNING("deal replace done file:%s is exist new_version:%lld, done_real_path:%s", 
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
    if (_only_one_donefile) {
        //只有一个done文件，需要在读取json之后分析
        _done_real_path = _done_common_path;
        return analyse_one_donefile();
    }    
    if (_modle == "replace") {
        return analyse_multi_donefile_replace();
    } else {
        return analyse_multi_donefile_update();
    }

}

int Fetcher::import_to_baikaldb() {
    std::ifstream done_ifs(_hdfs_mnt + _done_real_path + _done_name);
    std::string done_config(
        (std::istreambuf_iterator<char>(done_ifs)),
        std::istreambuf_iterator<char>());
    Json::Reader reader;
    Json::Value done_root;
    bool ret1 = reader.parse(done_config, done_root);
    if (!ret1) {
        DB_FATAL("fail parse %d", ret1);
        return -1;
    }
    int ret = 0;
    //BthreadCond cond;
    if (done_root["delete"].isArray()) {
        DB_WARNING("delete hdfs mnt:%s, real path:%s, charset:%s", _hdfs_mnt.c_str(), _done_real_path.c_str(), _charset.c_str());
        for (auto& node : done_root["delete"]) {
            ret = _importer.handle(node, DEL, _hdfs_mnt, _done_real_path, _charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["update"].isArray()) {
        DB_WARNING("update hdfs mnt:%s, real path:%s, charset:%s", _hdfs_mnt.c_str(), _done_real_path.c_str(), _charset.c_str());
        for (auto& node : done_root["update"]) {
            ret = _importer.handle(node, UP, _hdfs_mnt, _done_real_path, _charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["replace"].isArray()) {
        DB_WARNING("replace hdfs mnt:%s, real path:%s, charset:%s", _hdfs_mnt.c_str(), _done_real_path.c_str(), _charset.c_str());
        for (auto& node : done_root["replace"]) {
            ret = _importer.handle(node, REP, _hdfs_mnt, _done_real_path, _charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["select"].isArray()) {
        DB_WARNING("select hdfs mnt:%s, real path:%s, charset:%s", _hdfs_mnt.c_str(), _done_real_path.c_str(), _charset.c_str());
        for (auto& node : done_root["select"]) {
            ret = _importer.handle(node, SEL, _hdfs_mnt, _done_real_path, _charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["xbs"].isArray()) {
        DB_WARNING("xbs hdfs mnt:%s, real path:%s, charset:%s", _hdfs_mnt.c_str(), _done_real_path.c_str(), _charset.c_str());
        for (auto& node : done_root["xbs"]) {
            ret = _importer.handle(node, XBS, _hdfs_mnt, _done_real_path, _charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    if (done_root["xcube"].isArray()) {
        DB_WARNING("xcube hdfs mnt:%s, real path:%s, charset:%s", _hdfs_mnt.c_str(), _done_real_path.c_str(), _charset.c_str());
        for (auto& node : done_root["xcube"]) {
            ret = _importer.handle(node, XCUBE, _hdfs_mnt, _done_real_path, _charset);
            if (ret < 0) {
                return -1;
            }
        }
    }
    _import_line = _importer.get_import_lines();
    //cond.wait();
    return 0;
}

int Fetcher::select_new_task(baikal::client::ResultSet& result_set) {

    int affected_rows = 0;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    select_vec.push_back("*");
    where_map["status"] = "'idle'";
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

int Fetcher::fetch_task() {
    int ret = 0;
    bool fetch = false;
    baikal::client::ResultSet result_set;
    
    ret = select_new_task(result_set);
    if (ret <= 0) {
        //等于0则表示没有获取到
        DB_WARNING("fetch task fail:%d", ret);
        return -2;
    }
    
    while (result_set.next()) {
        std::string status;
        std::string table_info;
        std::string done_file;
        std::string cluster_name;
        std::string modle;
        std::string user_sql;
        std::string charset;
        int64_t id = 0;
        int64_t version = 0;
        int64_t ago_days = 0;
        int64_t interval_days = 0;
        int64_t bitflag = 0;
        result_set.get_string("status", &status);
        result_set.get_string("done_file", &done_file);
        result_set.get_string("cluster_name", &cluster_name);
        result_set.get_string("modle", &modle);
        result_set.get_string("user_sql", &user_sql);
        result_set.get_string("charset", &charset);
        result_set.get_string("table_info", &table_info);
        result_set.get_int64("id", &id);
        result_set.get_int64("version", &version);
        result_set.get_int64("ago_days", &ago_days);
        result_set.get_int64("interval_days", &interval_days);
        result_set.get_int64("bitflag", &bitflag);
        if (version == _today_version && modle == "replace") {
            //如果已经为当天版本则不必执行,update模式可能存在逆序产生的情况
            continue;
        }
        ret = update_task_doing(id, version);
        if (ret != 1) {
            continue;
        }
        _id = id;
        _old_version = version;
        _done_file = done_file;
        _cluster_name = cluster_name;
        _modle = modle;
        _user_sql = user_sql;
        _charset = charset;
        _table_info = table_info;
        _ago_days = ago_days;
        _old_bitflag = bitflag;
        if (_old_bitflag == 0) {
            _old_bitflag = 0xFFFFFFFF;
        }
        _new_bitflag = 0;
        _interval_days = interval_days;
        prepare();
        ret = analyse_version();
        if (ret < 0) {
            DB_WARNING("fetch task fail:%d, id:%d, done_file:%s, common path:%s, done name:%s, real path:%s, hdfs mnt:%s, "
            "only one donefile:%d, old_version:%lld, new_version:%lld, today version:%lld "
            "cluster name:%s, modle:%s, user sql:%s, internal_days:%d, bitflag:%x", 
            ret, _id, _done_file.c_str(), _done_common_path.c_str(), _done_name.c_str(), _done_real_path.c_str(), 
            _hdfs_mnt.c_str(), _only_one_donefile, _old_version, _new_version, 
            _today_version, _cluster_name.c_str(), _modle.c_str(), _user_sql.c_str(), _interval_days, bitflag);
            update_task_idle(id);
            continue;
        }
        fetch = true;
        DB_WARNING("fetch task succ id:%d, done_file:%s, common path:%s, done name:%s, real path:%s, hdfs mnt:%s, "
        "only one donefile:%d, old_version:%lld, new_version:%lld, today version:%lld "
        "cluster name:%s, modle:%s, user sql:%s, interval_days:%d, bitflag:%x", 
        _id, _done_file.c_str(), _done_common_path.c_str(), _done_name.c_str(), _done_real_path.c_str(), 
        _hdfs_mnt.c_str(), _only_one_donefile, _old_version, _new_version, 
        _today_version, _cluster_name.c_str(), _modle.c_str(), _user_sql.c_str(), _interval_days, bitflag);
        break;
    }
    if (!fetch) {
        return -2;
    }
    return 0;
}

std::string Fetcher::user_sql_replace(std::string &sql){
    std::string tmp_sql = sql;
    int64_t delete_day = get_ndays_date(_new_version, -14);
    std::string version_str = std::to_string(_new_version);
    std::string version_format = version_str.substr(0,4) + "-" + version_str.substr(4,2) + "-" + version_str.substr(6,2);
    if (tmp_sql.find("{TABLE}") != tmp_sql.npos) {
        tmp_sql.replace(tmp_sql.find("{TABLE}"), 7, _import_tbl);
    }
    if (tmp_sql.find("{VERSION}") != tmp_sql.npos) {
        tmp_sql.replace(tmp_sql.find("{VERSION}"), 9, version_format);
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
        std::string exec_sql = user_sql_replace(s_sql);
        ret = _baikaldb_gbk->query(0, exec_sql, &result_set);
        if (ret != 0) {
            DB_WARNING("finish task succ querybaikaldb user sql fail, sql:%s", exec_sql.c_str());
            return -1;
        }
        DB_WARNING("finish task succ querybaikaldb user sql success, sql:%s", exec_sql.c_str());
    } 
    return 0;
}

int Fetcher::begin_task() {
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> values_map;

    values_map["database_name"] = "'" + _import_db + "'";
    values_map["table_name"] = "'" + _import_tbl + "'";
    values_map["hostname"] = "'" + FLAGS_hostname + "'";
    values_map["old_version"] = std::to_string(_old_version);
    values_map["new_version"] = std::to_string(_new_version);
    values_map["status"] = "'doing'";

    std::string sql = gen_insert_sql(FLAGS_result_tbl, values_map);

    int ret = querybaikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query baikaldb begin task failed, sql:%s", sql.c_str());
        return ret;
    }

    _result_id = result_set.get_auto_insert_id();
    DB_WARNING("begin task success sql:%s, id:%llu", sql.c_str(), _result_id);
    return 0;
}

int Fetcher::finish_task(bool is_succ, std::string& result, std::string& time_cost) {
    baikal::client::ResultSet result_set;

    std::map<std::string, std::string> set_map;
    std::map<std::string, std::string> where_map;
    set_map["status"]   = "'idle'";
    if (is_succ) {
        set_map["version"]   = std::to_string(_new_version);
        set_map["bitflag"]   = std::to_string(_new_bitflag);
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
    set_map["status"]      = "'" + result + "'";
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

// 更新任务状态，避免由于任务执行期间挂掉导致的任务丢失
// 查找本节点做过的任务，可能由于进程crash导致任务中断
int Fetcher::update_task(bool is_result_table) {
    int ret = 0;
    int affected_rows = 0;
    std::vector<int64_t> ids;
    baikal::client::ResultSet result_set;
    std::vector<std::string> select_vec;
    std::map<std::string, std::string> where_map;

    std::string table_name;
    std::string status;
    if (is_result_table) {
        table_name = FLAGS_result_tbl;
        status = "failed!!! baikalImporter maybe exit!";
    } else {
        table_name = FLAGS_param_tbl;
        status = "idle";
    }

    select_vec.push_back("id");
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
    while (result_set.next()) {
        result_set.get_int64("id", &id);
        ids.push_back(id);
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

int Fetcher::run() {

    update_task(true);

    update_task(false);


    int ret = fetch_task();
    if (ret < 0) {
        DB_WARNING("fetch task fail:%d", ret);
        return -1;
    }

    ret = begin_task();
    if (ret < 0) {
        DB_WARNING("begin task fail:%d", ret);
        return -1;
    }

    baikaldb::TimeCost cost;
    ret = import_to_baikaldb();

    auto time_print = [] (int64_t cost) -> std::string {
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
    };

    std::string time_cost(time_print(cost.get_time()).c_str());
    if (ret == 0) {

        int ret1 = exec_user_sql();
        if (ret1 != 0) {
            std::string result = "execute user sql failed";
            finish_task(true, result, time_cost);
        } else {
            std::string result = "success";
            finish_task(true, result, time_cost);
            DB_NOTICE("import to baikaldb success, cost:%s", time_print(cost.get_time()).c_str());
        }

    } else if (ret < 0) {

        std::string result = "import failed";
        finish_task(false, result, time_cost);
        DB_NOTICE("import to baikaldb failed, cost:%s", time_print(cost.get_time()).c_str());

    } 

    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {

    google::ParseCommandLineFlags(&argc, &argv, true);
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    //brpc::StartDummyServerAt(8800);

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

    auto baikaldb = manager.get_service("baikaldb");
    if (baikaldb == NULL) {
        DB_FATAL("baikaldb is null");
        return -1;
    }

    auto baikaldb_gbk = manager.get_service("baikaldb_gbk");
    if (baikaldb_gbk == NULL) {
        DB_FATAL("charset gbk baikaldb is null");
        return -1;
    }

    auto baikaldb_utf8 = manager.get_service("baikaldb_utf8");
    if (baikaldb_utf8 == NULL) {
        DB_FATAL("charset utf8 baikaldb is null");
        return -1;
    }

    DB_WARNING("int baikaldb service success");

    while (true) {

        baikaldb::BackUpImport backup(baikaldb);
        backup.run();

        bthread_usleep(5*1000000);

        baikaldb::Fetcher fetcher;
        int ret = fetcher.init(baikaldb, baikaldb_gbk, baikaldb_utf8);
        if (ret < 0) {
            DB_WARNING("fetcher init faild:%d", ret);
            return -1;
        }

        fetcher.run();

        bthread_usleep(60 * 1000000);

        continue;
    }

    return 0;
}


