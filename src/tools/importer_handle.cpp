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

#include "importer_handle.h"
#include "rate_limiter.h"
#include <boost/algorithm/string/predicate.hpp>
#include <unistd.h>
#include <sys/stat.h>

namespace baikaldb {
DEFINE_string(data_path, "./data", "data path");
DEFINE_string(insert_mod, "replace", "replace|insert|insert ignore");
DEFINE_int32(sleep_intervel_s, 0, "debug for sst");
DEFINE_bool(need_escape_string, true, "need_escape_string");
DEFINE_bool(null_as_string, false, "null_as_string");
DEFINE_bool(is_mnt, false, "is_mnt");
DEFINE_bool(is_debug, false, "is_debug");
DEFINE_bool(select_first, false, "select_first");
DEFINE_int32(query_retry_times, 20, "debug for sst");
DEFINE_int32(atom_check_mode, 0, "0 check id and literal; 1 only check id; 2 only check literal");
DEFINE_int64(atom_min_id, 0, "debug for sst");
DEFINE_int64(atom_max_id, 1000000000000LL, "1 wan yi");
DEFINE_int64(atom_base_fields_cnt, 4, "4");
DEFINE_bool(open_bigtree_insert_fix, false, "bigtree diff insert fix");
DEFINE_bool(open_bigtree_delete_fix, false, "bigtree diff delete fix");
DEFINE_bool(check_from_mysql, false, "select count from mysql");
DEFINE_bool(check_diff_data, true, "check diff data baikaldb vs mysql");
DEFINE_bool(use_planid_filter, true, "check add planid");
DEFINE_bool(use_unitid_filter, true, "check add unitid");
DEFINE_bool(need_iconv, true, "check add unitid");
DEFINE_int64(check_diff_data_number, 5000000, "# to check diff data");
DEFINE_int64(bigtree_delay_to_insert, 1800, "bigtree delay, default:1800s");
DEFINE_string(default_mail_user, "", "default mail user for max_failure_percent");
DEFINE_int32(max_sql_fail_percent, 100, "default max_failure_percent");
DEFINE_string(input_path, "", "flash_back input_path");
DEFINE_string(output_path, "", "flash_back output_path");
DEFINE_string(err_sql_tbl,         "HDFS_TASK_FAILED_SQL",   "import error sql table");
DEFINE_string(param_db,            "HDFS_TASK_DB",           "param database");
DEFINE_int32(max_upload_err_sql_cnt, 10000, "max upload err_sql cnt");
DEFINE_string(hostname, "hostname", "host name");

int system_vfork(const char* cmd) {
    pid_t child_pid;
    int status;
    if (cmd == NULL) {
        return 1;
    }
    if ((child_pid = vfork()) < 0) {
        status = -1;
    } else if (child_pid == 0) {
        execl("/bin/sh", "sh", "-c", cmd, (char*)0);
        _exit(127);
    } else {
        while (waitpid(child_pid, &status, 0) < 0) {
            if (errno != EINTR) {
                status = -1;
                break;
            }
        }
    }
    return status;
}
//调用系统命令
int system_cmd(const char* cmd) {
    pid_t status = system_vfork(cmd);
    if (-1 == status) {
        DB_WARNING("System Call Fail_1: %s", cmd);
    } else {
        if (WIFEXITED(status)) {
            if (0 == WEXITSTATUS(status)) {
                return 0;
            } else {
                DB_WARNING("System Call Fail_3: %s", cmd);
            }
        } else {
            DB_WARNING("System Call Fail_2: %s", cmd);
        }
    }
    return -1;
}

inline std::string reverse_bytes(std::string pri) {
    uint64_t img_id = strtoull(pri.c_str(), NULL, 0); 
    char* ptr =(char*)&img_id;
    std::swap(ptr[0], ptr[7]);
    std::swap(ptr[1], ptr[6]);
    std::swap(ptr[2], ptr[5]);
    std::swap(ptr[3], ptr[4]);
    return std::to_string(img_id);
}

int ImporterHandle::init(const Json::Value& node, const std::string& path_prefix, FastImportTaskDesc& task) {
    int ret = init(node, path_prefix);
    if (ret < 0) {
        return -1;
    }
    task.db = _db;
    task.table = _table;
    task.delim = _delim;
    task.null_as_string = _null_as_string;
    task.empty_as_null_indexes = _empty_as_null_indexes;
    task.ignore_indexes = _ignore_indexes;
    task.ttl = _ttl;
    task.emails = _emails;
    task.binary_indexes = _binary_indexes;
    task.fields.clear();
    task.has_header = _has_header;
    task.file_type = _file_type;
    for (auto& filed : _fields) {
        task.fields.emplace_back(filed.substr(1, filed.size() - 2));
    }
    task.const_map.clear();
    for (auto& pair : _const_map) {
        std::string filed_name = pair.first;
        // 去掉`
        task.const_map[filed_name.substr(1, filed_name.size() - 2)] = pair.second;
    }
    return 0;
}

int ImporterHandle::init(const Json::Value& node, const std::string& path_prefix) {
    if (BASE_UP != _type && RECOVERY != _type) {
        _db = node["db"].asString();
        _table = node["table"].asString();
        _quota_table = "`" + _table + "`";
        _fields.clear();
        DB_TRACE("database:%s, table:%s, type:%d", _db.c_str(), _table.c_str(), _type);
        for (auto& field : node["pk_fields"]) {
            std::string name = "`" + field.asString() + "`";
            _pk_fields[name];
        }
        for (auto& field : node["dup_set_fields"]) {
            std::string name = "`" + field.asString() + "`";
            _dup_set_fields[name];
        }
        int i = 0;
        std::set<std::string> empty_as_null_fields;
        if (node.isMember("empty_as_null_fields") && node["empty_as_null_fields"].isArray()) {
            for (const auto& f : node["empty_as_null_fields"]) {
                if (f.isString()) {
                    empty_as_null_fields.insert(f.asString());
                } else {
                    DB_FATAL("parse empty_as_null_fields error.");
                    _import_ret << "parse empty_as_null_fields error.";
                    return -1;
                }
            }
        }
        _ignore_indexes.clear();
        _empty_as_null_indexes.clear();
        for (auto& field : node["fields"]) {
            std::vector<std::string> vec;
            std::string name = field.asString();
            boost::split(vec, name, boost::is_any_of("@"));
            name = "`" + vec[0] + "`";
            if (empty_as_null_fields.count(vec[0]) == 1) {
                _empty_as_null_indexes.insert(i);
            }
            if (_pk_fields.count(name) == 1) {
                _pk_fields[name] = i;
            }
            if (_dup_set_fields.count(name) == 1) {
                _dup_set_fields[name] = i;
            }
            _fields.push_back(name);
            if (vec[0].empty()) {
                _ignore_indexes.insert(i);
            }
            if (vec.size() > 1 && vec[1] == "binary") {
                _binary_indexes.insert(i);
            }
            i++;
        }
        for (auto& field : node["const_fields"]) {
            std::vector<std::string> vec;
            std::string name = field.asString();
            boost::split(vec, name, boost::is_any_of("="));
            if (vec.size() != 2) {
                DB_WARNING("const_field abnormal, name=%s, size=%ld", name.c_str(), vec.size());
                _import_ret << "const_field abnormal: " << name;
                return -1;
            }
            name = "`" + vec[0] + "`";
            _const_map[name] = vec[1];
        }
        _null_as_string = FLAGS_null_as_string;
        if (node.isMember("other_condition")) {
            _other_condition = node["other_condition"].asString();
            if (boost::icontains(_other_condition, " or ")) {
                DB_WARNING("other_condition abnormal, %s", _other_condition.c_str());
                _import_ret << "other_condition abnormal: " << _other_condition;
                return -1;
            }
        }
        if (node.isMember("null_as_string")) {
            _null_as_string = node["null_as_string"].asBool();
        }
        if (node.isMember("ttl")) {
            _ttl = node["ttl"].asInt64();
        }
        if (node.isMember("file_min_size")) {
            _file_min_size = node["file_min_size"].asInt64();
        }
        if (node.isMember("file_max_size")) {
            _file_max_size = node["file_max_size"].asInt64();
        }
        if (node.isMember("has_header")) {
            _has_header = node["has_header"].asBool();
        }
        if (node.isMember("mail")) {
            std::string mail = node["mail"].asString();
            std::vector<std::string> users;
            boost::split(users, mail, boost::is_any_of(" ;,\n\t\r"), boost::token_compress_on);
            for (auto& user : users) {
                _emails += user;
                if (!boost::iends_with(user, "@baidu.com")) {
                    _emails += "@baidu.com";
                }
                _emails += " ";
            }
        }
        if (node.isMember("tps")) {
            _tps = node["tps"].asInt();
            DB_WARNING("db: %s, table: %s, import TPS: %d", _db.c_str(), _table.c_str(), _tps);
        }
        _max_failure_percent = FLAGS_max_sql_fail_percent;
        // 最大失败阈值，超过阈值，导入任务认为失败，version不会更新，状态转为idle重试，并且email报警
        if (node.isMember("max_failure_percent")) {
            _max_failure_percent = node["max_failure_percent"].asInt64();
        }
    }

    //当 _is_local_done_json 为真时，done文件写在 HDFS_TASK_DB.HDFS_TASK_TABLE 表中。
    std::string tmp_path;
    if (node.isMember("path")) {
        tmp_path = node["path"].asString();
    }
    if (_done_path.empty() && !_is_local_done_json) {
        if (tmp_path[0] == '/') {
            std::vector<std::string> split_vec;
            boost::split(split_vec, tmp_path, boost::is_any_of("/"));
            tmp_path = split_vec.back();
        }
        if (FLAGS_is_mnt) {
            tmp_path = "import_data";
        }
        _path = FLAGS_data_path + "/" + tmp_path;
        DB_WARNING("path:%s", _path.c_str());
    } else {
        if (tmp_path[0] == '/') {
            _path = path_prefix + tmp_path;  
        } else {
            _path = _done_path + "/" + tmp_path;
        } 
        DB_WARNING("path:%s", _path.c_str());
    }
    
    _delim = node.get("delim", "\t").asString();

    _file_type = FileType::Text;
    if (node.isMember("file_type")) {
        std::string file_type_str = node["file_type"].asString();
        if (boost::algorithm::iequals(file_type_str, "parquet")) {
            _file_type = FileType::Parquet;
        } else {
            _file_type = FileType::Text;
        }
    }

    return 0;
}

int ImporterHandle::get_table_primary_key(std::set<std::string>& pk_fields) {
    if (_table.empty() || _db.empty()) {
        _import_ret << "db_name or table_name empty";
        return -1;
    }
    std::string sql = "desc " + _db +  "." + _table;
    baikal::client::ResultSet result_set;
    int ret = -1;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (int retry = 0; retry < FLAGS_query_retry_times; ++retry) {
        if (connection == nullptr) {
            bthread_usleep(1000000);
            connection = _baikaldb->fetch_connection();
            continue;
        }
        ret = connection->execute(sql, true, &result_set);
        if (ret == 0) {
            break;
        }
    } 
    if (ret != 0) {
        std::string err_reason = "fetch_connection fail";
        if (connection != nullptr) {
            err_reason = connection->get_error_des();
        }
        _import_ret << "get primary key fail, sql: " << sql << ", resaon: " << err_reason;
        return -1;
    }
    while (result_set.next()) {
        std::string filed;
        std::string key;
        result_set.get_string("Field", &filed);
        result_set.get_string("Key", &key);
        if (key.find("I_PRIMARY") != key.npos) {
            pk_fields.insert(filed);
            DB_WARNING("pk field: %s, %s", filed.c_str(), key.c_str());
        }
    }
    return 0;
 }

ImporterHandle* ImporterHandle::new_handle(OpType type,
                    baikal::client::Service* baikaldb,
                    baikal::client::Service* backup_db,
                    std::string done_path) {
    ImporterHandle* importer = nullptr;
    switch (type) {
    case DEL:
        importer = new ImporterDelHandle(type, baikaldb, done_path);
        break;
    case UP:
        importer = new ImporterUpHandle(type, baikaldb, done_path);
        break;
    case SQL_UP:
        importer = new ImporterSqlupHandle(type, baikaldb, done_path);
        break;
    case DUP_UP:
        importer = new ImporterDupupHandle(type, baikaldb, done_path);
        break;
    case SEL:
        importer = new ImporterSelHandle(type, baikaldb, done_path);
        break;
    case SEL_PRE:
        importer = new ImporterSelpreHandle(type, baikaldb, done_path);
        break;
    case REP:
        importer = new ImporterRepHandle(type, baikaldb, done_path);
        break;
    case BASE_UP:
        importer = new ImporterBaseupHandle(type, baikaldb, done_path);
        break;
    case TEST:
        importer = new ImporterTestHandle(type, baikaldb, done_path);
        break;
    case ATOM_IMPORT:
        importer = new ImporterAtomHandle(type, baikaldb, done_path);
        break;
    case ATOM_CHECK:
        importer = new ImporterAtomCheckHandle(type, baikaldb, done_path);
        break;
    case BIGTTREE_DIFF:
        importer = new ImporterBigtreeDiffHandle(type, baikaldb, backup_db, done_path);
        break;
    case TXN_UP:
        importer = new ImporterTxnUpHandle(type, baikaldb, done_path);
        break;
    case RECOVERY:
        importer = new ImporterRecoveryHandle(type, baikaldb, done_path);
        break;
    case DIFF_LINE:
        importer = new ImporterDiffLineHandle(type, baikaldb, done_path);
        break;
    default:
        DB_FATAL("invalid type: %d", type);
        return nullptr;
    }

    return importer;
}

int ImporterHandle::handle_files(
        const FieldsFunc& fields_func, const SplitFunc& split_func, const ConvertFunc& convert_func,
        ImporterFileSystemAdaptor* fs, const std::string& config, const ProgressFunc& progress_func,
        const FinishBlockFunc& finish_block_func) { 
    ImporterImpl Impl(_path, fs, fields_func, split_func, convert_func, config, 0, 0, _has_header, _file_type);
    size_t file_size = fs->all_file_size();
    if (_file_min_size != 0 && file_size < _file_min_size) {
        DB_FATAL("file_size:%lu less than file_min_size:%lu", file_size, _file_min_size);
        _import_ret << "file_size:" << file_size <<
            " less than file_min_size:" << _file_min_size;
        return -1;
    }
    if (_file_max_size != 0 && file_size > _file_max_size) {
        DB_FATAL("file_size:%lu greater than file_max_size:%lu", file_size, _file_max_size);
        _import_ret << "file_size:" << file_size <<
            " greater than file_max_size:" << _file_max_size;
        return -1;
    }
    int ret = Impl.run(progress_func, finish_block_func, _broken_point_continuing, _finished_blocks);
    if (ret < 0) {
        _import_ret << Impl.get_result();
        return ret;
    }
    return 0;
}

int ImporterHandle::query_dm_baikaldb(std::string sql, baikal::client::ResultSet& result_set) {
    int ret = 0;
    int retry = 0;
    if (_dm_baikaldb == nullptr) {
        return 0;
    }
    do {
        ret = _dm_baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    } while (++retry < 20);

    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        return ret;
    }
    DB_NOTICE("query %d times, sql:%s affected row:%lu", retry, sql.c_str(), result_set.get_affected_rows());
    return 0;
}

void ImporterHandle::insert_err_sqls(const std::string& err_sql) {
    baikal::client::ResultSet result_set;
    std::map<std::string, std::string> values_map;
    values_map["table_info"] = "'" + _table_info + "'";
    values_map["version"] = std::to_string(_version);
    std::string err_sql_str = boost::replace_all_copy(err_sql, "\\", "\\\\"); 
    err_sql_str = boost::replace_all_copy(err_sql_str, "'", "\\'");
    values_map["err_sql"] = "'" + err_sql_str + "'";

    std::string sql = "REPLACE INTO " + FLAGS_param_db + "." + FLAGS_err_sql_tbl;
    std::string names;
    std::string values;
    for (auto iter : values_map) {
        names += iter.first;
        names += ",";
        values += iter.second;
        values += ",";
    }
    names.pop_back();
    values.pop_back();
    sql += " (" + names + ") VALUES (" + values + ")";
    int ret = query_dm_baikaldb(sql, result_set);
    if (ret != 0) {
        DB_WARNING("query dm baikaldb failed, sql:%s", sql.c_str());
        return;
    }
    return;
}

int64_t ImporterHandle::run(ImporterFileSystemAdaptor* fs, const std::string& config,
                            const ProgressFunc& progress_func, const FinishBlockFunc& finish_block_func) {
    auto fields_func = [this] (const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields, 
                               BlockHandleResult* r) {
        handle_fields(path, fields, r);
        _import_lines += fields.size();
    };

    auto split_func = [this] (std::string& line, std::vector<std::string>& split_vec) {
        return split(line, split_vec);
    };

    auto convert_func = [this] (std::string& line) {
        return convert(line);
    };
    if (_tps > 0) {
        GenericRateLimiter::get_instance()->set_bytes_per_second(_tps);
    }
    int ret = handle_files(fields_func, split_func, convert_func, fs, config, progress_func, finish_block_func);
    if (ret < 0) {
        _import_ret << "\nhandle_files failed";
        DB_FATAL("handle_files failed");
        if (_emails != "") {
            std::string cmd = "echo \"" + _import_ret.str() + "\" | mail -s 'BaikalDB import fail, " +
                _db + "." + _table + "' '" + _emails + "' -- -f baikalDM@baidu.com";
            system_cmd(cmd.c_str());
        }
        return -1;
    }
    _err_fs.flush();
    DB_NOTICE("import total_lines:%ld succ_cnt:%ld ,err_cnt:%ld", 
        _import_lines.load(), _succ_cnt.load(), _err_cnt.load());
    std::ifstream fp(_err_name);
    int count = 0;
    while (fp.good()) {
        if (++count % 100 == 0) {
            DB_TRACE("handle path:%s , lines:%d", _err_name.c_str(), count);
        }
        std::string line;
        std::getline(fp, line);
        if (line.empty()) {
            continue;
        }
        query(line, true);
    }
    _err_fs_retry.flush();
    std::string emails = _emails;
    if (!FLAGS_default_mail_user.empty()) {
        emails += " " + FLAGS_default_mail_user;
    }
    if (_err_cnt_retry.load() * 100 > _max_failure_percent * (_succ_cnt.load() + _err_cnt_retry.load())) {
        // 邮件打印20条失败的sql，方便排查简单的问题，如表名写错，列数不对
        DB_WARNING("finish with error: _err_cnt: %lu, _succ_cnt: %lu, _max_failure_percent: %lu",
                    _err_cnt_retry.load(), _succ_cnt.load(), _max_failure_percent);
        _import_ret << "exceed max failure percent: " << _max_failure_percent << "%"
                    << " (sql error_cnt: " << _err_cnt_retry << ", "
                    << " sql succuss_cnt: " << _succ_cnt << ", "
                    << " retry_time: " << _retry_times << ")";
        std::ifstream fpr(_err_name_retry);
        std::ostringstream error_example;
        int row = 0;
        error_example << "DM host: " << FLAGS_hostname << "\n";
        error_example << "err reason: " << _sql_err_reason << "\n\n";
        while (fpr.good()) {
            if (row > 5) {
                break;
            }
            std::string line;
            std::getline(fpr, line);
            if (line.empty()) {
                continue;
            }
            line = boost::replace_all_copy(line, "\"", "\\\"");
            line = boost::replace_all_copy(line, "`", "\\`");
            error_example << line << "\n";
            row++;
        }
        if (emails != "" && _retry_times < 3) {
            // 邮件报警最多报3次
            error_example << "err_sqls:\n";
            std::string cmd = "echo \"" + _import_ret.str() + error_example.str() + "\" | mail -s 'BaikalDB import fail, " +
                _db + "." + _table + "' '" + emails + "' -- -f baikalDM@baidu.com";
            DB_WARNING("email cmd: %s", cmd.c_str());
            system_cmd(cmd.c_str());
        }
        return -1;
    }
    // 导入结束后的操作，例如replace rename table
    ret = close(); 
    if (ret < 0) {
        return -1;
    }

    return _import_lines.load();
}

int ImporterHandle::query(std::string sql, int quota, baikal::client::SmartConnection& connection, bool is_retry) {
    if (_tps > 0) {
        GenericRateLimiter::get_instance()->request(quota);
    }
    if (_need_generate_sql) {
        _need_generate_sql = false;
        _sample_sql = sql;
    }
    if (connection == nullptr) {
        return query(sql, is_retry);
    }
    TimeCost cost;
    int ret = 0;
    int retry = 0;
    int affected_rows = 0;
    if (_ttl > 0) {
        sql = "/*{\"ttl_duration\":" + std::to_string(_ttl) + "}*/" + sql;
    }
    do {
        baikal::client::ResultSet result_set;
        bthread_usleep(FLAGS_sleep_intervel_s * 1000 * 1000LL);
        ret = connection->execute(sql, true, &result_set);
        if (ret == 0) {
            affected_rows = result_set.get_affected_rows();
            break;
        }
        bthread_usleep(1000000);
        if (connection != nullptr) {
            connection->close();
        }
        connection = _baikaldb->fetch_connection();
        while (connection == nullptr) {
            if (retry > FLAGS_query_retry_times) {
                DB_FATAL("service fetch_connection() failed");
                break;
            }
            bthread_usleep(1000000);
            connection = _baikaldb->fetch_connection();
            ++retry;
        }
    } while (++retry < FLAGS_query_retry_times);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        if (_broken_point_continuing && _err_cnt.load() < FLAGS_max_upload_err_sql_cnt) {
            insert_err_sqls(sql);
        }
        sql += ";\n";
        if (is_retry) {
            _err_fs_retry << sql;
            ++_err_cnt_retry;
        } else {
            _err_fs << sql;
            ++_err_cnt;
        }
        if (_sql_err_reason.empty()) {
            _sql_err_reason = connection->get_error_des();
        }
        return -1;
    }
    if (FLAGS_is_debug) {
        DB_TRACE("affected_rows:%d, cost:%ld, sql_len:%lu, sql:%s", 
                affected_rows, cost.get_time(), sql.size(), sql.c_str());
    }
    ++_succ_cnt;
    return affected_rows;
}

int ImporterHandle::query(std::string sql, bool is_retry) {
    TimeCost cost;
    int ret = 0;
    int retry = 0;
    int affected_rows = 0;
    if (_ttl > 0) {
        sql = "/*{\"ttl_duration\":" + std::to_string(_ttl) + "}*/" + sql;
    }
    do {
        baikal::client::ResultSet result_set;
        bthread_usleep(FLAGS_sleep_intervel_s * 1000 * 1000LL);
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            affected_rows = result_set.get_affected_rows();
            break;
        }   
        bthread_usleep(1000000);
    } while (++retry < FLAGS_query_retry_times);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        if (_broken_point_continuing && _err_cnt.load() < FLAGS_max_upload_err_sql_cnt) {
            insert_err_sqls(sql);
        }
        sql += ";\n";
        if (is_retry) {
            _err_fs_retry << sql;
            ++_err_cnt_retry;
        } else {
            _err_fs << sql;
            ++_err_cnt;
        }
        return -1;
    }
    if (FLAGS_is_debug) {
        DB_TRACE("affected_rows:%d, cost:%ld, sql_len:%lu, sql:%s", 
                affected_rows, cost.get_time(), sql.size(), sql.c_str());
    }
    ++_succ_cnt;
    return affected_rows;
}

int ImporterHandle::rename_table(std::string old_name, std::string new_name) {
    MetaServerInteract interact;
    interact.init();
    std::string sql = "alter table " + _db + "." + old_name
        + " rename to " + _db + "." + new_name;
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

std::string ImporterHandle::_mysql_escape_string(baikal::client::SmartConnection& connection, const std::string& value) {
    if (!FLAGS_need_escape_string) {
        //std::string tmp = value;
        return boost::replace_all_copy(value, "'", "\\'");
        //return tmp;
    }
    if (value.size() > MAX_FIELD_SIZE) {
        DB_FATAL("value too long:%s", value.c_str());
        return boost::replace_all_copy(value, "'", "\\'");
    }
    char* str = new char[value.size() * 2 + 1];
    std::string escape_value;
    int retry = 0;
    while (connection == nullptr) {
        if (retry > 600) {
            break;
        }
        bthread_usleep(1000000);
        connection = _baikaldb->fetch_connection();
        ++retry;
    }
    if (connection) {
        MYSQL* RES = connection->get_mysql_handle();
        mysql_real_escape_string(RES, str, value.c_str(), value.size());
        escape_value = str;
    } else {
        LOG(WARNING) << "service fetch_connection() failed for 10min";
        delete[] str;
        return boost::replace_all_copy(value, "'", "\\'");
    }   
    delete[] str;
    return escape_value;
}

int ImporterRepHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    }
    if (_broken_point_continuing && _finished_blocks.size() > 0) {
        // 配置了断点续传，并且已经有完成的导入block，不需要再truncate temp表
        return 0;
    }

    std::string sql  = "truncate table ";
    sql += _db + "." + _table + "_tmp";
    ret = query(sql);
    if (ret < 0) {
        _import_ret << sql << " failed";
        DB_FATAL("truncate table fail");
        return -1;
    }
    return 0;
}

void ImporterRepHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult* result) {
    std::string sql;
    std::string insert_values;
    int cnt = 0;
    int block_diff_line = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            if (_import_diff_lines.load() % 1000 == 0) {
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
            }
            _import_diff_lines++;
            block_diff_line++;
            if (_diff_line_sample.empty()) {
                _diff_line_sample = _mysql_escape_string(connection, line);
            }
            continue;
        }
        ++cnt;
        int i = 0;
        insert_values += "(";
        for (auto& item : fields) {
            if (_ignore_indexes.count(i) == 0) { 
                if (_empty_as_null_indexes.count(i) == 1 && item == "") {
                    insert_values +=  "NULL ,";
                } else if ((_null_as_string || item != "NULL") && _binary_indexes.count(i) == 0) {
                    insert_values += "'" + _mysql_escape_string(connection, item) + "',";
                } else {
                    insert_values +=  item + ",";
                }
            }
            i++;
        }
        for (auto& pair : _const_map) {
            insert_values += "'" + pair.second + "',";
        }
        insert_values.pop_back();
        insert_values += "),";
    }
    if (result != nullptr) {
        result->diff_line = block_diff_line;
        result->import_line = fields_vec.size() - block_diff_line;
    }
    if (cnt == 0) {
        return;
    }
    sql = FLAGS_insert_mod + " into ";
    sql += _db + "." + _table;
    sql += "_tmp (";
    int i = 0;
    for (auto& field : _fields) {
        if (_ignore_indexes.count(i++) == 0) {
            sql += field + ",";
        }
    }
    for (auto& pair : _const_map) {
        sql += pair.first + ",";
    }
    sql.pop_back();
    sql += ") values ";
    insert_values.pop_back();
    sql += insert_values;
    int query_ret = query(sql, cnt, connection);
    if (result != nullptr) {
        if (query_ret < 0) {
            result->errsql_line += 1;
        } else {
            result->affected_row += query_ret;
            _import_affected_row += query_ret;
        }
    } 
}

int ImporterRepHandle::close() {
    int ret = 0;
    ret = rename_table(_quota_table, _table + "_tmp2");
    if (ret < 0) {
        DB_FATAL("rename fail old:%s new:%s", _table.c_str(), (_table + "_tmp2").c_str());
        return -1;
    }
    ret = rename_table(_table + "_tmp", _quota_table);
    if (ret < 0) {
        DB_FATAL("rename fail old:%s new:%s", (_table + "_tmp").c_str(), _table.c_str());
        return -1;
    }
    ret = rename_table(_table + "_tmp2", _table + "_tmp");
    if (ret < 0) {
        DB_FATAL("rename fail old:%s new:%s", (_table + "_tmp2").c_str(), 
            (_table + "_tmp").c_str());
        return -1;
    }
    return 0;
}

int ImporterUpHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterUpHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult* result) {
    std::string sql;
    std::string insert_values;
    int cnt = 0;
    int block_diff_line = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));

    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            if (_import_diff_lines.load() % 1000 == 0) {
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
            }
            _import_diff_lines++;
            block_diff_line++;
            if (_diff_line_sample.empty()) {
                _diff_line_sample = _mysql_escape_string(connection, line);
            }
            continue;
        }
        ++cnt;
        int i = 0;
        insert_values += "(";
        for (auto& item : fields) {
            if (_empty_as_null_indexes.count(i) == 1 && item == "") {
                    insert_values +=  "NULL ,";
            } else if (_ignore_indexes.count(i) == 0) { 
                if ((_null_as_string || item != "NULL") && _binary_indexes.count(i) == 0) {
                    insert_values += "'" + _mysql_escape_string(connection, item) + "',";
                } else {
                    insert_values +=  item + ",";
                }
            }
            i++;
        }
        for (auto& pair : _const_map) {
            insert_values += "'" + pair.second + "',";
        }
        insert_values.pop_back();
        insert_values += "),";
    }
    if (result != nullptr) {
        result->diff_line = block_diff_line;
        result->import_line = fields_vec.size() - block_diff_line;
    }
    if (cnt == 0) {
        return;
    }
    sql = FLAGS_insert_mod + " into ";
    sql += _db + "." + _quota_table;
    sql += "(";
    int i = 0;
    for (auto& field : _fields) {
        if (_ignore_indexes.count(i++) == 0) {
            sql += field + ",";
        }
    }
    for (auto& pair : _const_map) {
        sql += pair.first + ",";
    }
    sql.pop_back();
    sql += ") values ";
    insert_values.pop_back();
    sql += insert_values;
    int query_ret = query(sql, cnt, connection);
    if (result != nullptr) {
        if (query_ret < 0) {
            result->errsql_line += 1;
        } else {
            result->affected_row += query_ret;
            _import_affected_row += query_ret;
        }
    } 
}

int ImporterTxnUpHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

// 仅例行化事务模式sql导入用
void ImporterTxnUpHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult*) {
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    std::vector<std::string> sqls;
    int cnt = 0;
    std::string insert_values;
    auto generate_sql = [this, &sqls, &insert_values]() {
        std::string sql = FLAGS_insert_mod + " into ";
        sql += _db + "." + _quota_table;
        sql += "(";
        int i = 0;
        for (auto& field : _fields) {
            if (_ignore_indexes.count(i++) == 0) {
                sql += field + ",";
            }
        }
        for (auto& pair : _const_map) {
            sql += pair.first + ",";
        }
        sql.pop_back();
        sql += ") values ";
        insert_values.pop_back();
        sql += insert_values;
        sqls.emplace_back(sql);
        insert_values.clear();
    };
    
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            if (_import_diff_lines.load() % 1000 == 0) {
                std::string line;
                restore_line(fields, line);
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
            } 
            _import_diff_lines++;
            continue;
        }
        ++cnt;
        int i = 0;
        insert_values += "(";
        for (auto& item : fields) {
            if (_empty_as_null_indexes.count(i) == 1 && item == "") {
                    insert_values +=  "NULL ,";
            } else if (_ignore_indexes.count(i) == 0) { 
                if ((_null_as_string || item != "NULL") && _binary_indexes.count(i) == 0) {
                    insert_values += "'" + _mysql_escape_string(connection, item) + "',";
                } else {
                    insert_values +=  item + ",";
                }
            }
            i++;
        }
        for (auto& pair : _const_map) {
            insert_values += "'" + pair.second + "',";
        }
        insert_values.pop_back();
        insert_values += "),";

        // 每20行生成一个replace into sql
        if (cnt == 20) {
            generate_sql();
            cnt = 0;
        }
    }
    if (cnt > 0) {
        generate_sql();
    }
    run_txn(sqls);
}

void ImporterTxnUpHandle::run_txn(const std::vector<std::string>& sqls) {
    int ret = 0;
    int retry_time = 0;
    // 事务执行
    while (retry_time++ < FLAGS_query_retry_times) {
        baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
        if (connection == nullptr) {
            continue;
        }
        bool need_rollback = false;
        ON_SCOPE_EXIT(([&connection, &need_rollback]() {
            if (connection) {
                if (need_rollback) {
                    connection->execute("rollback", true, NULL);
                }
                connection->close();
            }
        }));
        ret = connection->execute("begin", true, NULL);
        if (ret < 0) {
            continue;
        }
        for (auto& sql : sqls) {
            ret = connection->execute(sql, true, NULL);
            if (ret < 0) {
                need_rollback = true;
                break;
            }
        }
        if (need_rollback) {
            continue;
        }
        ret = connection->execute("commit", true, NULL);
        if (ret < 0) {
            need_rollback = true;
            continue;
        }
        break;
    }
    if (ret == 0) {
        ++_succ_cnt;
    } else {
        // 失败的sql写到临时文件，最后非事务的进行重试
        for (auto sql : sqls) {
            DB_WARNING("query failed: %s", sql.c_str());
            ++_err_cnt;
            sql += ";\n";
            _err_fs << sql;
        }
    }
}

int ImporterSelHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterSelHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult*) {
    std::string sql;
    std::string insert_values;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        sql = "select * from ";
        sql += _db + "." + _quota_table + " where ";
        bool is_first = true;
        for (uint32_t i = 0; i < _fields.size(); i++) {
            if (_ignore_indexes.count(i) == 1) {
                continue;
            }
            if (!is_first) {
                sql += " and ";
            }
            sql += _fields[i] + " = '" + _mysql_escape_string(connection, fields[i]) + "'";
            is_first = false;
        }
        query(sql, 1, connection);
    }
}


int ImporterSelpreHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterSelpreHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult*) {
    std::string sql;
    std::string insert_values;
    int cnt = 0;
    baikal::client::SmartConnection connection;
    MYSQL* conn;
    MYSQL_STMT* stmt = nullptr;
    MYSQL_BIND* params = nullptr;
    if (_type == SEL_PRE) {
        int retry = 0;
        do {
            connection = _baikaldb->fetch_connection();
            if (connection != nullptr) {
                break;
            }   
            bthread_usleep(1000000);
        } while (++retry < 20);
        if (connection == nullptr) {
            DB_FATAL("connection error");
            return;
        }
        conn = connection->get_mysql_handle();
        stmt = mysql_stmt_init(conn);
        sql = "select id,content from ";
        sql += _db + "." + _quota_table + " where ";
        bool is_first = true;
        for (uint32_t i = 0; i < _fields.size(); i++) {
            if (_ignore_indexes.count(i) == 1) {
                continue;
            }
            if (!is_first) {
                sql += " and ";
            }
            sql += _fields[i] + " = ? ";
            is_first = false;
        }
        params = new MYSQL_BIND[2];
        memset(params, 0, sizeof(MYSQL_BIND) * 2);
        mysql_stmt_prepare(stmt, sql.c_str(), sql.size());
    }
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        ++cnt;
        if (_type == SEL_PRE) {
            char id[100];
            std::unique_ptr<char> content(new char[1024 * 1024]);
            uint64_t length_content = 0;
            snprintf(id, sizeof(id), "%s", fields[0].c_str());
            uint64_t length_id = fields[0].size();
            params[0].buffer_type = ::MYSQL_TYPE_STRING;
            params[0].buffer = id;
            params[0].buffer_length = sizeof(id);
            params[0].length = &length_id;
            params[1].buffer_type = ::MYSQL_TYPE_STRING;
            params[1].buffer = content.get();
            params[1].buffer_length = 1024 * 1024;
            params[1].length = &length_content;
            int ret = 0;
            ret = mysql_stmt_bind_param(stmt, params);
            if (ret != 0) {
                DB_WARNING("ret:%d, %s %ld %ld", ret, id, length_id, length_content);
                continue;
            }
            ret = mysql_stmt_bind_result(stmt, params);
            if (ret != 0) {
                DB_WARNING("ret:%d, %s %ld %ld", ret, id, length_id, length_content);
                continue;
            }
            ret = mysql_stmt_execute(stmt);
            if (ret != 0) {
                DB_WARNING("ret:%d, %s %ld %ld", ret, id, length_id, length_content);
                continue;
            }
            ret = mysql_stmt_fetch(stmt);
            if (ret != 0) {
                DB_WARNING("ret:%d, %s %ld %ld", ret, id, length_id, length_content);
                continue;
            }
            if (FLAGS_is_debug) {
                DB_TRACE("%s %ld %ld", id, length_id, length_content);
            }
        }
    }
    if (cnt == 0) {
        return;
    }
    if (_type == SEL_PRE) {
        mysql_stmt_close(stmt);
        connection->close();
    }
} 

int ImporterDelHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    TaskConfig config(_task_config);
    config.print();
    if (!config.include_all_pk_field) {
        return 0;
    }
    // 检查主键是否都包含在where里
    std::set<std::string> pk_fields;
    if (get_table_primary_key(pk_fields) < 0) {
        return -1;
    }
    for (const auto& field : pk_fields) {
        bool pk_field_in_done = false;
        for (auto f : _fields) {
            if (f == "`" + field + "`") {
                pk_field_in_done = true;
                break;
            }
        }
        if (!pk_field_in_done) {
            _import_ret << "pk filed " << field << " not in where conditions";
            return -1;
        }
    } 
    return 0;
}

void ImporterDelHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult* result) {
    std::string sql;
    int cnt = 0;
    int block_diff_line = 0;
    sql = "delete from ";
    sql += _db + "." + _quota_table + " where ";
    bool is_one_col =  _fields.size() - _ignore_indexes.size() == 1;
    
    if (!is_one_col) {
        sql += " (";
    }
    bool is_first = true;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (uint32_t i = 0; i < _fields.size(); i++) {
        if (_ignore_indexes.count(i) == 1) {
            continue;
        }
        if (!is_first) {
            sql += " , ";
        }
        sql += _fields[i];
        is_first = false;
    }
    if (!is_one_col) {
        sql += " ) in (";
    } else {
        sql += " in (";
    }
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            if (_import_diff_lines.load() % 1000 == 0) {
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
            }
            _import_diff_lines++;
            block_diff_line++;
            if (_diff_line_sample.empty()) {
                _diff_line_sample = _mysql_escape_string(connection, line);
            }
            continue;
        }
        cnt++;
        bool is_first = true;
        if (!is_one_col) {
            sql += "(";
        }
        for (uint32_t i = 0; i < _fields.size(); i++) {
            if (_ignore_indexes.count(i) == 1) {
                continue;
            }
            if (!is_first) {
                sql += " , ";
            }
            sql +=  "'" + _mysql_escape_string(connection, fields[i]) + "'";
            is_first = false;
        }
        if (!is_one_col) {
            sql += "),";
        } else {
            sql += ",";
        }
    }
    sql.pop_back();
    sql += ") ";
    if (!_other_condition.empty()) {
        sql += " and (" + _other_condition + ")";
    }
    if (result != nullptr) {
        result->diff_line = block_diff_line;
        result->import_line = fields_vec.size() - block_diff_line;
    }
    int query_ret = query(sql, cnt, connection);
    if (result != nullptr) {
        if (query_ret < 0) {
            result->errsql_line += 1;
        } else {
            result->affected_row += query_ret;
            _import_affected_row += query_ret;
        }
    } 
}

int ImporterSqlupHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    TaskConfig config(_task_config);
    config.print();
    if (!config.include_all_pk_field) {
        return 0;
    }
    // 检查主键是否都包含在where里
    std::set<std::string> pk_fields;
    if (get_table_primary_key(pk_fields) < 0) {
        return -1;
    }
    for (const auto& field : pk_fields) {
        if (_pk_fields.find("`" + field + "`") == _pk_fields.end()) {
            _import_ret << "pk filed " << field << " not in where conditions";
            return -1;
        }
    } 
    return 0;
}

void ImporterSqlupHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult* result) {
    std::string sql;
    int cnt = 0;
    int block_diff_line = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            if (_import_diff_lines.load() % 1000 == 0) {
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
            }
            _import_diff_lines++;
            block_diff_line++;
            if (_diff_line_sample.empty()) {
                _diff_line_sample = _mysql_escape_string(connection, line);
            }
            continue;
        }
        sql = "update ";
        sql += _db + "." + _quota_table + " set ";
        bool is_first = true;
        for (uint32_t i = 0; i < _fields.size(); i++) {
            if (_ignore_indexes.count(i) == 1) {
                continue;
            }
            if (_pk_fields.count(_fields[i]) == 1) {
                continue;
            }
            if (!is_first) {
                sql += " , ";
            }
            if ((_null_as_string || fields[i] != "NULL") && _binary_indexes.count(i) == 0) {
                sql += _fields[i] + " = '" + _mysql_escape_string(connection, fields[i]) + "'";
            } else {
                sql += _fields[i] + " = NULL";
            }
            is_first = false;
        }
        sql += " where ";
        is_first = true;
        for (auto& pair : _pk_fields) {
            if (!is_first) {
                sql += " and ";
            }
            sql += pair.first+ " = '" + _mysql_escape_string(connection, fields[pair.second]) + "'";
            is_first = false;
        }
        if (!_other_condition.empty()) {
            sql += " and (" + _other_condition + ")";
        }
        int query_ret = query(sql, 1, connection);
        if (result != nullptr) {
            if (query_ret < 0) {
                result->errsql_line += 1;
            } else {
                result->affected_row += query_ret;
                _import_affected_row += query_ret;
            }
        } 
        cnt++;
    }
    if (result != nullptr) {
        result->diff_line = block_diff_line;
        result->import_line = fields_vec.size() - block_diff_line;
    }
    if (cnt == 0) {
        bthread_usleep(1000);
    }
}

int ImporterDupupHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterDupupHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult* result) {
    std::string sql;
    std::string insert_values;
    int cnt = 0;
    int block_diff_line = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            if (_import_diff_lines.load() % 1000 == 0) {
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
            }
            _import_diff_lines++;
            block_diff_line++;
            if (_diff_line_sample.empty()) {
                _diff_line_sample = _mysql_escape_string(connection, line);
            }
            continue;
        }
        ++cnt;
        if (_type == DUP_UP) {
            int i = 0;
            insert_values += "(";
            for (auto& item : fields) {
                if (_empty_as_null_indexes.count(i) == 1 && item == "") {
                    insert_values +=  "NULL ,";
                } else if (_ignore_indexes.count(i) == 0) { 
                    if ((_null_as_string || item != "NULL") && _binary_indexes.count(i) == 0) {
                        insert_values += "'" + _mysql_escape_string(connection, item) + "',";
                    } else {
                        insert_values +=  item + ",";
                    }
                }
                i++;
            }
            insert_values.pop_back();
            insert_values += "),";
        }
    }
    if (result != nullptr) {
        result->diff_line = block_diff_line;
        result->import_line = fields_vec.size() - block_diff_line;
    }
    if (cnt == 0) {
        return;
    }
    if (_type == DUP_UP) {
        sql = "insert into ";
        sql += _db + "." + _quota_table;
        sql += "(";
        int i = 0;
        for (auto& field : _fields) {
            if (_ignore_indexes.count(i++) == 0) {
                sql += field + ",";
            }
        }
        sql.pop_back();
        sql += ") values ";
        insert_values.pop_back();
        sql += insert_values;
        sql += " on duplicate key update ";
        for (auto& pair : _dup_set_fields) {
            sql += pair.first + "=values(" + pair.first + "),";
        }
        sql.pop_back();
        int query_ret = query(sql, cnt, connection);
        if (result != nullptr) {
            if (query_ret < 0) {
                result->errsql_line += 1;
            } else {
                result->affected_row += query_ret;
                _import_affected_row += query_ret;
            }
        } 
    }

}

int ImporterBaseupHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 

    if (node["update"].isArray()) {
        size_t table_cnt = 0;
        for (auto& schema : node["update"]) {
            if (!schema.isMember("filter_field") || !schema.isMember("filter_value")) {
                DB_FATAL("has no filter_field, node");
                return -1;
            }
            table_cnt++;
            std::string filter_field = schema["filter_field"].asString();
            std::string filter_value = schema["filter_value"].asString();
            auto& table_info = _level_table_map[filter_value];
            table_info.filter_field = filter_field;
            table_info.filter_value = filter_value;
            table_info.db = schema["db"].asString();
            table_info.table = schema["table"].asString();
            table_info.fields.clear();
            table_info.ignore_indexes.clear();
            int i = 0;
            for (auto& field : schema["fields"]) {
                std::string name = "`" + field.asString() + "`";
                table_info.fields.push_back(name);
                if (field.asString().empty()) {
                    table_info.ignore_indexes.insert(i);
                } else if (field.asString() == filter_field) {
                    table_info.ignore_indexes.insert(i);
                    table_info.filter_idx = i;
                }
                i++;
            }

            table_info.empty_as_null_indexes.clear();
            std::set<std::string> empty_as_null_fields;
            if (schema.isMember("empty_as_null_fields") && schema["empty_as_null_fields"].isArray()) {
                for (const auto& f : schema["empty_as_null_fields"]) {
                    if (f.isString()) {
                        empty_as_null_fields.insert(f.asString());
                    } else {
                        DB_FATAL("parse empty_as_null_fields error.");
                        return -1;
                    }
                }
            }

            i = 0;
            for (auto& field : table_info.fields) {
                std::string name = field.substr(1, field.length() - 2); // 去掉反引号
                if (empty_as_null_fields.count(name) == 1) {
                    table_info.empty_as_null_indexes.insert(i);
                }
                i++;
            }
        }
        //如果table数和filtervalue数对不上，则有可能有重复的filter_value
        if (table_cnt != _level_table_map.size()) {
            DB_FATAL("table count:%ld, level_table map size:%lu", table_cnt, _level_table_map.size());
            return -1;
        }
    }
    return 0;
}

void ImporterBaseupHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult*) {
    std::map<std::string, std::string> level_insert_values;
    if (_level_table_map.size() == 0) {
        return;
    }
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    int cnt = 0;
    for (auto& fields : fields_vec) {
        bool is_find = false;
        for (auto iter = _level_table_map.begin(); iter != _level_table_map.end(); iter++) {
            if (fields[iter->second.filter_idx] != iter->second.filter_value) {
                continue;
            }
            if (fields.size() != iter->second.fields.size()) {
                std::string line;
                restore_line(fields, line);
                DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), iter->second.fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
                _import_diff_lines++;
                continue;
            }
            ++cnt;
            int i = 0;
            const std::set<int>& empty_as_null_indexes = iter->second.empty_as_null_indexes;
            std::string& insert_values = level_insert_values[iter->second.filter_value];
            insert_values += "(";
            for (auto& item : fields) {
                if (empty_as_null_indexes.count(i) == 1 && item == "") {
                    insert_values +=  "NULL ,";
                } else if (iter->second.ignore_indexes.count(i) == 0) { 
                    if (_null_as_string || item != "NULL") {
                        insert_values += "'" + _mysql_escape_string(connection, item) + "',";
                    } else {
                        insert_values +=  item + ",";
                    }
                }
                i++;
            }
            insert_values.pop_back();
            insert_values += "),";
            is_find = true;
            break;
        }
        if (!is_find) {
            //DB_WARNING("can`t find line:%s", line.c_str());
        }
    }
    if (cnt == 0) {
        return;
    }

    for (auto values_iter = level_insert_values.begin(); values_iter != level_insert_values.end(); values_iter++) {
        auto table_iter = _level_table_map.find(values_iter->first);
        if (table_iter == _level_table_map.end()) {
            DB_FATAL("can not find level:%s", values_iter->first.c_str());
            continue;
        }
        std::string sql;
        sql = FLAGS_insert_mod + " into ";
        sql += table_iter->second.db + "." + table_iter->second.table;
        sql += "(";
        int i = 0;
        for (auto& field : table_iter->second.fields) {
            if (table_iter->second.ignore_indexes.count(i++) == 0) {
                sql += field + ",";
            }
        }
        sql.pop_back();
        sql += ") values ";
        values_iter->second.pop_back();
        sql += values_iter->second;
        query(sql, cnt, connection);
    }
}

int ImporterBigtreeDiffHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    }
    return 0;
}

static std::map<std::string, std::string> count_name_map = {
    {"planinfo", "planid_count"},
    {"unitinfo", "unitid_count"},
    {"wordinfo", "winfoid_count"},
    {"ideainfo", "ideaid_count"},
};

static std::map<std::string, std::string> count_table_name_map = {
    {"planinfo", "planinfo_count"},
    {"unitinfo", "unitinfo_count"},
    {"wordinfo", "wordinfo_count"},
    {"ideainfo", "ideainfo_count"},
};

static std::map<std::string, std::vector<std::string>> table_fields_map = {
    {"planinfo", {"userid", "planid"}},
    {"unitinfo", {"userid", "planid", "unitid"}},
    {"ideainfo", {"userid", "planid", "unitid", "ideaid"}},
    {"wordinfo", {"userid", "planid", "unitid", "winfoid", "wordid"}},
};

static std::map<std::string, std::vector<std::string>> mysql_table_fields_map = {
    {"planinfo", {"userid", "planid", "delay"}},
    {"unitinfo", {"userid", "planid", "unitid", "delay"}},
    {"ideainfo", {"userid", "planid", "unitid", "ideaid", "delay"}},
    {"wordinfo", {"userid", "planid", "unitid", "winfoid", "wordid", "delay"}},
};

static std::map<std::string, std::vector<std::string>> count_table_fields_map = {
    {"planinfo", {"userid", "planid_count"}},
    {"unitinfo", {"userid", "planid", "unitid_count"}},
    {"ideainfo", {"userid", "planid", "unitid", "ideaid_count"}},
    {"wordinfo", {"userid", "planid", "unitid", "winfoid_count"}},
};

void ImporterBigtreeDiffHandle::gen_sql(const std::string& table_name, RowsBatch& row_batch, OptType type, std::string& sql) {
    sql.clear();
    if (row_batch.empty()) {
        return;
    }
    auto& table_fields = table_fields_map[table_name];
    auto& count_table_fields = count_table_fields_map[table_name];
    switch (type) {
    case OptType::SELECT: {
        // select userid, planid from Bigtree.planinfo where (userid, planid) in ((11, 11), (12,22), (33,44));
        sql = "select ";
        std::string fields;
        fields.reserve(50);
        for (auto& field : table_fields) {
            fields += field;
            fields += ",";
        }
        fields.pop_back();
        sql += fields;
        sql += " from Bigtree.";
        sql += table_name;
        sql +=  " where (";
        sql += fields;
        sql += ") in (";
        for (auto& row : row_batch) {
            sql += "(";
            for (auto& field : row) {
                sql += field;
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ")";
        break;
    }
    case OptType::INSERT: {
        // insert ignore Bigtree.planinfo(userid, planid) values(xxx,xxx);
        sql = " insert ignore into Bigtree.";
        sql += table_name;
        sql += " (";
        for (auto& field : table_fields) {
            sql += field;
            sql += ",";
        }
        sql.pop_back();
        sql += ") values";
        for (auto& row : row_batch) {
            sql += "(";
            for (auto& field : row) {
                sql += field;
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        break;
    }
    case OptType::INSERT_COUNT: {
        // insert Bigtree.planinfo_count(userid, planid_count) values(xxx,1) on duplicate key update planid_count = planid_count + 1; 
        sql = "insert into Bigtree.";
        sql += count_table_name_map[table_name];
        sql += " (";
        for (auto& field : count_table_fields) {
            sql += field;
            sql += ",";
        }
        sql.pop_back();
        sql += ") values";
        size_t field_size = count_table_fields.size();
        for (auto& row: row_batch) {
            sql += "(";
            for (int i = 0; i < field_size - 1; i++) {
                sql += row[i];
                sql += ",";
            }
            sql += "1),";
        }
        sql.pop_back();
        sql += " on duplicate key update ";
        sql += count_name_map[table_name];
        sql += " = ";
        sql += count_name_map[table_name];
        sql += " + 1";
        break;
    }
    case OptType::UPDATE_COUNT: {
        // update Bigtree.planinfo_count set planid_count = planid_count - v where userid = xxx;
        sql = "update Bigtree.";
        sql += count_table_name_map[table_name];
        sql += " set ";
        sql += count_name_map[table_name];
        sql += " = ";
        sql += count_name_map[table_name];
        sql += " - ";
        DataRow& row = row_batch[0];
        sql += row[row.size() - 1];
        sql += " where (";
        size_t field_size = count_table_fields.size();
        for (size_t i = 0; i < field_size - 1; i ++) {
            sql += count_table_fields[i];
            sql += ",";
        }
        sql.pop_back();
        sql += ") in (";
        for (auto& row : row_batch) {
            sql += "(";
            for (int i = 0; i < row.size() - 1; i++) {
                sql += row[i];
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ")";
        break;
    }
    case OptType::DELETE: {
        // delete from Bigtree.planinfo where (usedid, planid) in ((xxx,xx));
        sql = " delete from Bigtree.";
        sql += table_name;
        sql += " where (";
        for (auto& field : table_fields) {
            sql += field;
            sql += ",";
        }
        sql.pop_back();
        sql += ") in (";
        for (auto& row: row_batch) {
            sql += "(";
            for (auto& field : row) {
                sql += field;
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ")";
        break;
    }
    case OptType::DELETE_IDEA: {
        // delete from Bigtree.ideainfo where (usedid, planid, unitid) in ((xxx,xx, xxx));
        sql = " delete from Bigtree.ideainfo where (";
        for (auto& field : table_fields) {
            sql += field;
            sql += ",";
        }
        sql.pop_back();
        sql += ") in (";

        for (auto& row: row_batch) {
            sql += "(";
            for (auto& field : row) {
                sql += field;
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ")";
        break;
    }
    case OptType::DELETE_IDEA_COUNT: {
        // delete from Bigtree.ideainfo_count where (usedid, planid, unitid) in ((xxx,xx, xxx));
        sql = " delete from Bigtree.ideainfo_count where (";
        for (auto& field : table_fields) {
            sql += field;
            sql += ",";
        }
        sql.pop_back();
        sql += ") in (";

        for (auto& row: row_batch) {
            sql += "(";
            for (auto& field : row) {
                sql += field;
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ")";
        break;
    }
    case OptType::DELATE_WORD: {
        // delete from Bigtree.wordinfo where (usedid, planid, unitid) in ((xxx,xx, xxx));
        sql = " delete from Bigtree.wordinfo where (";
        for (auto& field : table_fields) {
            sql += field;
            sql += ",";
        }
        sql.pop_back();
        sql += ") in (";

        for (auto& row: row_batch) {
            sql += "(";
            for (auto& field : row) {
                sql += field;
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ")";
        break;
    }
    case OptType::DELATE_WORD_COUNT: {
        // delete from Bigtree.wordinfo_count where (usedid, planid, unitid) in ((xxx,xx, xxx));
        sql = " delete from Bigtree.wordinfo_count where (";
        for (auto& field : table_fields) {
            sql += field;
            sql += ",";
        }
        sql.pop_back();
        sql += ") in (";

        for (auto& row: row_batch) {
            sql += "(";
            for (auto& field : row) {
                sql += field;
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ")";
        break;
    }
    case OptType::DELETE_COUNT: {
        // delete  from Bigtree.planinfo_count where userid = xxx and planid_count <= 0;
        sql = "delete from Bigtree.";
        sql += count_table_name_map[table_name];
        sql += " where (";
        size_t field_size = count_table_fields.size();
        for (size_t i = 0; i < field_size - 1; i++) {
            sql += count_table_fields[i];
            sql += ",";
        }
        sql.pop_back();
        sql += ") in (";
        for (auto& row: row_batch) {
            sql += "(";
            for (size_t i = 0; i < field_size - 1; i++) {
                sql += row[i];
                sql += ",";
            }
            sql.pop_back();
            sql += "),";
        }
        sql.pop_back();
        sql += ") and ";
        sql += count_name_map[table_name];
        sql += " <= 0";

        break;
    }
    default:
        break;
    }
    DB_WARNING("sql: %s", sql.c_str());
}

int ImporterBigtreeDiffHandle::do_begin(baikal::client::SmartConnection& conn) {
    return conn->execute("begin", true, NULL);
}

int ImporterBigtreeDiffHandle::do_commit(baikal::client::SmartConnection& conn ) {
    return conn->execute("commit", true, NULL);
}

int ImporterBigtreeDiffHandle::do_rollback(baikal::client::SmartConnection& conn) {
    return conn->execute("rollback", true, NULL);
}

void ImporterBigtreeDiffHandle::gen_new_rows(const RowsBatch& old_batch, const RowsBatch& select_rows, RowsBatch& new_rows) {
    std::set<std::string> select_signs;
    for (auto& row : select_rows) {
        std::string sign;
        sign.reserve(64);
        for (int i = 0; i < row.size(); i++) {
            sign += row[i] + "_";
        }
        select_signs.emplace(sign);
    }
    for (auto& row : old_batch) {
        std::string sign;
        sign.reserve(64);
        for (auto& field : row) {
            sign += field + "_";
        }
        if (select_signs.count(sign) == 0) {
            new_rows.emplace_back(row);
        }
    }
}

int ImporterBigtreeDiffHandle::do_insert(const std::string& table_name, RowsBatch& old_row_batch) {
    TimeCost step_time;
    auto& table_fields = table_fields_map[table_name];
    baikal::client::SmartConnection conn = _baikaldb->fetch_connection(0);
    if (!conn) {
        LOG(WARNING) << "fetch connection from pool fail";
        return -1;
    }
    int ret = do_begin(conn);
    if (ret < 0) {
        conn->close();
        LOG(WARNING) << "begin failed";
        return -1;
    }
    RowsBatch select_rows;
    std::string sql;
    sql.reserve(8192);

    if (table_name == "ideainfo" || table_name == "wordinfo") {
        // 确保unitid存在
        auto& unit_fields = table_fields_map["unitinfo"];
        RowsBatch unit_rows;
        for (auto& row : old_row_batch) {
            DataRow tmp_row = row;
            while (tmp_row.size() != 3) {
                tmp_row.pop_back();
            }
            unit_rows.emplace_back(tmp_row);
        }
        baikal::client::ResultSet result_set;
        gen_sql("unitinfo", unit_rows, OptType::SELECT, sql);
        ret = conn->execute(sql, true, &result_set);
        if (ret < 0) {
            do_rollback(conn);
            conn->close();
            LOG(WARNING) << "SELECT failed sql: " << sql;
            return -1;
        }
        RowsBatch exists_unit_rows;
        while (result_set.next()) {
            DataRow row;
            for (auto& field : unit_fields) {
                std::string id;
                result_set.get_string(field, &id);
                row.emplace_back(id);
            }
            exists_unit_rows.emplace_back(row);
        }
        if (exists_unit_rows.empty()) {
            do_rollback(conn);
            conn->close();
            return 0;
        }
        std::set<std::string> signs;
        for (auto& row : exists_unit_rows) {
            std::string sign;
            sign.reserve(64);
            for (int i = 0; i < row.size(); i++) {
                sign += row[i] + "_";
            }
            signs.emplace(sign);
        }
        RowsBatch now_row_batch;
        for (auto& row : old_row_batch) {
            std::string sign;
            sign.reserve(64);
            for (int i = 0; i < unit_fields.size(); i++) {
                sign += row[i] + "_";
            }
            if (signs.count(sign)) {
                now_row_batch.emplace_back(row);
            }
        }
        if (now_row_batch.empty()) {
            LOG(WARNING) << "something wrong failed";
            do_rollback(conn);
            conn->close();
            return 0;
        }
        old_row_batch.swap(now_row_batch);
    }

    baikal::client::ResultSet result_set;
    gen_sql(table_name, old_row_batch, OptType::SELECT, sql);
    ret = conn->execute(sql, true, &result_set);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "SELECT failed sql: " << sql;
        return -1;
    }
    int select_time = step_time.get_time();
    step_time.reset();
    while (result_set.next()) {
        DataRow row;
        for (auto& field : table_fields) {
            std::string id;
            result_set.get_string(field, &id);
            row.emplace_back(id);
        }
        select_rows.emplace_back(row);
    }
    if (select_rows.size() == 0) {
        select_rows.swap(old_row_batch);
    } else {
        RowsBatch new_rows;
        gen_new_rows(old_row_batch, select_rows, new_rows);
        if (new_rows.size() == 0) {
            do_rollback(conn);
            conn->close();
            return 0;
        }
        select_rows.swap(new_rows);
    }
    int get_data_time = step_time.get_time();
    step_time.reset();
    
    gen_sql(table_name, select_rows, OptType::INSERT, sql);
    ret = conn->execute(sql, true, &result_set);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "INSERT failed sql: " << sql;
        return -1;
    }
    int insert_time = step_time.get_time();
    step_time.reset();
    gen_sql(table_name, select_rows, OptType::INSERT_COUNT, sql);
    ret = conn->execute(sql, true, &result_set);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "INSERT_COUNT failed sql: " << sql;
        return -1;
    }
    int insert_count_time = step_time.get_time();
    step_time.reset();
    ret = do_commit(conn);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "commit failed sql: " << sql;
        return -1;
    }
    conn->close();
    LOG(INFO) << "[select_time:" << select_time << "][get_data_time:" << get_data_time <<
        "][insert_time:" << insert_time << "][insert_count_time:" << insert_count_time << "]";
    return 0;
}

void ImporterBigtreeDiffHandle::gen_count_rows(const std::string& table_name, const RowsBatch& select_rows, std::map<int, RowsBatch>& count_rows_map) {
    std::map<std::string, int64_t> count_map;
    auto& count_table_fields = count_table_fields_map[table_name];
    for (auto& row : select_rows) {
        std::string sign;
        sign.reserve(64);
        for (size_t i = 0; i < count_table_fields.size() - 1 ; i++) {
            sign += row[i] + "_";
        }
        sign.pop_back();
        count_map[sign]++;
    }

    for (auto& iter : count_map) {
        DataRow row;
        row.reserve(4);
        boost::split(row, iter.first, boost::is_any_of("_"));
        row.emplace_back(std::to_string(iter.second));
        count_rows_map[iter.second].emplace_back(row);
    }
    for (auto& iter : count_rows_map) {
        std::sort(iter.second.begin(), iter.second.end(), [](const DataRow & a, const DataRow & b) -> bool
                {
                    for (size_t i = 0; i < a.size(); i++) {
                        if (a[i] > b[i]) {
                            return false;
                        }
                    }
                    return true;
                });
    }
}

int ImporterBigtreeDiffHandle::do_delete(const std::string& table_name, RowsBatch& old_row_batch) {
    auto& table_fields = table_fields_map[table_name];
    TimeCost step_time;

    baikal::client::SmartConnection conn = _baikaldb->fetch_connection(0);
    if (!conn) {
        LOG(WARNING) << "fetch connection from pool fail";
        return -1;
    }
    int ret = do_begin(conn);
    if (ret < 0) {
        conn->close();
        LOG(WARNING) << "begin failed";
        return -1;
    }
    std::string sql;
    sql.reserve(8192);
    RowsBatch select_rows;
    baikal::client::ResultSet result_set;
    gen_sql(table_name, old_row_batch, OptType::SELECT, sql);
    ret = conn->execute(sql, true, &result_set);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "SELECT failed sql: " << sql;
        return -1;
    }
    int select_time = step_time.get_time();
    step_time.reset();
    while (result_set.next()) {
        DataRow row;
        for (auto& field : table_fields) {
            std::string id;
            result_set.get_string(field, &id);
            row.emplace_back(id);
        }
        select_rows.emplace_back(row);
    }
    if (select_rows.size() == 0) {
        do_rollback(conn);
        conn->close();
        return 0;
    }
    int get_data_time = step_time.get_time();
    step_time.reset();
    gen_sql(table_name, select_rows, OptType::DELETE, sql);
    ret = conn->execute(sql, true, &result_set);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "DELETE failed sql:" << sql;
        return -1;
    }

    if (table_name == "unitinfo") {
        gen_sql(table_name, select_rows, OptType::DELETE_IDEA, sql);
        ret = conn->execute(sql, true, &result_set);
        if (ret < 0) {
            do_rollback(conn);
            conn->close();
            LOG(WARNING) << "DELETE_IDEA failed sql:" << sql;
            return -1;
        }
        gen_sql(table_name, select_rows, OptType::DELETE_IDEA_COUNT, sql);
        ret = conn->execute(sql, true, &result_set);
        if (ret < 0) {
            do_rollback(conn);
            conn->close();
            LOG(WARNING) << "DELETE_IDEA_COUNT failed sql:" << sql;
            return -1;
        }
        gen_sql(table_name, select_rows, OptType::DELATE_WORD, sql);
        ret = conn->execute(sql, true, &result_set);
        if (ret < 0) {
            do_rollback(conn);
            conn->close();
            LOG(WARNING) << "DELATE_WORD failed sql:" << sql;
            return -1;
        }
        gen_sql(table_name, select_rows, OptType::DELATE_WORD_COUNT, sql);
        ret = conn->execute(sql, true, &result_set);
        if (ret < 0) {
            do_rollback(conn);
            conn->close();
            LOG(WARNING) << "DELATE_WORD_COUNT failed sql:" << sql;
            return -1;
        }
    }
    int delete_time = step_time.get_time();
    step_time.reset();
    std::map<int, RowsBatch> count_rows_map;
    gen_count_rows(table_name, select_rows, count_rows_map);
    int get_count_time = step_time.get_time();
    step_time.reset();
    for (auto& iter : count_rows_map) {
        gen_sql(table_name, iter.second, OptType::UPDATE_COUNT, sql);
        ret = conn->execute(sql, true, &result_set);
        if (ret < 0) {
            do_rollback(conn);
            conn->close();
            LOG(WARNING) << "UPDATE_COUNT failed sql: " << sql;
            return -1;
        }
    }
    int update_count_time = step_time.get_time();
    step_time.reset();
    gen_sql(table_name, select_rows, OptType::DELETE_COUNT, sql);
    ret = conn->execute(sql, true, &result_set);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "DELETE_COUNT failed sql: " << sql;
        return -1;
    }
    int delete_count_time = step_time.get_time();
    step_time.reset();
    ret = do_commit(conn);
    if (ret < 0) {
        do_rollback(conn);
        conn->close();
        LOG(WARNING) << "commit failed sql: " << sql;
        return -1;
    }
    conn->close();
    LOG(INFO) << "[select_time:" << select_time << "][get_data_time:" << get_data_time <<
        "][delete_time:" << delete_time << "][get_count_time:" << get_count_time << "][update_count_time:" << update_count_time <<
        "][delete_count_time:" << delete_count_time << "]";
    return 0;
}

static std::vector<std::string> check_vector{"planinfo","unitinfo", "wordinfo", "ideainfo"};

static std::map<std::string, std::string> check_table_fields{
    {"planinfo", "userid,planid "},
    {"unitinfo", "userid,planid,unitid "},
    {"wordinfo", "userid,planid,unitid,winfoid,wordid "},
    {"ideainfo", "userid,planid,unitid,ideaid "}
};

void ImporterBigtreeDiffHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult*) {
    std::vector<std::string> diff_lines;
    int cnt = 0;
    
    for (auto& fields : fields_vec) {
        if (fields.size() != 2 && fields.size() != 3) {
            std::string line;
            restore_line(fields, line);
            DB_FATAL("size diffrent file column size:%lu field size:%lu", fields.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        process_rows++;
        ++cnt;
        std::string prefix = "/*\"partion_key\":\"";
        prefix += fields[0];
        prefix += "\"*/select ";
        for (auto& table : check_vector) {
            std::string baikal_sql_count = "select count(*) as c from Bigtree."+ table + " where userid=";
            baikal_sql_count += fields[0];
            std::string baikal_sql_fields = "select "+ check_table_fields[table] + " from Bigtree."+ table + " where userid=";
            baikal_sql_fields += fields[0];
            {
                BAIDU_SCOPED_LOCK(_mutex);
                if ((table == "planinfo" || !FLAGS_use_planid_filter) && processed_userid[table].count(fields[0]) != 0) {
                    continue;
                }
                processed_userid[table].emplace(fields[0]);
            }
            if (table != "planinfo" && FLAGS_use_planid_filter) {
                baikal_sql_count += " and planid=";
                baikal_sql_count += fields[1];
                baikal_sql_fields += " and planid=";
                baikal_sql_fields += fields[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && fields.size() == 3) {
                    baikal_sql_count += " and unitid=";
                    baikal_sql_count += fields[2];
                    baikal_sql_fields += " and unitid=";
                    baikal_sql_fields += fields[2];
                }
            }
            std::string baikaldb_count_sql = "select ifnull(sum(";
            baikaldb_count_sql += count_name_map[table];
            baikaldb_count_sql += "), 0) as c from Bigtree.";
            baikaldb_count_sql += count_table_name_map[table]; 
            baikaldb_count_sql += " where userid=";
            baikaldb_count_sql += fields[0];

            if (table != "planinfo" && FLAGS_use_planid_filter) {
                baikaldb_count_sql += " and planid=";
                baikaldb_count_sql += fields[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && fields.size() == 3) {
                    baikaldb_count_sql += " and unitid=";
                    baikaldb_count_sql += fields[2];
                }
            }

            std::string f1_sql = prefix;
            f1_sql += "count(*) from FC_Word." + table + " where userid=";
            f1_sql += fields[0];
            if (table != "planinfo" && FLAGS_use_planid_filter) {
                f1_sql += " and planid=";
                f1_sql += fields[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && fields.size() == 3) {
                    f1_sql += " and unitid=";
                    f1_sql += fields[2];
                }
            }
            f1_sql += " and isdel=0";
            if (FLAGS_check_from_mysql) {
                f1_sql += " and 1=1";    
            }

            std::string f1_sql_fields = prefix;
            f1_sql_fields += check_table_fields[table];
            f1_sql_fields += ",unix_timestamp(now()) - (unix_timestamp(modtime)) as delay";
            f1_sql_fields += " from FC_Word." + table + " where userid=";
            f1_sql_fields += fields[0];
            if (table != "planinfo" && FLAGS_use_planid_filter) {
                f1_sql_fields += " and planid=";
                f1_sql_fields += fields[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && fields.size() == 3) {
                    f1_sql_fields += " and unitid=";
                    f1_sql_fields += fields[2];
                }
            }
            f1_sql_fields += " and isdel=0";
            int64_t count = 0;
            int ret = count_diff_check(baikal_sql_count, baikaldb_count_sql, f1_sql, count);
            if (ret < 0 && ret != -1) {
                std::string line;
                restore_line(fields, line);
                diff_lines.emplace_back(line);
                if (ret == -3) {
                    DB_WARNING("baikaldb vs mysql diff");
                    _mysql_count_diff_sql_file.write("-----baikaldb vs mysql diff----");
                    _mysql_count_diff_sql_file.write(baikal_sql_count + ";");
                    _mysql_count_diff_sql_file.write(baikaldb_count_sql + ";");
                    _mysql_count_diff_sql_file.write(f1_sql + ";");
                    _mysql_count_diff_sql_file.write("---------");
                } else {
                    DB_WARNING("baikaldb vs baikaldb true diff");
                    _baikaldb_count_diff_sql_file.write("----baikaldb vs baikaldb diff-----");
                    _baikaldb_count_diff_sql_file.write(baikal_sql_count + ";");
                    _baikaldb_count_diff_sql_file.write(baikaldb_count_sql + ";");
                    _baikaldb_count_diff_sql_file.write(f1_sql + ";");
                    _baikaldb_count_diff_sql_file.write("---------");
                }
            }
            if (count >= FLAGS_check_diff_data_number) {
                DB_NOTICE("too_big data sql:%s", baikal_sql_count.c_str());
                continue;
            }
            if (FLAGS_check_diff_data) {
                fields_diff_check(table, baikal_sql_fields, f1_sql_fields);
            }
        }
    }
    BAIDU_SCOPED_LOCK(_mutex);
    if (print_log_interval.get_time() > 60 * 1000 * 1000) {
        print_log_interval.reset();
        DB_NOTICE("process cnt: %ld diff cnt: %ld", process_rows.load(), diff_rows.load());
    }
}

int ImporterBigtreeDiffHandle::db_query(baikal::client::Service* db, const std::string& field, std::string& sql, int64_t& count) {
    int ret = 0;
    int retry = 0;
    do {
        baikal::client::ResultSet result_set;
        bthread_usleep(FLAGS_sleep_intervel_s * 1000 * 1000LL);
        ret = db->query(0, sql, &result_set);
        if (ret == 0) {
            DataRow row;
            while (result_set.next()) {
                int64_t id;
                result_set.get_int64(field, &id);
                count = id;
            }
            break;
        }   
        bthread_usleep(1000000);
    } while (++retry < FLAGS_query_retry_times);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
        _err_fs << sql;
        ++_err_cnt;
        return -1;
    }
    return 0;
}

int ImporterBigtreeDiffHandle::db_query(baikal::client::Service* db, std::vector<std::string>& fields, std::string& sql, RowsBatch& row_batch) {
    int ret = 0;
    int retry = 0;
    do {
        baikal::client::ResultSet result_set;
        bthread_usleep(FLAGS_sleep_intervel_s * 1000 * 1000LL);
        ret = db->query(0, sql, &result_set);
        if (ret == 0) {
            while (result_set.next()) {
                DataRow row;
                for (auto& field : fields) {
                    std::string id;
                    result_set.get_string(field, &id);
                    row.emplace_back(id);
                }
                row_batch.emplace_back(row);
            }
            break;
        }   
        bthread_usleep(1000000);
    } while (++retry < FLAGS_query_retry_times);
    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
        _err_fs << sql;
        ++_err_cnt;
        return -1;
    }
    return 0;
}


static std::map<std::string, int32_t> delete_batch_map{
    {"planinfo", 5},
    {"unitinfo", 10},
    {"wordinfo", 1000},
    {"ideainfo", 1000}
};

int ImporterBigtreeDiffHandle::fields_diff_check(std::string& table_name, std::string& baikal_sql, std::string& f1_sql) {
    RowsBatch baikaldb_rows;
    int ret = db_query(_baikaldb, table_fields_map[table_name], baikal_sql, baikaldb_rows);
    if (ret != 0) {
        return -1;
    }
    RowsBatch mysql_rows;
    ret = db_query(_backup_db, mysql_table_fields_map[table_name], f1_sql, mysql_rows);
    if (ret != 0) {
        return -1;
    }
    RowsBatch row_batch_need_insert;
    RowsBatch row_batch_need_delete;
    size_t field_size = table_fields_map[table_name].size();
    std::set<std::string> baikaldb_signs;
    std::set<std::string> mysql_signs;
    for (auto& row : baikaldb_rows) {
        std::string sign;
        sign.reserve(64);
        for (size_t i = 0; i < field_size; i++) {
            sign += row[i] + "_";
        }
        baikaldb_signs.emplace(sign);
    }
    for (auto& row : mysql_rows) {
        std::string sign;
        sign.reserve(64);
        for (size_t i = 0; i < field_size; i++) {
            sign += row[i] + "_";
        }
        mysql_signs.emplace(sign);
    }
    for (auto& row : mysql_rows) {
        std::string sign;
        sign.reserve(64);
        for (size_t i = 0; i < field_size; i++) {
            sign += row[i] + "_";
        }
        // baikaldb没有,需要insert
        if (baikaldb_signs.count(sign) == 0) {
            int64_t delay = std::stol(row[row.size()- 1]);
            if (delay > FLAGS_bigtree_delay_to_insert) { // 延迟超过bigtree_delay_to_insert才更新
                sign += row[row.size()- 1];
                DB_NOTICE("need insert sign:%s delay: %ld > %ld", sign.c_str(), delay, FLAGS_bigtree_delay_to_insert);
                DataRow tmp_row = row;
                tmp_row.pop_back();
                row_batch_need_insert.emplace_back(tmp_row);
            }
        }
    }
    for (auto& row : baikaldb_rows) {
        std::string sign;
        sign.reserve(64);
        for (size_t i = 0; i < field_size; i++) {
            sign += row[i] + "_";
        }
        // mysql没有,需要删除
        if (mysql_signs.count(sign) == 0) {
            row_batch_need_delete.emplace_back(row);
            DB_NOTICE("need delete sign:%s", sign.c_str());
        }
    }

    if (!row_batch_need_insert.empty()) {
        diff_rows++;
        DB_NOTICE("true diff baikal_sql:%s f1_sql:%s", baikal_sql.c_str(), f1_sql.c_str());
        _baikaldb_diff_sql_file.write("----baikaldb not found------");
        _baikaldb_diff_sql_file.write(baikal_sql + ";");
        _baikaldb_diff_sql_file.write(f1_sql + ";");
        _baikaldb_diff_sql_file.write("---------");
        _true_diff_data_file.write("----baikaldb not found------");
        _true_diff_data_file.write(table_name);
        _true_diff_data_file.write(row_batch_need_insert);
        _true_diff_data_file.write("---------");
        if (FLAGS_open_bigtree_insert_fix) {
            do_insert(table_name, row_batch_need_insert);
        }
    }
    if (!row_batch_need_delete.empty()) {
        diff_rows++;
        DB_NOTICE("true diff baikal_sql:%s f1_sql:%s", baikal_sql.c_str(), f1_sql.c_str());
        _baikaldb_diff_sql_file.write("----mysql not found-----");
        _baikaldb_diff_sql_file.write(baikal_sql + ";");
        _baikaldb_diff_sql_file.write(f1_sql + ";");
        _baikaldb_diff_sql_file.write("---------");
        _true_diff_data_file.write("----mysql not found-----");
        _true_diff_data_file.write(table_name);
        _true_diff_data_file.write(row_batch_need_delete);
        _true_diff_data_file.write("---------");
        if (FLAGS_open_bigtree_delete_fix) {
            RowsBatch delete_row_batch;
            for (auto& row : row_batch_need_delete) {
                delete_row_batch.emplace_back(row);
                if (delete_row_batch.size() == delete_batch_map[table_name]) {
                    do_delete(table_name, delete_row_batch);
                    delete_row_batch.clear();
                }
            }
            if (!delete_row_batch.empty()) {
                do_delete(table_name, delete_row_batch);
            }
        }
    }
    return 0;
}

int ImporterBigtreeDiffHandle::count_diff_check(std::string& baikal_sql, std::string& baikal_count_sql,
        std::string& f1_sql, int64_t& count) {
    int64_t count1 = 0;
    int64_t count2 = 0;
    int64_t count3 = 0;
    int ret = db_query(_baikaldb, "c", baikal_sql, count1);
    if (ret != 0) {
        return -1;
    }
    ret = db_query(_baikaldb, "c", baikal_count_sql, count2);
    if (ret != 0) {
        return -1;
    }
    ret = db_query(_backup_db, "count(*)", f1_sql, count3);
    if (ret != 0) {
        return -1;
    }
    ++_succ_cnt;
    count = count1;
    if (count1 != count2) {
        DB_NOTICE("found baikaldb inter diff %ld - %ld - %ld baikal_sql:%s baikal_count_sql:%s", count1, count2, count3, 
            baikal_sql.c_str(), baikal_count_sql.c_str());
        return -2;
    } else if (count1 != count3) {
        DB_NOTICE("found baikaldb mysql diff %ld - %ld - %ld baikal_sql:%s f1_sql:%s", count1, count2, count3,
            baikal_sql.c_str(), f1_sql.c_str());
        return -3;
    }
    return 0;
}


int ImporterRecoveryHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    }
    _recovery_type = node["recovery_type"].asString();
    if (_recovery_type == "all_fields_flash_back") {
        _next_recovery_type = "update_fields_flash_back";
    } else if (_recovery_type == "update_fields_flash_back") {
        _next_recovery_type = "skip_fields_flash_back";
    } else if (_recovery_type == "skip_fields_flash_back") {
        _next_recovery_type = "split_fields_flash_back";
    }
    if (node.isMember("skip_fields") && node["skip_fields"].isArray()) {
        for (const auto& field : node["skip_fields"]) {
            _skip_fields.emplace(field.asString());
        }
    }

    if (node["recovery"].isArray()) {
        size_t table_cnt = 0;
        for (auto& schema : node["recovery"]) {
            std::string db = schema["db"].asString();
            std::string table = schema["table"].asString();
            FlashBackTableInfo& table_info = _table_fields_info[db + "." + table];
            table_info.db = db;
            table_info.table = table;
            
            if (schema.isMember("skip_fields") && schema["skip_fields"].isArray()) {
                for (const auto& field : schema["skip_fields"]) {
                    table_info.skip_fields.emplace(field.asString());
                }
            } else {
                table_info.skip_fields = _skip_fields;
            }
        }
    }
    
    _path = FLAGS_input_path;
    _output_path = FLAGS_output_path;
    return 0;
}

std::shared_ptr<FBOutFile> ImporterRecoveryHandle::output_file(const std::string& file_name) {
    std::lock_guard<bthread::Mutex> l(_mutex);
    auto iter = _output_files.find(file_name);
    if (iter == _output_files.end()) {
        auto of = std::make_shared<FBOutFile>(_output_path + "/" + file_name, false);
        of->open();
        _output_files[file_name] = of;
        return of;
    } else {
        return iter->second;
    }
}  

int ImporterRecoveryHandle::close() {
    bool need_gen_next_done = false;
    for (auto& iter : _output_files) {
        iter.second->close();
    }
    // 产出执行结果
    Json::Value result(Json::objectValue);
    result["result"] = Json::Value(Json::arrayValue);
    Json::Value& json_infos = result["result"];
    for (const auto& info : _table_fields_info) {
        Json::Value json_info(Json::objectValue);
        json_info["db"] = info.second.db;
        json_info["table"] = info.second.table;
        json_info["success_cnt"] = (Json::Int64)info.second.success_cnt;
        json_info["fail_cnt"]    = (Json::Int64)info.second.fail_cnt;
        json_info["legacy_cnt"]  = (Json::Int64)info.second.legacy_cnt;
        json_infos.append(json_info);
        if (info.second.legacy_cnt > 0) {
            need_gen_next_done = true;
        }
    }
    writer_json_to_file(result, FLAGS_output_path + "/result");
    // 有下一步时才产出下一步done文件
    if (!_next_recovery_type.empty() && need_gen_next_done) {
        Json::Value next_step_done_file(Json::objectValue);
        next_step_done_file["delim"] = _delim;
        next_step_done_file["recovery_type"] = _next_recovery_type;
        if (_next_recovery_type == "skip_fields_flash_back") {
            next_step_done_file["skip_fields"] = Json::Value(Json::arrayValue);
            Json::Value& skip_fields = next_step_done_file["skip_fields"];
            skip_fields.append("modtime");
        }
        next_step_done_file["recovery"] = Json::Value(Json::arrayValue);
        Json::Value& recoverys = next_step_done_file["recovery"];
        for (const auto& info : _table_fields_info) {
            Json::Value json_info(Json::objectValue);
            json_info["db"] = info.second.db;
            json_info["table"] = info.second.table;
            recoverys.append(json_info);
        }
        writer_json_to_file(next_step_done_file, FLAGS_output_path + "/done");
    }
    return 0;
}

int flatten_update_fields_values(const std::set<std::string>& pk_fields, const std::vector<std::string>& fields, 
        const std::vector<std::string>& old_values, const std::vector<std::string>& new_values, const std::set<std::string>& skip_fields, 
        std::map<std::string, std::string>& set_map, std::map<std::string, std::string>& where_map) {
    for (size_t i = 0; i < fields.size(); i++) {
        if (old_values[i] != new_values[i]) {
            if (pk_fields.count(fields[i]) > 0) {
                DB_FATAL("diff pk field: %s, value [%s vs %s]", fields[i].c_str(), old_values[i].c_str(), new_values[i].c_str());
                return -1;
            } else if (skip_fields.count(fields[i]) <= 0) {
                set_map[fields[i]] = old_values[i];
                where_map[fields[i]] = new_values[i];
            }
        } else if (pk_fields.count(fields[i]) > 0) {
            where_map[fields[i]] = new_values[i];
        }
    }
    return 0;
} 
int flatten_split_fields_values(const std::set<std::string>& pk_fields, const std::vector<std::string>& fields, 
        const std::vector<std::string>& old_values, const std::vector<std::string>& new_values, const std::set<std::string>& skip_fields, 
        std::map<std::string, std::pair<std::string, std::string>>& set_map, std::map<std::string, std::string>& pk_field_values) {
    for (size_t i = 0; i < fields.size(); i++) {
        if (old_values[i] != new_values[i]) {
            if (pk_fields.count(fields[i]) > 0) {
                DB_FATAL("diff pk field: %s, value [%s vs %s]", fields[i].c_str(), old_values[i].c_str(), new_values[i].c_str());
                return -1;
            } else if (skip_fields.count(fields[i]) <= 0) {
                set_map[fields[i]] = {old_values[i], new_values[i]};
            }
        } else if (pk_fields.count(fields[i]) > 0) {
            pk_field_values[fields[i]] = new_values[i];
        }
    }
    return 0;
} 
void ImporterRecoveryHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult*) {
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    std::vector<std::string> vec;
    boost::split(vec, path, boost::is_any_of("/"));
    if (vec.empty()) {
        DB_FATAL("path: %s canot split", path.c_str());
        return;
    }
    std::string file_name = vec[vec.size() - 1];
    if (file_name == "done" || file_name == "result") {
        DB_WARNING("path: %s not need recovery", path.c_str());
        return;
    }
    auto of = output_file(file_name);
    for (const auto& values : fields_vec) {
        std::string type;
        std::string db;
        std::string table;
        std::set<std::string> pk_fields;
        std::vector<std::string> fields;
        std::vector<std::string> old_values;
        std::vector<std::string> new_values;
        int ret = FlashBackExec::decode_values(values, type, db, table, pk_fields, fields, old_values, new_values);
        if (ret < 0) {
            DB_FATAL("decode values failed");
            continue;
        }

        if (_table_fields_info.count(db + "." + table) <= 0) {
            continue;
        }
        std::vector<std::string> sqls;
        sqls.reserve(3);
        FlashBackTableInfo& table_info = _table_fields_info[db + "." + table];
        if (type == "INSERT") {
            std::map<std::string, std::string> where_map;
            for (size_t i = 0; i < fields.size(); i++) {
                where_map[fields[i]] = new_values[i];
            }

            std::string sql = gen_delete_sql(connection, db, table, where_map);
            sqls.emplace_back(sql);
        } else if (type == "DELETE") {
            std::map<std::string, std::string> value_map;
            for (size_t i = 0; i < fields.size(); i++) {
                value_map[fields[i]] = old_values[i];
            }

            std::string sql = gen_insert_sql(connection, db, table, value_map);
            sqls.emplace_back(sql);
        } else if (type == "UPDATE") {
            std::map<std::string, std::string> set_map;
            std::map<std::string, std::string> where_map;
            if (_recovery_type == "all_fields_flash_back") {
                for (size_t i = 0; i < fields.size(); i++) {
                    if (pk_fields.count(fields[i]) > 0 && old_values[i] == new_values[i]) {
                        // 如果主键没有发生变化则set中不设置主键，因为修改主键走全局二级索引逻辑db和store交互次数多
                    } else {
                        set_map[fields[i]] = old_values[i];
                    }
                    where_map[fields[i]] = new_values[i];
                }
                std::string sql = gen_update_sql(connection, db, table, set_map, where_map);
                sqls.emplace_back(sql);
            } else if (_recovery_type == "update_fields_flash_back") {
                std::set<std::string> skip_fields;
                int ret = flatten_update_fields_values(pk_fields, fields, old_values, new_values, skip_fields, 
                            set_map, where_map);
                if (ret < 0) {
                    DB_FATAL("flatten_fields_values failed, db: %s, table: %s", db.c_str(), table.c_str());
                    continue;
                }
                std::string sql = gen_update_sql(connection, db, table, set_map, where_map);
                sqls.emplace_back(sql);
            } else if (_recovery_type == "skip_fields_flash_back") {
                int ret = flatten_update_fields_values(pk_fields, fields, old_values, new_values, _skip_fields, 
                            set_map, where_map);
                if (ret < 0) {
                    DB_FATAL("flatten_fields_values failed, db: %s, table: %s", db.c_str(), table.c_str());
                    continue;
                }
                std::string sql = gen_update_sql(connection, db, table, set_map, where_map);
                sqls.emplace_back(sql);
            } else if (_recovery_type == "split_fields_flash_back") {
                std::map<std::string, std::pair<std::string, std::string>> set_old_new;
                std::map<std::string, std::string> pk_field_values;
                int ret = flatten_split_fields_values(pk_fields, fields, old_values, new_values, _skip_fields, 
                        set_old_new, pk_field_values);
                if (ret < 0) {
                    DB_FATAL("flatten_fields_values failed, db: %s, table: %s", db.c_str(), table.c_str());
                    continue;
                }
                for (const auto& field : set_old_new) {
                    std::map<std::string, std::string> set_map;
                    std::map<std::string, std::string> where_map = pk_field_values;
                    set_map[field.first] =  field.second.first;
                    where_map[field.first] =  field.second.second;
                    std::string sql = gen_update_sql(connection, db, table, set_map, where_map);
                    sqls.emplace_back(sql);
                }

            } else if (_recovery_type == "user_define_flash_back") {
                // TODO
                DB_FATAL("not support user_define_flash_back");
            } else {
                DB_FATAL("not support recovery type: %s", _recovery_type.c_str());
                continue;
            }

        } else {
            continue;
        }
        int retry = 0;
        for (const std::string& sql : sqls) {
            if (FLAGS_is_debug) {
                DB_WARNING("flash_back sql: %s", sql.c_str());
            }
            while (true) {
                baikal::client::ResultSet result_set;
                int ret = _baikaldb->query(0, sql, &result_set);
                if (ret == 0) {
                    if (result_set.get_affected_rows() < 1 && type != "DELETE") {
                        std::lock_guard<bthread::Mutex> l(_mutex);
                        of->write(values, _delim);
                        table_info.legacy_cnt++;
                        _out_cnt++;
                    } else {
                        table_info.success_cnt++;
                    }
                    break;
                } else {
                    if (++retry < FLAGS_query_retry_times) {
                        continue;
                    } else {
                        // 执行失败
                        _err_fs << sql;
                        table_info.fail_cnt++;
                    }
                }  
                bthread_usleep(100000);
            }
        }
    }
}


int ImporterDiffLineHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterDiffLineHandle::handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult*) {
    for (auto& fields : fields_vec) {
        if (fields.size() != _fields.size()) {
            std::string line;
            restore_line(fields, line);
            bool find_all_pk = true;
            std::string pks;
            for (const auto& pk : _pk_fields) {
                if (pk.second >= fields.size()) {
                    find_all_pk = false;
                    continue;
                }
                pks += pk.first + ": " +  fields[pk.second] + ",";
            }
            if (find_all_pk) {
                pks.pop_back();
                DB_FATAL("FIND_DIFF_LINE: %s", pks.c_str());
            } else {
                DB_FATAL("FIND_DIFF_LINE: %s", line.c_str());
            }
            _import_diff_lines++;
            continue;
        }
    }
    return;
}

}
