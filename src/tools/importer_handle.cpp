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
DEFINE_bool(select_first, false, "is_debug");
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
        exit(127);
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
    if (BASE_UP != _type) {
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
            name = "`" + vec[0] + "`";
            _const_map[name] = vec[1];
        }
        _null_as_string = FLAGS_null_as_string;
        if (node.isMember("other_condition")) {
            _other_condition = node["other_condition"].asString();
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
        _max_failure_percent = FLAGS_max_sql_fail_percent;
        // 最大失败阈值，超过阈值，导入任务认为失败，version不会更新，状态转为idle重试，并且email报警
        if (node.isMember("max_failure_percent")) {
            _max_failure_percent = node["max_failure_percent"].asInt64();
        }
    }

    //当 _is_local_done_json 为真时，done文件写在 HDFS_TASK_DB.HDFS_TASK_TABLE 表中。
    std::string tmp_path = node["path"].asString();
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
    default:
        DB_FATAL("invalid type: %d", type);
        return nullptr;
    }

    return importer;
}

int ImporterHandle::handle_files(const LinesFunc& fn, ImporterFileSystermAdaptor* fs, const std::string& config, const ProgressFunc& progress_func) { 
    int32_t file_concurrency = 0;
    int32_t insert_values_count = 0;
    if (!config.empty()) {
        Json::Reader json_reader;
        Json::Value done_root;
        bool ret1 = json_reader.parse(config, done_root);
        if (ret1) {
            try {
                if (done_root.isMember("file_concurrency")) {
                    file_concurrency = done_root["file_concurrency"].asInt64();
                }
                if (done_root.isMember("insert_values_count")) {
                    insert_values_count = done_root["insert_values_count"].asInt64();
                }
            } catch (Json::LogicError& e) {
                DB_FATAL("fail parse what:%s ", e.what());
            }
        }
    }
    ImporterImpl Impl(_path, fs, fn, file_concurrency, insert_values_count, 0, 0, _has_header);
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
    int ret = Impl.run(progress_func);
    if (ret < 0) {
        _import_ret << Impl.get_result();
        return ret;
    }
    return 0;
}

int64_t ImporterHandle::run(ImporterFileSystermAdaptor* fs, const std::string& config, const ProgressFunc& progress_func) {
    auto handle_func = [this](const std::string& path, const std::vector<std::string>& lines) {
        _import_lines += lines.size();
        handle_lines(path, lines);
    };

    int ret = handle_files(handle_func, fs, config, progress_func);
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
        while (fpr.good()) {
            if (row > 20) {
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
            std::string cmd = "echo \"" + _import_ret.str() + "\nerr_sqls:\n" + error_example.str() + "\" | mail -s 'BaikalDB import fail, " +
                _db + "." + _table + "' '" + emails + "' -- -f baikalDM@baidu.com";
            DB_WARNING("email cmd: %s", cmd.c_str());
            system_cmd(cmd.c_str());
        }
        return -1;
    }
    // 导入结束后的操作，例如replace rename table
    if (_err_cnt_retry.load() > 0) {
        _import_ret << "sql_err: " << _err_cnt_retry.load();
    }
    ret = close(); 
    if (ret < 0) {
        return -1;
    }

    return _import_lines.load();
}

int ImporterHandle::query(std::string sql, baikal::client::SmartConnection& connection, bool is_retry) {
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

std::string ImporterHandle::_mysql_escape_string(baikal::client::SmartConnection connection, const std::string& value) {
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
    if (connection) {
        MYSQL* RES = connection->get_mysql_handle();
        mysql_real_escape_string(RES, str, value.c_str(), value.size());
        escape_value = str;
    } else {
        LOG(WARNING) << "service fetch_connection() failed";
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

void ImporterRepHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
    std::string sql;
    std::string insert_values;
    int cnt = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        if (split_vec.size() != _fields.size()) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        ++cnt;
        int i = 0;
        insert_values += "(";
        for (auto& item : split_vec) {
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
    query(sql, connection);    
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

void ImporterUpHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
    std::string sql;
    std::string insert_values;
    int cnt = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));

    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        if (split_vec.size() != _fields.size()) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        ++cnt;
        int i = 0;
        insert_values += "(";
        for (auto& item : split_vec) {
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
    query(sql, connection);
}

int ImporterSelHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterSelHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
    std::string sql;
    std::string insert_values;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        if (split_vec.size() != _fields.size()) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
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
            sql += _fields[i] + " = '" + _mysql_escape_string(connection, split_vec[i]) + "'";
            is_first = false;
        }
        query(sql, connection);
    }
}


int ImporterSelpreHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterSelpreHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
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
    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        if (split_vec.size() != _fields.size()) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        ++cnt;
        if (_type == SEL_PRE) {
            char id[100];
            std::unique_ptr<char> content(new char[1024 * 1024]);
            uint64_t length_content = 0;
            snprintf(id, sizeof(id), "%s", split_vec[0].c_str());
            uint64_t length_id = split_vec[0].size();
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
    return 0;
}

void ImporterDelHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
    std::string sql;
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
    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        if (split_vec.size() != _fields.size()) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
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
            sql +=  "'" + _mysql_escape_string(connection, split_vec[i]) + "'";
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
        sql += " and " + _other_condition;
    }
    query(sql, connection);
}

int ImporterSqlupHandle::init(const Json::Value& node, const std::string& path_prefix) {
    int ret = ImporterHandle::init(node, path_prefix);
    if (ret < 0) {
        return -1;
    } 
    return 0;
}

void ImporterSqlupHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
    std::string sql;
    int cnt = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        if (split_vec.size() != _fields.size()) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
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
            sql += _fields[i] + " = '" + _mysql_escape_string(connection, split_vec[i]) + "'";
            is_first = false;
        }
        sql += " where ";
        is_first = true;
        for (auto& pair : _pk_fields) {
            if (!is_first) {
                sql += " and ";
            }
            sql += pair.first+ " = '" + _mysql_escape_string(connection, split_vec[pair.second]) + "'";
            is_first = false;
        }
        query(sql, connection);
        cnt++;
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

void ImporterDupupHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
    std::string sql;
    std::string insert_values;
    int cnt = 0;
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
            connection->close();
        }  
    }));
    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        if (split_vec.size() != _fields.size()) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        ++cnt;
        if (_type == DUP_UP) {
            int i = 0;
            insert_values += "(";
            for (auto& item : split_vec) {
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
        query(sql, connection);
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
        }
        //如果table数和filtervalue数对不上，则有可能有重复的filter_value
        if (table_cnt != _level_table_map.size()) {
            DB_FATAL("table count:%ld, level_table map size:%lu", table_cnt, _level_table_map.size());
            return -1;
        }
    }
    return 0;
}

void ImporterBaseupHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {
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
    for (auto& line : lines) {
        bool is_find = false;
        std::vector<std::string> split_vec;
        if (!split(line, &split_vec)) {
            continue;
        }
        for (auto iter = _level_table_map.begin(); iter != _level_table_map.end(); iter++) {
            if (split_vec[iter->second.filter_idx] != iter->second.filter_value) {
                continue;
            }
            if (split_vec.size() != iter->second.fields.size()) {
                DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), iter->second.fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
                _import_diff_lines++;
                continue;
            }
            ++cnt;
            int i = 0;
            std::string& insert_values = level_insert_values[iter->second.filter_value];
            insert_values += "(";
            for (auto& item : split_vec) {
                if (iter->second.ignore_indexes.count(i++) == 0) { 
                    if (_null_as_string || item != "NULL") {
                        insert_values += "'" + _mysql_escape_string(connection, item) + "',";
                    } else {
                        insert_values +=  item + ",";
                    }
                }
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
        query(sql, connection);
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

void ImporterBigtreeDiffHandle::handle_lines(const std::string& path, const std::vector<std::string>& lines) {

    std::vector<std::string> diff_lines;
    int cnt = 0;
    
    for (auto& line : lines) {
        std::vector<std::string> split_vec;
        boost::split(split_vec, line, boost::is_any_of(_delim));
        if (split_vec.size() != 2 && split_vec.size() != 3) {
            DB_FATAL("size diffrent file column size:%lu field size:%lu", split_vec.size(), _fields.size());
            DB_FATAL("ERRLINE:%s", line.c_str());
            _import_diff_lines++;
            continue;
        }
        process_rows++;
        ++cnt;
        std::string prefix = "/*\"partion_key\":\"";
        prefix += split_vec[0];
        prefix += "\"*/select ";
        for (auto& table : check_vector) {
            std::string baikal_sql_count = "select count(*) as c from Bigtree."+ table + " where userid=";
            baikal_sql_count += split_vec[0];
            std::string baikal_sql_fields = "select "+ check_table_fields[table] + " from Bigtree."+ table + " where userid=";
            baikal_sql_fields += split_vec[0];
            {
                BAIDU_SCOPED_LOCK(_mutex);
                if ((table == "planinfo" || !FLAGS_use_planid_filter) && processed_userid[table].count(split_vec[0]) != 0) {
                    continue;
                }
                processed_userid[table].emplace(split_vec[0]);
            }
            if (table != "planinfo" && FLAGS_use_planid_filter) {
                baikal_sql_count += " and planid=";
                baikal_sql_count += split_vec[1];
                baikal_sql_fields += " and planid=";
                baikal_sql_fields += split_vec[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && split_vec.size() == 3) {
                    baikal_sql_count += " and unitid=";
                    baikal_sql_count += split_vec[2];
                    baikal_sql_fields += " and unitid=";
                    baikal_sql_fields += split_vec[2];
                }
            }
            std::string baikaldb_count_sql = "select ifnull(sum(";
            baikaldb_count_sql += count_name_map[table];
            baikaldb_count_sql += "), 0) as c from Bigtree.";
            baikaldb_count_sql += count_table_name_map[table]; 
            baikaldb_count_sql += " where userid=";
            baikaldb_count_sql += split_vec[0];

            if (table != "planinfo" && FLAGS_use_planid_filter) {
                baikaldb_count_sql += " and planid=";
                baikaldb_count_sql += split_vec[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && split_vec.size() == 3) {
                    baikaldb_count_sql += " and unitid=";
                    baikaldb_count_sql += split_vec[2];
                }
            }

            std::string f1_sql = prefix;
            f1_sql += "count(*) from FC_Word." + table + " where userid=";
            f1_sql += split_vec[0];
            if (table != "planinfo" && FLAGS_use_planid_filter) {
                f1_sql += " and planid=";
                f1_sql += split_vec[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && split_vec.size() == 3) {
                    f1_sql += " and unitid=";
                    f1_sql += split_vec[2];
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
            f1_sql_fields += split_vec[0];
            if (table != "planinfo" && FLAGS_use_planid_filter) {
                f1_sql_fields += " and planid=";
                f1_sql_fields += split_vec[1];
                if (table != "unitinfo" && FLAGS_use_unitid_filter && split_vec.size() == 3) {
                    f1_sql_fields += " and unitid=";
                    f1_sql_fields += split_vec[2];
                }
            }
            f1_sql_fields += " and isdel=0";
            int64_t count = 0;
            int ret = count_diff_check(baikal_sql_count, baikaldb_count_sql, f1_sql, count);
            if (ret < 0 && ret != -1) {
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

}
