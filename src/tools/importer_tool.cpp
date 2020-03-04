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

#include "importer_tool.h"
#include "proto/fc.pb.h"

namespace baikaldb {
DEFINE_string(data_path, "./data", "data path");
DEFINE_int32(concurrency, 5, "concurrency");
DEFINE_int32(file_concurrency, 30, "file_concurrency");
DEFINE_string(insert_mod, "replace", "replace|insert|insert ignore");
DEFINE_int32(insert_values_count, 100, "insert_values_count");
DEFINE_int32(file_block_size, 100, "split file to block to handle(MBytes)");
DEFINE_int32(loop_cnt, 1, "import loops, use for press");
DEFINE_int32(sleep_intervel_s, 0, "debug for sst");
DEFINE_bool(need_escape_string, true, "need_escape_string");
DEFINE_bool(null_as_string, false, "null_as_string");
DEFINE_bool(is_mnt, false, "is_mnt");
DEFINE_bool(is_debug, false, "is_debug");

typedef boost::filesystem::directory_iterator dir_iter;

inline std::string reverse_bytes(std::string pri) {
    uint64_t img_id = strtoull(pri.c_str(), NULL, 0); 
    char* ptr =(char*)&img_id;
    std::swap(ptr[0], ptr[7]);
    std::swap(ptr[1], ptr[6]);
    std::swap(ptr[2], ptr[5]);
    std::swap(ptr[3], ptr[4]);
    return std::to_string(img_id);
}


int Importer::init(baikal::client::Service* baikaldb_gbk, baikal::client::Service* baikaldb_utf8) {
    _baikaldb_gbk = baikaldb_gbk;
    _baikaldb_utf8 = baikaldb_utf8;
    char buf[100];
    time_t now = time(NULL);
    strftime(buf, 100, "%F_%T", localtime(&now));
    _err_name = "err_sql.";
    _err_name += buf;
    _err_name_retry = _err_name + "_retry";
    _err_fs.open(_err_name, std::ofstream::out | std::ofstream::app);
    _err_fs_retry.open(_err_name_retry, std::ofstream::out | std::ofstream::app);
    _err_cnt = 0;
    _succ_cnt = 0;
    return 0;
}

Importer::~Importer() {
    _err_fs.close();
    _err_fs_retry.close();
    boost::filesystem::remove_all(_err_name);
    if (_err_cnt_retry == 0) {
        boost::filesystem::remove_all(_err_name_retry);
    }
}

int Importer::rename_table(std::string old_name, std::string new_name) {
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

int Importer::all_block_count(std::string path) {
    if (boost::filesystem::is_symlink(path)) {
        DB_TRACE("path:%s is symlink", path.c_str());
        char buf[2000] = {0};
        readlink(path.c_str(), buf, sizeof(buf));
        boost::filesystem::path symlink(path);
        boost::filesystem::path symlink_dir = symlink.parent_path();
        if (buf[0] == '/') {
            path = buf;
        } else {
            path = symlink_dir.string() + "/" + buf;
        }
    }
    if (boost::filesystem::is_directory(path)) {
        int count = 0;
        DB_TRACE("path:%s is dir", path.c_str());
        dir_iter iter(path);
        dir_iter end;
        for (; iter != end; ++iter) {
            std::string child_path = iter->path().c_str();
            count += all_block_count(child_path);
        }
        return count;
    }
    if (!boost::filesystem::is_regular_file(path)) {
        return 0;
    }
    size_t file_size = boost::filesystem::file_size(path);
    size_t file_block_size = FLAGS_file_block_size * 1024 * 1024ULL;
    int blocks = file_size / file_block_size + 1;
    DB_TRACE("path:%s is file, size:%lu, blocks:%d", 
            path.c_str(), file_size, blocks);
    return blocks;
}

void Importer::recurse_handle(BthreadCond& file_concurrency_cond, std::string path) {
    DB_TRACE("path:%s", path.c_str());
    if (boost::filesystem::is_symlink(path)) {
        DB_TRACE("path:%s is symlink", path.c_str());
        char buf[2000] = {0};
        readlink(path.c_str(), buf, sizeof(buf));
        boost::filesystem::path symlink(path);
        boost::filesystem::path symlink_dir = symlink.parent_path();
        if (buf[0] == '/') {
            path = buf;
        } else {
            path = symlink_dir.string() + "/" + buf;
        }
    }
    if (boost::filesystem::is_directory(path)) {
        DB_TRACE("path:%s is dir", path.c_str());
        dir_iter iter(path);
        dir_iter end;
        for (; iter != end; ++iter) {
            std::string child_path = iter->path().c_str();
            recurse_handle(file_concurrency_cond, child_path);
        }
        return;
    }
    if (!boost::filesystem::is_regular_file(path)) {
        return;
    }
    size_t file_size = boost::filesystem::file_size(path);
    size_t file_block_size = FLAGS_file_block_size * 1024 * 1024ULL;
    int blocks = file_size / file_block_size + 1;
    DB_TRACE("path:%s is file, size:%lu, blocks:%d", 
            path.c_str(), file_size, blocks);
    for (size_t i = 0; i <= file_size / file_block_size; i++) {
        size_t start_pos = i * file_block_size;
        size_t end_pos = (i + 1) * file_block_size;
        end_pos = end_pos > file_size ? file_size : end_pos;
        DB_TRACE("path:%s start:%lu, end:%lu", path.c_str(), start_pos, end_pos);
        auto calc = [this, &file_concurrency_cond, path, start_pos, end_pos, file_size]() {
            std::vector<std::string> lines;
            std::vector<std::string> tmp_lines;
            lines.reserve(FLAGS_insert_values_count);
            tmp_lines.reserve(FLAGS_insert_values_count);
            BthreadCond concurrency_cond(-FLAGS_concurrency); // -n就是并发跑n个bthread
            std::ifstream fp(path);
            DB_TRACE("start block handle path:%s:%lu, bad:%d fail:%d, %m", 
                    path.c_str(), start_pos, fp.bad(), fp.fail());
            int64_t count = 0;
            // 非首快跳过一行
            if (start_pos != 0) {
                fp.seekg(start_pos);
                std::string line;
                std::getline(fp, line);
            }
            while (!fp.eof() && (size_t)fp.tellg() <= end_pos) {
                if (++count % 10000 == 0) {
                    DB_TRACE("handle block path:%s:%lu, lines:%ld; rate of progress:%d/%d cost:%ld",
                            path.c_str(), start_pos, count, 
                            _handled_block_count.load(), _all_block_count, _cost.get_time());
                }
                std::string line;
                std::getline(fp, line);
                if (line.empty()) {
                    continue;
                }
                tmp_lines.push_back(line);
                if ((int)tmp_lines.size() < FLAGS_insert_values_count) {
                    continue;
                }
                tmp_lines.swap(lines);
                tmp_lines.clear();
                _calc_in_bthread(lines, concurrency_cond);
            }
            tmp_lines.swap(lines);
            tmp_lines.clear();
            _calc_in_bthread(lines, concurrency_cond);
            concurrency_cond.wait(-FLAGS_concurrency);
            _handled_block_count++;
            DB_TRACE("end handle block path:%s:%lu, bad:%d, fail:%d, lines:%ld "
                    "rate of progress:%d/%d cost:%ld, %m", 
                    path.c_str(), start_pos, fp.bad(), fp.fail(), count - 1, 
                    _handled_block_count.load(), _all_block_count, _cost.get_time());
            _import_lines += count - 1;
            if (fp.bad()) {
                _error = true;
            }
            if (end_pos == file_size) {
                DB_NOTICE("end handle file path:%s", path.c_str());
            }
            file_concurrency_cond.decrease_signal();
        };
        file_concurrency_cond.increase_wait();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(calc);
    }
    return;
}

int Importer::handle(const Json::Value& node, OpType type, std::string hdfs_mnt, std::string done_path, std::string charset) {
    if (charset == "gbk") {
        _baikaldb = _baikaldb_gbk;
    } else {
        _baikaldb = _baikaldb_utf8;
    }
    _type = type;
    if (_type == BASE_UP) {
        if (node["update"].isArray()) {
            int_least64_t table_cnt = 0;
            for (auto& schema : node["update"]) {
                if (!schema.isMember("filter_field") || !schema.isMember("filter_value")) {
                    DB_FATAL("has no filter_field, node", node.asString());
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
                DB_FATAL("table count:%d, level_table map size:%d", table_cnt, _level_table_map.size());
                return -1;
            }
        }
    } else {
        _db = node["db"].asString();
        _table = node["table"].asString();
        _fields.clear();
        DB_TRACE("database:%s, table:%s, type:%d", _db.c_str(), _table.c_str(), _type);
        int i = 0;
        _ignore_indexes.clear();
        for (auto& field : node["fields"]) {
            std::string name = "`" + field.asString() + "`";
            _fields.push_back(name);
            if (name == "imageid") {
                _imageid_idx = i;
            }
            if (field.asString().empty()) {
                _ignore_indexes.insert(i);
            }
            i++;
        }
    }

    std::string hdfs_path = node["path"].asString();
    if (hdfs_mnt.empty()) {
        if (hdfs_path[0] == '/') {
            std::vector<std::string> split_vec;
            boost::split(split_vec, hdfs_path, boost::is_any_of("/"));
            hdfs_path = split_vec.back();
        }
        if (FLAGS_is_mnt) {
            hdfs_path = "import_data";
        }
        _path = FLAGS_data_path + "/" + hdfs_path;
        DB_WARNING("path:%s", _path.c_str());
    } else {
        if (hdfs_path[0] == '/') {
            _path = hdfs_mnt + hdfs_path;  
        } else if (hdfs_path[0] == '.') {
            _path = hdfs_mnt + done_path + hdfs_path;
        } else {
            DB_FATAL("don`t support path:%s", hdfs_path.c_str());
            return -1;
        }
        DB_WARNING("path:%s", _path.c_str());
    }
    _delim = node.get("delim", "\t").asString();
    DB_TRACE("path:%s %d", _path.c_str(), boost::filesystem::status(_path).type());
    if (_type == REP) {
        std::string sql  = "truncate table ";
        sql += _db + "." + _table + "_tmp";
        int ret = query(sql);
        if (ret < 0) {
            DB_FATAL("truncate table fail");
            return -1;
        }
    }

    int loop = 0;
    while (loop++ < FLAGS_loop_cnt) {
        _handled_block_count = 0;
        _import_lines = 0;
        _all_block_count = all_block_count(_path);
        DB_NOTICE("all_block_count:%d", _all_block_count);
        BthreadCond file_concurrency_cond(-FLAGS_file_concurrency); // -n就是并发跑n个bthread
        recurse_handle(file_concurrency_cond, _path);
        file_concurrency_cond.wait(-FLAGS_file_concurrency);
        if (_error) {
            DB_FATAL("error");
            return -1;
        }
        if (_err_cnt * 1.0 / _succ_cnt > 0.1) {
            DB_FATAL("err percent:%f", _err_cnt * 1.0 / _succ_cnt);
            return -1;
        }
        DB_NOTICE("import total_lines:%ld succ_cnt:%ld ,err_cnt:%ld", 
                _import_lines.load(), _succ_cnt, _err_cnt);
    }
    // retry 
    std::ifstream fp(_err_name);
    int count = 0;
    while (fp.good()) {
        if (++count % 100 == 0) {
            DB_TRACE("handle path:%s , lines:%ld", _err_name.c_str(), count);
        }
        std::string line;
        std::getline(fp, line);
        if (line.empty()) {
            continue;
        }
        query(line, true);
    }
    DB_NOTICE("import succ_cnt:%ld ,err_cnt_retry:%ld", _succ_cnt, _err_cnt_retry);
    // swap table and table_tmp
    if (_type == REP) {
        int ret = 0;
        ret = rename_table(_table, _table + "_tmp2");
        if (ret < 0) {
            DB_FATAL("rename fail old:%s new:%s", _table.c_str(), (_table + "_tmp2").c_str());
            return -1;
        }
        ret = rename_table(_table + "_tmp", _table);
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
    }
    return 0;
}
void Importer::other_calc_in_bthread(std::vector<std::string>& lines,
        BthreadCond& concurrency_cond) {
    auto calc = [this, lines, &concurrency_cond]() {
        auto decrease_func = [](BthreadCond* cond) {cond->decrease_signal();};
        std::shared_ptr<BthreadCond> auto_dec2(&concurrency_cond, decrease_func);
        std::string sql;
        std::string insert_values;
        int cnt = 0;
        for (auto& line : lines) {
            std::vector<std::string> split_vec;
            boost::split(split_vec, line, boost::is_any_of(_delim));
            if (split_vec.size() != _fields.size()) {
                DB_FATAL("size diffrent %lu %lu", split_vec.size(), _fields.size());
                DB_FATAL("ERRLINE:%s", line.c_str());
                continue;
            }
            ++cnt;
            if (_type == DEL) {
                sql = "delete from ";
                sql += _db + "." + _table + " where ";
                bool is_first = true;
                for (uint32_t i = 0; i < _fields.size(); i++) {
                    if (_ignore_indexes.count(i) == 1) {
                        continue;
                    }
                    if (!is_first) {
                        sql += " and ";
                    }
                    sql += _fields[i] + " = '" + _mysql_escape_string(split_vec[i]) + "'";
                    is_first = false;
                }
                query(sql);
            } else if (_type == UP || _type == DUP_UP || _type == REP) {
                int i = 0;
                insert_values += "(";
                for (auto& item : split_vec) {
                    if (_ignore_indexes.count(i++) == 0) { 
                        if (FLAGS_null_as_string || item != "NULL") {
                            insert_values += "'" + _mysql_escape_string(item) + "',";
                        } else {
                            insert_values +=  item + ",";
                        }
                    }
                }
                insert_values.pop_back();
                insert_values += "),";
            } else if (_type == SEL) {
                sql = "select * from ";
                sql += _db + "." + _table + " where ";
                bool is_first = true;
                for (uint32_t i = 0; i < _fields.size(); i++) {
                    if (_ignore_indexes.count(i) == 1) {
                        continue;
                    }
                    if (!is_first) {
                        sql += " and ";
                    }
                    sql += _fields[i] + " = '" + _mysql_escape_string(split_vec[i]) + "'";
                    is_first = false;
                    break;
                }
                query(sql);
            } else if (_type == XBS) {
                int i = 0;
                insert_values += "(" + reverse_bytes(split_vec[_imageid_idx]) + ",";
                for (auto& item : split_vec) {
                    if (_ignore_indexes.count(i++) == 0) { 
                        insert_values += "'" + _mysql_escape_string(item) + "',";
                    }
                }
                insert_values.pop_back();
                insert_values += "),";
            } else if (_type == XCUBE) {
                sql = "update ";
                sql += _db + "." + _table + " ";
                sql += " set " + _fields[1] + " = ";
                sql += "'" + _mysql_escape_string(split_vec[1]) + "'";
                sql += " where imageid_reverse = " + reverse_bytes(split_vec[0]);
                query(sql);
            }
        }
        if (cnt == 0) {
            return;
        }
        if (_type == UP || _type == REP || _type == XBS) {
            sql = FLAGS_insert_mod + " into ";
            sql += _db + "." + _table;
            if (_type == REP) {
                sql += "_tmp";
            }
            sql += "(";
            if (_type == XBS) {
                sql += "imageid_reverse,";
            }
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
            query(sql);
        } else if (_type == DUP_UP) {
            sql = "insert into ";
            sql += _db + "." + _table;
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
            i = 0;
            for (auto& field : _fields) {
                if (_ignore_indexes.count(i++) == 0) {
                    sql += field + "=values(" + field + "),";
                }
            }
            sql.pop_back();
            query(sql);
        }
        
    };
    concurrency_cond.increase_wait();
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run(calc);
}

void Importer::wattbase_calc_in_bthread(std::vector<std::string>& lines,
        BthreadCond& concurrency_cond) {
    auto calc = [this, lines, &concurrency_cond]() {
        auto decrease_func = [](BthreadCond* cond) {cond->decrease_signal();};
        std::shared_ptr<BthreadCond> auto_dec2(&concurrency_cond, decrease_func);
        std::map<std::string, std::string> level_insert_values;
        if (_level_table_map.size() == 0) {
            return;
        }
        int cnt = 0;
        for (auto& line : lines) {
            bool is_find = false;
            std::vector<std::string> split_vec;
            boost::split(split_vec, line, boost::is_any_of(_delim));
            for (auto iter = _level_table_map.begin(); iter != _level_table_map.end(); iter++) {
                if (split_vec[iter->second.filter_idx] != iter->second.filter_value) {
                    continue;
                }
                if (split_vec.size() != iter->second.fields.size()) {
                    DB_FATAL("size diffrent %lu %lu", split_vec.size(), iter->second.fields.size());
                    DB_FATAL("ERRLINE:%s", line.c_str());
                    continue;
                }
                ++cnt;
                int i = 0;
                std::string& insert_values = level_insert_values[iter->second.filter_value];
                insert_values += "(";
                for (auto& item : split_vec) {
                    if (iter->second.ignore_indexes.count(i++) == 0) { 
                        if (FLAGS_null_as_string || item != "NULL") {
                            insert_values += "'" + _mysql_escape_string(item) + "',";
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
                DB_WARNING("can`t find line:%s", line.c_str());
            }
        }
        if (cnt == 0) {
            return;
        }

        for (auto values_iter = level_insert_values.begin(); values_iter != level_insert_values.end(); values_iter++) {
            auto table_iter = _level_table_map.find(values_iter->first);
            if (table_iter == _level_table_map.end()) {
                DB_FATAL("can not find level:%lld", values_iter->first);
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
            query(sql);
        }

    };
    concurrency_cond.increase_wait();
    Bthread bth(&BTHREAD_ATTR_SMALL);
    bth.run(calc);
}


int Importer::query(std::string sql, bool is_retry) {
    TimeCost cost;
    int ret = 0;
    int retry = 0;
    int affected_rows = 0;
    do {
        baikal::client::ResultSet result_set;
        bthread_usleep(FLAGS_sleep_intervel_s * 1000 * 1000LL);
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            affected_rows = result_set.get_affected_rows();
            break;
        }   
        bthread_usleep(1000000);
    }
    while (++retry < 20);
    if (ret != 0) {
        DB_FATAL("sql_len:%d query fail : %s", sql.size(), sql.c_str());
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
        DB_TRACE("affected_rows:%d, cost:%ld, sql_len:%d, sql:%s", 
                affected_rows, cost.get_time(), sql.size(), sql.c_str());
    }
    ++_succ_cnt;
    return affected_rows;
}

std::string Importer::_mysql_escape_string(const std::string& value) {
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
    baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
    if (connection) {
        MYSQL* RES = connection->get_mysql_handle();
        mysql_real_escape_string(RES, str, value.c_str(), value.size());
        escape_value = str;
        connection->close();
    } else {
        LOG(WARNING) << "service fetch_connection() failed";
        delete[] str;
        return boost::replace_all_copy(value, "'", "\\'");
    }   
    delete[] str;
    return escape_value;
}
} // namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
