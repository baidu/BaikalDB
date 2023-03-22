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
#pragma once

#include <net/if.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <atomic>
#include <string>
#include <Configure.h>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include <json/json.h>
#include "baikal_client.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
#include "mut_table_key.h"
#include "file_executor.h"
#include <baidu/feed/mlarch/babylon/lite/iconv.h>
#include "capture_tool.h"
#include "flash_back.h"

namespace baikaldb {
namespace babylon = baidu::feed::mlarch::babylon;
DECLARE_string(insert_mod);
DECLARE_int32(atom_check_mode);
DECLARE_int64(atom_min_id);
DECLARE_int64(atom_max_id);
DECLARE_int64(atom_base_fields_cnt);
DECLARE_bool(select_first);
class ImporterFileSystemAdaptor;

enum OpType {
    DEL = 0,
    UP,
    SQL_UP,
    DUP_UP,
    SEL,
    SEL_PRE,
    REP,
    BASE_UP, //导入基准,基准内可能有多个表的数据，需要根据第一列的level值判断
    TEST,
    ATOM_IMPORT,    // atom 导入
    ATOM_CHECK,     // atom 校验
    BIGTTREE_DIFF,  // bigtree diff  
    TXN_UP,         // 事务模式导入 
    RECOVERY, 
    DIFF_LINE       // 打印diff lines
};

struct OpDesc {
    std::string name;
    OpType      type;
};

static std::vector<OpDesc> op_name_type = {
    {"delete",            DEL},
    {"update",            UP},
    {"sql_update",        SQL_UP},
    {"dup_key_update",    DUP_UP},
    {"select",            SEL},
    {"select_prepare",    SEL_PRE},
    {"replace",           REP},
    {"test",              TEST},
    {"atom_import",       ATOM_IMPORT},
    {"atom_check",        ATOM_CHECK},
    {"bigtree_diff",      BIGTTREE_DIFF},
    {"txn_up",            TXN_UP},
    {"recovery",          RECOVERY},
    {"diff_line",         DIFF_LINE},
};

struct ImportTableInfo{
    std::string db;
    std::string table;
    std::vector<std::string> fields;
    std::set<int> ignore_indexes;
    std::set<int> empty_as_null_indexes;
    std::string filter_field;
    std::string filter_value;
    int filter_idx;
};

struct FlashBackTableInfo{
    std::string db;
    std::string table;
    std::set<std::string> skip_fields;

    // 统计信息
    std::atomic<int64_t> success_cnt {0};
    std::atomic<int64_t> fail_cnt    {0};
    std::atomic<int64_t> legacy_cnt  {0};
};

struct FastImportTaskDesc {
    std::string charset;
    std::string baikaldb_resource;
    std::string emails;
    std::string delim;
    std::string meta_bns;
    std::string cluster_name;
    std::string user_name;
    std::string password;
    std::string db;
    std::string table;
    std::string file_path;
    std::string table_info;
    std::string table_namespace;
    std::string done_json;
    std::string conf;
    bool need_iconv = false;
    bool is_replace = false;
    bool null_as_string = false;
    bool has_header = false;
    int64_t old_version = 0;
    int64_t new_version = 0;
    int64_t id = 0;
    int64_t main_id = 0;
    int64_t start_pos = 0;
    int64_t end_pos = 0;
    int64_t ttl = 0;
    std::vector<std::string>  fields;
    std::set<int> ignore_indexes;
    std::set<int> empty_as_null_indexes;
    std::set<int> binary_indexes;
    std::map<std::string, std::string> const_map;
    FileType file_type = FileType::Text;

    void reset() {
        file_path.clear();
        meta_bns.clear();
        cluster_name.clear();
        user_name.clear();
        password.clear();
        db.clear();
        table.clear();
        conf.clear();
        id = 0;
        main_id = 0;
        start_pos = 0;
        end_pos = 0;
        ttl = 0;
        done_json.clear();
        charset.clear();
        baikaldb_resource.clear();
        fields.clear();
        ignore_indexes.clear();
        empty_as_null_indexes.clear();
        binary_indexes.clear();
        const_map.clear();
        file_type = FileType::Text;
    }
};

typedef std::vector<std::string> DataRow;
typedef std::vector<DataRow> RowsBatch;

class ImporterHandle {
const uint32_t MAX_FIELD_SIZE = 1024 * 1024;
public:
    ImporterHandle(OpType type, baikal::client::Service* db) : _type(type), _baikaldb(db), _done_path("") {
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
        _import_lines = 0;
        _import_ret.str("");
    }

    ImporterHandle(OpType type, baikal::client::Service* db, std::string done_path) : _type(type), _baikaldb(db), _done_path(done_path) {
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
        _import_lines = 0;
    }

    virtual ~ImporterHandle() {
        _err_fs.close();
        _err_fs_retry.close();
        boost::filesystem::remove_all(_err_name);
        if (_err_cnt_retry == 0) {
            boost::filesystem::remove_all(_err_name_retry);
        }
    }

    int init(const Json::Value& node, const std::string& path_prefix, FastImportTaskDesc& task);

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) {};

    virtual int close() { return 0; }

    int handle_files(const FieldsFunc& fileds_func, const SplitFunc& split_func, const ConvertFunc& convert_func, 
                     ImporterFileSystemAdaptor* fs, const std::string& config, const ProgressFunc& progress_func,
                     const FinishBlockFunc& finish_block_func);

    int64_t run(ImporterFileSystemAdaptor* fs, const std::string& config, const ProgressFunc& progress_func,
                const FinishBlockFunc& finish_block_func);

    int query(std::string sql, int quota, baikal::client::SmartConnection& connection, bool is_retry = false);

    int query(std::string sql, bool is_retry = false);

    int query_dm_baikaldb(std::string sql, baikal::client::ResultSet& result_set);
    
    void insert_err_sqls(const std::string& err_sql);

    int get_table_primary_key(std::set<std::string>& pk_fields);

    int rename_table(std::string old_name, std::string new_name);

    std::string _mysql_escape_string(baikal::client::SmartConnection& connection, const std::string& value);

    static ImporterHandle* new_handle(OpType type,
            baikal::client::Service* baikaldb,
            baikal::client::Service* backup_db,
            std::string done_path);
    
    static ImporterHandle* new_handle(OpType type,
            baikal::client::Service* baikaldb,
            std::string done_path) {
        return new_handle(type, baikaldb, nullptr, done_path);
    }

    static Json::Value get_node_json(const Json::Value& done_root) {
        for (auto& name_type : op_name_type) {
            if (done_root.isMember(name_type.name)) {
                if (done_root[name_type.name].isArray() && done_root[name_type.name].size() > 0) {
                    Json::Value node_tmp = done_root[name_type.name][0];
                    return node_tmp;
                }
            }
        }
        return {};
    }

    static bool is_launch_capture_task(const Json::Value& done_root) {
       try {
           for (auto& name_type : op_name_type) {
               if (done_root.isMember(name_type.name)) {
                   for (auto& node : done_root[name_type.name]) {
                       if (node.isMember("capture_task")) {
                           if (node["capture_task"].asBool()) {
                               return true;
                           }
                       }
                   }
               }
           } 
           return false;
       } catch (Json::LogicError& e) {
           DB_FATAL("fail parse what:%s", e.what());
           return false;
       }
    }

    std::string handle_result() {
        return _import_ret.str();
    }

    void set_need_iconv(bool need_iconv) {
        _need_iconv = need_iconv;
    }

    void set_charset(const std::string& charset) {
        _charset = charset;
    }

    void set_local_json(bool is_local_done_json) {
        _is_local_done_json = is_local_done_json;
    }

    void set_retry_times(int64_t retry_times) {
        _retry_times = retry_times;
    }

    int64_t get_import_lines() {
        return _import_lines.load();
    }

    int64_t get_import_diff_lines() {
        return _import_diff_lines.load();
    }
        
    int64_t get_import_affected_row() {
        return _import_affected_row.load();
    }

    int64_t get_err_sql_cnt() {
        return _err_cnt_retry.load();
    }

    std::string get_diffline_sample() {
        return _diff_line_sample;
    }

    std::string get_sql_err_reason() {
        return _sql_err_reason;
    }

    void set_upload_errsql_info(const std::string& table_info, 
                                int64_t version, 
                                baikal::client::Service* dm_baikaldb) {
        _table_info = table_info;
        _version = version;
        _dm_baikaldb = dm_baikaldb;
    }

    void set_finished_blocks(bool is_broken_point_continuing, 
                             std::unordered_map<std::string, std::map<int64_t, int64_t>>& finished_blocks) {
        _broken_point_continuing = is_broken_point_continuing;
        _finished_blocks = finished_blocks;
    }

    std::string get_sample_sqls() {
        return _sample_sql;
    }

    void set_task_config(const std::string& conf_str) {
        _task_config = conf_str;
    }

    std::string get_db() {
        return _db;
    }

    std::string get_table() {
        return _table;
    }

    std::string get_all_import_result(int64_t import_line_last_import, 
                                      int64_t affected_row_last_import, 
                                      int64_t diffline_last_import,
                                      int64_t err_sql_last_import) {
        return _db + "." + _table 
                + ": import_line[" + std::to_string(_import_lines.load() + import_line_last_import - _import_diff_lines.load()) 
                + "], affected_row[" + std::to_string(_import_affected_row.load() + affected_row_last_import)
                + "], diffline[" + std::to_string(_import_diff_lines.load() + diffline_last_import)
                + "], error_sql[" + std::to_string(_err_cnt_retry.load() + err_sql_last_import) + "]; ";  
    }

protected:
    virtual bool split(std::string& line, std::vector<std::string>& split_vec) {
        if (!convert(line)) {
            return false;
        }
        boost::split(split_vec, line, boost::is_any_of(_delim));
        return true;
    }

    virtual bool convert(std::string& line) {
        if (_need_iconv) {
            std::string new_line;
            if (_charset == "utf8") {
                if (0 != babylon::iconv_convert<babylon::Encoding::UTF8, 
                    babylon::Encoding::GB18030, babylon::IconvOnError::IGNORE>(new_line, line)) {
                    _import_lines++;
                    _import_diff_lines++;
                    return false;
                }
            } else {
                if (0 != babylon::iconv_convert<babylon::Encoding::GB18030, 
                    babylon::Encoding::UTF8, babylon::IconvOnError::IGNORE>(new_line, line)) {
                    _import_lines++;
                    _import_diff_lines++;
                    return false;
                }
            }
            std::swap(line, new_line);
        }
        return true;
    }

    void restore_line(const std::vector<std::string>& fields, std::string& line) {
        line = boost::join(fields, _delim);
    }

    std::string gen_insert_sql(baikal::client::SmartConnection& connection, const std::string& database_name, const std::string& table_name, 
                            const std::map<std::string, std::string>& values_map) {

        std::string sql = "INSERT IGNORE INTO " + database_name + "." + table_name;
        std::string names;
        std::string values;

        for (auto iter : values_map) {
            names += iter.first;
            names += ",";
            if (iter.second != "__NULL__") {
                values += "'" + _mysql_escape_string(connection, iter.second) + "'";
            }
            values += ",";
        }

        names.pop_back();
        values.pop_back();

        sql += " (" + names + ") VALUES (" + values + ")";

        return sql;
    }
    std::string gen_delete_sql(baikal::client::SmartConnection& connection, const std::string& database_name, const std::string& table_name, 
                            const std::map<std::string, std::string>& where_map) {

        std::string sql = "DELETE FROM " + database_name + "." + table_name;


        sql += " WHERE ";
        std::vector<std::string> where_vec;
        where_vec.reserve(5);
        for (auto iter : where_map) {
            if (iter.second == "__NULL__") {
                where_vec.push_back(iter.first + " is null");
            } else {
                where_vec.push_back(iter.first + "='" + _mysql_escape_string(connection, iter.second) + "'");
            }
        }
        sql += boost::join(where_vec, " AND ");

        return sql;
    }

    std::string gen_update_sql(baikal::client::SmartConnection& connection, const std::string& database_name, const std::string& table_name, 
                            const std::map<std::string, std::string>& set_map, const std::map<std::string, std::string>& where_map) {
        std::string sql = "UPDATE " + database_name + "." + table_name;
        sql += " SET ";
        std::vector<std::string> set_vec;
        set_vec.reserve(5);
        for (auto iter : set_map) {
            if (iter.second == "__NULL__") {
                set_vec.push_back(iter.first + "=NULL");
            } else {
                set_vec.push_back(iter.first + "='" + _mysql_escape_string(connection, iter.second) + "'");
            }
        }
        sql += boost::join(set_vec, ", ");
        sql += " WHERE ";
        std::vector<std::string> where_vec;
        where_vec.reserve(5);
        for (auto iter : where_map) {
            if (iter.second == "__NULL__") {
                where_vec.push_back(iter.first + " is null");
            } else {
                where_vec.push_back(iter.first + "='" + _mysql_escape_string(connection, iter.second) + "'");
            }
        }
        sql += boost::join(where_vec, " AND ");

        return sql;
    }

    // Construct
    OpType _type;
    baikal::client::Service* _baikaldb;
    std::string _done_path;
    ImporterFileSystemAdaptor* _fs { nullptr };

    // init
    std::string _db;
    std::string _table;
    std::string _quota_table;
    std::string _path;
    std::string _delim;
    std::vector<std::string>   _fields;
    std::map<std::string, int> _pk_fields;
    std::map<std::string, int> _dup_set_fields;
    std::string _other_condition;
    std::set<int> _ignore_indexes;
    std::map<std::string, std::string> _const_map;
    int64_t _ttl = 0;
    size_t _file_min_size = 0;
    size_t _file_max_size = 0;
    bool _has_header = false;
    FileType _file_type = FileType::Text;
    int _tps = 0;
    bool _need_generate_sql = true;
    std::string _sample_sql;

    bool _broken_point_continuing = false;
    // file_path -> start_pos -> end_pos
    std::unordered_map<std::string, std::map<int64_t, int64_t>> _finished_blocks;
    // for upload err sqls
    baikal::client::Service* _dm_baikaldb = nullptr;
    std::string _table_info;
    int64_t _version;

    std::string _err_name;
    std::string _err_name_retry;
    std::ofstream _err_fs;
    std::ofstream _err_fs_retry;
    std::string _emails;
    std::atomic<int64_t> _err_cnt_retry{0};
    std::atomic<int64_t> _err_cnt{0};
    std::atomic<int64_t> _succ_cnt{0};
    std::string _diff_line_sample;
    std::string _sql_err_reason;

    bool    _error = false;
    bool    _null_as_string = false;
    std::atomic<int64_t> _import_lines{0};
    std::atomic<int64_t> _import_diff_lines{0};
    std::atomic<int64_t> _import_affected_row{0};
    std::ostringstream   _import_ret;
    TimeCost _cost;
    bool    _need_iconv = false;
    std::string _charset;
    bool    _is_local_done_json = false;
    std::set<int> _empty_as_null_indexes;
    std::set<int> _binary_indexes;
    int64_t _max_failure_percent = 100;
    int64_t _retry_times = 0;
    std::string _task_config;
};

class TestOutFile {
public:
    TestOutFile(std::string file_name) : _file_name(file_name) {
        bthread_mutex_init(&_mutex, NULL);
        _out.open("./outdata/" + file_name, std::ofstream::out | std::ofstream::app);
    }

    ~TestOutFile() {
        DB_WARNING("file: %s, line_size:%d", _file_name.c_str(), cnt);
        _out.close();
        bthread_mutex_destroy(&_mutex);
    }

    void write(const std::vector<std::string>& lines) {
        BAIDU_SCOPED_LOCK(_mutex);
        for (auto& line : lines) {
            cnt++;
            std::string line_ = line + "\n";
            _out << line_;  
        }
        _out.flush();
    }
    void write(const std::string& line) {
        BAIDU_SCOPED_LOCK(_mutex);
        std::string line_ = line + "\n";
        _out << line_;  
        _out.flush();
    }

    void write(const RowsBatch& row_batch) {
        BAIDU_SCOPED_LOCK(_mutex);
        for (auto& row : row_batch) {
            std::string line_;
            for (auto& field : row) {
                line_ += field + "\t";
            }
            line_ += "\n";
            _out << line_;
        }
        _out.flush();
    }

    void write(const std::vector<std::vector<std::string>>& fields_vec, const std::string& delim) {
        BAIDU_SCOPED_LOCK(_mutex);
        for (const auto& fields : fields_vec) {
            cnt++;
            std::string line = boost::join(fields, delim);
            line += "\n";
            _out << line;  
        }
        _out.flush();
    }

private:
    int cnt = 0;
    std::string _file_name;
    bthread_mutex_t _mutex;
    std::ofstream _out;
};

class ImporterTestHandle : public ImporterHandle {
public:
    ImporterTestHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {
        bthread_mutex_init(&_mutex, NULL);
    }

    ImporterTestHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~ImporterTestHandle() {
        bthread_mutex_destroy(&_mutex);
    }

    virtual int init(const Json::Value& node, const std::string& path_prefix) {
        int ret = ImporterHandle::init(node, path_prefix);
        if (ret < 0) {
            return -1;
        }

        return 0;
    }
    
    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override {
        std::vector<std::string> split_vec;
        boost::split(split_vec, path, boost::is_any_of("/"));
        std::string file_name = split_vec.back();
        {
            BAIDU_SCOPED_LOCK(_mutex);
            auto it = _out_file.find(file_name);
            if (it == _out_file.end()) {
                auto ptr = std::make_shared<TestOutFile>(file_name);
                _out_file[file_name] = ptr;
            }
        }
        auto it = _out_file.find(file_name);
        if (it == _out_file.end()) {
            return;
        }
        if (it->second == nullptr) {
            return;
        }
        it->second->write(fields_vec, _delim);
    }

private:
    bthread_mutex_t _mutex;
    std::map<std::string, std::shared_ptr<TestOutFile>> _out_file;
};

class ImporterAtomHandle : public ImporterHandle {
public:
    ImporterAtomHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db), 
                                        _insert_fial_file("insert_fial_file") {
        bthread_mutex_init(&_mutex, NULL);
    }

    ImporterAtomHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path), 
                                        _insert_fial_file("insert_fial_file") {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~ImporterAtomHandle() {
        DB_WARNING("=======finshed====== ignore cnt: %ld, select cnt: %ld, succ cnt: %ld, fail cnt: %ld, diff cnt: %ld, max_id: %ld", 
            ignore_rows.load(), select_rows.load(), succ_rows.load(), fail_rows.load(), diff_rows.load(), max_id.load());
        bthread_mutex_destroy(&_mutex);
        
    }

    virtual int init(const Json::Value& node, const std::string& path_prefix) {
        int ret = ImporterHandle::init(node, path_prefix);
        if (ret < 0) {
            return -1;
        }

        return 0;
    }

    void handle_fields_insert(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) {
        std::string sql;
        std::string insert_values;
        int cnt = 0;
        if (print_log_interval.get_time() > 60 * 1000 * 1000) {
            print_log_interval.reset();
            DB_WARNING("ignore cnt: %ld, select cnt: %ld, succ cnt: %ld, fail cnt: %ld, diff cnt: %ld", 
                ignore_rows.load(), select_rows.load(), succ_rows.load(), fail_rows.load(), diff_rows.load());
        }
        std::map<uint64_t, std::string> id_literal_map;
        int ret = id_literal_need_insert(fields_vec, id_literal_map);
        if (ret == -1) {
            _insert_fial_file.write(fields_vec, _delim);
            fail_rows += fields_vec.size();
            return;
        } else if (ret == -2) {
            return;
        }
        baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
        ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
                connection->close();
            }  
        }));
        for (auto& id_literal : id_literal_map) {
            std::vector<std::string> split_vec;
            split_vec.reserve(2);
            split_vec.emplace_back(std::to_string(id_literal.first));
            split_vec.emplace_back(id_literal.second);
            cnt++;
            int i = 0;
            insert_values += "(";
            for (auto& item : split_vec) {
                if (_ignore_indexes.count(i++) == 0) { 
                    if (_null_as_string || item != "NULL") {
                        insert_values += "'" + _mysql_escape_string(connection, item) + "',";
                    } else {
                        insert_values +=  item + ",";
                    }
                }
            }
            insert_values.pop_back();
            insert_values += "),";
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

        sql.pop_back();
        sql += ") values ";
        insert_values.pop_back();
        sql += insert_values;
        ret = query(sql, false); 
        if (ret < 0) {
            _insert_fial_file.write(fields_vec, _delim);
            DB_TRACE("atom_insert fail sql:%s", sql.c_str());
            fail_rows += fields_vec.size();
        } else {
            succ_rows += cnt;
        }

    }

    void handle_fields_diff(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) {
        int cnt = 0;
        if (print_log_interval.get_time() > 60 * 1000 * 1000) {
            print_log_interval.reset();
            DB_WARNING("ignore cnt: %ld, select cnt: %ld, succ cnt: %ld, fail cnt: %ld, diff cnt: %ld", 
                ignore_rows.load(), select_rows.load(), succ_rows.load(), fail_rows.load(), diff_rows.load());
        }
        std::map<uint64_t, std::string> id_literal_map;
        int ret = id_literal_need_insert(fields_vec, id_literal_map);
        if (ret == -1) {
            _insert_fial_file.write(fields_vec, _delim);
            fail_rows += fields_vec.size();
            return;
        } else if (ret == -2) {
            return;
        }

        if (id_literal_map.empty()) {
            return;
        }
        cnt = id_literal_map.size();
        std::unique_ptr<char[]> tmp_buf(new char[1024]); 
        std::unique_ptr<char[]> literal_buf(new char[1024]);

        std::string select_id_sql = "select id, literal from " + _db + "." + 
                _table + " where id in ("; 
        std::string select_literal_sql = "select id, literal from " + _db + "." + 
                _table + " where literal in ("; 

        for (const auto& id_literal : id_literal_map) {
            select_id_sql.append(" ").append(std::to_string(id_literal.first)).append(",");
            _convert_literal_sql(id_literal.second, literal_buf.get());
            snprintf(tmp_buf.get(), 1024, "'%s',", literal_buf.get());
            select_literal_sql += tmp_buf.get();
        }

        select_id_sql[select_id_sql.size() -1] = ')';
        select_literal_sql[select_literal_sql.size() -1] = ')';

        std::unordered_map<int64_t, std::string> id_wordid_showword_map;
        std::unordered_map<std::string, int64_t> id_showword_wordid_map;
        std::unordered_map<int64_t, std::string> literal_wordid_showword_map;
        std::unordered_map<std::string, int64_t> literal_showword_wordid_map;

        ret = select_data_from_mysql(select_id_sql, id_wordid_showword_map, id_showword_wordid_map);
        if (ret != 0) {
            _insert_fial_file.write(fields_vec, _delim);
            fail_rows += fields_vec.size();
            return;
        }

        ret = select_data_from_mysql(select_literal_sql, literal_wordid_showword_map, literal_showword_wordid_map);
        if (ret != 0) {
            _insert_fial_file.write(fields_vec, _delim);
            fail_rows += fields_vec.size();
            return;
        }

        bool need_insert = false;
        std::string insert_sql = "replace into Atom.base_diff (literal, mysql_id, baikaldb_id_main_table, baikaldb_id_global_index) values ";
        for (const auto& id_literal : id_literal_map) {
            const auto& it = id_showword_wordid_map.find(id_literal.second);
            const auto& it2 = literal_showword_wordid_map.find(id_literal.second);
            uint64_t id = 0;
            uint64_t id2 = 0;
            if (it != id_showword_wordid_map.end()) {
                id = it->second;
            }

            if (it2 != literal_showword_wordid_map.end()) {
                id2 = it2->second;
            }

            if (id == id2 && id != 0) {
                continue;
            }
            need_insert = true;
            _convert_literal_sql(id_literal.second, literal_buf.get());
            snprintf(tmp_buf.get(), 1024, "('%s', %lu, %lu, %lu),", literal_buf.get(), id_literal.first, id, id2);
            insert_sql  += tmp_buf.get();

        }

        insert_sql.erase(insert_sql.size() - 1);

        if (!need_insert) {
            return;
        }

        ret = query(insert_sql, false); 
        if (ret < 0) {
            _insert_fial_file.write(fields_vec, _delim);
            DB_TRACE("atom_insert fail sql:%s", insert_sql.c_str());
            fail_rows += fields_vec.size();
        } else {
            succ_rows += cnt;
        }

    }

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override {
        if (FLAGS_insert_mod == "select") {
            handle_fields_diff(path, fields_vec);
        } else {
            handle_fields_insert(path, fields_vec);
        }
    }

    bool split(std::string& s, std::vector<std::string>& split_vec) override {
        std::vector<std::string> elems;
        boost::split(split_vec, s, boost::is_any_of(_delim));

        if (FLAGS_atom_base_fields_cnt == 2) {
            if (split_vec.size() == 2) {
                return true;
            } else if (split_vec.size() > 2) {
                elems.emplace_back(split_vec[0]);
                std::string tmp_str;
                for (int i = 1; i < split_vec.size(); i++) {
                    tmp_str += split_vec[i];
                    if (i < (split_vec.size() - 1)) {
                        tmp_str += _delim;
                    }
                }
                elems.emplace_back(tmp_str);
            } 
        } else if (FLAGS_atom_base_fields_cnt == 4) {
            if (split_vec.size() == 4) {
                elems.emplace_back(split_vec[1]);
                elems.emplace_back(split_vec[3]);
            } else if (split_vec.size() > 4) {
                elems.emplace_back(split_vec[1]);
                std::string tmp_str;
                for (int i = 3; i < split_vec.size(); i++) {
                    tmp_str += split_vec[i];
                    if (i < (split_vec.size() - 1)) {
                        tmp_str += _delim;
                    }
                }
                elems.emplace_back(tmp_str);
            } 
        }

        std::swap(elems, split_vec);
        return true;
    }

    bool convert(std::string& line) override {
        return true;
    }

private:
    void _convert_literal_sql(const std::string& literal, char *buf) {
        int32_t literallen = literal.length();
        int32_t i = 0, j = 0;

        while (i < literallen) {
            // special char in sql statements
            if ((literal[i] & 0x80) == 0) {
                // for english char
                if (literal[i] == '\'' || literal[i] == '\\') {
                    buf[j++] = '\\';
                }
            } else {
                buf[j++] = literal[i++];
            }

            if (i < literallen) {
                buf[j++] = literal[i++];
            }
        }

        buf[j] = 0;
    }

    int id_literal_need_insert(const std::vector<std::vector<std::string>>& fields_vec, std::map<uint64_t, std::string>& id_literal_map) {
        std::map<uint64_t, std::string> tmp_map;
        for (auto& fields : fields_vec) {
            if (fields.size() != 2) {
                diff_rows++;
                continue;
            }

            int64_t tmp_id = atoll(fields[0].c_str());
            if (max_id.load() < tmp_id) {
                max_id = tmp_id;
            }
            if (tmp_id < FLAGS_atom_min_id || tmp_id > FLAGS_atom_max_id) {
                ignore_rows++;
                continue;
            }
            tmp_map[tmp_id] = fields[1];
        }

        if (tmp_map.empty()) {
            return -2;
        }
        if (FLAGS_select_first) {
            std::string select_id_sql = "select id, literal from " + _db + "." + 
                _table + " where id in ("; 

            std::unordered_map<int64_t, std::string> tmp_wordid_showword_map;
            std::unordered_map<std::string, int64_t> tmp_showword_wordid_map;
            for (auto it : tmp_map) {
                select_id_sql.append(" ").append(std::to_string(it.first)).append(",");
            }

            select_id_sql[select_id_sql.size() -1] = ')';

            int ret = select_data_from_mysql(select_id_sql, tmp_wordid_showword_map, tmp_showword_wordid_map);
            if (ret < 0) {
                DB_TRACE("exec fail sql:%s", select_id_sql.c_str());
                return -1;
            }

            for (auto it : tmp_map) {
                auto it2 = tmp_wordid_showword_map.find(it.first);
                if (it2 == tmp_wordid_showword_map.end()) {
                    id_literal_map[it.first] = it.second;
                    DB_WARNING("con not found in baikaldb id: %ld", it.first);
                }
            }
            select_rows += id_literal_map.size();
        } else {
            id_literal_map = tmp_map;
        }

        if (id_literal_map.empty()) {
            return -2;
        }

        return 0;
    }

    int select_data_from_mysql(const std::string& select_sql,
            std::unordered_map<int64_t, std::string>& wordid_showword_map,
            std::unordered_map<std::string, int64_t>& showword_wordid_map) {
        int ret = 0; 
        int retry_times = 0;
        baikal::client::ResultSet result_set;
        do {
            ret = _baikaldb->query(0, select_sql, &result_set);
        } while (ret != 0  && retry_times++ <= 3);
        
        if (ret < 0) {
            DB_WARNING("ProxyMysqlService connection->execute failed. ret:%d", ret);
            return -1;
        }
        
        if (result_set.get_mysql_res() == NULL) {
            DB_WARNING("ProxyMysqlResultSet is null, connection->execute ret:%d", ret);
            return -1;
        }
        
        int columns = result_set.get_field_count();
        
        while (result_set.next()) {
            for (int col_idx = 0; col_idx < columns; col_idx++) {
                std::string showword;
                int64_t wordid;
                int ret = result_set.get_int64(0, &wordid);
                if (ret != 0) {
                    DB_WARNING("get wordid from result fail, ret:%d", ret);
                    return -1;
                }
                ret = result_set.get_string(1, & showword);
                if (ret != 0) {
                    DB_WARNING("get showword from result fail, ret:%d", ret);
                    return -1;
                }
                wordid_showword_map[wordid] = showword;
                showword_wordid_map[showword] = wordid;
            }
        }
        return 0;
    }

    TimeCost print_log_interval;
    bthread_mutex_t _mutex;
    TestOutFile _insert_fial_file;
    std::atomic<int64_t> max_id = {0};
    std::atomic<int64_t> diff_rows = {0};
    std::atomic<int64_t> succ_rows = {0};
    std::atomic<int64_t> fail_rows = {0};
    std::atomic<int64_t> ignore_rows = {0};
    std::atomic<int64_t> select_rows = {0};
};

enum class OptType {
    SELECT,
    INSERT,
    INSERT_COUNT,
    UPDATE_COUNT,
    DELETE,
    DELETE_COUNT,
    DELETE_IDEA,
    DELETE_IDEA_COUNT,
    DELATE_WORD,
    DELATE_WORD_COUNT
};

class ImporterBigtreeDiffHandle : public ImporterHandle {
public:
    ImporterBigtreeDiffHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db),
                                _true_diff_data_file("_true_diff_data_file"),
                                _baikaldb_diff_sql_file("_baikaldb_diff_sql_file"),
                                _baikaldb_count_diff_sql_file("_baikaldb_count_diff_sql_file"),
                                _mysql_count_diff_sql_file("_mysql_count_diff_sql_file") {
        bthread_mutex_init(&_mutex, NULL);
    }
    ImporterBigtreeDiffHandle(OpType type,
                    baikal::client::Service* db,
                    baikal::client::Service* backup_db,
                    std::string done_path) : ImporterHandle(type, db, done_path),
                        _true_diff_data_file("_true_diff_data_file"),
                        _baikaldb_diff_sql_file("_baikaldb_diff_sql_file"),
                        _baikaldb_count_diff_sql_file("_baikaldb_count_diff_sql_file"),
                        _mysql_count_diff_sql_file("_mysql_count_diff_sql_file"),
                        _backup_db(backup_db)
                     {
        bthread_mutex_init(&_mutex, NULL);
    }
    ~ImporterBigtreeDiffHandle() {
        DB_NOTICE("=======finish========= process cnt: %ld  diff cnt: %ld", 
                    process_rows.load(), diff_rows.load());
        bthread_mutex_destroy(&_mutex);
    }

    virtual int init(const Json::Value& node, const std::string& path_prefix) override;

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

    int db_query(baikal::client::Service* db, std::vector<std::string>& fields,
            std::string& sql, RowsBatch& row_batch);
    int db_query(baikal::client::Service* db, const std::string& field, std::string& sql, int64_t& count);
    int count_diff_check(std::string& baikal_sql, std::string& baikal_count_sql,
            std::string& f1_sql, int64_t& count);

    int fields_diff_check(std::string& table, std::string& baikal_sql, std::string& f1_sql);

    void gen_sql(const std::string& table_name, RowsBatch& row_batch, OptType type, std::string& sql);
    void gen_new_rows(const RowsBatch& old_batch, const RowsBatch& select_rows, RowsBatch& new_rows);
    void gen_count_rows(const std::string& table_name, const RowsBatch& select_rows,
            std::map<int, RowsBatch>& count_rows_map);

    int do_insert(const std::string& table_name, RowsBatch& old_row_batch);

    int do_delete(const std::string& table_name, RowsBatch& old_row_batch);

    int do_begin(baikal::client::SmartConnection& conn);
    int do_commit(baikal::client::SmartConnection& conn);
    int do_rollback(baikal::client::SmartConnection& conn);

    bool split(std::string& s, std::vector<std::string>& split_vec) override {
        boost::split(split_vec, s, boost::is_any_of(_delim));
        return true;
    }

    bool convert(std::string& line) override {
        return true;
    }

private:
    bthread_mutex_t _mutex;
    TimeCost print_log_interval;
    TestOutFile _true_diff_data_file;
    TestOutFile _baikaldb_diff_sql_file;
    TestOutFile _baikaldb_count_diff_sql_file;
    TestOutFile _mysql_count_diff_sql_file;
    baikal::client::Service* _backup_db;
    std::atomic<int64_t> diff_rows = {0};
    std::atomic<int64_t> process_rows = {0};
    std::map<std::string, std::set<std::string>> processed_userid;
};

class ImporterAtomCheckHandle : public ImporterHandle {
public:
    ImporterAtomCheckHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db), 
                                        _id_fial_file("id_fail_file"), _literal_fial_file("literal_fial_file"), 
                                        _id_notfound_file("id_notfound_file"), _literal_notfound_file("literal_notfound_file")  {
        bthread_mutex_init(&_mutex, NULL);
    }

    ImporterAtomCheckHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path), 
                                        _id_fial_file("id_fail_file"), _literal_fial_file("literal_fial_file"), 
                                        _id_notfound_file("id_notfound_file"), _literal_notfound_file("literal_notfound_file")  {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~ImporterAtomCheckHandle() {
        DB_WARNING("=======finish========= id literal succ cnt: %ld vs %ld, fail cnt: %ld vs %ld, not found cnt: %ld vs %ld", 
                        id_succ_rows.load(), literal_succ_rows.load(), id_fail_rows.load(), literal_fail_rows.load(), 
                        id_notfound_rows.load(), literal_notfound_rows.load());
        bthread_mutex_destroy(&_mutex);

    }

    virtual int init(const Json::Value& node, const std::string& path_prefix) {
        int ret = ImporterHandle::init(node, path_prefix);
        if (ret < 0) {
            return -1;
        }

        return 0;
    }
   
    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override {
        std::string sql;
        std::string insert_values;
        int cnt = 0;

        std::string select_id_sql = "select id, literal from " + _db + "." + 
            _table + " where id in ("; 
        std::string select_literal_sql = "select id, literal from " + _db + "." + 
            _table + " where literal in ("; 

        std::unordered_map<int64_t, std::string> tmp_wordid_showword_map;
        std::unordered_map<std::string, int64_t> tmp_showword_wordid_map;
        baikal::client::SmartConnection connection = _baikaldb->fetch_connection();
        ON_SCOPE_EXIT(([&connection]() {
        if (connection) {
                connection->close();
            }  
        }));
        for (auto& fields : fields_vec) {
            if (fields_vec.size() != 2) {
                diff_rows++;
                continue;
            }
            int64_t wordid = atoll(fields[0].c_str());
            std::string showword = fields[1];
            tmp_wordid_showword_map[wordid] = showword;
            tmp_showword_wordid_map[showword] = wordid;
            select_id_sql.append(" ").append(fields[0]).append(",");
            select_literal_sql.append(" \"").append(_mysql_escape_string(connection, fields[1])).append("\",");
            ++cnt;
        }

        select_id_sql[select_id_sql.size() -1] = ')';
        select_literal_sql[select_literal_sql.size() -1] = ')';
        if (cnt == 0) {
            return;
        }

        std::vector<std::string> id_not_found_lines;
        id_not_found_lines.reserve(fields_vec.size());
        std::vector<std::string> literal_not_found_lines;
        literal_not_found_lines.reserve(fields_vec.size());
        std::unordered_map<int64_t, std::string> wordid_showword_map;
        std::unordered_map<std::string, int64_t> showword_wordid_map;
        int ret = select_data_from_mysql(select_id_sql, wordid_showword_map, showword_wordid_map);
        if (ret < 0) {
            _id_fial_file.write(fields_vec, _delim);
            id_fail_rows += cnt;
        } else {
            id_succ_rows += cnt;
            for (auto it : tmp_wordid_showword_map) {
                if (wordid_showword_map.count(it.first) <= 0) {
                    std::string tmp_line = std::to_string(it.first);
                    tmp_line.append(_delim).append(it.second);
                    id_not_found_lines.emplace_back(tmp_line);
                    DB_WARNING("id not found:%s", tmp_line.c_str());
                }
            }
        }

        wordid_showword_map.clear();
        showword_wordid_map.clear();
        ret = select_data_from_mysql(select_literal_sql, wordid_showword_map, showword_wordid_map);
        if (ret < 0) {
            _literal_fial_file.write(fields_vec, _delim);
            literal_fail_rows += cnt;
        } else {
            literal_succ_rows += cnt;
            for (auto it : tmp_showword_wordid_map) {
                if (showword_wordid_map.count(it.first) <= 0) {
                    std::string tmp_line = std::to_string(it.second);
                    tmp_line.append(_delim).append(it.first);
                    literal_not_found_lines.emplace_back(tmp_line);
                    DB_WARNING("literal not found:%s", tmp_line.c_str());
                }
            }
        }

        if (!id_not_found_lines.empty()) {
            _id_notfound_file.write(id_not_found_lines); 
            id_notfound_rows += id_not_found_lines.size(); 
        }

        if (!literal_not_found_lines.empty()) {
            _literal_notfound_file.write(literal_not_found_lines); 
            literal_notfound_rows += literal_not_found_lines.size(); 
        }

        if (print_log_interval.get_time() > 60 * 1000 * 1000) {
            print_log_interval.reset();
            DB_WARNING("id literal succ cnt: %ld vs %ld, fail cnt: %ld vs %ld, not found cnt: %ld vs %ld", 
                        id_succ_rows.load(), literal_succ_rows.load(), id_fail_rows.load(), literal_fail_rows.load(), 
                        id_notfound_rows.load(), literal_notfound_rows.load());
        }

    }

    bool split(std::string& s, std::vector<std::string>& split_vec) override {
        size_t len = s.length();
        size_t delim_len = _delim.length();
        if (delim_len == 0) return true;
        auto find_pos = s.find(_delim);
        if (find_pos != s.npos) {
            split_vec.emplace_back(s.substr(0, find_pos));
            split_vec.emplace_back(s.substr(find_pos + delim_len, len - find_pos - delim_len));
        } 
        return true;
    }

    bool convert(std::string& line) override {
        return true;
    }

private:
    int select_data_from_mysql(const std::string& select_sql,
            std::unordered_map<int64_t, std::string>& wordid_showword_map,
            std::unordered_map<std::string, int64_t>& showword_wordid_map) {
        int ret = 0; 
        int retry_times = 0;
        baikal::client::ResultSet result_set;
        do {
            ret = _baikaldb->query(0, select_sql, &result_set);
        } while (ret != 0  && retry_times++ <= 3);
        
        if (ret < 0) {
            DB_WARNING("ProxyMysqlService connection->execute failed. ret:%d", ret);
            return -1;
        }
        
        if (result_set.get_mysql_res() == NULL) {
            DB_WARNING("ProxyMysqlResultSet is null, connection->execute ret:%d", ret);
            return -1;
        }
        
        int columns = result_set.get_field_count();
        
        while (result_set.next()) {
            for (int col_idx = 0; col_idx < columns; col_idx++) {
                std::string showword;
                int64_t wordid;
                int ret = result_set.get_int64(0, &wordid);
                if (ret != 0) {
                    DB_WARNING("get wordid from result fail, ret:%d", ret);
                    return -1;
                }
                ret = result_set.get_string(1, & showword);
                if (ret != 0) {
                    DB_WARNING("get showword from result fail, ret:%d", ret);
                    return -1;
                }
                wordid_showword_map[wordid] = showword;
                showword_wordid_map[showword] = wordid;
            }
        }
        return 0;
    }

    bthread_mutex_t _mutex;
    TimeCost print_log_interval;
    TestOutFile _id_fial_file;
    TestOutFile _literal_fial_file;
    TestOutFile _id_notfound_file;
    TestOutFile _literal_notfound_file;
    std::atomic<int64_t> diff_rows = {0};
    std::atomic<int64_t> id_succ_rows = {0};
    std::atomic<int64_t> literal_succ_rows = {0};
    std::atomic<int64_t> id_fail_rows = {0};
    std::atomic<int64_t> literal_fail_rows = {0};
    std::atomic<int64_t> id_notfound_rows = {0};
    std::atomic<int64_t> literal_notfound_rows = {0};
};

class ImporterRepHandle : public ImporterHandle {
public:
    ImporterRepHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}
    
    ImporterRepHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

    virtual int close();

};

class ImporterUpHandle : public ImporterHandle {
public:
    ImporterUpHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}
    
    ImporterUpHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

};

class ImporterTxnUpHandle : public ImporterHandle {
public:
    ImporterTxnUpHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}
    
    ImporterTxnUpHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

    void run_txn(const std::vector<std::string>& sqls);
};


class ImporterSelHandle : public ImporterHandle {
public:
    ImporterSelHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}

    ImporterSelHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

};

class ImporterSelpreHandle : public ImporterHandle {
public:
    ImporterSelpreHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}

    ImporterSelpreHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

};

class ImporterDelHandle : public ImporterHandle {
public:
    ImporterDelHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {} 

    ImporterDelHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

};

class ImporterSqlupHandle : public ImporterHandle {
public:
    ImporterSqlupHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}

    ImporterSqlupHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

};

class ImporterDupupHandle : public ImporterHandle {
public:
    ImporterDupupHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}

    ImporterDupupHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

};

class ImporterBaseupHandle : public ImporterHandle {
public:
    ImporterBaseupHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}

    ImporterBaseupHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;

private:
    std::map<std::string, ImportTableInfo> _level_table_map;
};

class ImporterRecoveryHandle : public ImporterHandle {
public:
    ImporterRecoveryHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {
    }

    ImporterRecoveryHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {
    }

    ~ImporterRecoveryHandle() {
        
    }

    virtual int init(const Json::Value& node, const std::string& path_prefix);
    virtual int close();
    virtual void handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields_vec, BlockHandleResult* result) override;

    std::shared_ptr<FBOutFile> output_file(const std::string& file_name);   


private:
    bthread::Mutex _mutex;
    std::string _output_path;
    std::map<std::string, std::shared_ptr<FBOutFile>> _output_files;
    std::atomic<int64_t> _out_cnt {0};
    std::map<std::string, FlashBackTableInfo> _table_fields_info;
    std::string _recovery_type;
    std::string _next_recovery_type;
    std::set<std::string> _skip_fields;
};
class ImporterDiffLineHandle : public ImporterHandle {
public:
    ImporterDiffLineHandle(OpType type, baikal::client::Service* db) : ImporterHandle(type, db) {}

    ImporterDiffLineHandle(OpType type, baikal::client::Service* db, std::string done_path) : ImporterHandle(type, db, done_path) {}

    virtual int init(const Json::Value& node, const std::string& path_prefix);

    virtual void handle_fields(const std::string& path, 
                               const std::vector<std::vector<std::string>>& fields_vec,
                               BlockHandleResult* result) override;
};
}
