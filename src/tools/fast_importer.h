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
#include <string>
#include <Configure.h>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include <json/json.h>
#include "baikal_client.h"
#include "common.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
#include "mut_table_key.h"
#include "importer_handle.h"
#include "backup_import.h"
#include "backup_tool.h"
#include "sst_file_writer.h"

namespace baikaldb { 
DECLARE_string(fast_importer_db);
DECLARE_string(fast_importer_tbl);
DECLARE_string(resource_tag);
DECLARE_string(hostname);
DECLARE_int64(fast_importer_send_sst_concurrency);
class FastImporterImpl;
class FastImporterCtrl {
public:
    FastImporterCtrl(baikal::client::Service* baikaldb,
                     const std::map<std::string, baikal::client::Service*>& baikaldb_map,
                     baikal::client::Service* baikaldb_user = nullptr) :
                     _task_baikaldb(baikaldb), _baikaldb_map(baikaldb_map), _user_baikaldb(baikaldb_user) {
    }

    ~FastImporterCtrl() { };

    int parse_done();
    int init(const FastImportTaskDesc& task_desc);
    int query_task_baikaldb(std::string sql, baikal::client::ResultSet &result_set);

    int handle_restart();
    int fetch_task();
    int update_local_task(const std::map<std::string, std::string>& set_map);
    int has_gen_subtasks(bool& has_gened);
    int gen_subtasks();
    int check_subtasks(int& todo_cnt, int& doing_cnt, int& success_cnt, int& fail_cnt);
    int select_new_task(baikal::client::ResultSet& result_set);
    int update_task_doing();
    int rename_table(std::string old_name, std::string new_name);
    int query(const std::string& sql);
    int get_table_schema();
    int update_cluster_params(bool load_finish);
    int wait_cluster_rocksdb_normal();
    int get_store_rocks_statistic(const std::string& store, uint64_t& level0_sst, uint64_t& compaction_size,
                                   uint64_t& slowdown_write_sst_cnt, uint64_t& stop_write_sst_cnt,
                                   uint64_t& rocks_soft_pending_compaction_g, uint64_t& rocks_hard_pending_compaction_g);
    int run_main_task();
    int run_sub_task();
    int import_to_baikaldb(FastImporterImpl& fast_importer, const FieldsFunc& fields_func,
                           const SplitFunc& split_func, const ConvertFunc& convert_func);

    static void shutdown() {
        _shutdown = true;
    }

    static bool is_shutdown() {
        return _shutdown;
    }

    int64_t get_import_line() {
        return _import_line;
    }

    int64_t get_import_diff_line() {
        return _import_diff_line;
    }

    std::string get_result() {
        return _result.str();
    }

private:
    //字符串需要用单引号''
    std::string gen_select_sql(const std::string& table_name,
                               const std::vector<std::string>& select_vec,
                               const std::map<std::string, std::string>& where_map) {
        return _gen_select_sql(FLAGS_fast_importer_db, table_name, select_vec, where_map);
    }

    std::string gen_update_sql(const std::string& table_name,
                               const std::map<std::string, std::string>& set_map,
                               const std::map<std::string, std::string>& where_map) {
        return _gen_update_sql(FLAGS_fast_importer_db, table_name, set_map, where_map);
    }

    void reset() {
        _task.reset();
        _sub_file_paths.clear();
        _file_path_prefix.clear();
        _result.clear();
        _stores.clear();
    }

    bool _is_main_task;
    int64_t _import_line = 0;
    int64_t _import_diff_line = 0;
    std::vector<std::string> _sub_file_paths;
    std::string _file_path_prefix;
    FastImportTaskDesc _task;
    std::set<std::string> _stores;
    // 表和region信息
    pb::SchemaInfo _schema_info;
    ::google::protobuf::RepeatedPtrField< ::baikaldb::pb::RegionInfo > _region_infos;

    baikal::client::Service* _task_baikaldb;
    baikal::client::Service* _user_baikaldb;
    const std::map<std::string, baikal::client::Service*>& _baikaldb_map;
    MetaServerInteract       _meta_interact;

    static bool _shutdown;
    std::ostringstream _result;

    // 集群rocksdb参数
    uint64_t _slowdown_write_sst_cnt = 0;
    uint64_t _stop_write_sst_cnt = 0;
    uint64_t _rocks_soft_pending_compaction_g = 0;
    uint64_t _rocks_hard_pending_compaction_g = 0;
};

// 暂时不用如果写sst太慢，考虑send buf
// class SSTFileReader : public FileReader {
// public:
//     SSTFileReader(int64_t region_id) : _region_id(region_id) { }
//     virtual ~SSTFileReader() { }
//     virtual int open() override { return 0; }
//     // 内部缓存已经读取的buf，加速重复读
//     virtual int64_t read(int64_t pos, char** buf, bool* eof) override {
//         BAIDU_SCOPED_LOCK(_lock);
//         return 0;
//     }

//     virtual void close() override {
//         BAIDU_SCOPED_LOCK(_lock);
//         _pos_buf_map.clear();
//     }
//     }
// private:
// class BufMgr {
// public:
//     BufMgr() : _buf_real_size(0), _buf_capacity(BUF_SIZE), _buf(new char[_buf_capacity]) {}
//     ~BufMgr() {
//         if (_buf != nullptr) {
//             delete[] _buf;
//             _buf = nullptr;
//         }
//     }

//     int64_t size() const { return _buf_real_size; }
//     void set_size(int64_t size) { _buf_real_size = size; }
//     int64_t capacity() const { return _buf_capacity; }
//     char* buf() { return _buf; }
// private:
//     int64_t _buf_real_size = 0;
//     int64_t _buf_capacity  = 0;
//     char*   _buf           = nullptr;
//     DISALLOW_COPY_AND_ASSIGN(BufMgr); //不允许copy，避免来回拷贝内存难以理解
// };
// // std::make_shared<RowBatch>();
//     int64_t _region_id;
//     bthread::Mutex _lock;
//     std::map<int64_t, std::shared_ptr<BufMgr>> _pos_buf_map; // 位置偏移与buf的映射关系
// };

class SSTsender {
public:
    SSTsender(int64_t table_id, int64_t region_id, const std::set<std::string>& peers, int64_t index_size) :
        _table_id(table_id), _region_id(region_id), _peers(peers), _index_size(index_size) {
        _path = "./sst_file_region_" + std::to_string(region_id);
    }
    
    ~SSTsender() {
        butil::DeleteFile(butil::FilePath(_path), false); 
    }

    int write_sst_file();

    void send_concurrency(const std::set<std::string>& peers, std::unordered_map<std::string, int>& peer_ret_map);

    int get_peers(std::set<std::string>& peers, std::set<std::string>& unstable_followers);
    int check_peers(std::set<std::string>& need_send_peers, std::set<std::string>& unstable_followers);
    int send(bool retry_no_limit);
    int64_t _index_size = 1;
    int64_t _row_size = 0;         // region num_table_line需要add的数 = _rocksdb_kv_count / _index_size
    int64_t _rocksdb_kv_count = 0; // 发送出去的sst kv数
private:
struct SendStatus {
    enum Status {
        SEND_INIT = 0,
        SEND_SUCCESS,
        SEND_FAIL
    };
    int retry_times = 0;
    Status status = SEND_INIT;
};
    int64_t _table_id;
    int64_t _region_id;
    std::set<std::string> _peers;
    std::map<std::string, SendStatus> _peer_send_status_map;
    std::string _path;
};

class FastImporterImpl {
public:
    FastImporterImpl() {};

    FastImporterImpl(const FastImportTaskDesc& task, baikal::client::Service* baikaldb) : _task(task), _baikaldb(baikaldb) {};

    ~FastImporterImpl() {};

    int init(pb::SchemaInfo& schema_info,
             ::google::protobuf::RepeatedPtrField< ::baikaldb::pb::RegionInfo >& region_infos);

    bool split(std::string& line, std::vector<std::string>& split_vec) {
        if (!convert(line)) {
            return false;
        }
        boost::split(split_vec, line, boost::is_any_of(_task.delim));
        return true;
    }

    bool convert(std::string& line) {
        if (_task.need_iconv) {
            std::string new_line;
            if (_task.charset == "utf8") {
                if (0 != babylon::iconv_convert<babylon::Encoding::UTF8, 
                    babylon::Encoding::GB18030, babylon::IconvOnError::IGNORE>(new_line, line)) {
                    _import_diff_lines++;
                    return false;
                }
            } else {
                if (0 != babylon::iconv_convert<babylon::Encoding::GB18030, 
                    babylon::Encoding::UTF8, babylon::IconvOnError::IGNORE>(new_line, line)) {
                    _import_diff_lines++;
                    return false;
                }
            }
            std::swap(line, new_line);
        }
        return true;
    }

    void restore_line(const std::vector<std::string>& fields, std::string& line) {
        line = boost::join(fields, _task.delim);
    }

    // 主任务不调用该函数，子任务会多次调用直至结束
    void handle_fields(const std::string& path, const std::vector<std::vector<std::string>>& fields);

    int put_secondary(int64_t region,
                      IndexInfo& index,
                      SmartRecord record,
                      std::vector<std::string>& keys,
                      std::vector<std::string>& values);

    int put_primary(int64_t region,
                    IndexInfo& pk_index,
                    SmartRecord record,
                    std::vector<std::string>& keys,
                    std::vector<std::string>& values);

    int close() {
        _closed = true;
        _multi_thread_cond.wait();
        return 0;
    }

    void start_db_statistics();
    void progress_dialog(std::string progressing);
    int complate();

    std::string result() {return "";}
    int64_t send_failed_sst_count() { return _send_sst_fail_cnt.load(); }

    bool need_redo_task() {
        return _need_redo_task;
    }

    void set_handle_files_failed() {
        _handle_files_fail = true;
    }

    int reset_region_peers_from_meta();

    bool get_diff_lines_fail() {
        return _diff_lines_fail;
    }

private:
    FastImportTaskDesc                          _task;

    pb::SchemaInfo                              _schema_info;
    SmartIndex                                  _pk_index;
    SmartTable                                  _table_info;
    std::vector<SmartIndex>                     _indexes;        // 表所有的索引信息
    std::vector<FieldInfo*>                     _field_infos;    // done文件的列顺序
    std::unordered_map<std::string, FieldInfo*> _const_field_infos; // const_fields
    bool                                        _use_ttl = false;
    std::unordered_map<int64_t, std::set<std::string>>    _region_peers_map;
    std::unordered_map<int64_t, int64_t>        _region_index_map;
    // 并发控制
    bool                                        _closed = false;
    BthreadCond                                 _writing_cond;
    BthreadCond                                 _compacting_cond;
    BthreadCond                                 _multi_thread_cond;
    // 统计
    std::atomic<int64_t>                        _import_lines{0};
    std::atomic<int64_t>                        _import_diff_lines{0};
    std::atomic<int64_t>                        _compaction_times{0};
    std::atomic<int64_t>                        _send_sst_cnt{0};
    std::atomic<int64_t>                        _send_sst_fail_cnt{0};
    std::atomic<int64_t>                        _send_sst_lines_cnt{0};

    baikal::client::Service*                    _baikaldb;
    int64_t                                     _total_compaction_cost = 0;
    int64_t                                     _total_send_sst_cost = 0;
    TimeCost                                    _total_cost;

    bthread::Mutex                              _failed_region_ids_mutex;
    std::set<int64_t>                           _failed_region_ids;

    bool                                        _need_redo_task = false;
    bool                                        _handle_files_fail = false;
    bool                                        _diff_lines_fail = false;
};

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
