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
#include "common.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
#include "mut_table_key.h"

namespace baikaldb { 
enum OpType {
    DEL = 0,
    UP,
    DUP_UP,
    SEL,
    REP,
    XBS,
    XCUBE,
    BASE_UP //导入基准,基准内可能有多个表的数据，需要根据第一列的level值判断
};
struct ImportTableInfo{
    std::string db;
    std::string table;
    std::vector<std::string> fields;
    std::set<int> ignore_indexes;
    std::string filter_field;
    std::string filter_value;
    int filter_idx;
};
class Importer {
const uint32_t MAX_FIELD_SIZE = 1024 * 1024;
public:
    ~Importer();
    int init(baikal::client::Service* baikaldb_gbk, baikal::client::Service* baikaldb_utf8);
    int rename_table(std::string old_name, std::string new_name);
    int all_block_count(std::string path);
    void recurse_handle(BthreadCond& file_concurrency_cond, std::string path);
    int handle(const Json::Value& node, OpType type, std::string hdfs_mnt, std::string done_path, std::string charset);
    int query(std::string sql, bool is_retry = false);
    std::string _mysql_escape_string(const std::string& value);
    int64_t get_import_lines() {
        return _import_lines.load();
    }
private:
    void _calc_in_bthread(std::vector<std::string>& lines, 
            BthreadCond& concurrency_cond) {
        if (lines.size() == 0) {
            return;
        }
        if (_type == BASE_UP) {
            wattbase_calc_in_bthread(lines, concurrency_cond);
        } else {
            other_calc_in_bthread(lines, concurrency_cond);
        }
    }
    void other_calc_in_bthread(std::vector<std::string>& lines, 
        BthreadCond& concurrency_cond);
    void wattbase_calc_in_bthread(std::vector<std::string>& lines, 
        BthreadCond& concurrency_cond);
private:
    std::string _db;
    std::string _table;
    std::string _path;
    std::string _delim;
    OpType _type;
    std::vector<std::string> _fields;
    int _imageid_idx = 0;
    std::set<int> _ignore_indexes;
    baikal::client::Service* _baikaldb;
    baikal::client::Service* _baikaldb_gbk;
    baikal::client::Service* _baikaldb_utf8;
    std::string _err_name;
    std::string _err_name_retry;
    std::ofstream _err_fs;
    std::ofstream _err_fs_retry;
    int64_t _err_cnt_retry = 0;
    int64_t _err_cnt = 0;
    int64_t _succ_cnt = 0;
    std::atomic<int64_t> _import_lines;
    bool _error = false;
    int _all_block_count = 0;
    std::atomic<int> _handled_block_count;
    TimeCost _cost;
    std::map<std::string, ImportTableInfo> _level_table_map;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
