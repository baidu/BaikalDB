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

#include <limits>
#include <vector>
#include <queue>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include "google/protobuf/arena.h"
#include "proto/event.pb.h"
#include "common.h"
#include "schema_factory.h"
#include "store_interact.hpp"
#include "table_record.h"
#include "expr_value.h"
#if BAIDU_INTERNAL
#include <json/json.h>
#endif
#include "baikal_heartbeat.h"
#include "mut_table_key.h"

namespace baikaldb {

DECLARE_string(meta_server_bns);

struct EventDeleter {
    void operator() (mysub::Event* p) const {
    }
};

enum CaptureStatus {
    CS_SUCCESS,
    CS_EMPTY,
    CS_FAIL,
    CS_TIMEOUT,
    CS_LESS_THEN_OLDEST
};
void print_event(int64_t commit_ts, const std::shared_ptr<mysub::Event>& event);
using StoreReqPtr = std::shared_ptr<baikaldb::pb::StoreReq>;
struct StoreReqWithCommit {
    int64_t commit_ts;
    StoreReqPtr req_ptr;
    StoreReqWithCommit(int64_t ts, StoreReqPtr ptr) : commit_ts(ts), req_ptr(ptr) {}
    bool operator<(const StoreReqWithCommit& rhs) const {
        return commit_ts >= rhs.commit_ts;
    }
};

using BinLogPriorityQueue = std::priority_queue<StoreReqWithCommit>;

struct TwoWaySync {
    TwoWaySync(std::string name) : two_way_sync_table_name(name) {}
    std::string two_way_sync_table_name;
};

struct CapturerSQLInfo {
    std::string database;
    std::string table;
    int64_t txn_id = 0;
    uint64_t sign = 0;
    std::string user_name;
    std::string user_ip;
    std::string sql;
};

class CapturerSingleton {
public:
    int init(const std::string& namespace_, const std::map<std::string, SubTableNames>& table_infos);
    static CapturerSingleton* get_instance() {
        static CapturerSingleton instance;
        return &instance;
    }
private:
    CapturerSingleton() {}
    bthread::Mutex _lock;
    bool _is_init = false;
};

class Capturer {
public:
#if BAIDU_INTERNAL
    int init(Json::Value& value);
#endif
    explicit Capturer(bool flash_back_read = false) : _flash_back_read(flash_back_read) {}
    int init();
    int64_t get_offset(uint32_t timestamp);
    uint32_t get_timestamp(int64_t offset);
	CaptureStatus subscribe(std::vector<std::shared_ptr<mysub::Event>>& event_vec, 
            std::vector<std::shared_ptr<CapturerSQLInfo>>& sqlinfo_vec,
            int64_t& commit_ts, int32_t fetch_num, int64_t wait_microsecs, google::protobuf::Arena* p_arena = nullptr);
    CaptureStatus subscribe(std::vector<std::shared_ptr<mysub::Event>>& event_vec, 
            int64_t& commit_ts, int32_t fetch_num, int64_t wait_microsecs = 0, google::protobuf::Arena* p_arena = nullptr) {
        std::vector<std::shared_ptr<CapturerSQLInfo>> sqlinfo_vec;
        return subscribe(event_vec, sqlinfo_vec, commit_ts, fetch_num, wait_microsecs, p_arena);
    }

    TwoWaySync* get_two_way_sync() const {
        return _two_way_sync.get();
    }

    int get_binlog_regions(std::map<int64_t, pb::RegionInfo>& region_map, std::map<int64_t, int64_t>& region_id_ts_map, int64_t commit_ts);

    void insert_binlog(int64_t region_id, int64_t commit_ts, StoreReqPtr req_ptr) {
        _region_binlogs_map[region_id][commit_ts] = req_ptr;
    }

    int64_t merge_binlog(BinLogPriorityQueue& queue);

    void update_commit_ts(int64_t commit_ts);

    std::string remain_info() {
        std::string info;
        for (const auto& iter : _region_binlogs_map) {
            info += "[" + std::to_string(iter.first) + ", " + std::to_string(iter.second.size()) + "]";
        }

        return info;
    }
    
    void clear() {
        _schema_factory = nullptr;
        _region_binlogs_map.clear();
        _two_way_sync.reset();
        baikaldb::BinlogNetworkServer::get_instance()->close_schema_heartbeat();
    }

    void set_skip_regions(const std::set<int64_t>& skip_regions) {
        _skip_regions = skip_regions;
    }

    const std::string& user_name() const {
        return _user_name;
    }
    const std::string& user_ip() const {
        return _user_ip;
    }
    const std::set<std::string>& db_tables() const {
        return _db_tables;
    }
    const std::set<uint64_t>& signs() const {
        return _signs;
    }
    const std::set<int64_t>& txn_ids() const {
        return _txn_ids;
    }
    bool flash_back_read() const {
        return _flash_back_read;
    }
    int64_t binlog_id() const {
        return _binlog_id;
    }
    int64_t partition_id() const {
        return _partition_id;
    }

private:
    int64_t _partition_id = 0;
    int64_t _binlog_id = 0;
    baikaldb::SchemaFactory* _schema_factory {nullptr};
    std::string _namespace;
    std::unique_ptr<TwoWaySync> _two_way_sync;
    int64_t _last_commit_ts = 0;
    std::map<int64_t, std::map<int64_t, StoreReqPtr>> _region_binlogs_map;
    std::set<int64_t> _skip_regions; // 仅capturer tool使用，线上为空
    // sql闪回使用，用于binlog过滤
    std::string _user_name;
    std::string _user_ip;
    std::set<std::string> _db_tables;
    std::set<uint64_t> _signs;
    std::set<int64_t>  _txn_ids;
    bool _flash_back_read;
    DISALLOW_COPY_AND_ASSIGN(Capturer);
};

class FetchBinlog {
public:
    FetchBinlog(Capturer* capturer, uint64_t log_id, int64_t wait_microsecs, int64_t commit_ts) 
        : _capturer(capturer), _log_id(log_id), _wait_microsecs(wait_microsecs), _commit_ts(commit_ts) {
            result.clear();
            _binlog_id = _capturer->binlog_id();
            _partition_id = _capturer->partition_id();
        }
    CaptureStatus run(int32_t fetch_num);
    CaptureStatus read_binlog(const pb::RegionInfo& region_info, int64_t commit_ts, int fetch_num_per_region, pb::StoreRes& response, const std::string& peer);
    const std::map<int64_t, std::shared_ptr<pb::StoreRes>>& get_result() const {
        return _region_res;
    } 
    std::ostringstream result;
private:
    Capturer* _capturer;
    bool _is_finish = false;
    std::map<int64_t, std::shared_ptr<pb::StoreRes>> _region_res;
    std::mutex _region_res_mutex;
    uint64_t _log_id = 0;
    int64_t _wait_microsecs = 0;
    int64_t _commit_ts = 0;
    int64_t _binlog_id = 0;
    int64_t _partition_id = 0;
};


class MergeBinlog {
public:
    MergeBinlog(Capturer* capturer, const std::map<int64_t, std::shared_ptr<pb::StoreRes>>& fetcher_result, uint64_t log_id) 
        : _capturer(capturer), _fetcher_result(fetcher_result), _log_id(log_id) {}
    CaptureStatus run();

    BinLogPriorityQueue& get_result() {
        return _queue;
    }

public:
    int consume_cnt = 0;
private:
    Capturer* _capturer;
    const std::map<int64_t, std::shared_ptr<pb::StoreRes>>& _fetcher_result;
    BinLogPriorityQueue _queue;
    uint64_t _log_id;
};

class BinLogTransfer {
private:
    using UpdatePair = std::pair<SmartRecord, SmartRecord>;
    using RecordMap = std::unordered_map<std::string, SmartRecord>;
    using UpdatePairVec = std::vector<UpdatePair>;
    
    struct RecordCollection {
        RecordMap insert_records;
        RecordMap delete_records;
        UpdatePairVec update_records;
    };
    struct CapInfo {
        std::string db_name;
        SmartTable table_info;
        IndexInfo pri_info;
        std::map<int32_t, bool> signed_map;
        std::map<int32_t, bool> pk_map;
        std::set<int32_t> fields;
        std::set<int32_t> monitor_fields;
    };
public:
    BinLogTransfer(Capturer* capturer, BinLogPriorityQueue& queue, 
        std::vector<std::shared_ptr<mysub::Event>>& event_vec, std::vector<std::shared_ptr<CapturerSQLInfo>>& sqlinfo_vec, uint64_t logid, 
        TwoWaySync* two_way_sync, google::protobuf::Arena* p_arena) 
            : _capturer(capturer), _event_vec(event_vec), _sqlinfo_vec(sqlinfo_vec), _queue(queue), _log_id(logid), _two_way_sync(two_way_sync), _p_arena(p_arena) {
            _binlog_id = _capturer->binlog_id();
            _partition_id = _capturer->partition_id();
        }

    int init(const std::map<int64_t, SubTableIds>& sub_table_ids);

    int64_t run(int64_t& commit_ts);
    int64_t fake_cnt   = 0;
    int64_t table_filter_cnt = 0;
    int64_t two_way_sync_filter_txn_cnt = 0;
    int64_t monitor_fields_filter_cnt = 0;
private:

    void make_heartbeat_message(int64_t fake_ts);

    int multi_records_update_to_event(const UpdatePairVec& update_records, int64_t commit_ts, int64_t table_id, uint64_t partition_key);

    int multi_records_to_event(const RecordMap& records, mysub::EventType event_type, int64_t commit_ts, int64_t table_id, uint64_t partition_key);

    int single_record_to_event(const std::pair<TableRecord*, TableRecord*>& delete_insert_records, 
        mysub::EventType event_type, int64_t commit_ts, int64_t table_id, uint64_t partition_key);

    template<typename Repeated>
    int deserialization(const Repeated& repeat, RecordMap& records_map, int64_t table_id) {
        //过滤table_id
        auto& cap_info = _cap_infos[table_id];
        for (const auto& str : repeat) {
            auto new_record = baikaldb::SchemaFactory::get_instance()->new_record(*cap_info.table_info);
            if (new_record->decode(str) == -1) {
                DB_WARNING("decode record error.");
                return -1;
            }
            MutTableKey mt_key;
            int ret = 0;
            ret = new_record->encode_key(cap_info.pri_info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, index:%ld ret:%d", cap_info.pri_info.id, ret);
                return ret;
            }
            records_map[mt_key.data()] = new_record;
        }
        return 0;
    }

    void group_records(RecordCollection& records);

    int transfer_mutation(const pb::TableMutation& mutation, RecordCollection& records);
    
private:
    Capturer* _capturer;
    std::vector<std::shared_ptr<mysub::Event>>& _event_vec;
    std::vector<std::shared_ptr<CapturerSQLInfo>>& _sqlinfo_vec;
    BinLogPriorityQueue& _queue;
    std::map<int64_t, CapInfo> _cap_infos;
    uint64_t _log_id;
    TwoWaySync* _two_way_sync = nullptr;
    int64_t _binlog_id = 0;
    int64_t _partition_id = 0;
    google::protobuf::Arena* _p_arena = nullptr;
};

}
