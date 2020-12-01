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
#include <limits>
#include <atomic>
#include <vector>
#include <map>
#include <string>
#include <chrono>
#include <unordered_set>
#include <unordered_map>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <boost/heap/priority_queue.hpp>
#include "proto/event.pb.h"
#include "common.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
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

enum CaptureStatus {
    CS_SUCCESS,
    CS_EMPTY,
    CS_FAIL,
    CS_TIMEOUT,
    CS_LESS_THEN_OLDEST
};

using StoreReqPtr = std::shared_ptr<baikaldb::pb::StoreReq>;
struct StoreReqWithCommit {
    int64_t commit_ts;
    StoreReqPtr req_ptr;
    StoreReqWithCommit(int64_t ts, StoreReqPtr ptr) : commit_ts(ts), req_ptr(ptr) {}
};

struct StoreReqPtrBinLogCompare {
    bool operator()(const StoreReqWithCommit& lhs, const StoreReqWithCommit& rhs) const {
        return lhs.commit_ts >= rhs.commit_ts;
    }
};

using BinLogPriorityQueue = boost::heap::priority_queue<StoreReqWithCommit, boost::heap::compare<StoreReqPtrBinLogCompare>>;

class Capturer {
public:
    static Capturer* get_instance() {
        static Capturer capturer;
        return &capturer;
    }
#if BAIDU_INTERNAL
    int init(Json::Value& value);
#endif
    int init();
    int64_t get_offset(uint32_t timestamp);
    uint32_t get_timestamp(int64_t offset);
    CaptureStatus subscribe(std::vector<std::shared_ptr<mysub::Event>>& event_vec, 
        int64_t& commit_ts, int32_t fetch_num, int64_t wait_microsecs = 0);

private:
    std::vector<std::string> _table_infos;
    int64_t _partition_id = 0;
    int64_t _binlog_id = 0;
    std::unordered_set<int64_t> _origin_ids;
    baikaldb::SchemaFactory* _schema_factory {nullptr};
    std::string _namespace;
};

class FetchBinlog {
public:
    FetchBinlog(uint64_t log_id, int64_t wait_microsecs, int64_t commit_ts, int64_t binlog_id, int64_t partition_id) 
        : _log_id(log_id), _wait_microsecs(wait_microsecs), _commit_ts(commit_ts), _binlog_id(binlog_id), _partition_id(partition_id){}
    CaptureStatus run(int32_t fetch_num);
    const std::map<int64_t, std::shared_ptr<pb::StoreRes>>& get_result() const {
        return _region_res;
    } 
private:
    bool _is_finish {false};
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
    MergeBinlog(const std::map<int64_t, std::shared_ptr<pb::StoreRes>>& fetcher_result, uint64_t log_id) 
        : _fetcher_result(fetcher_result), _log_id(log_id) {}
    CaptureStatus run(int64_t& commit_ts);

    BinLogPriorityQueue& get_result() {
        return _queue;
    }
private:
    std::map<int64_t, std::vector<StoreReqWithCommit>> _binlogs_map;
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
    };
public:
    BinLogTransfer(int64_t binlog_id, BinLogPriorityQueue& queue, 
        std::vector<std::shared_ptr<mysub::Event>>& event_vec, const std::unordered_set<int64_t>& origin_ids, uint64_t logid) 
            : _binlog_id(binlog_id), _event_vec(event_vec), _queue(queue), _origin_ids(origin_ids), _log_id(logid) {}

    int init();

    int64_t run(int64_t& commit_ts);
private:

    int multi_records_update_to_event(const UpdatePairVec& update_records, int64_t commit_ts, int64_t tid);

    int multi_records_to_event(const RecordMap& records, mysub::EventType event_type, int64_t commit_ts, int64_t tid);

    int single_record_to_event(mysub::Event* event, 
        const std::pair<TableRecord*, TableRecord*>& delete_insert_records, mysub::EventType event_type, int64_t commit_ts, int64_t tid);

    template<typename Repeated>
    int deserialization(const Repeated& repeat, RecordMap& records_map, int64_t table_id) {
        //过滤table_id
        auto& cap_info = _cap_infos[table_id];
        for (const auto& str : repeat) {
            auto new_record = baikaldb::SchemaFactory::get_instance()->new_record(table_id);
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
            records_map.emplace(mt_key.data(), new_record);
        }
        return 0;
    }

    void group_records(RecordCollection& records);

    int transfer_mutation(const pb::TableMutation& mutation, RecordCollection& records);
    
private:
    int64_t _binlog_id;
    std::vector<std::shared_ptr<mysub::Event>>& _event_vec;
    BinLogPriorityQueue& _queue;
    const std::unordered_set<int64_t>& _origin_ids;
    std::map<int64_t, CapInfo> _cap_infos;
    uint64_t _log_id;
};
}
