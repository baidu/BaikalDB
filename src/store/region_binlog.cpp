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

#include "region.h"
#include <algorithm>
#include <fstream>
#include <boost/filesystem.hpp>
#include "table_key.h"
#include "runtime_state.h"
#include "mem_row_descriptor.h"
#include "exec_node.h"
#include "table_record.h"
#include "my_raft_log_storage.h"
#include "log_entry_reader.h"
#include "raft_log_compaction_filter.h"
#include "split_compaction_filter.h"
#include "rpc_sender.h"
#include "concurrency.h"
#include "store.h"
#include "closure.h"
#include "rocksdb_filesystem.h"

namespace baikaldb {
DECLARE_int64(retry_interval_us);
DEFINE_int64(binlog_timeout_us, 10 * 1000 * 1000LL, "binlog timeout us : 10s");
DEFINE_int64(binlog_warn_timeout_minute, 15, "binlog warn timeout min : 15min");
DEFINE_int64(read_binlog_max_size_bytes, 100 * 1024 * 1024, "100M");
DEFINE_int64(read_binlog_timeout_us, 10 * 1000 * 1000, "10s");
DEFINE_int64(check_point_rollback_interval_s, 60, "60s");
DEFINE_int64(binlog_multiget_batch, 1024, "1024");
DEFINE_int64(binlog_seek_batch, 10000, "10000");
DEFINE_int64(binlog_use_seek_interval_min, 60, "1h");
DEFINE_int64(offline_binlog_size_peer_sst, 1073741824LL, "defualt 1GB, -1 means no limit");
DEFINE_bool(binlog_force_get, false, "false");
DECLARE_int64(print_time_us);
DECLARE_string(meta_server_bns);
DECLARE_string(db_path);

#define IF_DONE_SET_RESPONSE(done, errcode, err_message) \
    do {\
        if (done != nullptr && ((BinlogClosure*)done)->response != nullptr) {\
            ((BinlogClosure*)done)->response->set_errcode(errcode);\
            ((BinlogClosure*)done)->response->set_errmsg(err_message);\
        }\
    }while (0);
#define IF_DONE_SET_RESPONSE_FATAL(done, errcode, err_message) \
    do {\
        DB_FATAL("region_id: %ld %s", _region_id, err_message);\
        if (done != nullptr && ((BinlogClosure*)done)->response != nullptr) {\
            ((BinlogClosure*)done)->response->set_errcode(errcode);\
            ((BinlogClosure*)done)->response->set_errmsg(err_message);\
        }\
    }while (0);
    

void print_oldest_ts(std::ostream& os, void*) {
    int64_t oldest_ts = RocksWrapper::get_instance()->get_oldest_ts_in_binlog_cf();
    os << std::to_string(oldest_ts) << "(" << ts_to_datetime_str(oldest_ts) << ")";
}
static bvar::PassiveStatus<std::string> binlog_oldest_ts(
        "binlog_oldest_ts", print_oldest_ts, NULL);

//该函数联调时测试 TODO by YUZHENGQUAN
int Region::get_primary_region_info(int64_t primary_region_id, pb::RegionInfo& region_info) {
    MetaServerInteract&   meta_server_interact = Store::get_instance()->get_meta_server_interact();
    pb::QueryRequest query_request;
    pb::QueryResponse query_response;
    query_request.set_op_type(pb::QUERY_REGION);
    query_request.add_region_ids(primary_region_id);
    if (meta_server_interact.send_request("query", query_request, query_response) != 0) {
        DB_FATAL("send query request to meta server fail primary_region_id: %ld "
                "region_id:%ld res: %s", primary_region_id, _region_id, query_response.ShortDebugString().c_str());
        return -1;
    }

    if (query_response.errcode() != pb::SUCCESS) {
        DB_FATAL("send query request to meta server fail primary_region_id: %ld "
                "region_id:%ld res: %s", primary_region_id, _region_id, query_response.ShortDebugString().c_str());
        return -1;
    }

    region_info = query_response.region_infos(0);
    return 0;
}

void Region::binlog_query_primary_region(const int64_t& start_ts, const int64_t& txn_id, pb::RegionInfo& region_info, int64_t rollback_ts) {
    pb::StoreReq request;
    pb::StoreRes response;
    request.set_op_type(pb::OP_TXN_QUERY_PRIMARY_REGION);
    request.set_region_id(region_info.region_id());
    request.set_region_version(region_info.version());
    pb::TransactionInfo* pb_txn = request.add_txn_infos();
    pb_txn->set_txn_id(txn_id);
    pb_txn->set_seq_id(0);
    pb_txn->set_start_ts(start_ts);
    pb_txn->set_open_binlog(true);
    //pb_txn->set_seq_id(txn->seq_id());
    pb_txn->set_txn_state(pb::TXN_PREPARED);

    int retry_times = 1;
    bool success = false;
    int64_t commit_ts = 0;
    do {
        RpcSender::send_query_method(request, response, region_info.leader(), region_info.region_id());
        switch (response.errcode()) {
            case pb::SUCCESS: {
                pb::StoreReq binlog_request;
                auto txn_info = response.txn_infos(0);
                if (txn_info.txn_state() == pb::TXN_ROLLBACKED) {
                    binlog_request.set_op_type(pb::OP_ROLLBACK_BINLOG);
                    commit_ts = rollback_ts;
                } else if (txn_info.txn_state() == pb::TXN_COMMITTED) {
                    binlog_request.set_op_type(pb::OP_COMMIT_BINLOG);
                    commit_ts = txn_info.commit_ts();
                } else {
                    // primary没有查到rollback_tag，认为是commit，但是secondary不是PREPARE状态
                    DB_FATAL("primary committed, secondary need catchup log, region_id:%ld,"
                        "primary_region_id: %ld txn_id: %lu",
                        _region_id, region_info.region_id(), txn_id);
                    success = true;
                    return;
                }
                if (commit_ts <= start_ts) {
                    DB_FATAL("commit_ts: %ld, start_ts: %ld, txn_id: %ld", commit_ts, start_ts, txn_id);
                    return;
                }
                binlog_request.set_region_id(_region_id);
                binlog_request.set_region_version(get_version());
                //查询时对端region需要设置commit_ts TODO by YUZHENGQUAN
                auto binlog_desc = binlog_request.mutable_binlog_desc();
                binlog_desc->set_binlog_ts(commit_ts);
                binlog_desc->set_txn_id(txn_info.txn_id());
                binlog_desc->set_start_ts(start_ts);
                binlog_desc->set_primary_region_id(region_info.region_id());

                butil::IOBuf data;
                butil::IOBufAsZeroCopyOutputStream wrapper(&data);
                if (!binlog_request.SerializeToZeroCopyStream(&wrapper)) {
                    DB_FATAL("region_id: %ld serialize failed", _region_id);
                    return;
                }

                BinlogClosure* c = new BinlogClosure;
                c->cost.reset();
                braft::Task task;
                task.data = &data;
                task.done = c;
                _node.apply(task);
                success = true;
                DB_WARNING("send txn query success request:%s response: %s",
                    request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
                break;
            }
            case pb::NOT_LEADER: {
                if (response.leader() != "0.0.0.0:0") {
                    region_info.set_leader(response.leader());
                }
                DB_WARNING("send txn query NOT_LEADER , request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                bthread_usleep(retry_times * FLAGS_retry_interval_us);
                break;
            }
            case pb::VERSION_OLD: {
                for (auto r : response.regions()) {
                    if (r.region_id() == region_info.region_id()) {
                        region_info.CopyFrom(r);
                        request.set_region_version(region_info.version());
                    }
                }
                DB_WARNING("send txn query VERSION_OLD , request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                bthread_usleep(retry_times * FLAGS_retry_interval_us);
                break;
            }
            default: {
                DB_WARNING("send txn query failed , request:%s response: %s",
                    request.ShortDebugString().c_str(),
                    response.ShortDebugString().c_str());
                bthread_usleep(retry_times * FLAGS_retry_interval_us);
                break;
            }
        }
        retry_times++;
    } while (!success && retry_times <= 5);
}

// 没有commit或rollback的prewrite binlog超时检查
void Region::binlog_timeout_check() {
    std::vector<int64_t> start_ts;
    std::vector<int64_t> txn_id;
    std::vector<int64_t> primary_region_id;

    start_ts.reserve(3);
    txn_id.reserve(3);
    primary_region_id.reserve(3);

    {
        std::unique_lock<bthread::Mutex> lck(_binlog_param_mutex);

        if (_binlog_param.ts_binlog_map.size() == 0) {
            return;
        }

        for (const auto& iter : _binlog_param.ts_binlog_map) {
            if (iter.second.time.get_time() > FLAGS_binlog_timeout_us) {
                RocksdbVars::get_instance()->binlog_not_commit_max_cost << iter.second.time.get_time();
                //告警
                DB_WARNING("region_id: %ld, start_ts: %ld, txn_id:%ld, primary_region_id: %ld timeout", 
                    _region_id, iter.first, iter.second.txn_id, iter.second.primary_region_id);
                if (iter.second.time.get_time() > FLAGS_binlog_warn_timeout_minute * 60 * 1000 * 1000LL) {
                    DB_FATAL("region_id: %ld not commited for a long time", _region_id);
                }

                // 避免过长
                if (_binlog_param.timeout_start_ts_done.size() > 1000) {
                    _binlog_param.timeout_start_ts_done.erase(_binlog_param.timeout_start_ts_done.begin());
                }
                _binlog_param.timeout_start_ts_done[iter.first] = false;
                if (is_leader() && iter.second.binlog_type == PREWRITE_BINLOG) {
                    start_ts.emplace_back(iter.first);
                    txn_id.emplace_back(iter.second.txn_id);
                    primary_region_id.emplace_back(iter.second.primary_region_id);
                }
            } else {
                break;
            }
        }
    }

    //  反查primary region，确认prewrite binlog是否已经commit 或 rollback
    for (size_t i = 0; i < start_ts.size(); ++i) {
        pb::RegionInfo region_info;
        int ret = get_primary_region_info(primary_region_id[i], region_info);
        if (ret < 0) {
            continue;
        }

        int64_t rollback_ts = Store::get_instance()->get_tso();
        if (rollback_ts < 0) {
            continue;
        }

        binlog_query_primary_region(start_ts[i], txn_id[i], region_info, rollback_ts);
    }

}

void Region::binlog_fake(int64_t ts, BthreadCond& cond) {

    pb::StoreReq request;

    request.set_op_type(pb::OP_FAKE_BINLOG);
    request.set_region_id(_region_id);
    request.set_region_version(get_version());
    //测试需要将ts设为time TODO
    request.mutable_binlog_desc()->set_binlog_ts(ts);

    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!request.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("region_id: %ld serialize failed", _region_id);
        return;
    }

    DB_WARNING("region_id: %ld, FAKE BINLOG, ts: %ld, %s", _region_id, ts, ts_to_datetime_str(ts).c_str());

    BinlogClosure* c = new BinlogClosure(&cond);
    cond.increase_wait();
    c->cost.reset();
    braft::Task task;
    task.data = &data;
    task.done = c;
    _node.apply(task);
    return;
}

void Region::binlog_fill_exprvalue(const pb::BinlogDesc& binlog_desc, pb::OpType op_type, std::map<std::string, ExprValue>& field_value_map) {
    if (binlog_desc.has_binlog_ts()) {
        ExprValue value(pb::INT64);
        value._u.int64_val = binlog_desc.binlog_ts();
        field_value_map["ts"] = value;
    }

    if (binlog_desc.has_txn_id()) {
        ExprValue value(pb::INT64);
        value._u.int64_val = binlog_desc.txn_id();
        field_value_map["txn_id"] = value;
    }

    if (binlog_desc.has_start_ts()) {
        ExprValue value(pb::INT64);
        value._u.int64_val = binlog_desc.start_ts();
        field_value_map["start_ts"] = value;
    }  

    if (binlog_desc.has_primary_region_id()) {
        ExprValue value(pb::INT64);
        value._u.int64_val = binlog_desc.primary_region_id();
        field_value_map["primary_region_id"] = value;
    }

    {
        ExprValue value(pb::INT64);
        value._u.int64_val = _region_id;
        field_value_map["binlog_region_id"] = value;
    } 

    {
        ExprValue value(pb::INT64);
        value._u.int64_val = _region_info.partition_id();
        field_value_map["partition_key"] = value;
    }

    if (binlog_desc.has_binlog_row_cnt()) {
        ExprValue value(pb::INT64);
        value._u.int64_val = binlog_desc.binlog_row_cnt();
        field_value_map["binlog_row_cnt"] = value;
    } 

    if (binlog_desc.has_user_name()) {
        ExprValue value(pb::STRING);
        value.str_val = binlog_desc.user_name();
        field_value_map["user_name"] = value;
    }

    if (binlog_desc.has_user_ip()) {
        ExprValue value(pb::STRING);
        value.str_val = binlog_desc.user_ip();
        field_value_map["user_ip"] = value;
    }

    if (binlog_desc.db_tables_size() > 0) {
        ExprValue value(pb::STRING);
        for (const auto& db_table : binlog_desc.db_tables()) {
            value.str_val += db_table + ";";
        }
        value.str_val.pop_back();
        field_value_map["db_tables"] = value;
    }

    if (binlog_desc.signs_size() > 0) {
        ExprValue value(pb::STRING);
        for (uint64_t sign : binlog_desc.signs()) {
            value.str_val += std::to_string(sign) + ";";
        }
        value.str_val.pop_back();
        field_value_map["signs"] = value;
    }

    ExprValue binlog_type;
    binlog_type.type = pb::INT64;
    switch (op_type)  {
        case pb::OP_PREWRITE_BINLOG: {
            binlog_type._u.int64_val = static_cast<int64_t>(PREWRITE_BINLOG);
            break;
        }
        case pb::OP_COMMIT_BINLOG: {
            binlog_type._u.int64_val = static_cast<int64_t>(COMMIT_BINLOG);
            break;
        }
        case pb::OP_ROLLBACK_BINLOG: {
            binlog_type._u.int64_val = static_cast<int64_t>(ROLLBACK_BINLOG);
            break;
        }
        default: {
            binlog_type._u.int64_val = static_cast<int64_t>(FAKE_BINLOG);
            break;
        }
    }

    field_value_map["binlog_type"] = binlog_type;
}

int64_t Region::binlog_get_int64_val(const std::string& name, const std::map<std::string, ExprValue>& field_value_map) {
    auto iter = field_value_map.find(name);
    if (iter == field_value_map.end()) {
        return -1;
    }

    return iter->second.get_numberic<int64_t>();
}

std::string Region::binlog_get_str_val(const std::string& name, const std::map<std::string, ExprValue>& field_value_map) {
    auto iter = field_value_map.find(name);
    if (iter == field_value_map.end()) {
        return "";
    }

    return iter->second.get_string();
}

void Region::binlog_get_scan_fields(std::map<int32_t, FieldInfo*>& field_ids, std::vector<int32_t>& field_slot, 
        SmartTable& binlog_table, SmartIndex& binlog_pri) {
    field_slot.resize(binlog_table->fields.back().id + 1);

    std::set<int32_t> pri_field_ids;
    for (auto& field_info : binlog_pri->fields) {
        pri_field_ids.insert(field_info.id);
    }

    for (auto& field : binlog_table->fields) {
        field_slot[field.id] = field.id;
        if (pri_field_ids.count(field.id) == 0) {
            field_ids[field.id] = &field;
        }
    }
}

void Region::binlog_get_field_values(std::map<std::string, ExprValue>& field_value_map, SmartRecord& record, SmartTable& binlog_table) {
    for (auto& field : binlog_table->fields) {
        auto f = record->get_field_by_tag(field.id);
        field_value_map[field.short_name] = record->get_value(f);
    }
}

int Region::binlog_reset_on_snapshot_load_restart() {
    std::unique_lock<bthread::Mutex> lck(_binlog_param_mutex); 

    //重新读取check point
    _binlog_param.check_point_ts = _meta_writer->read_binlog_check_point(_region_id);
    _binlog_param.oldest_ts      = _meta_writer->read_binlog_oldest_ts(_region_id);
    _binlog_param.ts_binlog_map.clear();
    if (_binlog_param.check_point_ts > 0) {
        int ret = binlog_scan_when_restart();
        if (ret != 0) {
            DB_FATAL("region_id: %ld, snapshot load failed", _region_id);
            return -1;
        }
    }
    // 修复oldest_ts被误设置为-1的情况
    if (_binlog_param.oldest_ts < 0) {
        _meta_writer->write_binlog_oldest_ts(_region_id, _binlog_param.check_point_ts);
        _binlog_param.oldest_ts = _binlog_param.check_point_ts;
    }
    DB_WARNING("region_id: %ld, check_point_ts: [%ld, %s], oldest_ts: [%ld, %s]", _region_id, 
        _binlog_param.check_point_ts, ts_to_datetime_str(_binlog_param.check_point_ts).c_str(), 
        _binlog_param.oldest_ts, ts_to_datetime_str(_binlog_param.oldest_ts).c_str());
    return 0;
}

//add peer时不拉取binlog快照，需要重置oldest binlog ts和check point
int Region::binlog_reset_on_snapshot_load() {
    std::unique_lock<bthread::Mutex> lck(_binlog_param_mutex);
    TimeCost time;
    _binlog_param.check_point_ts = _meta_writer->read_binlog_check_point(_region_id);
    _binlog_param.oldest_ts      = _meta_writer->read_binlog_oldest_ts(_region_id);
    _binlog_param.ts_binlog_map.clear();
    if (_binlog_param.check_point_ts > 0) {
        int ret = binlog_scan_when_restart();
        if (ret != 0) {
            DB_FATAL("region_id: %ld, snapshot load failed", _region_id);
            return -1;
        }
        _binlog_param.max_ts_applied = std::max(_binlog_param.check_point_ts, _binlog_param.max_ts_applied);
        // binlog数据没有拉取过来，需要将oldest更新为最大的ts
        ret = _meta_writer->write_binlog_oldest_ts(_region_id, _binlog_param.max_ts_applied);
        if (ret != 0) {
            DB_FATAL("region_id: %ld, snapshot load failed write ts failed", _region_id);
            return -1;
        }
        _binlog_param.oldest_ts = _binlog_param.max_ts_applied;
    } else {
        DB_FATAL("region_id: %ld, snapshot load check point %ld invaild", _region_id, _binlog_param.check_point_ts);
        return -1;
    }

    DB_WARNING("region_id: %ld, check_point_ts: [%ld, %s], oldest_ts: [%ld, %s], time: %ld", _region_id, 
        _binlog_param.check_point_ts, ts_to_datetime_str(_binlog_param.check_point_ts).c_str(), 
        _binlog_param.oldest_ts, ts_to_datetime_str(_binlog_param.oldest_ts).c_str(), time.get_time());
    return 0;
}

// 只在重启或迁移的时候调用，用于重建map
int Region::binlog_scan_when_restart() {
    TimeCost cost;
    int64_t begin_ts = _binlog_param.check_point_ts;
    int64_t scan_rows = 0;
    SmartTable binlog_table = _factory->get_table_info_ptr(get_table_id());
    SmartIndex binlog_pri   = _factory->get_index_info_ptr(get_table_id());

    SmartRecord left_record  = _factory->new_record(*binlog_table);
    SmartRecord right_record = _factory->new_record(*binlog_table);
    if (left_record == nullptr || right_record == nullptr) {
        return -1;
    }

    ExprValue value;
    value.type = pb::INT64;
    value._u.int64_val = begin_ts;
    left_record->set_value(left_record->get_field_by_tag(1), value);
    right_record->decode("");

    IndexRange range(left_record.get(), right_record.get(), binlog_pri.get(), binlog_pri.get(),
                        &_region_info, 1, 0, false, false, false);

    std::map<int32_t, FieldInfo*> field_ids;
    std::vector<int32_t> field_slot;

    binlog_get_scan_fields(field_ids, field_slot, binlog_table, binlog_pri);

    TableIterator* table_iter = Iterator::scan_primary(nullptr, range, field_ids, field_slot, false, true);
    if (table_iter == nullptr) {
        DB_WARNING("open TableIterator fail, table_id:%ld", get_table_id());
        return -1;
    }

    ON_SCOPE_EXIT(([this, table_iter]() {
        delete table_iter;
    }));

    bool is_first_ts = true;
    int64_t first_ts = 0;
    int64_t last_ts = 0;
    SmartRecord record = _factory->new_record(*binlog_table);
    while (1) {
        record->clear();
        if (!table_iter->valid()) {
            break;
        }

        int ret = table_iter->get_next(record);
        if (ret < 0) {
            break;
        }

        std::map<std::string, ExprValue> field_value_map;
        binlog_get_field_values(field_value_map, record, binlog_table);
        int64_t tmp_ts = binlog_get_int64_val("ts", field_value_map);
        if (is_first_ts) {
            is_first_ts = false;
            first_ts = tmp_ts;
        }
        last_ts = tmp_ts;
        //binlog 元信息更新map
        binlog_update_map_when_scan(field_value_map);
        scan_rows++;
    }

    // 一轮扫描结束后，处理内存map，更新check point
    binlog_update_check_point();
    DB_WARNING("region_id: %ld, scan_rows: %ld, scan_range[%ld, %ld]; binlog_scan map size: %lu, check point ts: %ld, cost: %ld", 
        _region_id, scan_rows, first_ts, last_ts, _binlog_param.ts_binlog_map.size(), 
        _binlog_param.check_point_ts, cost.get_time());
    return 0;
}

//scan时更新map，因为不会和写入并发，扫描ts严格递增
void Region::binlog_update_map_when_scan(const std::map<std::string, ExprValue>& field_value_map) {
    BinlogDesc binlog_desc;
    int64_t ts = binlog_get_int64_val("ts", field_value_map);
    BinlogType binlog_type = static_cast<BinlogType>(binlog_get_int64_val("binlog_type", field_value_map));
    int64_t txn_id = binlog_get_int64_val("txn_id", field_value_map);
    int64_t start_ts = binlog_get_int64_val("start_ts", field_value_map);

    //扫描时小于max ts直接跳过
    if (ts < _binlog_param.max_ts_applied) {
        DB_WARNING("region_id: %ld scan_ts: %ld < max_ts: %ld, txn_id: %ld", _region_id, ts, _binlog_param.max_ts_applied, txn_id);
        return;
    }

    if (binlog_type == FAKE_BINLOG) {
        binlog_desc.binlog_type = binlog_type;
        _binlog_param.ts_binlog_map[ts] = binlog_desc;
        DB_WARNING("region_id: %ld, ts: [%ld, %s], type: %s, txn_id: %ld", 
            _region_id, ts, ts_to_datetime_str(ts).c_str(), binlog_type_name(binlog_type), txn_id);
    } else if (binlog_type == PREWRITE_BINLOG) {
        binlog_desc.binlog_type = binlog_type;
        binlog_desc.txn_id = txn_id;
        binlog_desc.primary_region_id = binlog_get_int64_val("primary_region_id", field_value_map);
        _binlog_param.ts_binlog_map[ts] = binlog_desc;
        DB_WARNING("region_id: %ld, start_ts: [%ld, %s], type: %s, txn_id: %ld", 
            _region_id, ts, ts_to_datetime_str(ts).c_str(), binlog_type_name(binlog_type), txn_id);
    } else if (binlog_type == COMMIT_BINLOG || binlog_type == ROLLBACK_BINLOG) {
        if (start_ts < _binlog_param.check_point_ts) {
            DB_WARNING("region_id: %ld, type: %s, start_ts: [%ld, %s] < check point: %ld, ts: [%ld, %s], txn_id: %ld", 
                _region_id, binlog_type_name(binlog_type), start_ts, ts_to_datetime_str(start_ts).c_str(), 
                _binlog_param.check_point_ts, ts, ts_to_datetime_str(ts).c_str(), txn_id);
            return;
        }

        auto iter = _binlog_param.ts_binlog_map.find(start_ts);
        if (iter == _binlog_param.ts_binlog_map.end()) {
            if (binlog_type == ROLLBACK_BINLOG) {
                DB_WARNING("region_id: %ld, type: %s, start_ts: [%ld, %s] can not find in map, ts: [%ld, %s], txn_id: %ld", 
                    _region_id, binlog_type_name(binlog_type), start_ts, ts_to_datetime_str(start_ts).c_str(), 
                    ts, ts_to_datetime_str(ts).c_str(), txn_id);
            } else {
                DB_FATAL("region_id: %ld, type: %s, start_ts: [%ld, %s] can not find in map, ts: [%ld, %s], txn_id: %ld", 
                    _region_id, binlog_type_name(binlog_type), start_ts, ts_to_datetime_str(start_ts).c_str(), 
                    ts, ts_to_datetime_str(ts).c_str(), txn_id);
            }
            return;
        } else {
            _binlog_param.ts_binlog_map.erase(start_ts);
            DB_WARNING("region_id: %ld, type: %s, start_ts: [%ld, %s] erase, commit_ts: [%ld, %s] txn_id: %ld", 
                _region_id, binlog_type_name(binlog_type), start_ts, ts_to_datetime_str(start_ts).c_str(), 
                ts, ts_to_datetime_str(ts).c_str(), txn_id);
        }

    }

    _binlog_param.check_point_ts = _binlog_param.ts_binlog_map.empty() ? ts : _binlog_param.ts_binlog_map.begin()->first;
    _binlog_param.max_ts_applied  = std::max(_binlog_param.max_ts_applied, ts);

    return;
}

//on_apply中只有相关start_ts/commit_ts/rollback_ts和map区间有重合时才可以更新map
int Region::binlog_update_map_when_apply(const std::map<std::string, ExprValue>& field_value_map, const std::string& remote_side) {
    int ret = 0;
    BinlogDesc binlog_desc;
    int64_t ts = binlog_get_int64_val("ts", field_value_map);
    BinlogType type = static_cast<BinlogType>(binlog_get_int64_val("binlog_type", field_value_map));
    int64_t txn_id = binlog_get_int64_val("txn_id", field_value_map);
    int64_t start_ts = binlog_get_int64_val("start_ts", field_value_map);
    
    if (_binlog_param.check_point_ts == -1) {
        if (type == FAKE_BINLOG) {
            binlog_desc.binlog_type = type;
            _binlog_param.ts_binlog_map[ts] = binlog_desc;
            ret = _meta_writer->write_binlog_check_point(_region_id, ts);
            if (ret < 0) {
                DB_FATAL("region_id: %ld, txn_id: %ld, ts: %ld write binlog check point failed", 
                    _region_id, txn_id, ts);
                return -1;
            }
            ret = _meta_writer->write_binlog_oldest_ts(_region_id, ts);
            if (ret < 0) {
                DB_FATAL("region_id: %ld, txn_id: %ld, ts: %ld write oldest ts failed", 
                    _region_id, txn_id, ts);
                return -1;
            }
            _binlog_param.check_point_ts = ts;
            _binlog_param.oldest_ts      = ts;
            _binlog_param.max_ts_applied  = std::max(_binlog_param.max_ts_applied, ts);
            DB_WARNING("region_id: %ld, ts: %ld, %s, FAKE BINLOG reset check point and oldest ts", _region_id, ts, ts_to_datetime_str(ts).c_str());
        } else {
            DB_WARNING("region_id :%ld, txn_id: %ld, start_ts: %ld, %s, commit_ts: %ld, %s, binlog_type: %s, discard", 
                _region_id, txn_id, start_ts, ts_to_datetime_str(start_ts).c_str(), ts, ts_to_datetime_str(ts).c_str(), binlog_type_name(type));
        }
        return 0;
    }

    if (ts <= _binlog_param.check_point_ts) {
        // check point 变小说明写入ts小于check point，告警
        if (tso::get_timestamp_internal(_binlog_param.check_point_ts) - tso::get_timestamp_internal(ts)
             > FLAGS_check_point_rollback_interval_s) {
            DB_FATAL("region_id: %ld, new ts: %ld, %s, < old check point ts: %ld, %s, "
                "check point rollback interval too long, remote_side: %s", _region_id, ts, ts_to_datetime_str(ts).c_str(), 
                _binlog_param.check_point_ts, ts_to_datetime_str(_binlog_param.check_point_ts).c_str(), remote_side.c_str());
        } else {
            DB_WARNING("region_id: %ld, new ts: %ld, %s, < old check point ts: %ld, %s, remote_side: %s", 
                _region_id, ts, ts_to_datetime_str(ts).c_str(), 
                _binlog_param.check_point_ts, ts_to_datetime_str(_binlog_param.check_point_ts).c_str(), remote_side.c_str());
        }
    } 

    if (type == FAKE_BINLOG) {
        if (ts <= _binlog_param.check_point_ts) {
            DB_WARNING("region_id: %ld, ts: %ld, FAKE BINLOG", _region_id, ts);
            return 0;
        }
        binlog_desc.binlog_type = type;
        _binlog_param.ts_binlog_map[ts] = binlog_desc;
        DB_DEBUG("region_id: %ld, ts: %ld, %s, FAKE BINLOG", _region_id, ts, ts_to_datetime_str(ts).c_str());
    } else if (type == PREWRITE_BINLOG) {
        binlog_desc.binlog_type = type;
        binlog_desc.txn_id = txn_id;
        binlog_desc.primary_region_id = binlog_get_int64_val("primary_region_id", field_value_map);
        _binlog_param.ts_binlog_map[ts] = binlog_desc;
        DB_DEBUG("region_id: %ld, ts: %ld, %s, txn_id: %ld, PREWRITE BINLOG, remote_side: %s", 
            _region_id, ts, ts_to_datetime_str(ts).c_str(), txn_id, remote_side.c_str());
    } else if (type == COMMIT_BINLOG || type == ROLLBACK_BINLOG) {
        if (start_ts < _binlog_param.check_point_ts) {
            auto timeout_start_ts_iter = _binlog_param.timeout_start_ts_done.find(start_ts);
            if (timeout_start_ts_iter != _binlog_param.timeout_start_ts_done.end()) {
                bool has_committed = timeout_start_ts_iter->second;
                if (!has_committed) {
                    // start_ts 设置为已经commit
                    _binlog_param.timeout_start_ts_done[start_ts] = true;
                } else {
                    // 重复commit
                    DB_WARNING("region_id: %ld, type: %s, txn_id: %ld, commit_ts: %ld, %s, start_ts: %ld, %s, remote_side: %s", 
                        _region_id, binlog_type_name(type), txn_id, ts, ts_to_datetime_str(ts).c_str(), start_ts, 
                        ts_to_datetime_str(start_ts).c_str(), remote_side.c_str());
                    return 0;
                }
            }

            if (type == COMMIT_BINLOG && ts < _binlog_read_max_ts.load()) {
                DB_FATAL("region_id: %ld, type: %s, txn_id: %ld, commit_ts: %ld, %s, start_ts: %ld, %s, read_max_ts: %ld, %s, remote_side: %s", 
                    _region_id, binlog_type_name(type), txn_id, ts, ts_to_datetime_str(ts).c_str(), start_ts, ts_to_datetime_str(start_ts).c_str(), 
                    _binlog_read_max_ts.load(), ts_to_datetime_str(_binlog_read_max_ts.load()).c_str(), remote_side.c_str());
            }
            return 0;
        }

        auto iter = _binlog_param.ts_binlog_map.find(start_ts);
        if (iter == _binlog_param.ts_binlog_map.end()) {
            if (type == COMMIT_BINLOG) {
                DB_FATAL("region_id: %ld, type: %s, txn_id: %ld, commit_ts: %ld, %s, start_ts: %ld, %s can not find in map, remote_side: %s", 
                    _region_id, binlog_type_name(type), txn_id, ts, ts_to_datetime_str(ts).c_str(), start_ts, ts_to_datetime_str(start_ts).c_str(),
                    remote_side.c_str());
            } else {
                DB_WARNING("region_id: %ld, type: %s, txn_id: %ld, commit_ts: %ld, %s, start_ts: %ld, %s can not find in map, remote_side: %s", 
                    _region_id, binlog_type_name(type), txn_id, ts, ts_to_datetime_str(ts).c_str(), start_ts, ts_to_datetime_str(start_ts).c_str(),
                    remote_side.c_str());
            }
            return 0;
        } else {
            bool repeated_commit = false;
            _binlog_param.ts_binlog_map.erase(start_ts);
            auto timeout_start_ts_iter = _binlog_param.timeout_start_ts_done.find(start_ts);
            if (timeout_start_ts_iter != _binlog_param.timeout_start_ts_done.end()) {
                bool has_committed = timeout_start_ts_iter->second;
                if (!has_committed) {
                    // start_ts 设置为已经commit
                    repeated_commit = true;
                    _binlog_param.timeout_start_ts_done[start_ts] = true;
                } 
            }

            DB_DEBUG("region_id: %ld, type: %s, txn_id: %ld, commit_ts: %ld, %s start_ts: %ld, %s remote_side: %s repeated_commit: %d, erase", 
                _region_id, binlog_type_name(type), txn_id, ts, ts_to_datetime_str(ts).c_str(), start_ts, 
                ts_to_datetime_str(start_ts).c_str(), remote_side.c_str(), repeated_commit);
        }
    }

    _binlog_param.check_point_ts = _binlog_param.ts_binlog_map.empty() ? ts : _binlog_param.ts_binlog_map.begin()->first;
    _binlog_param.max_ts_applied  = std::max(_binlog_param.max_ts_applied, ts);

    return 0;
}

//扫描一轮或者新写入binlog之后，更新map，更新扫描进度点
int Region::binlog_update_check_point() {
    if (_binlog_param.check_point_ts == -1) {
        return 0;
    }

    auto iter = _binlog_param.ts_binlog_map.begin();
    while (iter != _binlog_param.ts_binlog_map.end()) {
        BinlogType type = iter->second.binlog_type;

        if (type == COMMIT_BINLOG || type == ROLLBACK_BINLOG) {
            DB_FATAL("binlog type: %s , txn_id: %ld", binlog_type_name(type), iter->second.txn_id);
        }

        if (type == PREWRITE_BINLOG) {
            break;
        }

        // erase FAKE_BINLOG
        iter = _binlog_param.ts_binlog_map.erase(iter);
    }

    int64_t check_point_ts = _binlog_param.ts_binlog_map.empty() ? _binlog_param.max_ts_applied : _binlog_param.ts_binlog_map.begin()->first;
    if (_binlog_param.check_point_ts >= check_point_ts) {
        return 0;
    }

    int ret = _meta_writer->write_binlog_check_point(_region_id, check_point_ts);
    if (ret != 0) {
        DB_FATAL("region_id: %ld, check_point_ts: %ld, write check_point_ts failed", _region_id, check_point_ts);
        return -1;
    }

    DB_WARNING("region_id: %ld, check point ts %ld, %s => %ld, %s", _region_id, _binlog_param.check_point_ts, 
        ts_to_datetime_str(_binlog_param.check_point_ts).c_str(), check_point_ts, ts_to_datetime_str(check_point_ts).c_str());
    _binlog_param.check_point_ts = check_point_ts;

    return 0;
}

int Region::write_binlog_record(SmartRecord record) {
    SmartIndex binlog_pri   = _factory->get_index_info_ptr(get_table_id());
    MutTableKey key;
    // DB_WARNING("region_id: %ld, pk_id: %ld", _region_id, pk_index.id);
    key.append_i64(_region_id).append_i64(binlog_pri->id);
    if (0 != key.append_index(*binlog_pri, record.get(), -1, true)) {
        DB_FATAL("Fail to append_index, reg=%ld, tab=%ld", _region_id, binlog_pri->id);
        return -1;
    }

    std::string value;
    int ret = record->encode(value);
    if (ret != 0) {
        DB_FATAL("encode record failed: reg=%ld, tab=%ld", _region_id, binlog_pri->id);
        return -1;
    }

    rocksdb::WriteOptions write_options;
    rocksdb::WriteBatch batch;
    batch.Put(_meta_writer->get_handle(), 
                _meta_writer->applied_index_key(_region_id), 
                _meta_writer->encode_applied_index(_applied_index, _data_index));
    batch.Put(_data_cf, key.data(), value);
    auto s = _rocksdb->write(write_options, &batch);
    if (!s.ok()) {
        DB_FATAL("write binlog failed, region_id: %ld, status: %s", _region_id, s.ToString().c_str());
        return -1;
    }
    
    return 0;
}

//ts(primary key),   no null 
//txn_id,            default 0
//binlog_type,       no null
//partition_key,       default 0
//start_ts,          default -1
//primary_region_id, default -1
int Region::write_binlog_value(const std::map<std::string, ExprValue>& field_value_map) {
    SmartTable binlog_table = _factory->get_table_info_ptr(get_table_id());
    SmartRecord record    = _factory->new_record(*binlog_table);
    if (record == nullptr) {
        DB_WARNING("table_id: %ld nullptr", get_table_id());
        return -1;
    }

    for (auto& field : binlog_table->fields) {
        auto iter = field_value_map.find(field.short_name);
        if (iter == field_value_map.end()) {
            //default
            if (field.default_expr_value.is_null()) {
                continue;
            }
            ExprValue default_value = field.default_expr_value;
            if (field.default_value == "(current_timestamp())") {
                default_value = ExprValue::Now();
                default_value.cast_to(field.type);
            }
            if (0 != record->set_value(record->get_field_by_tag(field.id), default_value)) {
                DB_WARNING("fill insert value failed");
                return -1;
            }
        } else {
            if (0 != record->set_value(record->get_field_by_tag(field.id), iter->second)) {
                DB_WARNING("fill insert value failed");
                return -1;
            }
        }
    }

    int ret = write_binlog_record(record);
    if (ret != 0) {
        return -1;
    }

    return 0;
}

void Region::apply_binlog(const pb::StoreReq& request, braft::Closure* done) {
    std::unique_lock<bthread::Mutex> lck(_binlog_param_mutex);
    
    pb::OpType op_type = request.op_type();
    TimeCost cost;

    std::map<std::string, ExprValue> field_value_map;

    switch (op_type) {
        case pb::OP_PREWRITE_BINLOG: 
        case pb::OP_COMMIT_BINLOG: 
        case pb::OP_ROLLBACK_BINLOG: 
        case pb::OP_FAKE_BINLOG: {
            binlog_fill_exprvalue(request.binlog_desc(), op_type, field_value_map);
            break;
        }
        case pb::OP_UPDATE_OFFLINE_BINLOG: {
            update_offline_binlog_info(request, done);
            return;
        }
        default: {
            DB_FATAL("unsupport request type, op_type:%d, region_id: %ld", 
                    request.op_type(), _region_id);
            if (done != nullptr && ((BinlogClosure*)done)->response != nullptr) {
                ((BinlogClosure*)done)->response->set_errcode(pb::UNSUPPORT_REQ_TYPE); 
                ((BinlogClosure*)done)->response->set_errmsg("unsupport request type");
            }
            DB_NOTICE("op_type: %s, region_id: %ld, applied_index:%ld", 
                pb::OpType_Name(request.op_type()).c_str(), _region_id, _applied_index);
            break;
        }
    }

    if (field_value_map.size() > 0) {
        if (0 != write_binlog_value(field_value_map)) {
            if (done != nullptr && ((BinlogClosure*)done)->response != nullptr) {
                ((BinlogClosure*)done)->response->set_errcode(pb::PUT_VALUE_FAIL); 
                ((BinlogClosure*)done)->response->set_errmsg("write rocksdb fail");
            }
            DB_FATAL("write binlog failed, region_id: %ld", _region_id);
        }

        binlog_update_map_when_apply(field_value_map, done ? ((BinlogClosure*)done)->remote_side : "");
        binlog_update_check_point();
    } else {
        DB_FATAL("region_id: %ld field value invailde", _region_id);
    }

    if (done != nullptr && ((BinlogClosure*)done)->response != nullptr) {
        ((BinlogClosure*)done)->response->set_errcode(pb::SUCCESS); 
        ((BinlogClosure*)done)->response->set_errmsg("apply binlog success");
    }
    Store::get_instance()->dml_time_cost << cost.get_time();

}

BinlogReadMgr::BinlogReadMgr(int64_t region_id, int64_t begin_ts, const std::string& capture_ip, uint64_t log_id, int64_t need_read_cnt, bool is_read_offline_binlog) : 
    _region_id(region_id), _begin_ts(begin_ts), _capture_ip(capture_ip), _log_id(log_id), _need_read_cnt(need_read_cnt), _read_offline_binlog(is_read_offline_binlog) {
    _rocksdb = RocksWrapper::get_instance();
    _oldest_ts_in_binlog_cf = _rocksdb->get_oldest_ts_in_binlog_cf();
    // 暂时不用seek
    // int64_t interval_ms = tso::clock_realtime_ms() - (begin_ts >> tso::logical_bits);
    // _mode = interval_ms > FLAGS_binlog_use_seek_interval_min * 60000LL ? SEEK : MULTIGET;
    // _bacth_size = _mode == SEEK ? FLAGS_binlog_seek_batch : FLAGS_binlog_multiget_batch;
    _mode = MULTIGET;
    _bacth_size = FLAGS_binlog_multiget_batch;
    if (_oldest_ts_in_binlog_cf <= 0 || FLAGS_binlog_force_get) {
        _mode = GET;
    }
}
// for test only
BinlogReadMgr::BinlogReadMgr(int64_t region_id, GetMode mode) : _region_id(region_id), _mode(mode) {
    _rocksdb = RocksWrapper::get_instance();
    _oldest_ts_in_binlog_cf = _rocksdb->get_oldest_ts_in_binlog_cf();
    _bacth_size = _mode == SEEK ? FLAGS_binlog_seek_batch : FLAGS_binlog_multiget_batch;
}

int BinlogReadMgr::binlog_add_to_response(int64_t commit_ts, const std::string& binlog_value, pb::StoreRes* response) {
    if ((_total_binlog_size + binlog_value.size() < FLAGS_read_binlog_max_size_bytes && 
                    _time.get_time() < FLAGS_read_binlog_timeout_us) 
            || _is_first_binlog) {
        _is_first_binlog = false;
        _total_binlog_size += binlog_value.size();
        (*response->add_binlogs()) = binlog_value;
        response->add_commit_ts(commit_ts);
        if (_first_commit_ts == -1) {
            _first_commit_ts = commit_ts;
        }
        _last_commit_ts = commit_ts;
        _binlog_num++;
        return 0;
    } else {
        return -1;
    }
}

int BinlogReadMgr::multiget(std::map<int64_t, std::string>& start_binlog_map) {
    if (start_binlog_map.empty()) {
        return 0;
    }

    int batch = start_binlog_map.size();
    std::vector<std::string> keys_str;
    std::vector<rocksdb::Slice> keys;
    keys_str.reserve(batch);
    keys.reserve(batch);
    int64_t num_keys = 0;

    std::string prefix = "";
    if (_read_offline_binlog) {
        int64_t region_id_endian = KeyEncoder::to_endian_u64(
                                    KeyEncoder::encode_i64(_region_id));
        prefix.append((char*)&region_id_endian, sizeof(int64_t));
    }

    for (auto& iter : start_binlog_map) {
        std::string key = prefix;
        uint64_t ts_endian = KeyEncoder::to_endian_u64(
                                KeyEncoder::encode_i64(iter.first));
        key.append((char*)&ts_endian, sizeof(int64_t));
        keys_str.emplace_back(key);
        keys.emplace_back(keys_str[num_keys]);
        num_keys++;
        DB_DEBUG("start_ts: %ld", iter.first);
    }
    std::vector<rocksdb::PinnableSlice> values(num_keys);
    std::vector<rocksdb::Status> statuses(num_keys);
    rocksdb::ReadOptions option;
    option.fill_cache = true;
    TimeCost time;
    if (!_read_offline_binlog) {
        _rocksdb->get_db()->MultiGet(option, _rocksdb->get_bin_log_handle(), num_keys, keys.data(), values.data(), statuses.data(), true);
    } else {
        auto cold_db = _rocksdb->get_cold_db();
        auto cold_binlog_cf = _rocksdb->get_cold_binlog_handle();
        if (cold_db == nullptr || cold_binlog_cf == nullptr) {
            DB_FATAL("region_id: %ld read offline binlog data but cold db or cold binlog cf is nullptr", _region_id);
            return -1;
        }
        cold_db->MultiGet(option, cold_binlog_cf, num_keys, keys.data(), values.data(), statuses.data(), true);
    }
    RocksdbVars::get_instance()->rocksdb_multiget_time << time.get_time();
    RocksdbVars::get_instance()->rocksdb_multiget_count << keys.size();
    bool failed = false;
    for (int i = 0; i < num_keys; i++) {
        int ts_pos = 0;
        if (_read_offline_binlog) {
            ts_pos = sizeof(int64_t);
        } 
        int64_t start_ts = TableKey(keys[i]).extract_i64(ts_pos);
        if (!statuses[i].ok()) {
            failed = true;
            // raft leader 有可能在apply之后才写raftlog，此处有可能notfound，但是不影响数据正确性，返错之后capture会请求其他peer（follower不存在此问题）
            DB_WARNING("multiget failed, idx: %d, start_ts: %ld, status: %s", i, start_ts, statuses[i].ToString().c_str());
        } else {
            DB_DEBUG("multiget start_ts: %ld, value size: %ld", start_ts, values[i].size());
            start_binlog_map[start_ts].assign(values[i].data(), values[i].size());
        }
    }

    return failed ? -1 : 0;
}

int BinlogReadMgr::seek(std::map<int64_t, std::string>& start_binlog_map) {
    if (start_binlog_map.empty()) {
        return 0;
    }
    TimeCost time;
    auto map_iter = start_binlog_map.begin();

    int ts_pos = 0;
    std::string prefix = "";
    if (_read_offline_binlog) {
        int64_t region_id_endian = KeyEncoder::to_endian_u64(
                                    KeyEncoder::encode_i64(_region_id));
        prefix.append((char*)&region_id_endian, sizeof(int64_t));
        ts_pos = sizeof(int64_t);
    }

    std::string start_key = prefix;
    uint64_t start_endian = KeyEncoder::to_endian_u64(
                        KeyEncoder::encode_i64(map_iter->first));
    start_key.append((char*)&start_endian, sizeof(int64_t));

    std::string end_key = prefix;
    const uint64_t max = UINT64_MAX;
    end_key.append((char*)&max, sizeof(uint64_t));
    rocksdb::Slice upper_bound_slice = end_key;

    rocksdb::ReadOptions option;
    option.iterate_upper_bound = &upper_bound_slice;
    // option.prefix_same_as_start = true;
    option.total_order_seek = true;
    option.fill_cache = false;

    bool failed = false;
    int64_t begin_ts = map_iter->first;
    int64_t end_ts = 0;
    int seek_cnt = 0;
    int real_seek_cnt = 0;
    std::unique_ptr<rocksdb::Iterator> rocksdb_iter = nullptr;
    if (!_read_offline_binlog) {
        rocksdb_iter.reset(_rocksdb->new_iterator(option, _rocksdb->get_bin_log_handle()));
    } else {
        rocksdb_iter.reset(_rocksdb->new_cold_iterator(option, RocksWrapper::COLD_BINLOG_CF));
    }
    if (rocksdb_iter == nullptr) {
        DB_FATAL("region_id: %ld, seek binlog fail", _region_id);
        return -1;
    }
    rocksdb_iter->Seek(start_key);
    for (; rocksdb_iter->Valid() && map_iter != start_binlog_map.end(); rocksdb_iter->Next()) {
        ++seek_cnt;
        int64_t ts = TableKey(rocksdb_iter->key()).extract_i64(ts_pos);
        if (ts < map_iter->first) {
            DB_DEBUG("ts: %ld, < map start_ts: %ld, region_id: %ld", ts, map_iter->first, _region_id);
            continue;
        } else if (ts == map_iter->first) {
            map_iter->second = std::string(rocksdb_iter->value().data(), rocksdb_iter->value().size());
            end_ts = ts;
            DB_DEBUG("seek ts: %ld success", ts);
            ++map_iter;
            ++real_seek_cnt;
            continue;
        } else {
            // 不符合预期
            failed = true;
            DB_FATAL("ts: %ld > map start_ts: %ld, region_id: %ld", ts, map_iter->first, _region_id);
            break;
        }
    }

    if (map_iter != start_binlog_map.end() || failed) {
        DB_FATAL("seek failed: %d, region_id: %ld", failed, _region_id);
        return -1;
    } else {
        DB_WARNING("region_id: %ld, seek: [%ld => %ld], [%s => %s], seek_cnt: %d, real_seek_cnt: %d, time: %ld",
            _region_id, begin_ts, end_ts, ts_to_datetime_str(begin_ts).c_str(), ts_to_datetime_str(end_ts).c_str(), 
            seek_cnt, real_seek_cnt, time.get_time());
        return 0;
    }
}

// only used by offline binlog, read online binlog data
int BinlogReadMgr::get_prewrite_binlog(int64_t start_ts, std::map<int64_t, std::string>& start_binlog_map, bool& batch_finish, bool read_finished = false) {
    int ret = 0;
    batch_finish = false;
    if (!read_finished) {
        start_binlog_map[start_ts] = "";
        if (start_binlog_map.size() < _bacth_size) {
            return 0;
        }
    }
    if (_mode == GET) {
        for (auto& pair : start_binlog_map) {
            ret = _rocksdb->get_binlog_value(pair.first, start_binlog_map[pair.first]);
            if (ret != 0) {
                DB_WARNING("get ts:%ld from rocksdb binlog cf fail, region_id: %ld", pair.first, _region_id);         
                return -1;
            }
        }
    } else {
        if (_mode == MULTIGET) {
            ret = multiget(start_binlog_map);
        } else {
            ret = seek(_start_binlog_map);
        }
        if (ret != 0) {
            DB_WARNING("get binlog failed, region_id: %ld", _region_id);
            return -1;
        }
    } 
    batch_finish = true;
    return 0;
}

int BinlogReadMgr::get_binlog_value(int64_t commit_ts, int64_t start_ts, pb::StoreRes* response, int64_t binlog_row_cnt) {
    int ret = 0;
    if (commit_ts == start_ts) {
        _fake_binlog_cnt++;
    }
     
    _binlog_total_row_cnts += binlog_row_cnt;
    if (_mode == GET) {
        // 兼容模式，bin_log_new cf刚创建，从新老两个cf中查找，上线一个小时后此分支不会再走到
        std::string binlog_value;
        if (commit_ts == start_ts) {
            // fake binlog
            ret = fill_fake_binlog(commit_ts, binlog_value);
            if (ret != 0) {
                DB_WARNING("fill fake binlog fail, ts:%ld, region_id: %ld", start_ts, _region_id); 
                return -1;
            }
        } else {
            if (!_read_offline_binlog) {
                ret = _rocksdb->get_binlog_value(start_ts, binlog_value);
            } else {
                ret = _rocksdb->get_offline_binlog_value(_region_id, start_ts, binlog_value);
            }
            if (ret != 0) {
                DB_WARNING("get ts:%ld from rocksdb binlog cf fail, region_id: %ld", start_ts, _region_id);         
                return -1;
            }
        }

        if (0 != binlog_add_to_response(commit_ts, binlog_value, response)) {
            _finish = true;
            return 1;
        } else {
            return 0;
        }

    } else {
        // 获取近一个小时内的的binlog使用multiget
        _commit_start_map[commit_ts] = start_ts;
        if (commit_ts == start_ts) {
            // fake binlog
            std::string binlog_value;
            ret = fill_fake_binlog(commit_ts, binlog_value);
            if (ret != 0) {
                return -1;
            }
            _fake_binlog_map[start_ts] = binlog_value;
        } else {
            _start_binlog_map[start_ts] = "";
        }
        if (_commit_start_map.size() < _bacth_size) {
            return 0;
        }

        if (_mode == MULTIGET) {
            ret = multiget(_start_binlog_map);
        } else {
            ret = seek(_start_binlog_map);
        }
        if (ret != 0) {
            DB_WARNING("get binlog failed, region_id: %ld", _region_id);
            return -1;
        }

        for (const auto& iter : _commit_start_map) {
            if (iter.first == iter.second) {
                // fake binlog
                if (0 != binlog_add_to_response(iter.first, _fake_binlog_map[iter.second], response)) {
                    _finish = true;
                    return 1;
                }
            } else {
                if (0 != binlog_add_to_response(iter.first, _start_binlog_map[iter.second], response)) {
                    _finish = true;
                    return 1;
                }
            }
        }

        _commit_start_map.clear();
        _start_binlog_map.clear();
        _fake_binlog_map.clear();
    } 

    return 0;
}

int BinlogReadMgr::fill_fake_binlog(int64_t fake_ts, std::string& binlog) {
    pb::StoreReq fake_binlog;
    fake_binlog.set_op_type(pb::OP_FAKE_BINLOG);
    fake_binlog.set_region_id(_region_id);
    fake_binlog.set_region_version(1); // 对端不需要这个字段
    fake_binlog.mutable_binlog_desc()->set_binlog_ts(fake_ts);
    fake_binlog.mutable_binlog()->set_type(pb::FAKE);
    fake_binlog.mutable_binlog()->set_start_ts(fake_ts);
    fake_binlog.mutable_binlog()->set_commit_ts(fake_ts);

    binlog.clear();
    if (!fake_binlog.SerializeToString(&binlog)) {
        DB_FATAL("region_id: %ld serialize failed", _region_id);
        return -1;
    }

    return 0;
}

void BinlogReadMgr::print_log() {
    if (_binlog_total_row_cnts > 0 && _time.get_time() > FLAGS_print_time_us) {
        DB_WARNING("region_id[%ld], begin_ts[%ld, %s], total_binlog_size[%ld], mode[%d], need_read_cnt[%ld], fake_cnt[%ld], binlog_num[%d], binlog_row_cnt[%ld]"             
                "first_commit_ts[%ld, %s], last_commit_ts[%ld, %s], time[%ld], remote_capture_ip[%s], log_id[%lu] get binlog finish.",          
                _region_id, _begin_ts, ts_to_datetime_str(_begin_ts).c_str(), _total_binlog_size, _mode, _need_read_cnt, _fake_binlog_cnt,
                _binlog_num, _binlog_total_row_cnts, _first_commit_ts,                   
                ts_to_datetime_str(_first_commit_ts).c_str(), _last_commit_ts,                              
                ts_to_datetime_str(_last_commit_ts).c_str(), _time.get_time(), 
                _capture_ip.c_str(), _log_id);                     
    }
}

int BinlogReadMgr::get_binlog_finish(pb::StoreRes* response) {
    if (_finish || _mode == GET || (_start_binlog_map.empty() && _fake_binlog_map.empty())) {
        print_log();
        return 0;
    }

    int ret = 0;
    if (_mode == MULTIGET) {
        ret = multiget(_start_binlog_map);
    } else {
        ret = seek(_start_binlog_map);
    }
    if (ret != 0) {
        DB_WARNING("get binlog failed, region_id: %ld", _region_id);
        return -1;
    }

    for (const auto& iter : _commit_start_map) {
        if (iter.first == iter.second) {
            // fake binlog
            if (0 != binlog_add_to_response(iter.first, _fake_binlog_map[iter.second], response)) {
                break;
            }
        } else {
            if (0 != binlog_add_to_response(iter.first, _start_binlog_map[iter.second], response)) {
                break;
            }
        }
    }

    print_log();
    return 0;
}

int64_t Region::read_data_cf_oldest_ts() {
    int64_t begin_ts = 0;
    SmartTable binlog_table = _factory->get_table_info_ptr(get_table_id());
    SmartIndex binlog_pri   = _factory->get_index_info_ptr(get_table_id());
    SmartRecord left_record  = _factory->new_record(*binlog_table);
    SmartRecord right_record = _factory->new_record(*binlog_table);
    if (left_record == nullptr || right_record == nullptr) {
        return -1;
    }

    ExprValue value;
    value.type = pb::INT64;
    value._u.int64_val = begin_ts;
    left_record->set_value(left_record->get_field_by_tag(1), value);
    right_record->decode("");

    IndexRange range(left_record.get(), right_record.get(), binlog_pri.get(), binlog_pri.get(),
                        &_region_info, 1, 0, false, false, false);

    std::map<int32_t, FieldInfo*> field_ids;
    std::vector<int32_t> field_slot;
    binlog_get_scan_fields(field_ids, field_slot, binlog_table, binlog_pri);

    TableIterator* table_iter = Iterator::scan_primary(nullptr, range, field_ids, field_slot, false, true);
    if (table_iter == nullptr) {
        DB_WARNING("open TableIterator fail, table_id:%ld", get_table_id());
        return -1;
    }

    ON_SCOPE_EXIT(([this, table_iter]() {
        delete table_iter;
    }));

    SmartRecord record = _factory->new_record(*binlog_table);
    int ret = 0;
    record->clear();
    if (!table_iter->valid()) {
        DB_WARNING("region_id: %ld table_iter is invalid", _region_id);
        return -1;
    }

    ret = table_iter->get_next(record);
    if (ret < 0) {
        DB_WARNING("region_id: %ld get_next failed", _region_id);
        return -1;
    }

    std::map<std::string, ExprValue> field_value_map;
    binlog_get_field_values(field_value_map, record, binlog_table);
    int64_t ts = binlog_get_int64_val("ts", field_value_map);
    return ts;
}

bool Region::flash_back_need_read(const pb::StoreReq* request, const std::map<std::string, ExprValue>& field_value_map) {
    std::set<std::string> req_db_tables;
    if (request->binlog_desc().db_tables_size() > 0) {
        for (const std::string& db_table : request->binlog_desc().db_tables()) {
            req_db_tables.insert(db_table);
        }
    }
    std::set<uint64_t> req_signs;
    if (request->binlog_desc().signs_size() > 0) {
        for (const uint64_t sign : request->binlog_desc().signs()) {
            req_signs.insert(sign);
        }
    }
    std::set<int64_t> req_txn_ids;
    if (request->binlog_desc().txn_ids_size() > 0) {
        for (const uint64_t txn_id : request->binlog_desc().txn_ids()) {
            req_txn_ids.insert(txn_id);
        }
    }

    if (request->binlog_desc().has_user_name()) {
        std::string user_name = request->binlog_desc().user_name();
        if (binlog_get_str_val("user_name", field_value_map) != user_name) {
            return false;
        }
    }
    if (request->binlog_desc().has_user_ip()) {
        std::string user_ip = request->binlog_desc().user_ip();
        if (binlog_get_str_val("user_ip", field_value_map) != user_ip) {
            return false;
        }
    }
    if (!req_db_tables.empty()) {
        std::string local_db_tables = binlog_get_str_val("db_tables", field_value_map);
        if (local_db_tables.empty()) {
            return false;
        }
        bool find = false;
        std::vector<std::string> vec;
        boost::split(vec, local_db_tables, boost::is_any_of(";"));
        for (const std::string& db_table : vec) {
            if (req_db_tables.count(db_table) > 0) {
                find = true;
                break;
            }
        }

        if (!find) {
            return false;
        }
    }
    if (!req_signs.empty()) {
        std::string local_signs = binlog_get_str_val("signs", field_value_map);
        if (local_signs.empty()) {
            return false;
        }
        bool find = false;
        std::vector<std::string> vec;
        boost::split(vec, local_signs, boost::is_any_of(";"));
        for (const std::string& sign_str : vec) {
            if (req_signs.count(boost::lexical_cast<uint64_t>(sign_str)) > 0) {
                find = true;
                break;
            }
        }

        if (!find) {
            return false;
        }

    }
    if (!req_txn_ids.empty()) {
        int64_t txn_id = binlog_get_int64_val("txn_id", field_value_map);
        if (req_txn_ids.count(txn_id) <= 0) {
            return false;
        }
    }

    return true;

}

void Region::read_binlog(const pb::StoreReq* request,
                   pb::StoreRes* response, const std::string& remote_side, 
                   uint64_t log_id) {

    SmartTable binlog_table = _factory->get_table_info_ptr(get_table_id());
    SmartIndex binlog_pri = _factory->get_index_info_ptr(get_table_id());
    TimeCost timecost;
    int64_t binlog_cnt = request->binlog_desc().read_binlog_cnt();
    int64_t begin_ts = request->binlog_desc().binlog_ts();
    _binlog_alarm.check_read_ts(remote_side, _region_id, begin_ts);
    DB_DEBUG("read_binlog request %s", request->ShortDebugString().c_str());
    int64_t check_point_ts = 0;
    int64_t oldest_ts = 0;
    bool is_read_offline_binlog = request->binlog_desc().read_offline_binlog();
    if (!is_read_offline_binlog) {
        // 获取check point时应加锁，避免被修改
        std::unique_lock<bthread::Mutex> lck(_binlog_param_mutex);

        check_point_ts = _binlog_param.check_point_ts;
        if (check_point_ts < 0) {
            DB_FATAL("region_id: %ld, get check point failed", _region_id);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("get binlog check point ts");
            return;
        }

        oldest_ts = std::max(_rocksdb->get_oldest_ts_in_binlog_cf(), _binlog_param.oldest_ts);
        if (oldest_ts < 0) {
            DB_FATAL("region_id: %ld, get oldest ts failed", _region_id);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("get binlog oldest ts failed");
            return;
        }
    } else {
        BAIDU_SCOPED_LOCK(_offline_binlog_param_mutex);
        if (0 == _offline_binlog_param.oldest_ts || 0 == _offline_binlog_param.newest_ts) {
            DB_FATAL("region_id: %ld, begin_ts: %ld, store has no offline binlog: [%ld, %ld]", 
                    _region_id, begin_ts, _offline_binlog_param.oldest_ts, _offline_binlog_param.newest_ts);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("store has no offline binlog");
            return;
        }
        if (begin_ts < _offline_binlog_param.oldest_ts || begin_ts > _offline_binlog_param.newest_ts) {
            DB_FATAL("region_id: %ld, begin_ts: %ld not in offline ts: [%ld, %ld]", 
                    _region_id, begin_ts, _offline_binlog_param.oldest_ts, _offline_binlog_param.newest_ts);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("begin ts not in offline binlog ts range");
            return;
        }
        oldest_ts = _offline_binlog_param.oldest_ts;
        check_point_ts = _offline_binlog_param.newest_ts;
    }
    if (begin_ts == 0) {
        DB_FATAL("region_id: %ld, begin_ts is 0", _region_id);
        response->set_errcode(pb::GET_VALUE_FAIL); 
        response->set_errmsg("get binlog check point ts");
        return;
    }

    if (begin_ts < oldest_ts) {
        DB_WARNING("region_id: %ld, begin ts : %ld < oldest ts : %ld", _region_id, begin_ts, oldest_ts);
        response->set_errcode(pb::LESS_THAN_OLDEST_TS); 
        response->set_errmsg("begin ts gt oldest ts");
        return;
    }

    if (begin_ts >= check_point_ts) {
        // 返回空
        response->set_errcode(pb::SUCCESS); 
        response->set_errmsg("begin_ts >= check_point_ts");
        return;
    }

    SmartRecord left_record  = _factory->new_record(*binlog_table);
    SmartRecord right_record = _factory->new_record(*binlog_table);
    if (left_record == nullptr || right_record == nullptr) {
        response->set_errcode(pb::GET_VALUE_FAIL); 
        response->set_errmsg("get index info failed");
        return;
    }

    ExprValue value;
    value.type = pb::INT64;
    value._u.int64_val = begin_ts;
    left_record->set_value(left_record->get_field_by_tag(1), value);
    right_record->decode("");

    IndexRange range(left_record.get(), right_record.get(), binlog_pri.get(), binlog_pri.get(),
                        &_region_info, 1, 0, false, false, false);

    std::map<int32_t, FieldInfo*> field_ids;
    std::vector<int32_t> field_slot;

    binlog_get_scan_fields(field_ids, field_slot, binlog_table, binlog_pri);

    TableIterator* table_iter = Iterator::scan_binlog_primary(range, field_ids, field_slot, is_read_offline_binlog);
    if (table_iter == nullptr) {
        DB_WARNING("open TableIterator fail, table_id:%ld", get_table_id());
        return;
    }

    ON_SCOPE_EXIT(([this, table_iter]() {
        delete table_iter;
    }));

    int64_t max_fake_binlog = 0;
    std::map<std::string, ExprValue> field_value_map;
    SmartRecord record = _factory->new_record(*binlog_table);
    BinlogReadMgr binlog_reader(_region_id, begin_ts, remote_side, log_id, binlog_cnt, is_read_offline_binlog);
    int ret = 0;
    while (1) {
        record->clear();
        if (!table_iter->valid()) {
            // DB_WARNING("region_id: %ld not valid", _region_id);
            break;
        }

        ret = table_iter->get_next(record);
        if (ret < 0) {
            DB_WARNING("region_id: %ld get_next failed", _region_id);
            break;
        }
        std::map<std::string, ExprValue> field_value_map;
        binlog_get_field_values(field_value_map, record, binlog_table);
        int64_t ts = binlog_get_int64_val("ts", field_value_map); // type 为 COMMIT 时，ts 为 commit_ts
        BinlogType binlog_type = static_cast<BinlogType>(binlog_get_int64_val("binlog_type", field_value_map));
        int64_t start_ts = binlog_get_int64_val("start_ts", field_value_map);
        int64_t binlog_row_cnt = binlog_get_int64_val("binlog_row_cnt", field_value_map);
        if (binlog_row_cnt <= 0) {
            binlog_row_cnt = 1;
        }
        if (ts < begin_ts || ts > check_point_ts) {
            DB_WARNING("region_id: %ld, ts: %ld, begin_ts: %ld, check_point_ts: %ld", _region_id, ts, begin_ts, check_point_ts);
            break;
        }

        if (binlog_type == FAKE_BINLOG && ts > begin_ts) {
            ret = binlog_reader.get_binlog_value(ts, ts, response, 1);
            if (ret < 0) {
                DB_WARNING("get fake binlog ts:%ld fail, region_id: %ld", ts, _region_id);
                response->set_errcode(pb::GET_VALUE_FAIL); 
                response->set_errmsg("read fake binlog failed");  
                return;
            }
            continue;
        }

        if (binlog_type != COMMIT_BINLOG || ts == begin_ts) {
            continue;
        }

        // SQL闪回读取时过滤
        if ((request->binlog_desc().flash_back_read() || request->binlog_desc().read_offline_binlog())
                 && !flash_back_need_read(request, field_value_map)) {
            continue;
        }

        if (start_ts < oldest_ts) {
            DB_WARNING("region_id: %ld, start ts : %ld < oldest ts : %ld", _region_id, start_ts, oldest_ts);
            //特殊错误码 TODO
            response->set_errcode(pb::LESS_THAN_OLDEST_TS); 
            response->set_errmsg("start ts gt oldest ts");
            return;
        }
        
        if (binlog_cnt < 0) {
            break;
        }
        binlog_cnt -= binlog_row_cnt;
        ret = binlog_reader.get_binlog_value(ts, start_ts, response, binlog_row_cnt);
        if (ret < 0) {
            DB_WARNING("get ts:%ld from rocksdb binlog cf fail, region_id: %ld", start_ts, _region_id);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("read binlog failed");  
            return;
        } else if (ret == 1) {
            break;
        }
    }

    ret = binlog_reader.get_binlog_finish(response);
    if (ret < 0) {
        DB_WARNING("get value from rocksdb binlog cf fail, region_id: %ld", _region_id);
        response->set_errcode(pb::GET_VALUE_FAIL); 
        response->set_errmsg("read binlog failed");  
        return;
    }

    response->set_errcode(pb::SUCCESS); 
    response->set_errmsg("read binlog success");    
}

// 强制往前推进check point, 删除map中首个binlog
void Region::recover_binlog() {
    std::unique_lock<bthread::Mutex> lck(_binlog_param_mutex);

    // 重置，将checkpoint强制推进到当前最大ts
    _binlog_param.check_point_ts = _binlog_param.max_ts_applied;
    _binlog_param.ts_binlog_map.clear();
    _meta_writer->write_binlog_check_point(_region_id, _binlog_param.check_point_ts);

    DB_WARNING("region_id: %ld force recover binlog, check point [%ld, %s], oldest ts [%ld, %s]", _region_id, 
        _binlog_param.check_point_ts, ts_to_datetime_str(_binlog_param.check_point_ts).c_str(), 
        _binlog_param.oldest_ts, ts_to_datetime_str(_binlog_param.oldest_ts).c_str());
}

inline bool can_follower(const pb::OpType& type) {
    return type == pb::OP_READ_BINLOG 
            || type == pb::OP_RECOVER_BINLOG
            || type == pb::OP_QUERY_BINLOG
            || type == pb::OP_QUERY_OFFLINE_BINLOG;
}

void Region::query_binlog_ts(const pb::StoreReq* request,
                   pb::StoreRes* response) {
    int64_t check_point_ts = 0;
    int64_t oldest_ts = 0;
    {
        // 获取check point时应加锁，避免被修改
        std::unique_lock<bthread::Mutex> lck(_binlog_param_mutex);
        check_point_ts = _binlog_param.check_point_ts;
        oldest_ts = _binlog_param.oldest_ts;
    }

    int64_t begin_ts = 0;
    int64_t region_oldest_ts = _meta_writer->read_binlog_oldest_ts(_region_id);
    int64_t binlog_cf_oldest_ts = RocksWrapper::get_instance()->get_oldest_ts_in_binlog_cf();
    int64_t data_cf_oldest_ts = read_data_cf_oldest_ts();
    auto binlog_info = response->mutable_binlog_info();
    binlog_info->set_region_id(request->region_id());
    binlog_info->set_check_point_ts(check_point_ts);
    binlog_info->set_oldest_ts(std::max(std::max(oldest_ts, binlog_cf_oldest_ts), region_oldest_ts));
    binlog_info->set_region_oldest_ts(region_oldest_ts);
    binlog_info->set_binlog_cf_oldest_ts(binlog_cf_oldest_ts);
    binlog_info->set_data_cf_oldest_ts(data_cf_oldest_ts);
    response->set_errcode(pb::SUCCESS);
    return;
}


void Region::query_binlog(google::protobuf::RpcController* controller,
                   const pb::StoreReq* request,
                   pb::StoreRes* response,
                   google::protobuf::Closure* done) {
    _multi_thread_cond.increase();
    ON_SCOPE_EXIT([this]() {
        _multi_thread_cond.decrease_signal();
    });
    _time_cost.reset();
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller;
    uint64_t log_id = 0;
    if (cntl->has_log_id()) { 
        log_id = cntl->log_id();
    }

    const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
    const char* remote_side     = remote_side_tmp.c_str();
    if ((!is_leader()) && (!can_follower(request->op_type()) || _shutdown || !_init_success)) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response->set_errmsg("not leader");
        DB_WARNING("not leader, leader:%s, region_id: %ld, log_id:%lu, remote_side:%s",
                        butil::endpoint2str(_node.leader_id().addr).c_str(), 
                        _region_id, log_id, remote_side);
        return;
    }

    response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str()); // 每次都返回leader
    if (validate_version(request, response) == false) {
        DB_WARNING("region version too old, region_id: %ld, log_id:%lu,"
                   " request_version:%ld, region_version:%ld optype:%s remote_side:%s",
                    _region_id, log_id, request->region_version(), _region_info.version(),
                    pb::OpType_Name(request->op_type()).c_str(), remote_side);
        return;
    }

    // 启动时，或者follow落后太多，需要读leader
    if (request->op_type() == pb::OP_READ_BINLOG && request->region_version() > _region_info.version()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
        response->set_errmsg("not leader");
        DB_WARNING("not leader, leader:%s, region_id: %ld, version:%ld, log_id:%lu, remote_side:%s",
                        butil::endpoint2str(_node.leader_id().addr).c_str(), 
                        _region_id, _region_info.version(), log_id, remote_side);
        return;
    }

    switch (request->op_type()) {
        case pb::OP_READ_BINLOG: {
            read_binlog(request, response, std::string(remote_side), log_id);
            break;
        }
        case pb::OP_QUERY_OFFLINE_BINLOG: {
            query_offline_binlog_info(response);
            break;
        }
        case pb::OP_RECOVER_OFFLINE_BINLOG: {
            recover_offline_binlog_info(request, response);
            break;
        }
        case pb::OP_QUERY_BINLOG: {
            query_binlog_ts(request, response);
            break;
        }
        case pb::OP_RECOVER_BINLOG: {
            recover_binlog();
            response->set_errcode(pb::SUCCESS); 
            response->set_errmsg("recover binlog success"); 
            break;
        }
        case pb::OP_PREWRITE_BINLOG:
        case pb::OP_COMMIT_BINLOG:
        case pb::OP_ROLLBACK_BINLOG:
        case pb::OP_FAKE_BINLOG: {
            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper(&data);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
                return;
            }

            BinlogClosure* c = new BinlogClosure;
            c->remote_side = butil::endpoint2str(cntl->remote_side()).c_str();
            c->cost.reset();
            c->response = response;
            c->done = done_guard.release();
            braft::Task task;
            task.data = &data;
            task.done = c;
            _node.apply(task);
            break;
        }
        default:
            response->set_errcode(pb::UNSUPPORT_REQ_TYPE);
            response->set_errmsg("unsupport request type");
            DB_WARNING("not support op_type when binlog request, op_type:%s region_id: %ld, log_id:%lu",
                        pb::OpType_Name(request->op_type()).c_str(), _region_id, log_id);
    }
    return;
}


/*
 * offline binlog sst writer
 */ 
enum OfflineBinlogType {
    DATA = 0,
    BINLOG
};

void print_external_binlog_info(const rocksdb::ExternalSstFileInfo& info) {
    TableKey smallestkey(info.smallest_key);
    TableKey largestkey(info.largest_key);
    int64_t smallest_region_id = smallestkey.extract_i64(0);
    int64_t smallest_ts  = smallestkey.extract_u64(sizeof(int64_t));
    int64_t largest_region_id  = largestkey.extract_i64(0);
    int64_t largest_ts   = largestkey.extract_i64(sizeof(int64_t));

    DB_NOTICE("binlog ExternalSstFileInfo[file_path: %s]; "
        "smallestkey[region_id: %ld ts: %ld]; largestkey[region_id: %ld ts: %ld]; "
        "smallest_range_del_key: %s, largest_range_del_key: %s; "
        "sequence_number: %lu, file_size: %lu, num_entries: %lu, num_range_del_entries: %lu, version: %d",
        info.file_path.c_str(), 
        smallest_region_id, smallest_ts, largest_region_id, largest_ts,
        info.smallest_range_del_key.c_str(), info.largest_range_del_key.c_str(), 
        info.sequence_number, info.file_size, info.num_entries, info.num_range_del_entries, info.version);
}

class OfflineBinlogSstWriter {
public:
    OfflineBinlogSstWriter(int64_t table_id, int64_t region_id, OfflineBinlogType type, 
                           int64_t start_ts, int64_t end_ts) : 
                            _table_id(table_id), _region_id(region_id), _type(type), 
                            _start_ts(start_ts), _end_ts(end_ts) {
        _tmp_file_name.clear();
        _external_files.clear();
        if (FLAGS_offline_binlog_size_peer_sst > 0) {
            _size_peer_sst = FLAGS_offline_binlog_size_peer_sst;
        }
    }
    ~OfflineBinlogSstWriter() {
        if (!_tmp_file_name.empty()) {
            delete_file(_tmp_file_name);
        }
    }
    int open_writer();
    int write_kv(const rocksdb::Slice& key, const rocksdb::Slice& value);
    int write_binlog_batch(std::map<int64_t, std::string>& start_binlog_map);
    int finish();

    std::vector<std::string> external_files() {
        return _external_files;
    }
private:
    void delete_file(const std::string& file_name) {
        butil::FilePath file_path(file_name);
        if (butil::PathExists(file_path)) {
            butil::DeleteFile(file_path, true);
        }
    }
    OfflineBinlogType _type;
    const int64_t _table_id;
    const int64_t _region_id;
    const int64_t _start_ts;
    const int64_t _end_ts;
    int64_t _count_per_sst = INT64_MAX;
    TimeCost _cost;
    uint64_t _write_count = 0;
    std::string _tmp_file_name;
    std::unique_ptr<SstFileWriter> _writer = nullptr;
    std::vector<std::string> _external_files;
    int64_t _size_peer_sst = INT64_MAX;
};

int OfflineBinlogSstWriter::open_writer() {
    rocksdb::Options option = RocksWrapper::get_instance()->get_cold_options();
    option.env = rocksdb::Env::Default();
    _writer.reset(new SstFileWriter(option, false));
    if (_writer == nullptr) {
        DB_FATAL("region_id: %ld backup task fail, SstFileWriter is nullptr", _region_id);
        return -1;
    }
    _write_count = 0;
    _cost.reset();
    // regionID_startTS_endTS_idx_now().type
    _tmp_file_name = std::to_string(_region_id) 
                    + "_" + std::to_string(_start_ts) + "_" + std::to_string(_end_ts) 
                    + "_" + std::to_string(_external_files.size())
                    + "_" + std::to_string(butil::gettimeofday_us());
    if (_type == OfflineBinlogType::DATA) {
        _tmp_file_name += ".data";
    } else if (_type == OfflineBinlogType::BINLOG) {
        _tmp_file_name += ".binlog";
    }
    butil::FilePath file_path(_tmp_file_name);
    if (butil::PathExists(file_path)) {
        DB_FATAL("tmp file: %s exists", _tmp_file_name.c_str());
        return -1;
    } 
    rocksdb::Status s = _writer->open(_tmp_file_name);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld open sst file: %s fail", _region_id, _tmp_file_name.c_str());
        return -1;
    }
    return 0;
}

int OfflineBinlogSstWriter::write_kv(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    if (_writer == nullptr) {
        if (open_writer() != 0) {
            return -1;
        }
    }
    rocksdb::Status s = _writer->put(key, value);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld write kv fail", _region_id);
        return -1;
    }
    if (++_write_count % 1000 == 0 && _writer->file_size() >= _size_peer_sst) {
        return finish();
    }
    return 0;
}

int OfflineBinlogSstWriter::write_binlog_batch(std::map<int64_t, std::string>& start_binlog_map) {
    for (const auto& pair : start_binlog_map) {
        MutTableKey key;
        key.append_i64(_region_id).append_i64(pair.first);
        int ret = write_kv(key.data(), pair.second);
        if (ret < 0) {
            DB_FATAL("region_id: %ld write kv failed, ts: %ld", _region_id, pair.first);
            return -1;
        }
    }
    start_binlog_map.clear();
    return 0;
}

int OfflineBinlogSstWriter::finish() {
    if (_writer == nullptr) {
        return 0;
    }

    rocksdb::ExternalSstFileInfo sst_info;
    rocksdb::Status s = _writer->finish(&sst_info);
    if (!s.ok()) {
        DB_FATAL("region_id: %ld finish file: %s fail", _region_id, _tmp_file_name.c_str());
        return -1;
    }
    if (_type == OfflineBinlogType::DATA) {
        print_external_info(sst_info);
    } else {
        print_external_binlog_info(sst_info);
    }

    uint64_t size = _writer->file_size();
    if (size != sst_info.file_size) {
        DB_FATAL("file: %s diff size %lu vs %lu", _tmp_file_name.c_str(), size, sst_info.file_size);
        return -1;
    }
    _writer.reset(nullptr);

    std::vector<std::string> vec;
    vec.reserve(2);
    boost::split(vec, _tmp_file_name, boost::is_any_of("."));
    if (vec.empty()) {
        DB_FATAL("split %s failed", _tmp_file_name.c_str());
        return -1;
    }
    
    std::string external_file;
    // offline_binlog/meta/tableID/regionID_startTS_endTS_idx_now()_size_lines.(binlogsst/datasst)
    std::string user_define_path = "offline_binlog/" + FLAGS_meta_server_bns + "/" + std::to_string(_table_id) 
                                   + "/" + vec[0] + "_" + std::to_string(size) + "_" + std::to_string(_write_count);
    if (_type == OfflineBinlogType::DATA) {
        user_define_path += ".datasst";
    } else {
        user_define_path += ".binlogsst";
    }
    int ret = copy_file(_tmp_file_name, user_define_path, external_file, size);
    if (ret < 0) {
        DB_FATAL("copy file: %s failed", _tmp_file_name.c_str());
        return -1;
    }
    // 成功在此处删除临时文件, write/finish失败的临时文件在析构函数里面删除
    delete_file(_tmp_file_name);
    _external_files.emplace_back(external_file);
    DB_NOTICE("local file: %s external_file: %s size: %lu cost: %ld", _tmp_file_name.c_str(), external_file.c_str(), size, _cost.get_time());
    _tmp_file_name.clear();
    return 0;
}

/*
 *  offline binlog about
 */ 
struct OfflineSSTInfo {
    int64_t region_id = 0;
    int64_t start_ts = 0;
    int64_t end_ts = 0;
    OfflineBinlogType type;
    std::string remote_sst_path;

    int parse_remote_offline_binlog_sst(std::string external_file) {
        clear();
        remote_sst_path = external_file;
        std::vector<std::string> split_vec;
        boost::split(split_vec, external_file, boost::is_any_of("/"));
        if (split_vec.empty()) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }

        std::vector<std::string> vec;
        vec.reserve(8);
        boost::split(vec, split_vec.back(), boost::is_any_of("._"));
        if (vec.empty()) {
            DB_FATAL("split %s failed", external_file.c_str());
            return -1;
        }
        if (vec.back() == "binlogsst" || vec.back() == "datasst") {
            // backup binlog sst: regionID_startTS_endTS_idx_now()_size_lines.(binlogsst/datasst)
            if (vec.back() == "binlogsst") {
                type = OfflineBinlogType::BINLOG;
            } else {
                type = OfflineBinlogType::DATA;
            }
            if (vec.size() != 8) {
                DB_FATAL("split %s failed", external_file.c_str());
                return -1;
            }
            region_id = boost::lexical_cast<uint64_t>(vec[0]);
            start_ts = boost::lexical_cast<uint64_t>(vec[1]);
            end_ts = boost::lexical_cast<uint64_t>(vec[2]);
        } else if (vec.back() != "extsst") {
            DB_FATAL("not offline binlog file: %s with abnormal file type: %s", external_file.c_str(), vec.back().c_str());
            return -1;
        }
        return 0;
    }

    void clear() {
        region_id = 0;
        start_ts = 0;
        end_ts = 0;
        remote_sst_path.clear();
    }
};

void Region::query_offline_binlog_info(pb::StoreRes* response) {
    int64_t oldest_ts = 0;
    int64_t newest_ts = 0;
    std::set<std::string> valid_sst;
    {
        BAIDU_SCOPED_LOCK(_offline_binlog_param_mutex);
        oldest_ts = _offline_binlog_param.oldest_ts;
        newest_ts = _offline_binlog_param.newest_ts;
        for (const auto& f : _offline_binlog_param.data_ssts) {
            valid_sst.insert(f.second);
        }
        for (const auto& f : _offline_binlog_param.binlog_ssts) {
            valid_sst.insert(f.second);
        }
    }
    // rocksdb元信息
    std::vector<rocksdb::LiveFileMetaData> metadata;
    _rocksdb->get_cold_live_files(&metadata);
    // sstLinker元信息
    std::map<std::string, SstExtLinker::ExtFileInfo> sst_ext_map;
    SstExtLinker::get_instance()->sst_ext_map(sst_ext_map);

    // check
    OfflineSSTInfo info;
    for (const auto& sst : metadata) {
        std::string shortname = sst.relative_filename;
        if (sst_ext_map.count(shortname) == 0) {
            DB_FATAL("offline binlog sst: %s cannot find in SstExtLinker", shortname.c_str());
            continue;
        }
        std::string remote_file = sst_ext_map[shortname].full_name;
        int ret = info.parse_remote_offline_binlog_sst(remote_file);
        if (ret != 0 || info.region_id != _region_id) {
            continue;
        }
        print_metadata_info(sst);
        if (valid_sst.count(remote_file) == 0) {
            DB_WARNING("offline binlog sst: %s, remote: %s cannot find in meta_cf",
                       shortname.c_str(), remote_file.c_str());
            valid_sst.insert(remote_file);
            continue;
        }
    }
    if (response != nullptr) {
        auto info = response->mutable_extra_res()->mutable_offline_binlog_info();
        info->set_oldest_ts(oldest_ts);
        info->set_newest_ts(newest_ts);
        for (const auto& f : valid_sst) {
            info->add_external_full_path(f);
        }
        response->set_errcode(pb::SUCCESS);
    }
    return;
}

int Region::restore_offline_binlog_info_on_snapshot_load(bool need_ingest_sst) {
    TimeCost cost;
    pb::RegionOfflineBinlogInfo offline_binlog_info;
    int ret = _meta_writer->read_region_offline_binlog_info(_region_id, offline_binlog_info);
    if (ret < 0) {
        DB_FATAL("region_id: %ld read offline binlog info failed", _region_id);
        return -1;
    } 
    OfflineSSTInfo info;
    std::set<std::string> all_ssts;
    std::vector<std::string> data_ssts;
    std::vector<std::string> binlog_ssts;
    data_ssts.reserve(5);
    binlog_ssts.reserve(5);
    {
        BAIDU_SCOPED_LOCK(_offline_binlog_param_mutex);
        _offline_binlog_param.oldest_ts = offline_binlog_info.oldest_ts();
        _offline_binlog_param.newest_ts = offline_binlog_info.newest_ts();
        for (const auto& f : offline_binlog_info.external_full_path()) {
            ret = info.parse_remote_offline_binlog_sst(f);
            if (ret != 0 || info.region_id != _region_id) {
                continue;
            }
            if (info.type == OfflineBinlogType::DATA) {
                data_ssts.emplace_back(f);
                _offline_binlog_param.data_ssts.emplace(info.end_ts, f);
            } else {
                binlog_ssts.emplace_back(f);
                _offline_binlog_param.binlog_ssts.emplace(info.end_ts, f);
            }   
        }
    }
    if (need_ingest_sst) {
        ret = ingest_offline_binlog_sst(data_ssts, binlog_ssts);
        if (ret < 0) {
            DB_FATAL("region_id: %ld ingest offline binlog sst failed", _region_id);
            return -1;
        }
    }
    DB_NOTICE("region_id: %ld RegionOfflineBinlogInfo: %s cost: %ld", 
              _region_id, offline_binlog_info.ShortDebugString().c_str(), cost.get_time());
    return 0;
}


int Region::ingest_offline_binlog_sst(const std::vector<std::string>& data_ssts, 
                                      const std::vector<std::string>& binlog_ssts) {
    TimeCost cost;
    if (!data_ssts.empty()) {
        rocksdb::Status s = _rocksdb->ingest_offline_binlog_sst(data_ssts, false);
        if (!s.ok()) {
            DB_FATAL("region_id: %ld ingest data_ssts failed, err: %s, cost: %ld", _region_id, s.ToString().c_str(), cost.get_time());
            return -1;
        }
    }
    if (!binlog_ssts.empty()) {
        rocksdb::Status s = _rocksdb->ingest_offline_binlog_sst(binlog_ssts, true);
        if (!s.ok()) {
            DB_FATAL("region_id: %ld ingest binlog_ssts failed, err: %s, cost: %ld", _region_id, s.ToString().c_str(), cost.get_time());
            return -1;
        }
    }
    DB_WARNING("region_id: %ld ingest data_sst cnt: %ld, binlog_sst cnt: %ld finish, cost: %ld", 
            _region_id, data_ssts.size(), binlog_ssts.size(), cost.get_time());
    return 0;
}

void Region::update_offline_binlog_info(const pb::StoreReq& request, braft::Closure* done) {
    TimeCost cost;
    int ret = 0;
    const pb::RegionOfflineBinlogInfo& pb_info = request.extra_req().offline_binlog_info();
    int64_t oldest_ts = _offline_binlog_param.oldest_ts;
    int64_t newest_ts = _offline_binlog_param.newest_ts;
    // 每次构造一个新的
    OfflineBinlogParam new_offline_info;
    // check
    if (newest_ts != 0 && newest_ts != pb_info.task_start_ts()) {
        DB_FATAL("region_id: %ld offline binlog not continuous, now ts is [%ld, %ld], task ts is [%ld, %ld]",
        _region_id, _binlog_param.oldest_ts, newest_ts, pb_info.task_start_ts(), pb_info.task_end_ts());
    }
    if (pb_info.oldest_ts() == 0 && pb_info.newest_ts() == 0) {
        RegionControl::remove_cold_data(_region_id);
        RegionControl::remove_cold_binlog(_region_id);
    } else {
        // cold_data_cf, cold_binlog_cf: 每个binlog region删自己的无效数据
        ret = RegionControl::remove_expired_offline_data(_region_id, _table_id, pb_info.oldest_ts(), newest_ts);
        if (ret != 0) {
            IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "ttl expired data failed");
            return;
        }
    }

    OfflineSSTInfo sst_info;
    std::vector<std::string> data_sst_files;
    std::vector<std::string> binlog_sst_files;
    data_sst_files.reserve(5);
    binlog_sst_files.reserve(5);
    
    for (const auto& file : pb_info.external_full_path()) {
        ret = sst_info.parse_remote_offline_binlog_sst(file);
        if (ret != 0 || sst_info.region_id != _region_id) {    
            IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "parse sst path failed");
            return;
        }
        if (sst_info.type == OfflineBinlogType::DATA) {
            new_offline_info.data_ssts.emplace(sst_info.end_ts, file);
            if (sst_info.start_ts >= newest_ts) {
                data_sst_files.emplace_back(file);
            }
        } else {
            new_offline_info.binlog_ssts.emplace(sst_info.end_ts, file);
            if (sst_info.start_ts >= newest_ts) {
                binlog_sst_files.emplace_back(file);
            }
        }
    }
    // ingest cold data cf
    ret = ingest_offline_binlog_sst(data_sst_files, binlog_sst_files);
    if (ret < 0) {
        // 可能binlog_cf ingest失败, data_cf ingest成功, 重新删除离线binlog ts范围外的数据
        RegionControl::remove_expired_offline_data(_region_id, _table_id, pb_info.oldest_ts(), newest_ts);
        IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "ingest cold data sst failed");
        DB_FATAL("region_id: %ld, ingest cold binlog sst failed, task_ts: [%ld, %ld]", 
                _region_id, pb_info.task_start_ts(), pb_info.task_end_ts()); 
        return;
    }

    // sync meta_cf
    ret = _meta_writer->write_region_offline_binlog_info(_region_id, pb_info);
    if (ret < 0) {
        RegionControl::remove_expired_offline_data(_region_id, _table_id, pb_info.oldest_ts(), newest_ts);
        IF_DONE_SET_RESPONSE(done, pb::EXEC_FAIL, "write metainfo to rocksdb failed");
        return;
    }

    // update _offline_binlog_param
    new_offline_info.oldest_ts = pb_info.oldest_ts();
    new_offline_info.newest_ts = pb_info.newest_ts();
    {
        BAIDU_SCOPED_LOCK(_offline_binlog_param_mutex);
        _offline_binlog_param = new_offline_info;
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_WARNING("region_id: %ld, update backup binlog range: [%ld, %ld]:[%s, %s] -> [%ld, %ld]:[%s, %s], request: %s, cost: %ld", 
            _region_id, 
            oldest_ts, 
            newest_ts,
            ts_to_datetime_str(oldest_ts).c_str(), 
            ts_to_datetime_str(newest_ts).c_str(),
            _offline_binlog_param.oldest_ts, 
            _offline_binlog_param.newest_ts,
            ts_to_datetime_str(_offline_binlog_param.oldest_ts).c_str(), 
            ts_to_datetime_str(_offline_binlog_param.newest_ts).c_str(),
            request.ShortDebugString().c_str(), cost.get_time());
    return;
}


void Region::recover_offline_binlog_info(const pb::StoreReq* request, pb::StoreRes* response) {
    TimeCost cost;
    const pb::RegionOfflineBinlogInfo& pb_info = request->extra_req().offline_binlog_info();
    int64_t oldest_ts = _offline_binlog_param.oldest_ts;
    int64_t newest_ts = _offline_binlog_param.newest_ts;

    // update _offline_binlog_param
    {
        BAIDU_SCOPED_LOCK(_offline_binlog_param_mutex);
        _offline_binlog_param.oldest_ts = pb_info.oldest_ts();
        _offline_binlog_param.newest_ts = pb_info.newest_ts();
    }
    response->set_errcode(pb::SUCCESS);
    response->set_errmsg("success");
    DB_WARNING("region_id: %ld, recovery backup binlog range: [%ld, %ld]:[%s, %s] -> [%ld, %ld]:[%s, %s], request: %s, cost: %ld", 
            _region_id, 
            oldest_ts, 
            newest_ts,
            ts_to_datetime_str(oldest_ts).c_str(), 
            ts_to_datetime_str(newest_ts).c_str(),
            _offline_binlog_param.oldest_ts, 
            _offline_binlog_param.newest_ts,
            ts_to_datetime_str(_offline_binlog_param.oldest_ts).c_str(), 
            ts_to_datetime_str(_offline_binlog_param.newest_ts).c_str(),
            request->ShortDebugString().c_str(), cost.get_time());
    return;
}

bool Region::has_enough_online_binlog_data() {
    int64_t online_binlog_oldest_ts = std::max(_binlog_param.oldest_ts, _rocksdb->get_oldest_ts_in_binlog_cf());
    if (online_binlog_oldest_ts < _offline_binlog_task.backup_task_start_ts) {
        return true;
    }
    DB_WARNING("region_id: %ld has not enough binlog, online binlog oldest_ts: %ld, backup_start_ts: %ld",
               _region_id, online_binlog_oldest_ts, _offline_binlog_task.backup_task_start_ts);
    return false;
} 

void Region::transfer_binlog_leader() {
    if (!is_leader()) {
        DB_WARNING("region_id: %ld not leader when transfer_binlog_leader", _region_id);
        return;
    }
    pb::StoreReq req;
    req.set_region_version(_version);
    req.set_region_id(_region_id);
    req.set_op_type(pb::OP_QUERY_BINLOG);
    std::vector<std::string> valid_peers;
    valid_peers.reserve(2);
    for (const auto& peer : _region_info.peers()) {
        if (peer == _address) {
            continue;
        }
        int64_t peer_oldest_ts = INT64_MAX;
        int ret = RpcSender::get_peer_binlog_oldest_ts(peer, req, peer_oldest_ts);
        if (ret != 0) {
            continue;
        }
        DB_WARNING("region_%ld ask %s, peer_oldest_ts: %ld", _region_id, peer.c_str(), peer_oldest_ts); 
        if (peer_oldest_ts < _offline_binlog_task.backup_task_start_ts) {
            valid_peers.emplace_back(peer);
        }
    }
    if (valid_peers.size() > 0) {
        int32_t random = butil::fast_rand() % valid_peers.size(); 
        std::string new_leader = valid_peers[random];
        if (make_region_status_doing() != 0) {
            return;
        }
        transfer_leader_to(new_leader);
        reset_region_status();
        DB_WARNING("region_id: %ld, trans leader to: %s", _region_id, new_leader.c_str());
    } else {
        DB_WARNING("region_id: %ld, want transfer leader but no valid peer", _region_id);
    }
    return;
}

bool Region::need_trigger_to_backup(const int backup_days) {
    bool is_trigger = false;
    uint32_t start_timestamp = 0;
    uint32_t end_timestamp = 0;
    time_t now_timestamp = time(NULL);
    time_t today_zero_clock_timestamp;
    get_current_day_timestamp(today_zero_clock_timestamp); //今天0点
    uint64_t ttl_timestamp = today_zero_clock_timestamp - backup_days * (24 * 3600);
    int64_t ttl_ts = timestamp_to_ts(ttl_timestamp);

    BAIDU_SCOPED_LOCK(_offline_binlog_param_mutex);
    if (_offline_binlog_param.newest_ts < ttl_ts) {
        // 第一次备份, 或者备份中断, 从昨天数据开始备份
        end_timestamp = today_zero_clock_timestamp; 
        start_timestamp = end_timestamp - 24 * 3600; 
        _offline_binlog_task.backup_task_start_ts = timestamp_to_ts(start_timestamp);
        _offline_binlog_task.backup_task_end_ts = timestamp_to_ts(end_timestamp);
        _offline_binlog_task.new_oldest_ts = _offline_binlog_task.backup_task_start_ts;
        _offline_binlog_task.new_newest_ts = _offline_binlog_task.backup_task_end_ts;
        is_trigger = true;
    } else {
        start_timestamp = tso::get_timestamp_internal(_offline_binlog_param.newest_ts);
        end_timestamp = start_timestamp + 24 * 3600;
        _offline_binlog_task.backup_task_start_ts = _offline_binlog_param.newest_ts;
        _offline_binlog_task.backup_task_end_ts = timestamp_to_ts(end_timestamp);
        _offline_binlog_task.new_oldest_ts = std::max(ttl_ts, _offline_binlog_param.oldest_ts);
        _offline_binlog_task.new_newest_ts = _offline_binlog_task.backup_task_end_ts;
        is_trigger = true;
    }
    if (is_trigger && now_timestamp - end_timestamp > 3600) {
        DB_WARNING("region_id: %ld, backup task: [ %ld, %ld ]-[ %s, %s ], cold_binlog ts: [ %ld, %ld ]-[ %s, %s ],"
                   " new cold_binlog ts: [ %ld, %ld ]-[ %s, %s ]", 
                    _region_id, 
                    _offline_binlog_task.backup_task_start_ts, 
                    _offline_binlog_task.backup_task_end_ts,
                    ts_to_datetime_str(_offline_binlog_task.backup_task_start_ts).c_str(), 
                    ts_to_datetime_str(_offline_binlog_task.backup_task_end_ts).c_str(),
                    _offline_binlog_param.oldest_ts, 
                    _offline_binlog_param.newest_ts,
                    ts_to_datetime_str(_offline_binlog_param.oldest_ts).c_str(), 
                    ts_to_datetime_str(_offline_binlog_param.newest_ts).c_str(),
                    _offline_binlog_task.new_oldest_ts,
                    _offline_binlog_task.new_newest_ts,
                    ts_to_datetime_str(_offline_binlog_task.new_oldest_ts).c_str(), 
                    ts_to_datetime_str(_offline_binlog_task.new_newest_ts).c_str());
        return true;
    }
    return false;
}

int Region::get_binlog_backup_days() {
    if (_shutdown || _removed) {
        return -1;
    }
    if (!is_binlog_region() || get_version() == 0) {
        return -1;
    }
    return _factory->get_binlog_backup_days(_table_id);
}

bool Region::need_clear_offline_binlog_sst() {
    if (_shutdown || _removed) {
        return false;
    }
    if (!is_binlog_region() || get_version() == 0) {
        return false;
    }
    int backup_days = _factory->get_binlog_backup_days(_table_id);
    if (backup_days <= 0 && (!_offline_binlog_param.data_ssts.empty() || !_offline_binlog_param.binlog_ssts.empty())) {
        return true;
    }
    return false;
}

void Region::clear_offline_binlog() {
    if (_shutdown || _removed || !is_binlog_region() || get_version() == 0 || !is_leader()) {
        return;
    }
    int backup_days = _factory->get_binlog_backup_days(_table_id);
    if (backup_days > 0) {
        return;
    }
    // 清空region offine binlog信息
    pb::StoreReq req;
    pb::StoreRes res;
    req.set_op_type(pb::OP_UPDATE_OFFLINE_BINLOG);
    req.set_region_id(_region_id);
    req.set_region_version(get_version());
    auto offline_binlog_info = req.mutable_extra_req()->mutable_offline_binlog_info();
    offline_binlog_info->set_oldest_ts(0);
    offline_binlog_info->set_newest_ts(0);
    offline_binlog_info->set_task_start_ts(0);
    offline_binlog_info->set_task_end_ts(0);
    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!req.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("serializeToString fail, region_id: %ld", _region_id);  
        return;
    }
    BthreadCond cond;
    BinlogClosure* c = new BinlogClosure(&cond);
    c->response = &res;
    c->check_status = true;
    braft::Task task; 
    task.data = &data; 
    task.done = c;
    cond.increase();
    _node.apply(task);
    cond.wait();
    if (res.errcode() != pb::SUCCESS) {
        DB_FATAL("region_id: %ld, sync offline binlog info: %s failed, response: %s", 
                _region_id, req.ShortDebugString().c_str(), res.ShortDebugString().c_str());
        return;
    }
    DB_NOTICE("region_id: %ld sync offline binlog info: %s", _region_id, req.ShortDebugString().c_str());
    return;
}

int Region::write_offline_binlog_data() {
    if (!has_enough_online_binlog_data()) {
        return -1; 
    }

    SmartTable binlog_table = _factory->get_table_info_ptr(get_table_id());
    SmartIndex binlog_pri   = _factory->get_index_info_ptr(get_table_id());
    TimeCost cost;
    /* 简化备份逻辑, 每次备份任务 startTs-> endTs
     * step1. 扫描data_cf: primary_key(ts)在[startTs, endTs)的kv, 写data_sst
     * step2. 如果1中的kv是OP_PREWRITE_BINLOG, 获取binlog_cf ts对应的kv, 写binlog_sst
     * 
     * cold_binlog cf key需要加region_id前缀: 
     * 加region_id前缀, 保证store上region删除(如load_balance),能删掉region对应的所有数据
     * 否则会出现: cold_rocksdb Link的afs文件, 被删后(如离线binlog ttl), store无法启动
     */

    std::map<int32_t, FieldInfo*> field_ids;
    std::vector<int32_t> field_slot;
    binlog_get_scan_fields(field_ids, field_slot, binlog_table, binlog_pri);

    MutTableKey prefix;
    prefix.append_i64(_region_id).append_i64(_table_id);
    MutTableKey start_key = prefix;
    MutTableKey end_key = prefix;
    start_key.append_i64(_offline_binlog_task.backup_task_start_ts);
    end_key.append_i64(_offline_binlog_task.backup_task_end_ts);
    rocksdb::Slice upper_bound_slice(end_key.data());

    rocksdb::ReadOptions options;
    options.total_order_seek = true;
    options.fill_cache = false;
    options.iterate_upper_bound = &upper_bound_slice;
    std::unique_ptr<rocksdb::Iterator> data_iter(_rocksdb->new_iterator(options, _data_cf));

    int64_t total_data_cnt = 0;
    int64_t total_binlog_cnt = 0;
    OfflineBinlogSstWriter data_sst_writer(_table_id, 
                                           _region_id, 
                                           OfflineBinlogType::DATA, 
                                           _offline_binlog_task.backup_task_start_ts,
                                           _offline_binlog_task.backup_task_end_ts);
    OfflineBinlogSstWriter binlog_sst_writer(_table_id,  
                                             _region_id, 
                                             OfflineBinlogType::BINLOG,
                                             _offline_binlog_task.backup_task_start_ts,
                                             _offline_binlog_task.backup_task_end_ts);
    ScopeGuard auto_decrease([this, &data_sst_writer, &binlog_sst_writer]() {
        auto fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
        if (fs == nullptr) {
            return;
        }
        for (const std::string& external_file : data_sst_writer.external_files()) {
            DB_WARNING("region_id: %ld backup fail, delete data external_file: %s", _region_id, external_file.c_str());
            fs->delete_path(external_file, false);
        }
        for (const std::string& external_file : binlog_sst_writer.external_files()) {
            DB_WARNING("region_id: %ld backup fail, delete binlog external_file: %s", _region_id, external_file.c_str());
            fs->delete_path(external_file, false);
        }
    });

    int ret = 0;
    bool batch_finish = false;
    std::map<std::string, ExprValue> field_value_map;
    std::map<int64_t, std::string> start_binlog_map;
    SmartRecord record = _factory->new_record(*binlog_table);
    BinlogReadMgr binlog_reader(_region_id, _offline_binlog_task.backup_task_start_ts, "backup_task", 0, 0, false);
    for (data_iter->Seek(start_key.data()); data_iter->Valid(); data_iter->Next()) {
        record->clear();
        rocksdb::Slice value_slice = data_iter->value();
        TupleRecord tuple_record(value_slice);
        int data_key_prefix = sizeof(int64_t) * 2;
        if (0 != tuple_record.decode_fields(field_ids, &field_slot, &record, 0, nullptr)) {
            DB_WARNING("region_id: %ld, decode value failed", _region_id);
            return -1;
        }
        if (0 != record->decode_key(*binlog_pri, data_iter->key(), data_key_prefix)) {
            DB_WARNING("region_id: %ld, decode key failed: %ld", _region_id, binlog_pri->id);
            return -1;
        } 

        std::map<std::string, ExprValue> field_value_map;
        binlog_get_field_values(field_value_map, record, binlog_table);
        int64_t ts = binlog_get_int64_val("ts", field_value_map);
        BinlogType binlog_type = static_cast<BinlogType>(binlog_get_int64_val("binlog_type", field_value_map));
        if (ts >= _offline_binlog_task.backup_task_end_ts) {
            DB_WARNING("region_id: %ld, search finish: search binlog ts : %ld >= backup end_ts : %ld", 
                       _region_id, ts, _offline_binlog_task.backup_task_end_ts);
            break;
        }
        if (ts < _offline_binlog_task.backup_task_start_ts) {
            DB_WARNING("region_id: %ld, search: search binlog ts : %ld < backup start_ts : %ld", 
                       _region_id, ts, _offline_binlog_task.backup_task_start_ts);
            continue;
        }

        total_data_cnt++;
        int ret = data_sst_writer.write_kv(data_iter->key(), data_iter->value());
        if (ret < 0) {
            DB_FATAL("region_id: %ld write kv failed, ts: %ld", _region_id, ts);
            return -1;
        }
        if (binlog_type != PREWRITE_BINLOG) {
            continue;
        }
        ret = binlog_reader.get_prewrite_binlog(ts, start_binlog_map, batch_finish);
        if (ret < 0) {
            return -1;
        }
        total_binlog_cnt++; 
        if (batch_finish) {
            ret =  binlog_sst_writer.write_binlog_batch(start_binlog_map);
            if (ret != 0) {
                return -1;
            }
        }
    }

    ret = binlog_reader.get_prewrite_binlog(0, start_binlog_map, batch_finish, true);
    if (ret < 0) {
        return -1;
    } 
    ret = binlog_sst_writer.write_binlog_batch(start_binlog_map);
    if (ret != 0) {
        return -1;
    }

    ret = data_sst_writer.finish();
    if (ret < 0) {
        DB_FATAL("region_id: %ld finish data writer failed", _region_id);
        return -1;
    }
    ret = binlog_sst_writer.finish();
    if (ret < 0) {
        DB_FATAL("region_id: %ld finish binlog writer failed", _region_id);
        return -1;
    }
    auto_decrease.release();
    _offline_binlog_task.data_ssts = data_sst_writer.external_files();
    _offline_binlog_task.binlog_ssts = binlog_sst_writer.external_files();
    DB_NOTICE("region_id: %ld flush data[total: %ld] binlog[total: %ld], cost: %ld", 
            _region_id, total_data_cnt, total_binlog_cnt, cost.get_time());
    return 0;
}

void Region::do_backup_binlog() {
    int backup_days = get_binlog_backup_days();
    if (backup_days <= 0) {
        return;
    }
    if (!is_leader()) {
        return;
    }
    _offline_binlog_task.clear();
    if (!need_trigger_to_backup(backup_days)) {
        return;
    }
    if (!has_enough_online_binlog_data()) {
        transfer_binlog_leader();
        return;
    }
    if (!is_leader()) {
        DB_WARNING("region_id: %ld not leader when trigger binlog backup task", _region_id);
        return;
    }

    if (make_region_status_doing() != 0) {
        return;
    }  
    ON_SCOPE_EXIT([this]() {
        reset_region_status();
    });

    if (write_offline_binlog_data() != 0) {
        return;
    }
    ScopeGuard auto_decrease([this]() {
        auto fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
        if (fs == nullptr) {
            return;
        }
        for (const std::string& file : _offline_binlog_task.data_ssts) {
            DB_WARNING("region_id: %ld backup fail, delete data external_file: %s", _region_id, file.c_str());
            fs->delete_path(file, false);
        }
        for (const std::string& file : _offline_binlog_task.binlog_ssts) {
            DB_WARNING("region_id: %ld backup fail, delete binlog external_file: %s", _region_id, file.c_str());
            fs->delete_path(file, false);
        }
    });
    if (!is_leader()) {
        DB_WARNING("region_id: %ld not leader", _region_id);
        return;
    }

    DB_WARNING("region_id, backup finish and do raft: %ld", _region_id);
    // 走raft
    pb::StoreReq req;
    pb::StoreRes res;
    req.set_op_type(pb::OP_UPDATE_OFFLINE_BINLOG);
    req.set_region_id(_region_id);
    req.set_region_version(get_version());
    auto offline_binlog_info = req.mutable_extra_req()->mutable_offline_binlog_info();

    offline_binlog_info->set_oldest_ts(_offline_binlog_task.new_oldest_ts);
    offline_binlog_info->set_newest_ts(_offline_binlog_task.new_newest_ts);
    offline_binlog_info->set_task_start_ts(_offline_binlog_task.backup_task_start_ts);
    offline_binlog_info->set_task_end_ts(_offline_binlog_task.backup_task_end_ts);
    for (const auto& data_sst : _offline_binlog_param.data_ssts) {
        if (data_sst.first <= _offline_binlog_task.new_oldest_ts) {
            continue;
        }
        offline_binlog_info->add_external_full_path(data_sst.second);
    }
    for (const auto& binlog_sst : _offline_binlog_param.binlog_ssts) {
        if (binlog_sst.first <= _offline_binlog_task.new_oldest_ts) {
            continue;
        }
        offline_binlog_info->add_external_full_path(binlog_sst.second);
    }
    // 新增
    for (const std::string& file : _offline_binlog_task.data_ssts) {
        offline_binlog_info->add_external_full_path(file);
    }
    for (const std::string& file : _offline_binlog_task.binlog_ssts) {
        offline_binlog_info->add_external_full_path(file);
    }

    butil::IOBuf data;
    butil::IOBufAsZeroCopyOutputStream wrapper(&data);
    if (!req.SerializeToZeroCopyStream(&wrapper)) {
        DB_FATAL("serializeToString fail, region_id: %ld", _region_id);  
        return;
    }

    BthreadCond cond;
    BinlogClosure* c = new BinlogClosure(&cond);
    c->response = &res;
    c->check_status = true;
    braft::Task task; 
    task.data = &data; 
    task.done = c;
    cond.increase();
    _node.apply(task);
    cond.wait();
    if (res.errcode() != pb::SUCCESS) {
        DB_FATAL("region_id: %ld, sync offline binlog info: %s failed, response: %s", 
                _region_id, req.ShortDebugString().c_str(), res.ShortDebugString().c_str());
        return;
    }
    auto_decrease.release();
    DB_NOTICE("region_id: %ld sync offline binlog info: %s", _region_id, req.ShortDebugString().c_str());
    return;
}

void Region::delete_remote_expired_file() {
    int backup_days = get_binlog_backup_days();
    if (backup_days <= 0) {
        return;
    }
    if (!is_leader()) {
        return;
    }
    int ret = 0;
    time_t now_timestamp = time(NULL);
    time_t ttl_timestamp = now_timestamp - backup_days * (3600 * 24);
    int64_t ttl_ts = timestamp_to_ts(ttl_timestamp);
    int64_t offline_binlog_oldest_ts = _offline_binlog_param.oldest_ts;
    
    if (ttl_ts < offline_binlog_oldest_ts) {
        DB_WARNING("region_id: %ld, ttl_timestamp: %ld: %s < offline_binlog_oldest_ts: %ld: %s, no need ttl",
            _region_id, 
            ttl_ts, ts_to_datetime_str(ttl_ts).c_str(), 
            offline_binlog_oldest_ts, ts_to_datetime_str(offline_binlog_oldest_ts).c_str());
        return;
    }
    // 必须确保所有peer没有link预删除文件
    // step1 列出afs上该region所有文件
    // step2 拿到所有peer link的文件
    // step3 删除无效文件
    OfflineSSTInfo info;
    std::string path = "offline_binlog/" + FLAGS_meta_server_bns + "/" + std::to_string(_table_id) + "/";
    std::unordered_map<std::string, int64_t> ttl_remote_files;
    std::set<std::string> list_remote_files; 
    std::shared_ptr<ExtFileSystem> fs = SstExtLinker::get_instance()->get_exteranl_filesystem();
    ret = fs->list("", path, list_remote_files);
    if (ret < 0) {
        DB_FATAL("list afs path fail: %s", path.c_str());
        return;
    }
    for (const auto& f : list_remote_files) {
        ret = info.parse_remote_offline_binlog_sst(f);
        if (ret != 0 || info.region_id != _region_id) {
            continue;
        }
        if (info.end_ts <= ttl_ts) {
            // 备份失败的afs文件等过期一起删除
            ttl_remote_files[f] = info.end_ts;
        }
    }
    if (ttl_remote_files.empty()) {
        DB_WARNING("region_id: %ld, ttl_timestamp: %ld: %s > offline_binlog_oldest_ts: %ld: %s, no expired afs file",
        _region_id, ttl_ts, ts_to_datetime_str(ttl_ts).c_str(), 
        offline_binlog_oldest_ts, ts_to_datetime_str(offline_binlog_oldest_ts).c_str());
        return;
    }
    DB_WARNING("region_id: %ld, ttl_timestamp: %ld: %s > offline_binlog_oldest_ts: %ld: %s, with some expired afs files, need ttl",
        _region_id, ttl_ts, ts_to_datetime_str(ttl_ts).c_str(), 
        offline_binlog_oldest_ts, ts_to_datetime_str(offline_binlog_oldest_ts).c_str());
    pb::StoreReq req;
    req.set_region_version(_version);
    req.set_region_id(_region_id);
    req.set_op_type(pb::OP_QUERY_OFFLINE_BINLOG);
    std::unordered_map<std::string, int64_t> valid_remote_files;
    for (const auto& peer : _region_info.peers()) {
        pb::StoreRes res;
        if (peer == _address) {
            query_offline_binlog_info(&res);
        } else {
            ret = RpcSender::get_peer_offline_binlog_info(peer, req, res);
            if (ret != 0) {
                DB_WARNING("get peer: %s, offline binlog fail, skip ttl", peer.c_str());
                return;
            }
        }
        const pb::RegionOfflineBinlogInfo& offline_info = res.extra_res().offline_binlog_info();
        for (const auto& f : offline_info.external_full_path()) {
            if (valid_remote_files.count(f) == 0) {
                ret = info.parse_remote_offline_binlog_sst(f);
                if (ret != 0 || info.region_id != _region_id) {
                    continue;
                }
                valid_remote_files[f] = info.end_ts;
            }
        }
    }

    // delete afs file
    for (const auto& f : ttl_remote_files) {
        if (valid_remote_files.count(f.first) > 0) {
                DB_FATAL("region_id: %ld need ttl sst: %s, end_ts: %s but has peer's rocksdb use it", 
                        _region_id, f.first.c_str(), ts_to_datetime_str(f.second).c_str());
                continue;
        }
        DB_WARNING("region_id: %ld ttl delete unused offline binlog sst: %s, end_ts: %ld, %s", 
                    _region_id, f.first.c_str(), f.second, ts_to_datetime_str(f.second).c_str());
        fs->delete_path(f.first, false);
    }
    return;
}

} // namespace baikaldb
