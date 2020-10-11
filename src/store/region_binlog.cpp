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

namespace baikaldb {
DECLARE_int32(retry_interval_us);
DEFINE_int64(binlog_timeout_us, 10 * 1000 * 1000LL, "binlog timeout us : 10s");

//该函数联调时测试 TODO by YUZHENGQUAN
int Region::get_primary_region_info(int64_t primary_region_id, pb::RegionInfo& region_info) {
    MetaServerInteract&   meta_server_interact = Store::get_instance()->get_meta_server_interact();
    pb::QueryRequest query_request;
    pb::QueryResponse query_response;
    query_request.set_op_type(pb::QUERY_REGION);
    query_request.set_region_id(primary_region_id);
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
void Region::binlog_timeout_check(int64_t rollback_ts) {
    int64_t start_ts = 0;
    int64_t txn_id   = 0;
    int64_t primary_region_id = 0;

    {
        _binlog_cond.increase_wait(1);
        ON_SCOPE_EXIT([this]() {
            //出作用域自动释放，避免阻塞binlog写
            _binlog_cond.decrease_signal();
        });

        if (_binlog_param.ts_binlog_map.size() == 0) {
            return;
        }

        auto iter = _binlog_param.ts_binlog_map.begin();
        if (iter->second.time.get_time() > FLAGS_binlog_timeout_us) {
            //告警
            DB_WARNING("region_id: %ld, start_ts: %ld, txn_id:%ld, primary_region_id: %ld timeout", _region_id, iter->first, txn_id, iter->second.primary_region_id);
            if (is_leader()) {
                start_ts = iter->first;
                txn_id = iter->second.txn_id;
                primary_region_id = iter->second.primary_region_id;
            } else {
                return;
            }
        } else {
            return;
        }
    }

    //  反查primary region，确认prewrite binlog是否已经commit 或 rollback
    pb::RegionInfo region_info;
    int ret = get_primary_region_info(primary_region_id, region_info);
    if (ret < 0) {
        return;
    }

    binlog_query_primary_region(start_ts, txn_id, region_info, rollback_ts);

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

    DB_WARNING("region_id: %ld, FAKE BINLOG", _region_id);

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
        ExprValue ts;
        ts.type = pb::INT64;
        ts._u.int64_val = binlog_desc.binlog_ts();
        field_value_map["ts"] = ts;
    }

    if (binlog_desc.has_txn_id()) {
        ExprValue txn_id;
        txn_id.type = pb::INT64;
        txn_id._u.int64_val = binlog_desc.txn_id();
        field_value_map["txn_id"] = txn_id;
    }

    if (binlog_desc.has_start_ts()) {
        ExprValue start_ts;
        start_ts.type = pb::INT64;
        start_ts._u.int64_val = binlog_desc.start_ts();
        field_value_map["start_ts"] = start_ts;
    }  

    if (binlog_desc.has_primary_region_id()) {
        ExprValue primary_region_id;
        primary_region_id.type = pb::INT64;
        primary_region_id._u.int64_val = binlog_desc.primary_region_id();
        field_value_map["primary_region_id"] = primary_region_id;
    }

    if (binlog_desc.has_partion_key()) {
        ExprValue partion_key;
        partion_key.type = pb::INT64;
        partion_key._u.int64_val = binlog_desc.partion_key();
        field_value_map["partion_key"] = partion_key;
    }   

    {
        ExprValue binlog_region_id;
        binlog_region_id.type = pb::INT64;
        binlog_region_id._u.int64_val = _region_id;
        field_value_map["binlog_region_id"] = binlog_region_id;
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

void Region::binlog_get_scan_fields(std::map<int32_t, FieldInfo*>& field_ids, std::vector<int32_t>& field_slot) {
    SmartTable  table_ptr = _factory->get_table_info_ptr(get_table_id());
    SmartIndex  pri_info  = _factory->get_index_info_ptr(get_table_id());

    field_slot.resize(table_ptr->fields.back().id + 1);

    std::set<int32_t> pri_field_ids;
    for (auto& field_info : pri_info->fields) {
        pri_field_ids.insert(field_info.id);
    }

    for (auto& field : table_ptr->fields) {
        field_slot[field.id] = field.id;
        if (pri_field_ids.count(field.id) == 0) {
            field_ids[field.id] = &field;
        }
    }
}

void Region::binlog_get_field_values(std::map<std::string, ExprValue>& field_value_map, SmartRecord record) {
    SmartTable  table_ptr = _factory->get_table_info_ptr(get_table_id());

    for (auto& field : table_ptr->fields) {
        auto f = record->get_field_by_tag(field.id);
        field_value_map[field.short_name] = record->get_value(f);
    }
}

void Region::binlog_reset_on_snapshot_load_restart() {
    _binlog_cond.increase_wait(1);
    ON_SCOPE_EXIT([this]() {
        _binlog_cond.decrease_signal();
    });

    //重新读取check point
    _binlog_param.check_point_ts = _meta_writer->read_binlog_check_point(_region_id);
    _binlog_param.oldest_ts      = _meta_writer->read_binlog_oldest_ts(_region_id);
    _binlog_param.max_ts_in_map  = _binlog_param.check_point_ts;
    _binlog_param.min_ts_in_map  = _binlog_param.check_point_ts;
    _binlog_param.ts_binlog_map.clear();
    DB_WARNING("region_id: %ld, check_point_ts: %ld, oldest_ts: %ld", _region_id, 
        _binlog_param.check_point_ts, _binlog_param.oldest_ts);
}

//add peer时不拉取binlog快照，需要重置oldest binlog ts和check point
void Region::binlog_reset_on_snapshot_load() {
    _binlog_cond.increase_wait(1);
    ON_SCOPE_EXIT([this]() {
        _binlog_cond.decrease_signal();
    });

    //重置等待FAKE BINLOG时更新，第一次FAKE BINLOG之前的都会舍弃
    _binlog_param.check_point_ts = -1;
    _binlog_param.oldest_ts      = -1;
    _binlog_param.max_ts_in_map  = -1;
    _binlog_param.min_ts_in_map  = -1;
    _binlog_param.ts_binlog_map.clear();
    DB_WARNING("region_id: %ld, snapshot load", _region_id);
}

void Region::binlog_scan() {

    _binlog_cond.increase_wait(1);
    ON_SCOPE_EXIT([this]() {
        _binlog_cond.decrease_signal();
    });
    TimeCost cost;
    int64_t begin_ts = 0;
    if (_binlog_param.max_ts_in_map == -1 || _binlog_param.min_ts_in_map == -1) {
        //重启或者刚拉取快照
        return;
    } 

    //接续 max ts 扫描
    begin_ts = _binlog_param.max_ts_in_map;

    SmartRecord left_record  = _factory->new_record(get_table_id());
    SmartRecord right_record = _factory->new_record(get_table_id());
    SmartIndex  pri_info     = _factory->get_index_info_ptr(get_table_id());
    if (left_record == nullptr || right_record == nullptr || pri_info == nullptr) {
        return;
    }

    ExprValue value;
    value.type = pb::INT64;
    value._u.int64_val = begin_ts;
    left_record->set_value(left_record->get_field_by_tag(1), value);
    right_record->decode("");

    IndexRange range(left_record.get(), right_record.get(), pri_info.get(), pri_info.get(),
                        &_region_info, 1, 0, false, false, false);

    std::map<int32_t, FieldInfo*> field_ids;
    std::vector<int32_t> field_slot;

    binlog_get_scan_fields(field_ids, field_slot);

    TableIterator* table_iter = Iterator::scan_primary(nullptr, range, field_ids, field_slot, false, true);
    if (table_iter == nullptr) {
        DB_WARNING("open TableIterator fail, table_id:%ld", get_table_id());
        return;
    }

    ON_SCOPE_EXIT(([this, table_iter]() {
        delete table_iter;
    }));

    SmartRecord record = _factory->new_record(get_table_id());
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
        binlog_get_field_values(field_value_map, record);

        //binlog 元信息更新map
        binlog_update_map_when_scan(field_value_map);
    }

    // 一轮扫描结束后，处理内存map，更新check point
    binlog_update_check_point();
    DB_WARNING("region_id: %ld, binlog_scan map size: %d, min ts: %ld, max ts: %ld, check point ts: %ld, cost: %ld", 
        _region_id, _binlog_param.ts_binlog_map.size(), _binlog_param.min_ts_in_map, _binlog_param.max_ts_in_map, 
        _binlog_param.check_point_ts, cost.get_time());
}

//scan时更新map，因为不会和写入并发，扫描ts严格递增
void Region::binlog_update_map_when_scan(const std::map<std::string, ExprValue>& field_value_map) {
    BinlogDesc binlog_desc;
    int64_t ts = binlog_get_int64_val("ts", field_value_map);
    BinlogType binlog_type = static_cast<BinlogType>(binlog_get_int64_val("binlog_type", field_value_map));
    int64_t txn_id = binlog_get_int64_val("txn_id", field_value_map);
    int64_t start_ts = binlog_get_int64_val("start_ts", field_value_map);

    //扫描时小于max ts直接跳过
    if (ts <= _binlog_param.max_ts_in_map) {
        DB_WARNING("region_id: %ld scan_ts: %ld <= max_ts: %ld, txn_id: %ld", _region_id, ts, _binlog_param.max_ts_in_map, txn_id);
        return;
    }

    if (binlog_type == FAKE_BINLOG) {
        binlog_desc.binlog_type = binlog_type;
        _binlog_param.ts_binlog_map[ts] = binlog_desc;
        // DB_WARNING("region_id: %ld, ts: %ld, type: %s, txn_id: %ld", _region_id, ts, binlog_type_name(binlog_type), txn_id);
    } else if (binlog_type == PREWRITE_BINLOG) {
        binlog_desc.binlog_type = binlog_type;
        binlog_desc.txn_id = txn_id;
        binlog_desc.primary_region_id = binlog_get_int64_val("primary_region_id", field_value_map);
        _binlog_param.ts_binlog_map[ts] = binlog_desc;
        // DB_WARNING("region_id: %ld, start_ts: %ld, type: %s, txn_id: %ld", _region_id, ts, binlog_type_name(binlog_type), txn_id);
    } else if (binlog_type == COMMIT_BINLOG || binlog_type == ROLLBACK_BINLOG) {
        if (start_ts < _binlog_param.min_ts_in_map) {
            DB_WARNING("region_id: %ld, type: %s, start_ts: %ld < min ts: %ld, ts: %ld, txn_id: %ld", _region_id, binlog_type_name(binlog_type), start_ts, 
                _binlog_param.min_ts_in_map, ts, txn_id);
            return;
        }

        auto iter = _binlog_param.ts_binlog_map.find(start_ts);
        if (iter == _binlog_param.ts_binlog_map.end()) {
            DB_FATAL("region_id: %ld, type: %s, start_ts: %ld can not find in map, ts: %ld, txn_id: %ld", 
                _region_id, binlog_type_name(binlog_type), start_ts, ts, txn_id);
            return;
        } else {
            _binlog_param.ts_binlog_map.erase(start_ts);
            DB_WARNING("region_id: %ld, type: %s, start_ts: %ld erase, commit_ts: %ld, txn_id: %ld", 
                _region_id, binlog_type_name(binlog_type), start_ts, ts, txn_id);
        }

    }

    _binlog_param.max_ts_in_map = ts;
    if (_binlog_param.ts_binlog_map.size() > 0) {
        _binlog_param.min_ts_in_map = _binlog_param.ts_binlog_map.begin()->first;
    } else {
        _binlog_param.min_ts_in_map = ts;
    }

    // DB_WARNING("region_id: %ld, type: %s, map size: %d, min ts: %ld, max ts: %ld, txn_id: %ld", _region_id, binlog_type_name(binlog_type), _binlog_param.ts_binlog_map.size(), 
    //                 _binlog_param.min_ts_in_map, _binlog_param.max_ts_in_map, txn_id);
    return;
}

//on_apply中只有相关start_ts/commit_ts/rollback_ts和map区间有重合时才可以更新map
int Region::binlog_update_map_when_apply(const std::map<std::string, ExprValue>& field_value_map) {
    int ret = 0;
    BinlogDesc binlog_desc;
    int64_t ts = binlog_get_int64_val("ts", field_value_map);
    int64_t type = binlog_get_int64_val("binlog_type", field_value_map);
    int64_t txn_id = binlog_get_int64_val("txn_id", field_value_map);
    int64_t start_ts = binlog_get_int64_val("start_ts", field_value_map);
    
    if (_binlog_param.max_ts_in_map == -1 || _binlog_param.min_ts_in_map == -1) {
        //on_apply之前会调用on_snapshot_load, 如果重启max_ts_in_map不会为-1，如果新拉取快照则需要在FAKE BINLOG时重置check point和oldest ts
        if (type == FAKE_BINLOG) {
            binlog_desc.binlog_type = static_cast<BinlogType>(type);
            _binlog_param.ts_binlog_map[ts] = binlog_desc;
            ret = _meta_writer->write_binlog_check_point(_region_id, ts);
            if (ret < 0) {
                return -1;
            }
            ret = _meta_writer->write_binlog_oldest_ts(_region_id, ts);
            if (ret < 0) {
                return -1;
            }
            _binlog_param.check_point_ts = ts;
            _binlog_param.oldest_ts      = ts;
            _binlog_param.max_ts_in_map  = ts;
            _binlog_param.min_ts_in_map  = ts;
            DB_WARNING("region_id: %ld, ts: %ld, FAKE BINLOG reset check point and oldest ts", _region_id, ts);
        } else {
            DB_WARNING("region_id :%ld, ts: %ld, binlog_type: %d, discard", _region_id, ts, type);
        }
        return 0;
    }

    if (ts > _binlog_param.max_ts_in_map) {
        // DB_WARNING("region_id: %ld, type:%d, ts: %ld > max ts: %ld, don't update map", _region_id, type, ts, _binlog_param.max_ts_in_map);
        return 0;
    } else {
        if (type == FAKE_BINLOG) {
            binlog_desc.binlog_type = static_cast<BinlogType>(type);
            _binlog_param.ts_binlog_map[ts] = binlog_desc;
            // DB_WARNING("region_id: %ld, ts: %ld, FAKE BINLOG", _region_id, ts);
        } else if (type == PREWRITE_BINLOG) {
            binlog_desc.binlog_type = static_cast<BinlogType>(type);
            binlog_desc.txn_id = txn_id;
            binlog_desc.primary_region_id = binlog_get_int64_val("primary_region_id", field_value_map);
            _binlog_param.ts_binlog_map[ts] = binlog_desc;
            DB_WARNING("region_id: %ld, ts: %ld, PREWRITE BINLOG", _region_id, ts);
        } else if (type == COMMIT_BINLOG || type == ROLLBACK_BINLOG) {
            if (start_ts < _binlog_param.min_ts_in_map) {
                DB_FATAL("region_id: %ld, type: %d, start_ts: %ld < min ts: %ld", _region_id, type, start_ts, _binlog_param.min_ts_in_map);
                return 0;
            }

            auto iter = _binlog_param.ts_binlog_map.find(start_ts);
            if (iter == _binlog_param.ts_binlog_map.end()) {
                DB_FATAL("region_id: %ld, type: %d, start_ts: %ld can not find in map", _region_id, type, start_ts);
                return 0;
            } else {
                _binlog_param.ts_binlog_map.erase(start_ts);
                DB_WARNING("region_id: %ld, start_ts: %ld erase", _region_id, start_ts);
            }
        }

        if (_binlog_param.ts_binlog_map.size() > 0) {
            _binlog_param.min_ts_in_map = _binlog_param.ts_binlog_map.begin()->first;
        } else {
            _binlog_param.min_ts_in_map = _binlog_param.max_ts_in_map;
        }

    }
    return 0;
}

//扫描一轮或者新写入binlog之后，更新map，更新扫描进度点
int Region::binlog_update_check_point() {
    if (_binlog_param.max_ts_in_map == -1 || _binlog_param.min_ts_in_map == -1) {
        return 0;
    }

    int ret = 0;
    int64_t check_point_ts = 0;
    if (_binlog_param.ts_binlog_map.empty()) {
        check_point_ts = _binlog_param.min_ts_in_map;
    } else {
        auto iter = _binlog_param.ts_binlog_map.begin();
        while (iter != _binlog_param.ts_binlog_map.end()) {
            check_point_ts = iter->first;
            BinlogType type = iter->second.binlog_type;

            if (type == PREWRITE_BINLOG) {
                break;
            }

            auto delete_iter = iter++;
            _binlog_param.ts_binlog_map.erase(delete_iter->first);
        }

        //更新min_ts_in_map
        if (!_binlog_param.ts_binlog_map.empty()) {
            _binlog_param.min_ts_in_map = _binlog_param.ts_binlog_map.begin()->first;
        } else {
            _binlog_param.min_ts_in_map = _binlog_param.max_ts_in_map;
        }
    }

    //write check point
    if (_binlog_param.check_point_ts > check_point_ts) {
        // check point 变小说明写入ts小于check point，告警
        DB_FATAL("region_id: %ld, new check point ts: %ld, < old check point ts: %ld", 
                    _region_id, check_point_ts, _binlog_param.check_point_ts);
    } else if (_binlog_param.check_point_ts == check_point_ts) {
        return 0;
    }

    ret = _meta_writer->write_binlog_check_point(_region_id, check_point_ts);
    if (ret != 0) {
        return -1;
    }

    DB_WARNING("region_id: %ld, check point ts %ld => %ld", _region_id, _binlog_param.check_point_ts, check_point_ts);
    _binlog_param.check_point_ts = check_point_ts;
    return 0;
}

int Region::write_binlog_record(SmartRecord record) {

    IndexInfo pk_index = _factory->get_index_info(get_table_id());

    MutTableKey key;
    // DB_WARNING("region_id: %ld, pk_id: %ld", _region_id, pk_index.id);
    key.append_i64(_region_id).append_i64(pk_index.id);
    if (0 != key.append_index(pk_index, record.get(), -1, true)) {
        DB_FATAL("Fail to append_index, reg=%ld, tab=%ld", _region_id, pk_index.id);
        return -1;
    }

    std::string value;
    int ret = record->encode(value);
    if (ret != 0) {
        DB_FATAL("encode record failed: reg=%ld, tab=%ld", _region_id, pk_index.id);
        return -1;
    }

    rocksdb::WriteOptions write_options;
    auto s = _rocksdb->put(write_options, _data_cf, key.data(), value);
    if (!s.ok()) {
        DB_FATAL("write binlog failed, region_id: %lld, status: %s", _region_id, s.ToString().c_str());
        return -1;
    }
    
    return 0;
}

//ts(primary key),   no null 
//txn_id,            default 0
//binlog_type,       no null
//partion_key,       default 0
//start_ts,          default -1
//primary_region_id, default -1
int Region::write_binlog_value(std::map<std::string, ExprValue> field_value_map) {
    SmartTable  table_ptr = _factory->get_table_info_ptr(get_table_id());
    SmartRecord record    = _factory->new_record(get_table_id());

    for (auto& field : table_ptr->fields) {
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
    _binlog_cond.increase_wait(1);
    ON_SCOPE_EXIT([this]() {
        _binlog_cond.decrease_signal();
    });
    
    pb::OpType op_type = request.op_type();

    std::map<std::string, ExprValue> field_value_map;

    switch (op_type) {
        case pb::OP_PREWRITE_BINLOG: 
        case pb::OP_COMMIT_BINLOG: 
        case pb::OP_ROLLBACK_BINLOG: 
        case pb::OP_FAKE_BINLOG: {
            binlog_fill_exprvalue(request.binlog_desc(), op_type, field_value_map);
            break;
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

        binlog_update_map_when_apply(field_value_map);
    }

    if (done != nullptr && ((BinlogClosure*)done)->response != nullptr) {
        ((BinlogClosure*)done)->response->set_errcode(pb::SUCCESS); 
        ((BinlogClosure*)done)->response->set_errmsg("apply binlog success");
    }

}

void Region::read_binlog(const pb::StoreReq* request,
                   pb::StoreRes* response) {
    int64_t binlog_cnt = request->binlog_desc().read_binlog_cnt();
    int64_t begin_ts = request->binlog_desc().binlog_ts();
    DB_DEBUG("read_binlog request %s", request->ShortDebugString().c_str());
    int64_t check_point_ts = 0;
    int64_t oldest_ts = 0;
    {
        // 获取check point时应加锁，避免被修改
        _binlog_cond.increase_wait(1);
        ON_SCOPE_EXIT([this]() {
            _binlog_cond.decrease_signal();
        });

        check_point_ts = _meta_writer->read_binlog_check_point(_region_id);
        if (check_point_ts < 0) {
            DB_FATAL("region_id: %ld, get check point failed", _region_id);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("get binlog check point ts");
            return;
        }

        oldest_ts = _meta_writer->read_binlog_oldest_ts(_region_id);
        if (oldest_ts < 0) {
            DB_FATAL("region_id: %ld, get oldest ts failed", _region_id);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("get binlog oldest ts failed");
            return;
        }
    }

    if (begin_ts == 0) {
        begin_ts = oldest_ts;
        DB_WARNING("region_id: %ld, begin_ts is 0, chg to: %ld", _region_id, oldest_ts);
    }

    if (begin_ts < oldest_ts) {
        DB_WARNING("region_id: %ld, begin ts : %ld < oldest ts : %ld", _region_id, begin_ts, oldest_ts);
        response->set_errcode(pb::LESS_THAN_OLDEST_TS); 
        response->set_errmsg("begin ts gt oldest ts");
        return;
    }

    SmartRecord left_record  = _factory->new_record(get_table_id());
    SmartRecord right_record = _factory->new_record(get_table_id());
    SmartIndex  pri_info     = _factory->get_index_info_ptr(get_table_id());
    if (left_record == nullptr || right_record == nullptr || pri_info == nullptr) {
        response->set_errcode(pb::GET_VALUE_FAIL); 
        response->set_errmsg("get index info failed");
        return;
    }

    // ExprValue left_value;
    // left_value.type = pb::INT64;
    // left_value._u.int64_val = begin_ts;
    // left_record->set_value(left_record->get_field_by_tag(1), left_value);
    // ExprValue right_value;
    // right_value.type = pb::INT64;
    // right_value._u.int64_val = check_point_ts;
    // right_record->set_value(right_record->get_field_by_tag(1), right_value);

    // IndexRange range(left_record.get(), right_record.get(), pri_info.get(), pri_info.get(),
    //                 &_region_info, 1, 1, false, false, false);


    ExprValue value;
    value.type = pb::INT64;
    value._u.int64_val = begin_ts;
    left_record->set_value(left_record->get_field_by_tag(1), value);
    right_record->decode("");

    IndexRange range(left_record.get(), right_record.get(), pri_info.get(), pri_info.get(),
                        &_region_info, 1, 0, false, false, false);

    std::map<int32_t, FieldInfo*> field_ids;
    std::vector<int32_t> field_slot;

    binlog_get_scan_fields(field_ids, field_slot);

    TableIterator* table_iter = Iterator::scan_primary(nullptr, range, field_ids, field_slot, false, true);
    if (table_iter == nullptr) {
        DB_WARNING("open TableIterator fail, table_id:%ld", get_table_id());
        return;
    }

    ON_SCOPE_EXIT(([this, table_iter]() {
        delete table_iter;
    }));

    int64_t max_fake_binlog = 0;
    int64_t return_nr = 0;
    std::map<std::string, ExprValue> field_value_map;
    SmartRecord record = _factory->new_record(get_table_id());
    while (1) {
        record->clear();
        if (!table_iter->valid()) {
            DB_WARNING("region_id: %ld not valid", _region_id);
            break;
        }

        int ret = table_iter->get_next(record);
        if (ret < 0) {
            DB_WARNING("region_id: %ld get_next failed", _region_id);
            break;
        }
        std::map<std::string, ExprValue> field_value_map;
        binlog_get_field_values(field_value_map, record);
        int64_t ts = binlog_get_int64_val("ts", field_value_map); // type 为 COMMIT 时，ts 为 commit_ts
        BinlogType binlog_type = static_cast<BinlogType>(binlog_get_int64_val("binlog_type", field_value_map));
        int64_t start_ts = binlog_get_int64_val("start_ts", field_value_map);
        if (ts < begin_ts || ts > check_point_ts) {
            DB_WARNING("region_id: %ld, ts: %ld, begin_ts: %ld, check_point_ts: %ld", _region_id, ts, begin_ts, check_point_ts);
            break;
        }

        if (binlog_type == FAKE_BINLOG) {
            if (ts > max_fake_binlog) {
                max_fake_binlog = ts;
            }
        }

        if (binlog_type != COMMIT_BINLOG || ts == begin_ts) {
            continue;
        }

        if (start_ts < oldest_ts) {
            DB_WARNING("region_id: %ld, start ts : %ld < oldest ts : %ld", _region_id, start_ts, oldest_ts);
            //特殊错误码 TODO
            response->set_errcode(pb::LESS_THAN_OLDEST_TS); 
            response->set_errmsg("start ts gt oldest ts");
            return;
        }
        
        if ((--binlog_cnt) < 0) {
            break;
        }
        DB_WARNING("region_id: %ld, get bin log start_ts: %ld, commit ts: %ld begin_ts: %ld", _region_id, start_ts, ts, begin_ts);
        return_nr++;
        char buf[sizeof(int64_t)];
        memcpy(buf, (void*)&start_ts, sizeof(int64_t));
        auto binlog_cf = _rocksdb->get_bin_log_handle();
        std::string binlog_value;
        rocksdb::Status status = _rocksdb->get(rocksdb::ReadOptions(), binlog_cf, 
            rocksdb::Slice(buf, sizeof(int64_t)), &binlog_value);
        if (!status.ok()) {
            DB_FATAL("get ts:%ld from rocksdb binlog cf fail, region_id: %ld",
                    start_ts, _region_id);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("read binlog failed");            
            return;
        }
        (*response->add_binlogs()) = binlog_value;
        response->add_commit_ts(ts);
    }

    if (return_nr == 0 && max_fake_binlog != 0) {
        pb::StoreReq fake_binlog;
        DB_WARNING("region_id: %ld, fake binlog ts: %ld", _region_id, max_fake_binlog);
        fake_binlog.set_op_type(pb::OP_FAKE_BINLOG);
        fake_binlog.set_region_id(_region_id);
        fake_binlog.set_region_version(get_version());
        fake_binlog.mutable_binlog_desc()->set_binlog_ts(max_fake_binlog);
        fake_binlog.mutable_binlog()->set_type(pb::FAKE);
        fake_binlog.mutable_binlog()->set_start_ts(max_fake_binlog);
        fake_binlog.mutable_binlog()->set_commit_ts(max_fake_binlog);

        std::string binlog_value;
        if (!fake_binlog.SerializeToString(&binlog_value)) {
            DB_FATAL("region_id: %ld serialize failed", _region_id);
            response->set_errcode(pb::GET_VALUE_FAIL); 
            response->set_errmsg("serialize fake binlog failed"); 
            return;
        }
        (*response->add_binlogs()) = binlog_value;
        response->add_commit_ts(max_fake_binlog);

    }

    response->set_errcode(pb::SUCCESS); 
    response->set_errmsg("read binlog success");

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
    if ((!is_leader()) && (request->op_type() != pb::OP_READ_BINLOG || _shutdown || !_init_success)) {
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
            read_binlog(request, response);
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

} // namespace baikaldb
