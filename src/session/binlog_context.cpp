// Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
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

#include "binlog_context.h"
#include "fetcher_store.h"
#include "meta_server_interact.hpp"
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#include <baidu/rpc/selective_channel.h>
#else
#include <brpc/channel.h>
#include <brpc/selective_channel.h>
#endif

namespace baikaldb {
DEFINE_bool(meta_tso_autoinc_degrade, false, "meta_tso_autoinc_degrade");
BRPC_VALIDATE_GFLAG(meta_tso_autoinc_degrade, brpc::PassValidate);
DECLARE_int64(print_time_us);
DECLARE_int64(retry_interval_us);
DECLARE_int32(fetcher_connect_timeout);
DECLARE_int32(fetcher_request_timeout);
DEFINE_int64(binlog_alarm_time_s, 30, "alarm, > binlog_alarm_time_s from prewrite to commit");
int TsoFetcher::init() {
    int ret = RepeatedTimerTask::init(tso::update_timestamp_interval_ms);
    if (ret != 0) {
        return ret;
    }
    start();
    return 0;
}

void TsoFetcher::run() {
    int64_t now = tso::clock_realtime_ms();
    int64_t prev_physical = 0;
    int64_t prev_logical = 0;
    {
        BAIDU_SCOPED_LOCK(_tso_mutex);
        prev_physical = _tso_obj.physical();
        prev_logical  = _tso_obj.logical();
    }
    int64_t delta = now - prev_physical;
    if (delta < 0) {
        DB_WARNING("physical time slow now:%ld prev:%ld", now, prev_physical);
    }
    int64_t next = now;
    if (delta > tso::update_timestamp_guard_ms) {
        next = now;
    } else if (prev_logical > tso::max_logical / 2) {
        next = now + tso::update_timestamp_guard_ms;
    } else {
        DB_WARNING("don't need update timestamp prev:%ld now:%ld ", prev_physical, now);
        return;
    }
    {
        BAIDU_SCOPED_LOCK(_tso_mutex);
        _tso_obj.set_physical(next);
        _tso_obj.set_logical(0);
    }
}

int64_t TsoFetcher::get_tso(int64_t count) {
    if (FLAGS_meta_tso_autoinc_degrade && _need_degrade) {
        //max_logical前8字节用作区分不同db
        //后10字节用作分配使用
        uint64_t mast_id = _instance_id & 0xFF;
        BAIDU_SCOPED_LOCK(_tso_mutex);
        _tso_obj.set_logical(_tso_obj.logical() + count);
        //超过1<<10就使用下一个ms
        //降级情况单db总共能支持(1000*1024/2)=50w/s的写事务
        if (_tso_obj.logical() & 0x3FF >= 1 << 10) {
            _tso_obj.set_physical(_tso_obj.physical() + 1);
            _tso_obj.set_logical(mast_id << 10 + count);
        }
        DB_WARNING("meta_tso_autoinc_degrade, local tso, physical:%ld, logical:%ld, count:%ld", 
                _tso_obj.physical(), _tso_obj.logical(), count);
        return (_tso_obj.physical() << tso::logical_bits) + _tso_obj.logical() - count;
    }
    pb::TsoRequest request;
    request.set_op_type(pb::OP_GEN_TSO);
    request.set_count(count);
    pb::TsoResponse response;
    int retry_time = 0;
    int ret = 0;
    tso_count << 1;
    for (;;) {
        retry_time++;
        ret = MetaServerInteract::get_tso_instance()->send_request("tso_service", request, response);
        if (ret < 0) {
            if (response.errcode() == pb::RETRY_LATER && retry_time < 5) {
                bthread_usleep(tso::update_timestamp_interval_ms * 1000LL);
                continue;  
            } else {
                DB_FATAL("get tso failed, response:%s", response.ShortDebugString().c_str());
                tso_error << 1;
                return ret;
            }
        }
        break;
    }
    //DB_WARNING("response:%s", response.ShortDebugString().c_str());
    auto&  tso = response.start_timestamp();
    int64_t timestamp = (tso.physical() << tso::logical_bits) + tso.logical();

    return timestamp;
}

SmartPartitionBinlog BinlogContext::get_partition_binlog_ptr(int64_t binlog_id,
                                                            BinlogInfo& binlog_info,
                                                            int64_t partition_id) {
    auto part_iter = binlog_info.binlog_by_partition.find(partition_id);
    if (part_iter != binlog_info.binlog_by_partition.end()) {
        return part_iter->second;
    } else {
        SmartPartitionBinlog partition_binlog_ptr = std::make_shared<PartitionBinlog>();
        int ret = _factory->get_binlog_region_by_partition_id(binlog_id, partition_id, partition_binlog_ptr->binlog_region);
        if (ret < 0) {
            DB_WARNING("get binlog region failed binlog_id:%lu",  binlog_id);
            return nullptr;
        }
        if (_repl_mark_record != nullptr) {
            partition_binlog_ptr->db_tables.insert(_repl_mark_table_info->name);
            pb::TableMutation* mutation = partition_binlog_ptr->binlog_value.add_mutations();
            mutation->add_sequence(pb::MutationType::INSERT);
            mutation->set_table_id(_repl_mark_table_info->id);
            mutation->set_sql(_repl_mark_sql);
            mutation->set_sign(_repl_mark_sign);
            std::string row;
            _repl_mark_record->encode(row);
            mutation->add_insert_rows(std::move(row));
        }
        _tso_count++;
        partition_binlog_ptr->partition_id = partition_id;
        binlog_info.binlog_by_partition[partition_id] = partition_binlog_ptr;
        return partition_binlog_ptr;
    }
}

void BinlogContext::add_mutation(SmartPartitionBinlog& binlog_ptr,
                    int64_t table_id,
                    const std::string& sql,
                    const uint64_t sign,
                    pb::MutationType type,
                    int64_t partition_id,
                    bool all_in_one,
                    const std::map<int64_t, std::vector<std::string>>& retrun_records,
                    const std::map<int64_t, std::vector<std::string>>& retrun_old_records) {
    pb::TableMutation* mutation = binlog_ptr->binlog_value.add_mutations();
    mutation->add_sequence(type);
    mutation->set_table_id(table_id);
    mutation->set_sql(sql);
    mutation->set_sign(sign);
    DB_DEBUG("sql:%s type:%s %ld,%ld", sql.c_str(), pb::MutationType_Name(type).c_str(), retrun_records.size(),
        retrun_records.size());
    switch (type) {
        case pb::MutationType::DELETE: {
            if (all_in_one) {
                for (auto& pair : retrun_records) {
                    for (auto& str_record : pair.second) {
                        mutation->add_deleted_rows(str_record);
                    }
                }
            } else {
                auto iter = retrun_records.find(partition_id);
                if (iter != retrun_records.end()) {
                    for (auto& str_record : iter->second) {
                        mutation->add_deleted_rows(str_record);
                    }
                }
            }
            break;
        }
        case pb::MutationType::INSERT: {
            if (all_in_one) {
                for (auto& pair : retrun_records) {
                    for (auto& str_record : pair.second) {
                        mutation->add_insert_rows(str_record);
                    }
                }
            } else {
                auto iter = retrun_records.find(partition_id);
                if (iter != retrun_records.end()) {
                    for (auto& str_record : iter->second) {
                        mutation->add_insert_rows(str_record);
                    }
                }
            }
            break;
        }
        case pb::MutationType::UPDATE: {
            if (all_in_one) {
                for (auto& pair : retrun_records) {
                    for (auto& str_record : pair.second) {
                        mutation->add_insert_rows(str_record);
                    }
                }
                for (auto& pair : retrun_old_records) {
                    for (auto& str_record : pair.second) {
                        mutation->add_deleted_rows(str_record);
                    }
                }
            } else {
                auto iter = retrun_records.find(partition_id);
                if (iter != retrun_records.end()) {
                    for (auto& str_record : iter->second) {
                        mutation->add_insert_rows(str_record);
                    }
                }
                auto old_iter = retrun_old_records.find(partition_id);
                if (old_iter != retrun_old_records.end()) {
                    for (auto& str_record : old_iter->second) {
                        mutation->add_deleted_rows(str_record);
                    }
                }
            }
            break;
        }
        default:
            break;
    } 
}

int BinlogContext::add_binlog_values(SmartTable& table_info,
                          const std::string& sql,
                          const uint64_t sign,
                          pb::MutationType type,
                          const std::map<int64_t, std::vector<std::string>>& retrun_records,
                          const std::map<int64_t, std::vector<std::string>>& retrun_old_records) {
    auto iter = table_info->binlog_ids.begin();
    while (iter != table_info->binlog_ids.end()) {
        int64_t binlog_id = iter->first;
        DB_DEBUG("add binlog_id :%ld retrun_records:%ld retrun_old_records:%ld", binlog_id,
            retrun_records.size(), retrun_old_records.size());
        auto binlog_iter = _table_binlogs.find(binlog_id);
        if (binlog_iter == _table_binlogs.end()) {
            auto binlog_table_info = _factory->get_table_info_ptr(binlog_id);
            if (binlog_table_info == nullptr) {
                ++iter;
                DB_WARNING("get table info for binlog_id: %ld failed", binlog_id);
                continue;
            }
            BinlogInfo info;
            info.binlog_table_info = binlog_table_info;
            info.partition_is_same_hint = iter->second;
            _table_binlogs[binlog_id] = info;
        }
        auto& binlog_info = _table_binlogs[binlog_id];
        if (binlog_info.binlog_table_info->partition_num == 1) { // binlog表不分区
            auto binlog_ptr = get_partition_binlog_ptr(binlog_id, binlog_info, 0);
            if (binlog_ptr == nullptr) {
                DB_WARNING("get binlog region failed binlog_id:%ld",  binlog_id);
                return -1;
            }
            binlog_ptr->db_tables.insert(table_info->name);
            binlog_ptr->signs.insert(sign);
            _partition_keys[0] = 0;
            add_mutation(binlog_ptr, table_info->id, sql, sign, type, 0, true,
                retrun_records, retrun_old_records);
        } else if (table_info->partition_num == binlog_info.binlog_table_info->partition_num
            && binlog_info.partition_is_same_hint) { // 分区方式一样
            SmartRecord record_template = _factory->new_record(table_info->id);
            if (record_template == nullptr) {
                DB_WARNING(" table_id: %ld binlog_id:%ld not found", table_info->id, binlog_id);
                return -1;
            }
            FieldInfo link_field;
            auto link_iter = table_info->link_field_map.find(binlog_id);
            if (link_iter != table_info->link_field_map.end()) {
                link_field = link_iter->second;
            } else {
                DB_WARNING("binlog link_field not found table_id: %ld binlog_id:%ld",
                table_info->id, binlog_id);
                return -1;
            }
            for (auto& pair : retrun_records) {
                int64_t partition_id = pair.first;
                auto binlog_ptr = get_partition_binlog_ptr(binlog_id, binlog_info, partition_id);
                if (binlog_ptr == nullptr) {
                    DB_WARNING("get binlog region failed binlog_id:%ld",  binlog_id);
                    return -1;
                }
                if (_partition_keys.count(partition_id) == 0) {
                    // 反解第一条record获取parttiton_key
                    auto& str_record = pair.second.front();
                    SmartRecord record = record_template->clone(false);
                    int ret = record->decode(str_record);
                    if (ret < 0) {
                        DB_FATAL("decode to record fail");
                        return -1;
                    }
                    auto field_desc = record->get_field_by_idx(link_field.pb_idx);
                    ExprValue value = record->get_value(field_desc);
                    _partition_keys[partition_id] = value.get_numberic<uint64_t>();
                }
                binlog_ptr->db_tables.insert(table_info->name);
                binlog_ptr->signs.insert(sign);
                add_mutation(binlog_ptr, table_info->id, sql, sign, type, partition_id, false,
                    retrun_records, retrun_old_records);
            }
        } else {
            std::map<int64_t, std::vector<std::string>>  return_str_records;
            std::map<int64_t, std::vector<std::string>>  return_str_old_records;
            SmartRecord record_template = _factory->new_record(table_info->id);
            if (record_template == nullptr) {
                DB_WARNING(" table_id: %ld binlog_id:%ld not found", table_info->id, binlog_id);
                return -1;
            }
            FieldInfo link_field;
            auto link_iter = table_info->link_field_map.find(binlog_id);
            if (link_iter != table_info->link_field_map.end()) {
                link_field = link_iter->second;
            } else {
                DB_WARNING("binlog link_field not found table_id: %ld binlog_id:%ld",
                table_info->id, binlog_id);
                return -1;
            }
            DB_DEBUG("binlog_id:%ld link_field:%s", binlog_id, link_field.name.c_str());
            for (auto& pair : retrun_records) {
                for (auto& str_record : pair.second) {
                    SmartRecord record = record_template->clone(false);
                    int ret = record->decode(str_record);
                    if (ret < 0) {
                        DB_FATAL("decode to record fail");
                        return -1;
                    }
                    int64_t partition_id = get_partition_id(binlog_info.binlog_table_info, record, link_field);
                    if (partition_id < 0) {
                        DB_WARNING("get partition_id failed table_id: %ld binlog_id:%ld",
                            table_info->id, binlog_id);
                        return -1;
                    }
                    return_str_records[partition_id].emplace_back(std::move(str_record));
                }
            }
            for (auto& pair : retrun_old_records) {
                for (auto& str_record : pair.second) {
                    SmartRecord record = record_template->clone(false);
                    int ret = record->decode(str_record);
                    if (ret < 0) {
                        DB_FATAL("decode to record fail");
                        return -1;
                    }
                    int64_t partition_id = get_partition_id(binlog_info.binlog_table_info, record, link_field);
                    if (partition_id < 0) {
                        DB_WARNING("get partition_id failed table_id: %ld binlog_id:%ld",
                            table_info->id, binlog_id);
                        return -1;
                    }
                    return_str_old_records[partition_id].emplace_back(std::move(str_record));
                }
            }
            for (auto& pair : return_str_records) {
                int64_t partition_id = pair.first;
                auto binlog_ptr = get_partition_binlog_ptr(binlog_id, binlog_info, partition_id);
                if (binlog_ptr == nullptr) {
                    DB_WARNING("get binlog region failed binlog_id:%ld",  binlog_id);
                    return -1;
                }
                binlog_ptr->db_tables.insert(table_info->name);
                binlog_ptr->signs.insert(sign);
                add_mutation(binlog_ptr, table_info->id, sql, sign, type, partition_id, false,
                    return_str_records, return_str_old_records);
            }
        }
        ++iter;
    }
    return 0;
}

int BinlogContext::add_binlog_values(SmartTable& table_info,
                          const std::string& sql,
                          const uint64_t sign,
                          pb::MutationType type,
                          const std::vector<SmartRecord>& retrun_records,
                          const std::vector<SmartRecord>& retrun_old_records) {
    auto iter = table_info->binlog_ids.begin();
    while (iter != table_info->binlog_ids.end()) {
        int64_t binlog_id = iter->first;
        DB_DEBUG("add binlog_id :%ld retrun_records:%ld retrun_old_records:%ld", binlog_id,
            retrun_records.size(), retrun_old_records.size());
        auto binlog_iter = _table_binlogs.find(binlog_id);
        if (binlog_iter == _table_binlogs.end()) {
            auto binlog_table_info = _factory->get_table_info_ptr(binlog_id);
            if (binlog_table_info == nullptr) {
                ++iter;
                continue;
            }
            BinlogInfo info;
            info.binlog_table_info = binlog_table_info;
            info.partition_is_same_hint = iter->second;
            _table_binlogs[binlog_id] = info;
        }
        auto& binlog_info = _table_binlogs[binlog_id];
        if (binlog_info.binlog_table_info->partition_num == 1) { // binlog表不分区
            std::map<int64_t, std::vector<std::string>> return_str_records;
            std::map<int64_t, std::vector<std::string>> return_str_old_records;
            SmartRecord record_template = _factory->new_record(table_info->id);
            if (record_template == nullptr) {
                DB_WARNING(" table_id: %ld binlog_id:%ld not found", table_info->id, binlog_id);
                return -1;
            }
            for (auto& record : retrun_records) {
                std::string row;
                record->encode(row);
                return_str_records[0].emplace_back(std::move(row));
            }
            for (auto& record : retrun_old_records) {
                std::string row;
                record->encode(row);
                return_str_old_records[0].emplace_back(std::move(row));
            }
            _partition_keys[0] = 0;
            auto binlog_ptr = get_partition_binlog_ptr(binlog_id, binlog_info, 0);
            if (binlog_ptr == nullptr) {
                DB_WARNING("get binlog region failed binlog_id:%ld",  binlog_id);
                return -1;
            }
            binlog_ptr->db_tables.insert(table_info->name);
            binlog_ptr->signs.insert(sign);
            add_mutation(binlog_ptr, table_info->id, sql, sign, type, 0, true,
                return_str_records, return_str_old_records);
        } else {
            std::map<int64_t, std::vector<std::string>>  return_str_records;
            std::map<int64_t, std::vector<std::string>>  return_str_old_records;
            FieldInfo link_field;
            auto link_iter = table_info->link_field_map.find(binlog_id);
            if (link_iter != table_info->link_field_map.end()) {
                link_field = link_iter->second;
            } else {
                DB_WARNING("binlog link_field not found table_id: %ld binlog_id:%ld",
                table_info->id, binlog_id);
                return -1;
            }
            DB_DEBUG("binlog_id:%ld link_field:%s", binlog_id, link_field.name.c_str());
            for (auto& record : retrun_records) {
                int64_t partition_id = get_partition_id(binlog_info.binlog_table_info, record, link_field);
                if (partition_id < 0) {
                    DB_WARNING("get partition_id failed table_id: %ld binlog_id:%ld",
                        table_info->id, binlog_id);
                    return -1;
                }
                std::string row;
                record->encode(row);
                return_str_records[partition_id].emplace_back(std::move(row));
            }
            for (auto& record : retrun_old_records) {
                int64_t partition_id = get_partition_id(binlog_info.binlog_table_info, record, link_field);
                if (partition_id < 0) {
                    DB_WARNING("get partition_id failed table_id: %ld binlog_id:%ld",
                        table_info->id, binlog_id);
                    return -1;
                }
                std::string row;
                record->encode(row);
                return_str_old_records[partition_id].emplace_back(std::move(row));
            }
            for (auto& pair : return_str_records) {
                int64_t partition_id = pair.first;
                auto binlog_ptr = get_partition_binlog_ptr(binlog_id, binlog_info, partition_id);
                if (binlog_ptr == nullptr) {
                    DB_WARNING("get binlog region failed binlog_id:%ld",  binlog_id);
                    return -1;
                }
                binlog_ptr->db_tables.insert(table_info->name);
                binlog_ptr->signs.insert(sign);
                add_mutation(binlog_ptr, table_info->id, sql, sign, type, partition_id, false,
                    return_str_records, return_str_old_records);
            }
        }
        ++iter;
    }
    return 0;
}

int BinlogContext::send_binlog_data(const WriteBinlogParam* param, const SmartPartitionBinlog& partition_binlog_ptr) {
    TimeCost write_binlog_cost; 
    pb::StoreReq req;
    pb::StoreRes res;

    req.set_db_conn_id(param->global_conn_id);
    req.set_log_id(param->log_id);
    auto binlog_desc = req.mutable_binlog_desc();
    binlog_desc->set_txn_id(param->txn_id);
    binlog_desc->set_start_ts(partition_binlog_ptr->start_ts);
    binlog_desc->set_primary_region_id(param->primary_region_id);
    binlog_desc->set_user_name(param->username);
    binlog_desc->set_user_ip(param->ip);
    auto binlog = req.mutable_binlog();
    binlog->set_start_ts(partition_binlog_ptr->start_ts);
    binlog->set_partition_key(_partition_keys[partition_binlog_ptr->partition_id]);
    if (param->op_type == pb::OP_PREPARE) {
        binlog->set_type(pb::BinlogType::PREWRITE);
        req.set_op_type(pb::OP_PREWRITE_BINLOG);
        binlog_desc->set_binlog_ts(partition_binlog_ptr->start_ts);
        partition_binlog_ptr->calc_binlog_row_cnt();
        binlog_desc->set_binlog_row_cnt(partition_binlog_ptr->binlog_row_cnt);
        auto prewrite_value = binlog->mutable_prewrite_value();
        prewrite_value->CopyFrom(partition_binlog_ptr->binlog_value);
    } else if (param->op_type == pb::OP_COMMIT) {
        binlog->set_type(pb::BinlogType::COMMIT);
        req.set_op_type(pb::OP_COMMIT_BINLOG);
        binlog_desc->set_binlog_ts(partition_binlog_ptr->commit_ts);
        binlog_desc->set_binlog_row_cnt(partition_binlog_ptr->binlog_row_cnt);
        binlog->set_commit_ts(partition_binlog_ptr->commit_ts);
        for (const std::string& db_table : partition_binlog_ptr->db_tables) {
            binlog_desc->add_db_tables(db_table);
        }
        for (uint64_t sign : partition_binlog_ptr->signs) {
            binlog_desc->add_signs(sign);
        }        
    } else if (param->op_type == pb::OP_ROLLBACK) {
        binlog->set_type(pb::BinlogType::ROLLBACK);
        req.set_op_type(pb::OP_ROLLBACK_BINLOG);
        binlog_desc->set_binlog_ts(partition_binlog_ptr->start_ts);
    } else {
        // todo DDL
    }
    int ret = 0;
    pb::RegionInfo& info = partition_binlog_ptr->binlog_region;
    int64_t region_id = info.region_id();
    req.set_region_id(region_id);
    req.set_region_version(info.version());
    int retry_times = 0;
    bool binlog_prepare_success = false;
    do {
        brpc::Channel channel;
        brpc::Controller cntl;
        cntl.set_log_id(param->log_id);
        brpc::ChannelOptions option;
        option.max_retry = 1;
        option.connect_timeout_ms = FLAGS_fetcher_connect_timeout;
        option.timeout_ms = FLAGS_fetcher_request_timeout;
        std::string addr = info.leader();
        if (retry_times == 0) {
            // 重试前已经选择了normal的实例
            // 或者store返回了正确的leader
            param->fetcher_store->choose_other_if_dead(info, addr);
        }
        ret = channel.Init(addr.c_str(), &option);
        if (ret != 0) {
            DB_WARNING("binlog channel init failed, addr:%s, ret:%d, log_id:%lu",
                    addr.c_str(), ret, param->log_id);
            return -1;
        }

        param->client_conn->insert_callid(addr, region_id, cntl.call_id());

        pb::StoreService_Stub(&channel).query_binlog(&cntl, &req, &res, NULL);
        if (cntl.Failed()) {
            DB_WARNING("binlog call failed  errcode:%d, error:%s, region_id:%ld log_id:%lu",
                cntl.ErrorCode(), cntl.ErrorText().c_str(), region_id, param->log_id);
            // 只有网络相关错误码才重试
            if (!param->fetcher_store->rpc_need_retry(cntl.ErrorCode())) {
                return -1;
            }
            if (cntl.ErrorCode() == ECANCELED) {
                return -1;
            }
            param->fetcher_store->other_normal_peer_to_leader(info, addr);
            bthread_usleep(FLAGS_retry_interval_us);
            retry_times++;
            continue;
        }
        DB_DEBUG("binlog fetch store req: %s log_id:%lu", req.ShortDebugString().c_str(), param->log_id);
        DB_DEBUG("binlog fetch store res: %s log_id:%lu", res.ShortDebugString().c_str(), param->log_id);
        if (res.errcode() == pb::NOT_LEADER) {
            DB_WARNING("binlog NOT_LEADER, addr:%s region_id:%ld retry:%d, new_leader:%s, log_id:%lu", addr.c_str(),
                region_id, retry_times, res.leader().c_str(), param->log_id);

            if (res.leader() != "0.0.0.0:0") {
                // store返回了leader，则相信store，不判断normal
                info.set_leader(res.leader());
                _factory->update_leader(info);
            } else {
                param->fetcher_store->other_normal_peer_to_leader(info, addr);
            }
            retry_times++;
            bthread_usleep(retry_times * FLAGS_retry_interval_us);
        } else if (res.errcode() == pb::VERSION_OLD) {
            DB_WARNING("VERSION_OLD, region_id: %ld, retry:%d, now:%s, log_id:%lu",
                    region_id, retry_times, info.ShortDebugString().c_str(), param->log_id);
            for (auto r : res.regions()) {
                DB_WARNING("new version region:%s", r.ShortDebugString().c_str());
                info.CopyFrom(r);
            }
            req.set_region_id(info.region_id());
            req.set_region_version(info.version());
        } else if (res.errcode() == pb::REGION_NOT_EXIST) {
            param->fetcher_store->other_normal_peer_to_leader(info, addr);
            retry_times++;
        } else if (res.errcode() != pb::SUCCESS) {
            DB_WARNING("errcode:%s, write_binlog failed, instance:%s region_id:%ld retry:%d log_id:%lu",
                    pb::ErrCode_Name(res.errcode()).c_str(), addr.c_str(), region_id, retry_times, param->log_id);
            return -1;
        } else {
            // success
            binlog_prepare_success = true;
            break;
        }
    } while (retry_times < 5);
    int64_t query_cost = write_binlog_cost.get_time();
    if (query_cost > FLAGS_print_time_us || retry_times > 0) {
        DB_WARNING("write binlog region_id:%ld log_id:%lu txn_id:%ld cost time:%ld op_type:%s ip:%s",
            region_id, param->log_id, param->txn_id, query_cost, pb::OpType_Name(param->op_type).c_str(), info.leader().c_str());
    }
    if (binlog_prepare_success) {
        if (param->op_type == pb::OP_PREPARE) {
            partition_binlog_ptr->binlog_prewrite_time.reset();
        } else if (param->op_type == pb::OP_COMMIT) {
            if (partition_binlog_ptr->binlog_prewrite_time.get_time() > FLAGS_binlog_alarm_time_s * 1000 * 1000LL) {
                // 报警日志
                DB_WARNING("binlog takes too long from prewrite to commit, txn_id: %ld, binlog_region_id: %ld, start_ts: %s, commit_ts: %s",
                    param->txn_id, region_id, ts_to_datetime_str(partition_binlog_ptr->start_ts).c_str(),
                    ts_to_datetime_str(partition_binlog_ptr->commit_ts).c_str());
            }
        } else {
            // do nothing
        }
    } else {
        DB_WARNING("exec failed log_id:%lu", param->log_id);
        return -1;
    }
    return 0;
}

int BinlogContext::write_binlog(const WriteBinlogParam* param) {
    ConcurrencyBthread write_bth(_tso_count, &BTHREAD_ATTR_NORMAL);
    int error = 0;
    for (auto& pair : _table_binlogs) {
        for (auto& pair2 : pair.second.binlog_by_partition) {
            if (error != 0) {
                break;
            }
            auto partition_binlog_ptr = pair2.second;
            auto write_binlog_func = [this, &error, param, partition_binlog_ptr]() {
                auto ret = send_binlog_data(param, partition_binlog_ptr);
                if (ret != 0) {
                    error = ret;
                }
            };
            write_bth.run(write_binlog_func);
        }
    }
    write_bth.join();
    return error;
}

} // namespace baikaldb
