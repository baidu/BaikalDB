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

#include "baikal_capturer.h"

namespace baikaldb {
DEFINE_string(capture_namespace, "TEST_NAMESPACE", "capture_namespace");
DEFINE_int64(capture_partition_id, 0, "capture_partition_id");
DEFINE_string(capture_tables, "db.tb1;tb.tb2", "capture_tables");
DEFINE_int64(max_cache_txn_num_per_region, 1000, "max_cache_txn_num_per_region");
DEFINE_bool(capture_print_event, false, "capture_print_event");

CaptureStatus FetchBinlog::run(int32_t fetch_num) {
    TimeCost tc;
    CaptureStatus ret = CS_SUCCESS;
    bool first_request_flag = true;
    while (!_is_finish) {

        if (!first_request_flag) {
            bthread_usleep(100 * 1000);
        }
        std::map<int64_t, pb::RegionInfo> region_map;
        std::map<int64_t, int64_t> region_id_ts_map;
        if (_capturer->get_binlog_regions(region_map, region_id_ts_map, _commit_ts) != 0) {
            DB_FATAL("table_id %ld partition %ld log_id %lu get binlog regions error.", _binlog_id, _partition_id, _log_id);
            return CS_FAIL;
        }

        if (_wait_microsecs != 0 && tc.get_time() > _wait_microsecs) {
            DB_WARNING("timeout %lu", _log_id);
            return CS_EMPTY;
        }
        
        ConcurrencyBthread req_threads {(int)region_map.size(), &BTHREAD_ATTR_NORMAL};
        for (auto& region_info : region_map) {
            if (_region_res.find(region_info.first) != _region_res.end()) {
                continue;
            } 
            int64_t commit_ts = region_id_ts_map[region_info.first];
            auto request_binlog_proc = [this, region_info, commit_ts, fetch_num, &ret]() -> int {
                int less_then_oldest_ts_num = 0;
                int no_data_num = 0;
                std::shared_ptr<pb::StoreRes> response(new pb::StoreRes);
                auto s = read_binlog(region_info.second, commit_ts, fetch_num, *response, region_info.second.leader());
                if (s == CS_SUCCESS) {
                    std::lock_guard<std::mutex> guard(_region_res_mutex);
                    result << "[" << region_info.first << ", " << region_info.second.leader() << ", " << response->binlogs_size() << "]";
                    _region_res.emplace(region_info.first, response);
                    return 0;
                } else if (s == CS_EMPTY) {
                    // leader没数据，存在延迟的情况下读follower，否则follower也可能没数据
                    time_t now = time(nullptr);
                    uint32_t delay = std::max(0, (int)(now - tso::get_timestamp_internal(_commit_ts)));
                    if (delay < 10) {
                        return 0;
                    }
                    ++no_data_num;
                } else if (s == CS_LESS_THEN_OLDEST) {
                    ++less_then_oldest_ts_num;
                }

                for (const auto& peer : region_info.second.peers()) {
                    if (peer == region_info.second.leader()) {
                        continue;
                    }
                    response->Clear();
                    s = read_binlog(region_info.second, commit_ts, fetch_num, *response, peer);
                    if (s == CS_SUCCESS) {
                        std::lock_guard<std::mutex> guard(_region_res_mutex);
                        result << "[" << region_info.first << ", " << peer << ", " << response->binlogs_size() << "]";
                        _region_res.emplace(region_info.first, response);
                        return 0;
                    } else if (s == CS_EMPTY) {
                        ++no_data_num;
                    } else if (s == CS_LESS_THEN_OLDEST) {
                        ++less_then_oldest_ts_num;
                    }
                }

                if (less_then_oldest_ts_num == region_info.second.peers_size()) {
                    DB_FATAL("all peer less then oldest ts logid %lu", _log_id);
                    ret = CS_LESS_THEN_OLDEST;
                } else if (no_data_num == 0) {
                    // 如果没有peer返回no data则说明全部失败
                    DB_FATAL("all peer failed, logid %lu", _log_id);
                    ret = CS_FAIL;
                }
                return 0;
            };
            req_threads.run(request_binlog_proc);
        }
        req_threads.join();
        if (ret == CS_LESS_THEN_OLDEST || ret == CS_FAIL) {
            return ret;
        }
        _is_finish = (_region_res.size() == region_map.size());
        first_request_flag = false;
    }
    DB_DEBUG("fetch binlog %s log[%lu] time[%ld]", result.str().c_str(), _log_id, tc.get_time());
    return CS_SUCCESS;
}

CaptureStatus FetchBinlog::read_binlog(const pb::RegionInfo& region_info, int64_t commit_ts, int fetch_num_per_region, pb::StoreRes& response, const std::string& peer) {
    pb::StoreReq request; 
    request.set_op_type(pb::OP_READ_BINLOG);
    request.set_region_id(region_info.region_id());
    request.set_region_version(region_info.version());
    auto binlog_ptr = request.mutable_binlog_desc();
    binlog_ptr->set_start_ts(commit_ts);
    binlog_ptr->set_binlog_ts(commit_ts);
    binlog_ptr->set_read_binlog_cnt(std::max(fetch_num_per_region, 1));
    if (_capturer->flash_back_read()) {
        binlog_ptr->set_flash_back_read(true);
        if (!_capturer->user_name().empty()) {
            binlog_ptr->set_user_name(_capturer->user_name());
        }
        if (!_capturer->user_ip().empty()) {
            binlog_ptr->set_user_ip(_capturer->user_ip());
        }
        if (!_capturer->db_tables().empty()) {
            for (const std::string& db_table : _capturer->db_tables()) {
                binlog_ptr->add_db_tables(db_table);
            }
        }
        if (!_capturer->signs().empty()) {
            for (const uint64_t sign : _capturer->signs()) {
                binlog_ptr->add_signs(sign);
            }
        }
        if (!_capturer->txn_ids().empty()) {
            for (const int64_t txn_id : _capturer->txn_ids()) {
                binlog_ptr->add_txn_ids(txn_id);
            }
        }
    } else {
        binlog_ptr->set_flash_back_read(false);
    }
    StoreInteract store_interact(peer);
    auto ret = store_interact.send_request_for_leader(_log_id, "query_binlog", request, response);
    if (ret != 0) {
        if (response.errcode() == pb::LESS_THAN_OLDEST_TS) {
            DB_WARNING("region %ld store %s less_then_oldest_ts log_id %lu", 
                region_info.region_id(), peer.c_str(), _log_id);
            return CS_LESS_THEN_OLDEST;
        }
        DB_WARNING("region %ld get binlog error store %s log_id %lu.", 
            request.region_id(), peer.c_str(), _log_id);
        return CS_FAIL;
    } else {
        if (response.binlogs_size() == 0) {
            DB_WARNING("region %ld no binlog data store %s log_id %lu.", 
                request.region_id(), peer.c_str(), _log_id);
            return CS_EMPTY;
        } else {
            if (response.binlogs_size() != response.commit_ts_size()) {
                DB_FATAL("binlog size != commit_ts size. log_id %lu", _log_id);
                return CS_FAIL;
            }
        }
    }
    return CS_SUCCESS;
}

CaptureStatus MergeBinlog::run() {
    //获取所有region中的最小commit_ts
    baikaldb::TimeCost tc;
    for (auto& res_info : _fetcher_result) {
        auto region_id = res_info.first;
        size_t commit_ts_index = 0; 
        for (const auto& binlog : res_info.second->binlogs()) {

            auto commit_ts = res_info.second->commit_ts(commit_ts_index++);
            StoreReqPtr store_req_ptr(new pb::StoreReq);
            if (!store_req_ptr->ParseFromString(binlog)) {
                DB_FATAL("StoreReq ParseFromString error log_id %lu.", _log_id);
                return CS_FAIL;
            }
            if (store_req_ptr->has_binlog()) {
                DB_DEBUG("get binlog type %d", int(store_req_ptr->binlog().type()));
                if (store_req_ptr->binlog().has_prewrite_value()) {
                    DB_DEBUG("binlog size %d log_id %lu.", 
                        store_req_ptr->binlog().prewrite_value().mutations_size(), _log_id);
                } else {
                    DB_DEBUG("no mutations log_id %lu.", _log_id);
                }

                _capturer->insert_binlog(region_id, commit_ts, store_req_ptr);
            }
        }
    }
    
    //merge排序
    int64_t all_min_commit_ts = _capturer->merge_binlog(_queue);
    consume_cnt = _queue.size();
    DB_DEBUG("after merge min_commit_ts[%lu, %s] consume_cnt[%d] log_id[%lu] time[%ld]", 
        all_min_commit_ts, ts_to_datetime_str(all_min_commit_ts).c_str(), consume_cnt, _log_id, tc.get_time());

    return CS_SUCCESS;
}

int BinLogTransfer::init(const std::map<int64_t, SubTableIds>& sub_table_ids) {
    //获取主键index_info
    for (const auto& ids : sub_table_ids) {
        int64_t table_id = ids.first;
        CapInfo& cap_info = _cap_infos[table_id];

        cap_info.fields = ids.second.fields;
        cap_info.monitor_fields = ids.second.monitor_fields;
        cap_info.table_info = baikaldb::SchemaFactory::get_instance()->get_table_info_ptr(table_id);
        if (cap_info.table_info == nullptr) {
            DB_FATAL("get table info error table_id %ld", table_id);
            return -1;
        }
        for (int64_t index_id : cap_info.table_info->indices) {
            SmartIndex info_ptr = baikaldb::SchemaFactory::get_instance()->get_index_info_ptr(index_id);
            if (info_ptr == nullptr) {
                DB_FATAL("no index info found with index_id: %ld", index_id);
                return -1;
            }
            if (info_ptr->type == pb::I_PRIMARY) {
                cap_info.pri_info = *info_ptr;
            }
        }
        const DatabaseInfo& db_info = baikaldb::SchemaFactory::get_instance()->get_database_info(cap_info.table_info->db_id);
        if (db_info.id == -1) {
            DB_FATAL("get database info error.");
            return -1;
        }

        for (const auto& field : cap_info.table_info->fields) {
            cap_info.signed_map[field.id] = is_signed(field.type);
        }

        for (const auto& pk_field : cap_info.pri_info.fields) {
            cap_info.pk_map[pk_field.id] = true;
        }
        cap_info.db_name = db_info.name;
    }
    return 0;
}

int64_t BinLogTransfer::run(int64_t& commit_ts) {
    int64_t insert_size = 0;
    int64_t delete_size = 0;
    int64_t update_size = 0;
    baikaldb::TimeCost tc;
    while (!_queue.empty()) {
        const auto store_req_ptr_commit = _queue.top();
        auto& binlog = store_req_ptr_commit.req_ptr->binlog();
        commit_ts = store_req_ptr_commit.commit_ts;
        int64_t txn_id = store_req_ptr_commit.req_ptr->binlog_desc().txn_id();
        if (binlog.type() == pb::FAKE) {
            if (!_capturer->flash_back_read()) {
                make_heartbeat_message(binlog.commit_ts());
            }
            _queue.pop();
            fake_cnt++;
            continue;
        }
        std::string user_name;
        std::string user_ip;
        if (_capturer->flash_back_read()) {
            auto& binlog_desc = store_req_ptr_commit.req_ptr->binlog_desc();
            user_name = binlog_desc.user_name();
            user_ip = binlog_desc.user_ip();
            if (!_capturer->user_name().empty() && _capturer->user_name() != binlog_desc.user_name()) {
                continue;
            }
            if (!_capturer->user_ip().empty() && _capturer->user_ip() != binlog_desc.user_ip()) {
                continue;
            }
            if (!_capturer->txn_ids().empty() && _capturer->txn_ids().count(binlog_desc.txn_id()) <= 0) {
                continue;
            }
        }
        auto partition_key = binlog.partition_key();
        for (const auto& mutation : binlog.prewrite_value().mutations()) {
            RecordCollection records;
            int64_t table_id = mutation.table_id();
            if (_cap_infos.count(table_id) == 0) {
                table_filter_cnt++;
                continue;
            }
            if (_two_way_sync != nullptr) {
                CapInfo& cap_info = _cap_infos[table_id];
                if (_two_way_sync->two_way_sync_table_name == cap_info.db_name + "." + cap_info.table_info->short_name) {
                    two_way_sync_filter_txn_cnt++;
                    break;
                }
            }
            if (_capturer->flash_back_read()) {
                if (!_capturer->signs().empty() && _capturer->signs().count(mutation.sign()) <= 0) {
                    continue;
                }

                std::shared_ptr<CapturerSQLInfo> info(new CapturerSQLInfo);
                CapInfo& cap_info = _cap_infos[table_id];
                info->database = cap_info.db_name;
                info->table = cap_info.table_info->short_name;
                info->txn_id = txn_id;
                info->sign = mutation.sign();
                info->sql = mutation.sql();
                info->user_name = user_name;
                info->user_ip = user_ip;
                _sqlinfo_vec.emplace_back(info);

            }
            if (transfer_mutation(mutation, records) != 0) {
                DB_FATAL("transfer mutation error.");
                return -1;
            }
            if (FLAGS_capture_print_event) {
                DB_NOTICE("commit_ts: %ld, insert_cnt: %d, delete_cnt: %d", commit_ts, mutation.insert_rows_size(), mutation.deleted_rows_size());
            }

            group_records(records);
            insert_size += records.insert_records.size();
            delete_size += records.delete_records.size();
            update_size += records.update_records.size();
            if (multi_records_to_event(records.insert_records, mysub::INSERT_EVENT, commit_ts, table_id, partition_key) != 0) {
                DB_FATAL("insert records to event error.");
                return -1;
            }
            if (multi_records_to_event(records.delete_records, mysub::DELETE_EVENT, commit_ts, table_id, partition_key) != 0) {
                DB_FATAL("delete records to event error.");
                return -1;
            }
            if (multi_records_update_to_event(records.update_records, commit_ts, table_id, partition_key) != 0) {
                DB_FATAL("update records to event error.");
                return -1;
            }
        }
        _queue.pop();
    }

    DB_DEBUG("binlog transfer result table_filter_cnt[%ld] two_way_sync_filter_txn_cnt[%ld] fake_cnt[%ld] "
        "insert[%ld] delete[%ld] update[%ld] log[%lu] time[%ld]", 
        table_filter_cnt, two_way_sync_filter_txn_cnt, fake_cnt, insert_size, delete_size, update_size, 
        _log_id, tc.get_time());
    return 0;
}

void BinLogTransfer::make_heartbeat_message(int64_t fake_ts) {
    uint32_t now_timestamp = tso::get_timestamp_internal(fake_ts);
    std::string timestamp_str = std::to_string(now_timestamp);

    std::shared_ptr<mysub::Event> event;
    mysub::Event* p_event = google::protobuf::Arena::CreateMessage<mysub::Event>(_p_arena);
    if (_p_arena != nullptr) {
        event.reset(p_event, EventDeleter());
    } else {
        event.reset(p_event);
    }
    event->set_db("baidu_dba");
    event->set_table("heartbeat");
    event->set_host("");
    event->set_port(0);
    event->set_timestamp(now_timestamp);
    event->set_event_type(mysub::EventType(0));
    event->set_shard(_partition_id);

    mysub::Row* row = event->mutable_row();
    if (row == NULL) {
        return;
    }
    mysub::Field* field1 = row->add_field();
    if (field1 == NULL) {
        return;
    }
    field1->set_name("id");
    field1->set_mysql_type(mysub::MYSQL_TYPE_VARCHAR);
    field1->set_is_signed(false);
    field1->set_is_pk(true);
    field1->set_new_value(std::to_string(_partition_id));
    field1->set_old_value(std::to_string(_partition_id));

    mysub::Field* field2 = row->add_field();
    if (field2 == NULL) {
        return;
    }
    field2->set_name("value");
    field2->set_mysql_type(mysub::MYSQL_TYPE_VARCHAR);
    field2->set_is_signed(false);
    field2->set_is_pk(false);
    field2->set_new_value(timestamp_str);
    field2->set_old_value(timestamp_str);

    _event_vec.push_back(std::move(event));
    return;
}

int BinLogTransfer::multi_records_update_to_event(const UpdatePairVec& update_records, int64_t commit_ts, int64_t table_id, uint64_t partition_key) {
    for (const auto& record : update_records) {
        auto delete_insert_records = std::make_pair(
            record.first.get(),
            record.second.get()
            );
        if (single_record_to_event(delete_insert_records, mysub::UPDATE_EVENT, commit_ts, table_id, partition_key) !=0) {
            DB_WARNING("insert update record error.");
            return -1;
        }
    }
    return 0;
}

int BinLogTransfer::multi_records_to_event(const RecordMap& records, mysub::EventType event_type, int64_t commit_ts, int64_t table_id, uint64_t partition_key) {
    for (const auto& record : records) {
        auto delete_insert_records = std::make_pair(
            event_type == mysub::DELETE_EVENT ? record.second.get() : nullptr,
            event_type == mysub::INSERT_EVENT ? record.second.get() : nullptr
            );
        if (single_record_to_event(delete_insert_records, event_type, commit_ts, table_id, partition_key) != 0) {
            DB_WARNING("insert/delete  record error.");
            return -1;
        }
    }
    return 0;
}

void print_event(int64_t commit_ts, const std::shared_ptr<mysub::Event>& event) {
    bool has_diff = false;
    std::string fields = "(";
    std::string diff_fields = "(";
    std::string values = "(";
    std::string diff_new_values = "(";
    std::string old_values = "(";
    std::string diff_old_values = "(";
    for (auto& field : event->row().field()) {
        fields += field.name() + ",";
        values += field.new_value() + ",";
        old_values += field.old_value() + ",";
        if (field.is_new_null() != field.is_old_null() || field.new_value() != field.old_value()) {
            diff_fields += field.name() + ",";
            diff_new_values += field.new_value() + ",";
            diff_old_values += field.old_value() + ",";
            has_diff = true;
        }
    }
    fields.pop_back();
    fields += ")";
    values.pop_back();
    values += ")";
    old_values.pop_back();
    old_values += ")";
    diff_fields.pop_back();
    diff_fields += ")";
    diff_new_values.pop_back();
    diff_new_values += ")";
    diff_old_values.pop_back();
    diff_old_values += ")";
    std::string ts = baikaldb::timestamp_to_str(event->timestamp());
    switch (event->event_type())
    {
    case mysub::INSERT_EVENT: {
        DB_NOTICE("table %s.%s event_timestamp: %s commit_ts: %ld insert rows:%s - %s old_values:%s", event->db().c_str(),
            event->table().c_str(), ts.c_str(), commit_ts, fields.c_str(), values.c_str(), old_values.c_str());
        break;
    }
    case mysub::DELETE_EVENT: {
        DB_NOTICE("table %s.%s event_timestamp: %s commit_ts: %ld delete rows:%s - %s old_values:%s", event->db().c_str(),
            event->table().c_str(), ts.c_str(), commit_ts, fields.c_str(), values.c_str(), old_values.c_str());
        break;
    }
    case mysub::UPDATE_EVENT: {
        if (!has_diff) {
            DB_NOTICE("table %s.%s event_timestamp: %s commit_ts: %ld update rows:%s - %s old_values:%s no diff", event->db().c_str(),
                event->table().c_str(), ts.c_str(), commit_ts, fields.c_str(), values.c_str(), old_values.c_str());
        } else {
            DB_NOTICE("table %s.%s event_timestamp: %s commit_ts: %ld update rows:%s - %s old_values:%s diff_fields: %s, "
                "diff_new_values: %s, diff_old_values: %s", event->db().c_str(), event->table().c_str(), ts.c_str(), commit_ts, 
                fields.c_str(), values.c_str(), old_values.c_str(), diff_fields.c_str(), diff_new_values.c_str(), diff_old_values.c_str());
        }
        break;
    }
    default:
        break;
    }     
}

int BinLogTransfer::single_record_to_event(const std::pair<TableRecord*, TableRecord*>& delete_insert_records, 
        mysub::EventType event_type, int64_t commit_ts, int64_t table_id, uint64_t partition_key) {
    std::shared_ptr<mysub::Event> event;
    mysub::Event* p_event = google::protobuf::Arena::CreateMessage<mysub::Event>(_p_arena);
    if (_p_arena != nullptr) {
        event.reset(p_event, EventDeleter());
    } else {
        event.reset(p_event);
    }
    CapInfo& cap_info = _cap_infos[table_id];
    auto delete_record = delete_insert_records.first;
    auto insert_record = delete_insert_records.second;
    event->set_db(cap_info.db_name);
    event->set_table(cap_info.table_info->short_name);
    event->set_event_type(event_type);
    event->set_binlog_type(event_type);
    event->set_timestamp(tso::get_timestamp_internal(commit_ts));
    event->set_charset(cap_info.table_info->charset == pb::UTF8 ? "utf8" : "gbk");
    event->set_partition_key(partition_key);
    event->set_shard(_partition_id);
    auto row_iter = event->mutable_row();
    bool update_need_sub = false;
    for (const auto& field : cap_info.table_info->fields) {
        mysub::Field tmp_field;
        auto field_iter = &tmp_field;//row_iter->add_field();
        field_iter->set_name(field.short_name.c_str());
        field_iter->set_mysql_type(mysub::MysqlType(to_mysql_type(field.type)));
        field_iter->set_is_signed(cap_info.signed_map[field.id]);
        field_iter->set_is_pk(cap_info.pk_map[field.id]);
        if (insert_record != nullptr) {
            bool is_null = false;
            std::string out;
            int ret = insert_record->field_to_string(field, &out, &is_null);
            if (ret != 0) {
                DB_WARNING("insert new value error.");
                return -1;
            }
            field_iter->set_new_value(out);
            field_iter->set_is_new_null(is_null);
        }
        if (delete_record != nullptr) {
            bool is_null = false;
            std::string out;
            int ret = delete_record->field_to_string(field, &out, &is_null);
            if (ret != 0) {
                DB_WARNING("delete old value error.");
                return -1;
            }
            field_iter->set_old_value(out);
            field_iter->set_is_old_null(is_null);
        }

        if (event_type == mysub::UPDATE_EVENT && !update_need_sub && 
                (cap_info.monitor_fields.count(field.id) > 0 || cap_info.monitor_fields.empty())) {
            if (field_iter->is_new_null() != field_iter->is_old_null() || field_iter->new_value() != field_iter->old_value()) {
                update_need_sub = true;
            }
        }
        if (cap_info.fields.empty() || cap_info.fields.count(field.id) > 0) {
            auto f = row_iter->add_field();
            f->Swap(field_iter);
        }

    }
    if (FLAGS_capture_print_event) {
        // capturer tool订阅时不过滤update
        print_event(commit_ts, event);
    }
    if (event_type != mysub::UPDATE_EVENT || update_need_sub) {
        _event_vec.push_back(std::move(event));
    }
    if (event_type == mysub::UPDATE_EVENT && !update_need_sub) {
        monitor_fields_filter_cnt++;
    }
    DB_DEBUG("event str %s log_id[%lu] commit_ts[%ld]", 
        event->ShortDebugString().c_str(), _log_id, commit_ts);
    return 0;
}

void BinLogTransfer::group_records(RecordCollection& records) {
    auto& update_records = records.update_records;
    auto& insert_records = records.insert_records;
    auto& delete_records = records.delete_records;
    for (auto insert_iter = insert_records.begin(); insert_iter != insert_records.end();) {
        auto delete_iter = delete_records.find(insert_iter->first);
        if (delete_iter != delete_records.end()) {
            update_records.emplace_back(
                delete_iter->second,
                insert_iter->second
            );
            delete_records.erase(delete_iter);
            insert_iter = insert_records.erase(insert_iter);
        } else {
            insert_iter++;
        }
    }
}

int BinLogTransfer::transfer_mutation(const pb::TableMutation& mutation, RecordCollection& records) {
    auto& insert_records = records.insert_records;
    auto& delete_records = records.delete_records;
    if (mutation.insert_rows_size() > 0) {
        if (deserialization(mutation.insert_rows(), insert_records, mutation.table_id()) != 0) {
            DB_FATAL("deserialization insert rows error.");
            return -1;
        }
    }
    if (mutation.deleted_rows_size() > 0) {
        if (deserialization(mutation.deleted_rows(), delete_records, mutation.table_id()) != 0) {
            DB_FATAL("deserialization delete rows error.");
            return -1;
        }
    }
    return 0;
}

int CapturerSingleton::init(const std::string& namespace_, const std::map<std::string, SubTableNames>& table_infos) {
    std::unique_lock<bthread::Mutex> l(_lock);
    if (_is_init) {
        return 0;
    }

    if (baikaldb::SchemaFactory::get_instance()->init() != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    } 

    baikaldb::BinlogNetworkServer* server = BinlogNetworkServer::get_instance();
    server->config(namespace_, table_infos);

    if (baikaldb::MetaServerInteract::get_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    
    if (!server->init()) {
        DB_FATAL("Failed to initail network server.");
        return -1;
    }

    server->schema_heartbeat();
    _is_init = true;
    return 0;
}
#if BAIDU_INTERNAL
int Capturer::init(Json::Value& config) {
    if (!config.isMember("db_shard") || !config["db_shard"].isNumeric()) {
        DB_FATAL("config db_shard info error.");
        return -1;
    }
    if (!config.isMember("bns") || !config["bns"].isString()) {
        DB_FATAL("bns info error.");
        return -1;
    }
    if (!config.isMember("namespace") || !config["namespace"].isString()) {
        DB_FATAL("namespace info error.");
        return -1;
    }
    _namespace = config["namespace"].asString();
    FLAGS_meta_server_bns = config["bns"].asString();
    _partition_id = config["db_shard"].asInt64();
    std::map<std::string, SubTableNames> table_infos;
    if (config.isMember("table_names") && config["table_names"].isArray()) {
        for (const auto& table_info : config["table_names"]) {
            if (!table_info.isMember("name")) {
                DB_FATAL("config table info error.");
                return -1;
            }
            SubTableNames& info = table_infos[table_info["name"].asString()];
            info.table_name = table_info["name"].asString();
            _db_tables.insert(info.table_name);
            bool is_monitor = false;
            if (table_info.isMember("monitor_fields")) {
                is_monitor = true;
            }
            if (table_info["fields"].isArray()) {
                for (auto& field : table_info["fields"]) {
                    info.fields.insert(field.asString());
                    if (!is_monitor) {
                        info.monitor_fields.insert(field.asString());
                    }
                }
            }
            if (is_monitor && table_info["monitor_fields"].isArray()) {
                for (auto& field : table_info["monitor_fields"]) {
                    info.monitor_fields.insert(field.asString());
                }
            }
        }
    }
    if (config.isMember("event_filter")) {
        DB_NOTICE("config twoway sync event_filter");
        auto&& event_filter_config = config["event_filter"];
        if (event_filter_config.isMember("type") && event_filter_config["type"].isString()) {
            if (event_filter_config["type"].asString() == "TwoWaySyncEventFilter") {
                _two_way_sync.reset(new TwoWaySync(event_filter_config["table_name"].asString()));
            }
        }
    }
    if (config.isMember("user_name")) {
        _user_name = config["user_name"].asString();
    }
    if (config.isMember("user_ip")) {
        _user_ip = config["user_ip"].asString();
    }
    if (config.isMember("signs") && config["signs"].isArray()) {
        for (auto& sign : config["signs"]) {
            _signs.insert(sign.asUInt64());
        }
    }
    if (config.isMember("txn_ids") && config["txn_ids"].isArray()) {
        for (auto& txn_id : config["txn_ids"]) {
            _txn_ids.insert(txn_id.asInt64());
        }
    }
    int ret = CapturerSingleton::get_instance()->init(_namespace, table_infos);
    if (ret != 0) {
        return -1;
    }
    _binlog_id = BinlogNetworkServer::get_instance()->get_binlog_target_id();
    _schema_factory = SchemaFactory::get_instance();
    return 0;
}
#endif
int Capturer::init() {
    _namespace = FLAGS_capture_namespace;
    _partition_id = FLAGS_capture_partition_id;
    std::vector<std::string> split_vec;
    boost::split(split_vec, FLAGS_capture_tables,
            boost::is_any_of(";"), boost::token_compress_on);
    std::map<std::string, SubTableNames> table_infos;
    for (const auto& tb : split_vec) {
        SubTableNames& info = table_infos[tb];
        info.table_name = tb;
    }
    int ret = CapturerSingleton::get_instance()->init(_namespace, table_infos);
    if (ret != 0) {
        return -1;
    }
    _binlog_id = BinlogNetworkServer::get_instance()->get_binlog_target_id();
    _schema_factory = SchemaFactory::get_instance();
    return 0;
}

int64_t Capturer::get_offset(uint32_t timestamp) {
    return timestamp_to_ts(timestamp);
}

uint32_t Capturer::get_timestamp(int64_t offset) {
    return tso::get_timestamp_internal(offset);
}

CaptureStatus Capturer::subscribe(std::vector<std::shared_ptr<mysub::Event>>& event_vec, 
        std::vector<std::shared_ptr<CapturerSQLInfo>>& sqlinfo_vec,
        int64_t& commit_ts, int32_t fetch_num, int64_t wait_microsecs, google::protobuf::Arena* p_arena) {
    auto ret = CS_SUCCESS;
    int64_t tmp_commit_ts = commit_ts;
    uint64_t log_id = butil::fast_rand();
    baikaldb::TimeCost tc;
    FetchBinlog fetcher(this, log_id, wait_microsecs, commit_ts);
    ret = fetcher.run(fetch_num);
    if (ret != CS_SUCCESS) {
        DB_NOTICE("fetcher binlog error. commit_ts[%ld] commit_timestamp[%s] time[%ld] log[%lu]",
            commit_ts, ts_to_datetime_str(commit_ts).c_str(), tc.get_time(), log_id);
        return ret;
    }
    
    //merge
    MergeBinlog merger(this, fetcher.get_result(), log_id);
    auto merge_status = merger.run();
    if (merge_status != CS_SUCCESS) {
        DB_NOTICE("merger binlog table_id[%ld] partition_id[%ld] commit_ts[%ld] commit_timestamp[%s] time[%ld] log[%lu]",
            _binlog_id, _partition_id, commit_ts, ts_to_datetime_str(commit_ts).c_str(), tc.get_time(), log_id);
        return merge_status;
    }

    BinLogTransfer transfer(this, merger.get_result(), event_vec, sqlinfo_vec, log_id, get_two_way_sync(), p_arena);
    if (transfer.init(BinlogNetworkServer::get_instance()->get_table_ids()) != 0) {
        DB_FATAL("BinLogTransfer error [%ld] return commit_ts [%ld] return commit_timestamp [%s]", log_id, commit_ts, ts_to_datetime_str(commit_ts).c_str());
        return CS_FAIL;
    }
    if (transfer.run(commit_ts) != 0) {
        DB_FATAL("insert to event error. log[%lu]", log_id);
        commit_ts = tmp_commit_ts;
        event_vec.clear();
        return CS_FAIL;
    }
    if (commit_ts < tmp_commit_ts) {
        commit_ts = tmp_commit_ts;
        event_vec.clear();
        return CS_FAIL;
    }
    update_commit_ts(commit_ts);
    time_t now = time(nullptr);
    uint32_t delay = std::max(0, (int)(now - tso::get_timestamp_internal(commit_ts)));
    DB_NOTICE("subscribe success binlog table_id[%ld] partition_id[%ld] need_fetch_num[%d] wait_microsecs[%ld] fetcher: %s "
        "merger: consume_cnt[%d] remain %s transfer: fake_cnt[%ld] table_filter[%ld] 2way_filter[%ld] monitor_filter[%ld] "
        "event_cnt[%ld] request_ts[%ld, %s] response_ts[%ld, %s] delay[%u]s log_id[%lu] time_cost[%ld] us", _binlog_id, 
        _partition_id, fetch_num, wait_microsecs, fetcher.result.str().c_str(), merger.consume_cnt, remain_info().c_str(), transfer.fake_cnt, 
        transfer.table_filter_cnt, transfer.two_way_sync_filter_txn_cnt, transfer.monitor_fields_filter_cnt, event_vec.size(), tmp_commit_ts, 
        ts_to_datetime_str(tmp_commit_ts).c_str(), commit_ts, ts_to_datetime_str(commit_ts).c_str(), delay, log_id, tc.get_time());
    return ret;
}

int Capturer::get_binlog_regions(std::map<int64_t, pb::RegionInfo>& region_map, std::map<int64_t, int64_t>& region_id_ts_map, int64_t commit_ts) {
    region_map.clear();
    region_id_ts_map.clear();
    if (_schema_factory->get_binlog_regions(_binlog_id, _partition_id, region_map) != 0 ||
        region_map.size() == 0) {
        DB_FATAL("table_id %ld partition %ld get binlog regions error.", _binlog_id, _partition_id);
        return -1;
    }

    if (commit_ts != _last_commit_ts) {
        _region_binlogs_map.clear();
        _last_commit_ts = commit_ts;
        for (const auto& iter : region_map) {
            region_id_ts_map[iter.first] = _last_commit_ts;
        }
        return 0;
    }

    auto region_info_iter = region_map.begin();
    while (region_info_iter != region_map.end()) {
        if (_skip_regions.count(region_info_iter->first) > 0) {
            region_map.erase(region_info_iter++);
            continue;
        }
        auto& ts_binlog_map = _region_binlogs_map[region_info_iter->first];
        if (ts_binlog_map.size() > FLAGS_max_cache_txn_num_per_region) {
            // 避免等待某个region时其他region过快
            region_map.erase(region_info_iter++);
        } else if (ts_binlog_map.size() == 0) {
            region_id_ts_map[region_info_iter->first] = _last_commit_ts;
            region_info_iter++;
        } else {
            region_id_ts_map[region_info_iter->first] = ts_binlog_map.rbegin()->first;
            region_info_iter++;
        }
    }

    return 0;
}

int64_t Capturer::merge_binlog(BinLogPriorityQueue& queue) {
    int64_t all_min_commit_ts = std::numeric_limits<long long>::max();
    for (const auto& iter : _region_binlogs_map) {
        if (iter.second.empty()) {
            return _last_commit_ts;
        }

        all_min_commit_ts = std::min(all_min_commit_ts, iter.second.rbegin()->first);
    }

    for (const auto& iter : _region_binlogs_map) {
        for (const auto& ts_binlog : iter.second) {
            if (ts_binlog.first <= all_min_commit_ts) {
                queue.push(StoreReqWithCommit(ts_binlog.first, ts_binlog.second));
            } else {
                break;
            }
        }
    }

    return all_min_commit_ts;
}

void Capturer::update_commit_ts(int64_t commit_ts) {
    for (auto& iter : _region_binlogs_map) {
        auto ts_binlog_iter = iter.second.begin();
        while (ts_binlog_iter != iter.second.end()) {
            if (ts_binlog_iter->first <= commit_ts) {
                iter.second.erase(ts_binlog_iter++);
            } else {
                break;
            }
        }
    }

    _last_commit_ts = commit_ts;
}

}
