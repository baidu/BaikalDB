#include "baikal_capturer.h"

namespace baikaldb {
DEFINE_string(capture_namespace, "TEST_NAMESPACE", "capture_namespace");
DEFINE_int64(capture_partition_id, 0, "capture_partition_id");
DEFINE_string(capture_tables, "db.tb1;tb.tb2", "capture_tables");

uint32_t get_timestamp_internal(int64_t offset) {
    return ((offset >> 18) + tso::base_timestamp_ms) / 1000;
}

CaptureStatus FetchBinlog::run(int32_t fetch_num) {
    //TODO 退出机制
    baikaldb::TimeCost tc;
    CaptureStatus ret = CS_SUCCESS;
    bool first_request_flag = true;
    while (!_is_finish) {

        if (!first_request_flag) {
            bthread_usleep(100 * 1000);
        }
        std::map<int64_t, pb::RegionInfo> region_map;
        if (baikaldb::SchemaFactory::get_instance()->get_binlog_regions(_binlog_id, _partition_id, region_map) != 0 ||
            region_map.size() == 0) {
            DB_FATAL("table_id %ld partition %ld, get binlog regions error.", _binlog_id, _partition_id);
            return CS_FAIL;
        }

        int32_t fetch_num_per_region = fetch_num / region_map.size();

        if (_wait_microsecs != 0 && tc.get_time() > _wait_microsecs) {
            DB_FATAL("timeout %lu", _log_id);
            return CS_TIMEOUT;
        }
        DB_DEBUG("get table_id %ld partition %ld region size %lu log_id: %lu", 
            _binlog_id, _partition_id, region_map.size(), _log_id);
        
        ConcurrencyBthread req_threads {(int)region_map.size(), &BTHREAD_ATTR_NORMAL};
        for (auto& region_info : region_map) {
            DB_DEBUG("request region %ld for binlog data log_id[%lu].", 
                region_info.second.region_id(), _log_id);
            if (_region_res.find(region_info.first) != _region_res.end()) {
                DB_DEBUG("get region %ld result", region_info.second.region_id());
            } else {
                auto request_binlog_proc = [this, region_info, fetch_num_per_region, &ret]() -> int {
                    int8_t less_then_oldest_ts_num = 0;
                    pb::StoreReq request; 
                    request.set_op_type(pb::OP_READ_BINLOG);
                    request.set_region_id(region_info.second.region_id());
                    request.set_region_version(region_info.second.version());
                    auto binlog_ptr = request.mutable_binlog_desc();
                    binlog_ptr->set_start_ts(_commit_ts);
                    binlog_ptr->set_binlog_ts(_commit_ts);
                    binlog_ptr->set_read_binlog_cnt(std::max(fetch_num_per_region, 1));
                    DB_DEBUG("request %s logid %lu", request.ShortDebugString().c_str(), _log_id);
                    auto request_binlog = [&request, &less_then_oldest_ts_num, this, &region_info](const std::string& peer) -> int{
                        std::shared_ptr<pb::StoreRes> response(new pb::StoreRes);
                        StoreInteract store_interact(peer);
                        auto ret = store_interact.send_request_for_leader(_log_id, "query_binlog", request, *response.get());
                        if (ret != 0) {
                            if (response->errcode() == pb::LESS_THAN_OLDEST_TS) {
                                DB_WARNING("less_then_oldest_ts %lu", _log_id);
                                less_then_oldest_ts_num++;
                            }
                            DB_WARNING("region %ld get binlog error store %s log_id %lu.", 
                                region_info.second.region_id(), peer.c_str(), _log_id);
                            return -1;
                        } else {
                            //没有数据，继续请求
                            if (response->binlogs_size() == 0) {
                                DB_WARNING("region %ld no binlog data store %s log_id %lu.", 
                                    region_info.second.region_id(), peer.c_str(), _log_id);
                                return -1;
                            } else {
                                if (response->binlogs_size() != response->commit_ts_size()) {
                                    DB_FATAL("binlog size != commit_ts size. log_id %lu", _log_id);
                                    return -1;
                                }
                                DB_NOTICE("region %ld binlog data store %s log_id %lu size %d.", 
                                    region_info.second.region_id(), peer.c_str(), _log_id, response->binlogs_size());
                                DB_DEBUG("fetcher store data %s", response->DebugString().c_str());
                                std::lock_guard<std::mutex> guard(_region_res_mutex);
                                _region_res.emplace(region_info.first, response);
                            }
                        }
                        return 0;
                    };
                    if (request_binlog(region_info.second.leader()) == 0) {
                        DB_NOTICE("request leader[%s] success %lu", region_info.second.leader().c_str(), _log_id);
                        return 0;
                    }
                    for (const auto& peer : region_info.second.peers()) {
                        if (peer == region_info.second.leader()) {
                            continue;
                        }
                        if (request_binlog(peer) == 0) {
                            DB_DEBUG("request peer[%s] success %lu", peer.c_str(), _log_id);
                            break;
                        }
                    }
                    if (less_then_oldest_ts_num == region_info.second.peers_size()) {
                        DB_WARNING("all peer less then oldest ts logid %lu", _log_id);
                        ret = CS_LESS_THEN_OLDEST;
                    }
                    return 0;
                };
                req_threads.run(request_binlog_proc);
            }
        }
        req_threads.join();
        if (ret == CS_LESS_THEN_OLDEST) {
            return ret;
        }
        _is_finish = (_region_res.size() == region_map.size());
        first_request_flag = false;
    }
    return CS_SUCCESS;
}

CaptureStatus MergeBinlog::run(int64_t& commit_ts) {
    //获取所有region中的最小commit_ts
    int64_t all_min_commit_ts = std::numeric_limits<long long>::max();
    DB_DEBUG("merge size %lu", _fetcher_result.size());
    for (auto& res_info : _fetcher_result) {
        int64_t current_max_commit_ts = -1;
        auto region_id = res_info.first;
        size_t commit_ts_index = 0; 
        for (const auto& binlog : res_info.second->binlogs()) {

            auto commit_ts = res_info.second->commit_ts(commit_ts_index++);
            StoreReqPtr store_req_ptr(new pb::StoreReq);
            if (!store_req_ptr->ParseFromString(binlog)) {
                DB_FATAL("StoreReq ParseFromString error log_id %lu.", _log_id);
                return CS_FAIL;
            }
            DB_DEBUG("binlog str %s log_id %lu", store_req_ptr->ShortDebugString().c_str(), _log_id);
            if (store_req_ptr->has_binlog()) {
                DB_DEBUG("get binlog type %d", int(store_req_ptr->binlog().type()));
                if (store_req_ptr->binlog().has_prewrite_value()) {
                    DB_DEBUG("binlog size %d log_id %lu.", 
                        store_req_ptr->binlog().prewrite_value().mutations_size(), _log_id);
                } else {
                    DB_DEBUG("no mutations log_id %lu.", _log_id);
                }
                if (commit_ts > current_max_commit_ts) {
                    current_max_commit_ts = commit_ts;
                }

                _binlogs_map[region_id].emplace_back(commit_ts, std::move(store_req_ptr));
            }
        }
        if (current_max_commit_ts < all_min_commit_ts) {
            all_min_commit_ts = current_max_commit_ts;
        }
    }
    DB_NOTICE("after merge min_commit_ts[%lu] log_id[%lu]", all_min_commit_ts, _log_id);
    //merge排序
    for (auto binlog : _binlogs_map) {
        auto& req_vector = binlog.second;

        for (const auto& req : req_vector) {
            if (req.commit_ts <= all_min_commit_ts) {
                DB_DEBUG("insert to queue commit_ts %ld", req.commit_ts);
                _queue.push(req);
            }
        }
    }
    DB_NOTICE("after merge queue size %lu log_id %lu", _queue.size(), _log_id);

    if (_queue.size() == 1) {
        const auto store_req_ptr_commit = _queue.top();
        auto& binlog = store_req_ptr_commit.req_ptr->binlog();
        if (binlog.type() == pb::FAKE) {
            commit_ts = store_req_ptr_commit.commit_ts;
            DB_NOTICE("queue size[1], commit_ts[%ld] log_id[%lu] binlog type[FAKE]", commit_ts, _log_id);
            return CS_EMPTY;
        }
    }
    return CS_SUCCESS;
}

int BinLogTransfer::init() {
    //获取主键index_info
    for (auto tid : _origin_ids) {
        DB_NOTICE("config table id [%ld]", tid);
        auto& cap_info = _cap_infos[tid];

        cap_info.table_info = baikaldb::SchemaFactory::get_instance()->get_table_info_ptr(tid);
        if (cap_info.table_info == nullptr) {
            DB_FATAL("get table info error table_id %ld", tid);
            return -1;
        }
        for (const auto index_id : cap_info.table_info->indices) {
            auto info_ptr = baikaldb::SchemaFactory::get_instance()->get_index_info_ptr(index_id);
            if (info_ptr == nullptr) {
                DB_FATAL("no index info found with index_id: %ld", index_id);
                return -1;
            }
            if (info_ptr->type == pb::I_PRIMARY) {
                cap_info.pri_info = *info_ptr;
            }
        }
        const auto& db_info = baikaldb::SchemaFactory::get_instance()->get_database_info(cap_info.table_info->db_id);
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
    while (!_queue.empty()) {
        const auto store_req_ptr_commit = _queue.top();
        auto& binlog = store_req_ptr_commit.req_ptr->binlog();
        commit_ts = store_req_ptr_commit.commit_ts;
        DB_DEBUG("get binlog commit_ts[%ld] logid[%lu]", commit_ts, _log_id);
        for (const auto& mutation : binlog.prewrite_value().mutations()) {
            RecordCollection records;
            int64_t tid = mutation.table_id();

            if (_cap_infos.count(tid) == 0) {
                DB_DEBUG("table_id[%ld] is filter.", tid);
                continue;
            }
            if (transfer_mutation(mutation, records) != 0) {
                DB_FATAL("transfer mutation error.");
                return -1;
            }
            DB_DEBUG("after trans insert %lu delete %lu", 
                records.insert_records.size(), records.delete_records.size());
            group_records(records);
            DB_DEBUG("after group insert %lu delete %lu update %lu", 
                records.insert_records.size(), records.delete_records.size(), records.update_records.size());
            insert_size += records.insert_records.size();
            delete_size += records.delete_records.size();
            update_size += records.update_records.size();
            if (multi_records_to_event(records.insert_records, mysub::INSERT_EVENT, commit_ts, tid) != 0) {
                DB_FATAL("insert records to event error.");
                return -1;
            }
            if (multi_records_to_event(records.delete_records, mysub::DELETE_EVENT, commit_ts, tid) != 0) {
                DB_FATAL("delete records to event error.");
                return -1;
            }
            if (multi_records_update_to_event(records.update_records, commit_ts, tid) != 0) {
                DB_FATAL("update records to event error.");
                return -1;
            }
        }
        _queue.pop();
    }
    DB_NOTICE("binlog result insert[%ld] delete[%ld] update[%ld] log[%lu]", 
        insert_size, delete_size, update_size, _log_id);
    return 0;
}
int BinLogTransfer::multi_records_update_to_event(const UpdatePairVec& update_records, int64_t commit_ts, int64_t tid) {
    for (const auto& record : update_records) {
        std::shared_ptr<mysub::Event> event(new mysub::Event);
        auto delete_insert_records = std::make_pair(
            record.first.get(),
            record.second.get()
            );
        if (single_record_to_event(event.get(), delete_insert_records, mysub::UPDATE_EVENT, commit_ts, tid) !=0) {
            DB_WARNING("insert update record error.");
            return -1;
        }
        _event_vec.push_back(std::move(event));
    }
    return 0;
}

int BinLogTransfer::multi_records_to_event(const RecordMap& records, mysub::EventType event_type, int64_t commit_ts, int64_t tid) {
    for (const auto& record : records) {
        std::shared_ptr<mysub::Event> event(new mysub::Event);
        auto delete_insert_records = std::make_pair(
            event_type == mysub::DELETE_EVENT ? record.second.get() : nullptr,
            event_type == mysub::INSERT_EVENT ? record.second.get() : nullptr
            );
        if (single_record_to_event(event.get(), delete_insert_records, event_type, commit_ts, tid) != 0) {
            DB_WARNING("insert/delete  record error.");
            return -1;
        }
        _event_vec.push_back(std::move(event));
    }
    return 0;
}

int BinLogTransfer::single_record_to_event(mysub::Event* event, 
    const std::pair<TableRecord*, TableRecord*>& delete_insert_records, mysub::EventType event_type, int64_t commit_ts, int64_t tid) {
    auto& cap_info = _cap_infos[tid];
    auto delete_record = delete_insert_records.first;
    auto insert_record = delete_insert_records.second;
    event->set_db(cap_info.db_name);
    event->set_table(cap_info.table_info->short_name);
    event->set_event_type(event_type);
    event->set_binlog_type(event_type);
    event->set_timestamp(get_timestamp_internal(commit_ts));
    event->set_charset(cap_info.table_info->charset == pb::UTF8 ? "utf8" : "gbk");
    auto row_iter = event->mutable_row();
    for (const auto& field : cap_info.table_info->fields) {
        auto field_iter = row_iter->add_field();
        field_iter->set_name(field.short_name.c_str());
        field_iter->set_mysql_type(mysub::MysqlType(to_mysql_type(field.type)));
        field_iter->set_is_signed(cap_info.signed_map[field.id]);
        field_iter->set_is_pk(cap_info.pk_map[field.id]);
        if (insert_record != nullptr) {
            int ret = insert_record->field_to_string(field, field_iter->mutable_new_value());
            if (ret != 0) {
                DB_WARNING("insert new value error.");
                return -1;
            }
        }
        if (delete_record != nullptr) {
            int ret = delete_record->field_to_string(field, field_iter->mutable_old_value());
            if (ret != 0) {
                DB_WARNING("delete old value error.");
                return -1;
            }
        }
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
            insert_records.erase(insert_iter++);
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
#if BAIDU_INTERNAL
int Capturer::init(Json::Value& config) {
    if (baikaldb::init_log("baikal_capture") != 0) {
        fprintf(stderr, "log init failed.");
    }
    DB_WARNING("log file load success");

    if (baikaldb::SchemaFactory::get_instance()->init() != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    } 

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
    if (config.isMember("table_names") && config["table_names"].isArray()) {
        for (const auto& table_info : config["table_names"]) {
            if (!table_info.isMember("name")) {
                DB_FATAL("config table info error.");
                return -1;
            }
            _table_infos.push_back(table_info["name"].asString());
        }
    }
    for (const auto& table_info : _table_infos) {
        DB_NOTICE("config namespace[%s] table[%s]", _namespace.c_str(), table_info.c_str());
        BinlogNetworkServer::get_instance()->config(_namespace, table_info);
    }

    if (baikaldb::MetaServerInteract::get_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    baikaldb::BinlogNetworkServer* server = baikaldb::BinlogNetworkServer::get_instance();
    if (!server->init()) {
        DB_FATAL("Failed to initail network server.");
        return -1;
    }

    _binlog_id = server->get_binlog_target_id();
    _origin_ids = server->get_binlog_origin_ids();
    server->schema_heartbeat();
    _schema_factory = baikaldb::SchemaFactory::get_instance();
    return 0;
}
#endif
int Capturer::init() {
    if (baikaldb::init_log("baikal_capture") != 0) {
        fprintf(stderr, "log init failed.");
    }
    DB_WARNING("log file load success");

    if (baikaldb::SchemaFactory::get_instance()->init() != 0) {
        DB_FATAL("SchemaFactory init failed");
        return -1;
    } 

    _namespace = FLAGS_capture_namespace;
    _partition_id = FLAGS_capture_partition_id;
    std::vector<std::string> split_vec;
    boost::split(split_vec, FLAGS_capture_tables,
            boost::is_any_of(";"), boost::token_compress_on);
    for (const auto& tb : split_vec) {
        _table_infos.push_back(tb);
    }
    for (const auto& table_info : _table_infos) {
        DB_NOTICE("config namespace[%s] table[%s]", _namespace.c_str(), table_info.c_str());
        BinlogNetworkServer::get_instance()->config(_namespace, table_info);
    }

    if (baikaldb::MetaServerInteract::get_instance()->init() != 0) {
        DB_FATAL("meta server interact init failed");
        return -1;
    }
    baikaldb::BinlogNetworkServer* server = baikaldb::BinlogNetworkServer::get_instance();
    if (!server->init()) {
        DB_FATAL("Failed to initail network server.");
        return -1;
    }

    _binlog_id = server->get_binlog_target_id();
    _origin_ids = server->get_binlog_origin_ids();
    server->schema_heartbeat();
    _schema_factory = baikaldb::SchemaFactory::get_instance();
    return 0;
}

int64_t Capturer::get_offset(uint32_t timestamp) {
    return (((int64_t)timestamp) * 1000 - tso::base_timestamp_ms) << 18;
}

uint32_t Capturer::get_timestamp(int64_t offset) {
    return get_timestamp_internal(offset);
}

CaptureStatus Capturer::subscribe(std::vector<std::shared_ptr<mysub::Event>>& event_vec, 
    int64_t& commit_ts, int32_t fetch_num, int64_t wait_microsecs) {

    auto ret = CS_SUCCESS;
    int64_t tmp_commit_ts = commit_ts;
    uint64_t log_id = butil::fast_rand();
    baikaldb::TimeCost tc;
    FetchBinlog fetcher(log_id, wait_microsecs, commit_ts, _binlog_id, _partition_id);
    ret = fetcher.run(fetch_num);
    if (ret != CS_SUCCESS) {
        DB_NOTICE("fetcher binlog error. commit_ts[%ld] time[%ld] log[%lu]",
            commit_ts, tc.get_time(), log_id);
        return ret;
    }
    
    DB_NOTICE("fetcher binlog table_id[%ld] partition_id[%ld] commit_ts[%ld] time[%ld] log[%lu]",
        _binlog_id, _partition_id, commit_ts, tc.get_time(), log_id);
    
    //merge
    MergeBinlog merger(fetcher.get_result(), log_id);
    auto merge_status = merger.run(commit_ts);
    if (merge_status != CS_SUCCESS) {
        DB_NOTICE("merger binlog table_id[%ld] partition_id[%ld] commit_ts[%ld] time[%ld] log[%lu]",
            _binlog_id, _partition_id, commit_ts, tc.get_time(), log_id);
        return merge_status;
    }

    BinLogTransfer transfer(_binlog_id, merger.get_result(), event_vec, _origin_ids, log_id);
    if (transfer.init() != 0) {
        DB_FATAL("BinLogTransfer error %ld return commit_ts %ld.", log_id, commit_ts);
        return CS_FAIL;
    }
    if (transfer.run(commit_ts) != 0) {
        DB_FATAL("insert to event error. log[%lu]", log_id);
        commit_ts = tmp_commit_ts;
        event_vec.clear();
        return CS_FAIL;
    }
    DB_NOTICE("return success request_commit_ts[%ld] response_commit_ts[%ld]", tmp_commit_ts, commit_ts);
    return ret;
}
}
