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

#include "baikal_heartbeat.h"
#include "schema_factory.h"
#include "task_fetcher.h"

namespace baikaldb {

DECLARE_int32(baikal_heartbeat_interval_us);

void BaikalHeartBeat::construct_heart_beat_request(pb::BaikalHeartBeatRequest& request, bool is_backup) {
    SchemaFactory* factory = nullptr;
    if (is_backup) {
        factory = SchemaFactory::get_backup_instance();
    } else {
        factory = SchemaFactory::get_instance();
    }
    if (!factory->is_inited()) {
        return;
    }

    auto schema_read_recallback = [&request, factory](const SchemaMapping& schema){
        for (auto& info_pair : schema.table_info_mapping) {
            if (info_pair.second->engine != pb::ROCKSDB &&
                    info_pair.second->engine != pb::ROCKSDB_CSTORE && 
                    info_pair.second->engine != pb::BINLOG) {
                continue;
            }
            //主键索引和全局二级索引都需要传递region信息
            for (auto& index_id : info_pair.second->indices) {
                auto& index_info_mapping = schema.index_info_mapping;
                if (index_info_mapping.count(index_id) == 0) {
                    continue;
                }
                IndexInfo index = *index_info_mapping.at(index_id);
                //主键索引
                if (index.type != pb::I_PRIMARY && !index.is_global) {
                    continue;
                }
                auto req_info = request.add_schema_infos();
                req_info->set_table_id(index_id);
                req_info->set_version(1);

                if (index_id == info_pair.second->id) {
                    req_info->set_version(info_pair.second->version);
                }
            }  
        }
    };
    request.set_last_updated_index(factory->last_updated_index());
    factory->schema_info_scope_read(schema_read_recallback);
    TaskFactory<pb::RegionDdlWork>::get_instance()->construct_heartbeat(request, 
        &pb::BaikalHeartBeatRequest::add_region_ddl_works);
    TaskFactory<pb::DdlWorkInfo>::get_instance()->construct_heartbeat(request, 
        &pb::BaikalHeartBeatRequest::add_ddl_works);
    request.set_can_do_ddlwork(is_backup ? false : true);
    //定时将request当中消息体更新 等待process_baikal_heartbeat处理更新至TableManager中的virtual_Index_sql_map
    auto sample = factory->get_virtual_index_info();
    auto index_id_name_map = sample.index_id_name_map;
    auto index_id_sample_sqls_map = sample.index_id_sample_sqls_map;
    //遍历map 更新至request 
    for (auto& it1 : index_id_name_map) {
        for (auto& it2 : index_id_sample_sqls_map[it1.first]) {
            pb::VirtualIndexInfluence virtual_index_influence;
            virtual_index_influence.set_virtual_index_id(it1.first);
            virtual_index_influence.set_virtual_index_name(it1.second);
            virtual_index_influence.set_influenced_sql(it2);
            pb::VirtualIndexInfluence* info_affect = request.add_info_affect();
            *info_affect = virtual_index_influence ;
        }
    }
}

void BaikalHeartBeat::process_heart_beat_response(const pb::BaikalHeartBeatResponse& response, bool is_backup) {
    SchemaFactory* factory = nullptr;
    if (is_backup) {
        factory = SchemaFactory::get_backup_instance();
    } else {
        factory = SchemaFactory::get_instance();
    }
    if (!factory->is_inited()) {
        return;
    }

    for (auto& info : response.schema_change_info()) {
        factory->update_table(info);
    }
    factory->update_regions(response.region_change_info());
    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info());
    }
    for (auto& info : response.privilege_change_info()) {
        factory->update_user(info);
    }
    factory->update_show_db(response.db_info());
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }
    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index()) {
        factory->set_last_updated_index(response.last_updated_index());
    }
    
    TaskFactory<pb::RegionDdlWork>::get_instance()->process_heartbeat(response, 
        static_cast<
            const google::protobuf::RepeatedPtrField<pb::RegionDdlWork>& (pb::BaikalHeartBeatResponse::*)() const
        >(&pb::BaikalHeartBeatResponse::region_ddl_works));
    TaskFactory<pb::DdlWorkInfo>::get_instance()->process_heartbeat(response, 
        static_cast<
            const google::protobuf::RepeatedPtrField<pb::DdlWorkInfo>& (pb::BaikalHeartBeatResponse::*)() const
        >(&pb::BaikalHeartBeatResponse::ddl_works));
}

void BaikalHeartBeat::process_heart_beat_response_sync(const pb::BaikalHeartBeatResponse& response) {
    TimeCost cost;
    SchemaFactory* factory = SchemaFactory::get_instance();
    factory->update_tables_double_buffer_sync(response.schema_change_info());

    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info());
    }
    for (auto& info : response.privilege_change_info()) {
        factory->update_user(info);
    }
    factory->update_show_db(response.db_info());
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }

    factory->update_regions_double_buffer_sync(response.region_change_info());
    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index()) {
        factory->set_last_updated_index(response.last_updated_index());
    }
    DB_NOTICE("sync time:%ld", cost.get_time());
}

// BaseBaikalHeartBeat
int BaseBaikalHeartBeat::init() {
    if (_is_inited) {
        return 0;
    }

    const int RETRY_TIMES = 3;
    int retry = 0;
    int ret = 0;
    do {
        ret = heartbeat(true);
        if (ret != 0) {
            DB_WARNING("heartbeat sync failed, retry_times: %d", retry);
            ++retry;
            bthread_usleep(30 * 1000 * 1000);
            continue;
        }
        break;
    } while (retry < RETRY_TIMES);

    if (ret != 0) {
        DB_FATAL("heartbeat sync failed, over retry_times: %d", retry);
        return -1;
    }
    
    _heartbeat_bth.run([this](){ report_heartbeat(); });

    _is_inited = true;
    DB_NOTICE("Succ to init BaseBaikalHeartBeat");
    
    return 0;
}

int BaseBaikalHeartBeat::heartbeat(bool is_sync) {
    TimeCost cost;
    pb::BaikalHeartBeatRequest request;
    pb::BaikalHeartBeatResponse response;

    BaikalHeartBeat::construct_heart_beat_request(request);
    request.set_can_do_ddlwork(false);
    request.set_need_heartbeat_table(true);
    for (const auto& full_table_table : _table_names) {
        auto* heartbeat_table = request.add_heartbeat_tables();
        if (heartbeat_table == nullptr) {
            DB_WARNING("baikal_heartbeat_table is nullptr");
            return -1;
        }
        heartbeat_table->set_namespace_name(full_table_table.namespace_name);
        heartbeat_table->set_database(full_table_table.database);
        heartbeat_table->set_table_name(full_table_table.table_name);
    }

    int64_t construct_req_cost = cost.get_time();
    
    if (MetaServerInteract::get_instance()->send_request("baikal_heartbeat", request, response) == 0) {
        if (is_sync) {
            BaikalHeartBeat::process_heart_beat_response_sync(response);
            DB_WARNING("sync report_heartbeat, construct_req_cost:%ld, process_res_cost:%ld",
                        construct_req_cost, cost.get_time());
        } else {
            BaikalHeartBeat::process_heart_beat_response(response);
            DB_WARNING("async report_heartbeat, construct_req_cost:%ld, process_res_cost:%ld",
                        construct_req_cost, cost.get_time());
        }
    } else {
        DB_WARNING("Send heart beat request to meta server fail");
        return -1;
    }

    return 0;
}

void BaseBaikalHeartBeat::report_heartbeat() {
    while (!_shutdown) {
        heartbeat(false);
        bthread_usleep_fast_shutdown(FLAGS_baikal_heartbeat_interval_us, _shutdown);
    }
}

//binlog network
bool BinlogNetworkServer::init() {
    // init val 
    TimeCost cost;
    // 先把meta数据都获取到
    pb::BaikalHeartBeatRequest request;
    pb::BaikalHeartBeatResponse response;
    //1、构造心跳请求
    BaikalHeartBeat::construct_heart_beat_request(request);
    request.set_can_do_ddlwork(false);
    request.set_need_heartbeat_table(true);
    request.set_need_binlog_heartbeat(true);
    for (const auto& info : _table_infos) {
        std::string db_table_name = info.second.table_name;
        std::vector<std::string> split_vec;
        boost::split(split_vec, db_table_name, boost::is_any_of("."));
        if (split_vec.size() != 2) {
            DB_FATAL("get table_name[%s] fail", db_table_name.c_str());
            continue;
        }
        const std::string& database = split_vec[0];
        const std::string& table_name = split_vec[1];

        auto* heartbeat_table = request.add_heartbeat_tables();
        if (heartbeat_table == nullptr) {
            DB_WARNING("baikal_heartbeat_table is nullptr");
            return -1;
        }
        heartbeat_table->set_namespace_name(_namespace);
        heartbeat_table->set_database(database);
        heartbeat_table->set_table_name(table_name);
    }

    //2、发送请求
    if (MetaServerInteract::get_instance()->send_request("baikal_heartbeat", request, response) == 0) {
        //处理心跳
        return process_heart_beat_response_sync(response);
        //DB_WARNING("req:%s  \nres:%s", request.DebugString().c_str(), response.DebugString().c_str());
    } else {
        DB_FATAL("send heart beat request to meta server fail");
        return false;
    }

    return true;
}

void BinlogNetworkServer::report_heart_beat() {
    while (!_shutdown) {
        TimeCost cost;
        pb::BaikalHeartBeatRequest request;
        pb::BaikalHeartBeatResponse response;
        //1、construct heartbeat request
        BaikalHeartBeat::construct_heart_beat_request(request);
        request.set_can_do_ddlwork(false);
        request.set_need_heartbeat_table(true);
        request.set_need_binlog_heartbeat(true);
        for (const auto& info : _table_infos) {
            std::string db_table_name = info.second.table_name;
            std::vector<std::string> split_vec;
            boost::split(split_vec, db_table_name, boost::is_any_of("."));
            if (split_vec.size() != 2) {
                DB_FATAL("get table_name[%s] fail", db_table_name.c_str());
                continue;
            }
            const std::string& database = split_vec[0];
            const std::string& table_name = split_vec[1];

            auto* heartbeat_table = request.add_heartbeat_tables();
            if (heartbeat_table == nullptr) {
                DB_WARNING("baikal_heartbeat_table is nullptr");
                return;
            }
            heartbeat_table->set_namespace_name(_namespace);
            heartbeat_table->set_database(database);
            heartbeat_table->set_table_name(table_name);
        }

        int64_t construct_req_cost = cost.get_time();
        cost.reset();
        //2、send heartbeat request to meta server
        if (MetaServerInteract::get_instance()->send_request("baikal_heartbeat", request, response) == 0) {
            //处理心跳
            process_heart_beat_response(response);
            DB_WARNING("report_heart_beat, construct_req_cost:%ld, process_res_cost:%ld",
                    construct_req_cost, cost.get_time());
        } else {
            DB_WARNING("send heart beat request to meta server fail");
        }
        bthread_usleep_fast_shutdown(FLAGS_baikal_heartbeat_interval_us, _shutdown);
    }
}

void BinlogNetworkServer::process_heart_beat_response(const pb::BaikalHeartBeatResponse& response) {
    DB_DEBUG("response %s", response.ShortDebugString().c_str());
    SchemaFactory* factory = SchemaFactory::get_instance();
    
    for (auto& info : response.schema_change_info()) {
        factory->update_table(info);
    }

    update_table_infos();

    RegionVec rv;
    auto& region_change_info = response.region_change_info();
    for (auto& region_info : region_change_info) {
        if (_binlog_id != region_info.table_id()) {
            DB_WARNING("skip region info table_id %ld", region_info.table_id());
            continue;
        }
        *rv.Add() = region_info;
    }
    factory->update_regions(rv);
    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info());
    }
    for (auto& info : response.privilege_change_info()) {
        factory->update_user(info);
    }
    factory->update_show_db(response.db_info());
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }
    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index()) {
        factory->set_last_updated_index(response.last_updated_index());
    }
}

int BinlogNetworkServer::update_table_infos() {
    SchemaFactory* factory = SchemaFactory::get_instance();
    std::vector<SmartTable> table_ptrs;
    std::map<int64_t, SubTableNames> table_id_names_map;
    for (const auto& info : _table_infos) {
        std::string db_table_name = info.second.table_name;
        if (db_table_name.find("*") != db_table_name.npos) {
            std::vector<std::string> vec;
            boost::split(vec, db_table_name, boost::is_any_of("."));
            if (vec.size() != 2 || vec[1] != "*") {
                DB_FATAL("get star table[%s.%s] fail", _namespace.c_str(), db_table_name.c_str());
                continue;
            }
            std::vector<SmartTable> tmp_table_ptrs;
            factory->get_all_table_by_db(_namespace, vec[0], tmp_table_ptrs);
            for (SmartTable t : tmp_table_ptrs) {
                if (t != nullptr && !t->is_linked) {
                    if (t->name != "baidu_dba.heartbeat") {
                        DB_WARNING("table[%s.%s] not linked", t->namespace_.c_str(), t->name.c_str());
                    }
                    continue;
                }
                SubTableNames& names = table_id_names_map[t->id];
                names.table_name = t->name;
                names.fields = info.second.fields;
                // FC_Word.*模式下monitor_fields清掉，任何列变更都下发
                names.monitor_fields.clear();
                table_ptrs.emplace_back(t);
                DB_NOTICE("get table_name[%s.%s] table_id[%ld]", _namespace.c_str(), t->name.c_str(), t->id);
            }
        } else {
            SmartTable t = factory->get_table_info_ptr_by_name(_namespace + "." + db_table_name);
            if (t == nullptr) {
                DB_FATAL("get table[%s.%s] fail", _namespace.c_str(), db_table_name.c_str());
                continue;
            }
            if (!t->is_linked) {
                if (t->name != "baidu_dba.heartbeat") {
                    DB_WARNING("table[%s.%s] not linked", t->namespace_.c_str(), t->name.c_str());
                }
                continue;
            }
            table_id_names_map[t->id] = info.second;
            table_ptrs.emplace_back(t);
            DB_NOTICE("get table_name[%s.%s] table_id[%ld]", _namespace.c_str(), db_table_name.c_str(), t->id);
        }
    }
    if (table_ptrs.empty()) {
        DB_WARNING("no sub table");
        return 0;
    }
    std::set<int64_t> all_binlog_ids;
    for (auto& table_ptr : table_ptrs) {
        if(table_ptr == nullptr) {
            continue;
        }
        for (auto pair : table_ptr->binlog_ids) {
            all_binlog_ids.emplace(pair.first);
        }
    }
    // 获取所有表公共的binlog表_binlog_id
    std::set<int64_t> common_binlog_ids;
    for (auto id : all_binlog_ids) {
        bool find = true;
        for (auto& table_ptr : table_ptrs) {
            if(table_ptr != nullptr && table_ptr->binlog_ids.count(id) == 0) {
                find = false;
                break;
            }
        }
        if (find) {
            common_binlog_ids.emplace(id);
        }
    }
    if (common_binlog_ids.size() == 0) {
        DB_FATAL("get binlog id error.");
        return -1;
    }
    if (common_binlog_ids.size() > 1) {
        DB_WARNING("selected multi binlog_ids: %ld maybe config error", common_binlog_ids.size());
    }
    for (SmartTable table : table_ptrs) {
        if (table != nullptr && common_binlog_ids.count(table->binlog_id) != 0) {
            _binlog_id = table->binlog_id;
        }
    }

    if (_binlog_id == -1) {
        DB_FATAL("get binlog id error.");
        return -1;
    }
    std::map<int64_t, SubTableIds> tmp_table_ids;
    for (SmartTable table : table_ptrs) {
        if (table->binlog_id != _binlog_id && table->binlog_ids.count(_binlog_id) == 0) {
            DB_WARNING("table[%s.%s] has different binlog id %ld", table->namespace_.c_str(), table->name.c_str(), table->binlog_id);
            continue;
        } else {
            DB_NOTICE("insert table[%s.%s] table_id: %ld, binlog table id %ld", 
                table->namespace_.c_str(), table->name.c_str(), table->id, table->binlog_id);
        }

        SubTableNames& names = table_id_names_map[table->id];
        SubTableIds table_ids;
        bool find_all_fields = true;
        for (const std::string& field_name : names.fields) {
            int id = table->get_field_id_by_short_name(field_name);
            if (id < 0) {
                DB_FATAL("table[%s] cant find field[%s]", table->name.c_str(), field_name.c_str());
                find_all_fields = false;
                break;
            }
            table_ids.fields.insert(id);
        }

        for (const std::string& field_name : names.monitor_fields) {
            int id = table->get_field_id_by_short_name(field_name);
            if (id < 0) {
                DB_FATAL("table[%s] cant find field[%s]", table->name.c_str(), field_name.c_str());
                find_all_fields = false;
                break;
            }
            table_ids.monitor_fields.insert(id);
        }  
        if (find_all_fields) {
            tmp_table_ids[table->id] = table_ids;
        }

    }

    std::lock_guard<bthread::Mutex> l(_lock);
    _table_ids = tmp_table_ids;

    return 0;
}

bool BinlogNetworkServer::process_heart_beat_response_sync(const pb::BaikalHeartBeatResponse& response) {
    DB_DEBUG("response %s", response.ShortDebugString().c_str());
    TimeCost cost;
    SchemaFactory* factory = SchemaFactory::get_instance();
    SchemaVec sv;
    std::vector<int64_t> tmp_tids;
    tmp_tids.reserve(10);
    factory->update_tables_double_buffer_sync(response.schema_change_info());

    if (update_table_infos() != 0) {
        return false;
    } 

    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info());
    }
    for (auto& info : response.privilege_change_info()) {
        factory->update_user(info);
    }
    RegionVec rv;
    auto& region_change_info = response.region_change_info();
    for (auto& region_info : region_change_info) {
        if (_binlog_id != region_info.table_id()) {
            DB_DEBUG("skip region info table_id %ld", region_info.table_id());
            continue;
        }
        *rv.Add() = region_info;
    }
    factory->update_regions_double_buffer_sync(rv);
    factory->update_show_db(response.db_info());
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }

    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index()) {
        factory->set_last_updated_index(response.last_updated_index());
    }
    DB_NOTICE("sync time:%ld", cost.get_time());
    return true;
}
}
