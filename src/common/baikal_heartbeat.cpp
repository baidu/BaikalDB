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

#include <boost/filesystem.hpp>
#include "baikal_heartbeat.h"
#include "proto_process.hpp"
#include "schema_factory.h"
#include "task_fetcher.h"
namespace baikaldb {

DEFINE_bool(enable_dblink, false, "enable dblink");
DEFINE_bool(can_do_ddlwork, true, "can_do_ddlwork");
DECLARE_int32(baikal_heartbeat_interval_us);
DEFINE_string(baikal_resource_tag, "", "resource tag");
DECLARE_int32(baikal_port);
DEFINE_int32(baikal_port, 28282, "Server port");

void BaikalHeartBeat::construct_heart_beat_request(pb::BaikalHeartBeatRequest& request, bool is_backup, bool is_binlog) {
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
            const int64_t meta_id = ::baikaldb::get_meta_id(info_pair.second->id);
            if (meta_id != 0) {
                // 跳过非主Meta数据
                continue;
            }
            if (info_pair.second->engine == pb::DBLINK) {
                // DBLINK表没有索引信息，单独处理
                auto req_info = request.add_schema_infos();
                req_info->set_table_id(info_pair.second->id);
                req_info->set_version(info_pair.second->version);
                continue;
            }
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
                const IndexInfo& index = *index_info_mapping.at(index_id);
                //主键索引
                if (index.type != pb::I_PRIMARY && !index.is_global) {
                    continue;
                }
                auto* req_info = request.add_schema_infos();
                req_info->set_table_id(index_id);
                req_info->set_version(1);
                if (index_id == info_pair.second->id) {
                    req_info->set_version(info_pair.second->version);
                }
            }
        }
    };
    factory->schema_info_scope_read(schema_read_recallback);
    request.set_last_updated_index(factory->last_updated_index());
    TaskFactory<pb::RegionDdlWork>::get_instance()->construct_heartbeat(request, 
        &pb::BaikalHeartBeatRequest::add_region_ddl_works);
    TaskFactory<pb::DdlWorkInfo>::get_instance()->construct_heartbeat(request, 
        &pb::BaikalHeartBeatRequest::add_ddl_works);
    request.set_can_do_ddlwork(is_backup ? false : FLAGS_can_do_ddlwork);
    //定时将request当中消息体更新 等待process_baikal_heartbeat处理更新至TableManager中的virtual_Index_sql_map
    auto sample = factory->get_virtual_index_info();
    auto index_id_name_map = sample.index_id_name_map;
    auto index_id_sample_sqls_map = sample.index_id_sample_sqls_map;
    //遍历map 更新至request 
    for (auto& it1 : index_id_name_map) {
        const int64_t meta_id = ::baikaldb::get_meta_id(it1.first);
        if (meta_id != 0) {
            // 跳过非主Meta数据
            continue;
        }
        for (auto& it2 : index_id_sample_sqls_map[it1.first]) {
            pb::VirtualIndexInfluence virtual_index_influence;
            virtual_index_influence.set_virtual_index_id(it1.first);
            virtual_index_influence.set_virtual_index_name(it1.second);
            virtual_index_influence.set_influenced_sql(it2);
            pb::VirtualIndexInfluence* info_affect = request.add_info_affect();
            *info_affect = virtual_index_influence;
        }
    }
    // 添加baikal相关信息
    // binlog不携带此部分
    if (!is_backup && !is_binlog) {
        pb::BaikalStatus* status = request.mutable_baikal_status();
        std::string address = SchemaFactory::get_instance()->get_address();
        status->set_address(address);
        status->set_resource_tag(FLAGS_baikal_resource_tag);
        // 检查是否迁移
        bool is_stoped = boost::filesystem::exists("_stop.check");
        status->set_status(!is_stoped ? pb::Status::NORMAL : pb::Status::FAULTY);
    }
}

// 处理非主Meta的心跳
void BaikalHeartBeat::construct_heart_beat_request(
        std::unordered_map<int64_t, pb::BaikalHeartBeatRequest>& meta_request_map) {
    if (!FLAGS_enable_dblink) {
        return;
    }
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (factory == nullptr) {
        DB_WARNING("factory is nullptr");
        return;
    }
    // 用于存储所有DBLINK表对应的外部表
    std::unordered_set<int64_t> exist_external_table_ids;
    // 用于存储没有DBLINK表对应的外部表
    std::unordered_set<int64_t> non_exist_external_table_ids;

    // 获取所有的DBLINK表信息
    std::vector<pb::DBLinkInfo> dblink_infos;
    factory->get_all_dblink_infos(dblink_infos);
    // 获取所有外部映射表信息
    for (auto& dblink_info : dblink_infos) {
        const int64_t meta_id = dblink_info.meta_id();
        if (meta_id == 0) {
            // 外部表为主Meta表不需要重新同步，跳过
            continue;
        }
        pb::BaikalHeartBeatRequest& request = meta_request_map[meta_id];
        pb::BaikalHeartBeatTable* heartbeat_table = request.add_heartbeat_tables();
        heartbeat_table->set_namespace_name(dblink_info.namespace_name());
        heartbeat_table->set_database(dblink_info.database_name());
        heartbeat_table->set_table_name(dblink_info.table_name());
        std::string external_table_name = 
            dblink_info.namespace_name() + "." + dblink_info.database_name() + "." + dblink_info.table_name();
        external_table_name = ::baikaldb::get_add_meta_name(meta_id, external_table_name);
        int64_t external_table_id = -1;
        if (factory->get_table_id(external_table_name, external_table_id) == 0) {
            // 填充db侧的外部表id，用于表删除或表重命名场景的旧表信息同步
            exist_external_table_ids.emplace(external_table_id);
            heartbeat_table->set_table_id(external_table_id);
        } else {
            // 新增外部映射表，需要获取心跳基准数据
            heartbeat_table->set_is_new(true);
        }
    }
    auto schema_read_recallback = [&meta_request_map, &exist_external_table_ids, 
                                   &non_exist_external_table_ids, factory] (const SchemaMapping& schema) {
        for (auto& info_pair : schema.table_info_mapping) {
            const int64_t meta_id = ::baikaldb::get_meta_id(info_pair.second->id);
            if (meta_id == 0) {
                // 跳过主Meta数据
                continue;
            }
            if (exist_external_table_ids.find(info_pair.second->id) == exist_external_table_ids.end()) {
                // 获取没有DBLINK表对应的外部表
                non_exist_external_table_ids.insert(info_pair.second->id);
                continue;
            }
            // 非主Meta不处理DBLINK表
            if (info_pair.second->engine != pb::ROCKSDB &&
                    info_pair.second->engine != pb::ROCKSDB_CSTORE && 
                    info_pair.second->engine != pb::BINLOG) {
                continue;
            }
            pb::BaikalHeartBeatRequest& request = meta_request_map[meta_id];
            // 主键索引和全局二级索引都需要传递region信息
            for (auto& index_id : info_pair.second->indices) {
                auto& index_info_mapping = schema.index_info_mapping;
                if (index_info_mapping.count(index_id) == 0) {
                    continue;
                }
                const IndexInfo& index = *index_info_mapping.at(index_id);
                if (index.type != pb::I_PRIMARY && !index.is_global) {
                    continue;
                }
                auto* req_info = request.add_schema_infos();
                req_info->set_table_id(index_id);
                req_info->set_version(1);
                if (index_id == info_pair.second->id) {
                    req_info->set_version(info_pair.second->version);
                }
            }
        }
    };
    factory->schema_info_scope_read(schema_read_recallback);

    // 删除没有DBLINK表对应的外部表
    for (const auto& non_exist_table_id : non_exist_external_table_ids) {
        factory->delete_table(non_exist_table_id);
    }

    auto sample = factory->get_virtual_index_info();
    auto index_id_name_map = sample.index_id_name_map;
    auto index_id_sample_sqls_map = sample.index_id_sample_sqls_map;
    for (auto& kv : meta_request_map) {
        const int64_t meta_id = kv.first;
        pb::BaikalHeartBeatRequest& request = kv.second;
        // 非主Meta不处理Online DDL任务
        request.set_can_do_ddlwork(false);
        // 非主Meta上次更新的index
        request.set_last_updated_index(factory->last_updated_index(meta_id));
        // 非主Meta虚拟索引信息
        for (auto& it1 : index_id_name_map) {
            const int64_t meta_id_tmp = ::baikaldb::get_meta_id(it1.first);
            if (meta_id != meta_id_tmp) {
                // 处理对应Meta数据
                continue;
            }
            for (auto& it2 : index_id_sample_sqls_map[it1.first]) {
                pb::VirtualIndexInfluence virtual_index_influence;
                virtual_index_influence.set_virtual_index_id(it1.first);
                virtual_index_influence.set_virtual_index_name(it1.second);
                virtual_index_influence.set_influenced_sql(it2);
                pb::VirtualIndexInfluence* info_affect = request.add_info_affect();
                *info_affect = virtual_index_influence;
            }
        }
        // 非主Meta按需返回，获取主表和全局索引信息（目前未获取Binlog表信息）
        request.set_need_heartbeat_table(true);
        request.set_need_global_index_heartbeat(true);
    }
}

void BaikalHeartBeat::process_heart_beat_response(
        const pb::BaikalHeartBeatResponse& response, bool is_backup, const int64_t meta_id) {
    SchemaFactory* factory = nullptr;
    if (is_backup) {
        factory = SchemaFactory::get_backup_instance();
    } else {
        factory = SchemaFactory::get_instance();
    }
    if (!factory->is_inited()) {
        return;
    }
    if (response.has_last_updated_index() && 
            response.last_updated_index() > factory->last_updated_index(meta_id)) {
        factory->set_last_updated_index(response.last_updated_index(), meta_id);
    }
    for (auto& info : response.schema_change_info()) {
        factory->update_table(info);
    }
    factory->update_regions(response.region_change_info());
    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info(), meta_id);
    }

    if (meta_id == 0) {
        factory->update_show_db(response.db_info());
        for (auto& info : response.privilege_change_info()) {
            factory->update_user(info);
        }
        if (response.statistics().size() > 0) {
            factory->update_statistics(response.statistics());
        }
        TaskFactory<pb::RegionDdlWork>::get_instance()->process_heartbeat(response, 
            static_cast<
                const google::protobuf::RepeatedPtrField<pb::RegionDdlWork>& (pb::BaikalHeartBeatResponse::*)() const
            >(&pb::BaikalHeartBeatResponse::region_ddl_works));
        TaskFactory<pb::DdlWorkInfo>::get_instance()->process_heartbeat(response, 
            static_cast<
                const google::protobuf::RepeatedPtrField<pb::DdlWorkInfo>& (pb::BaikalHeartBeatResponse::*)() const
            >(&pb::BaikalHeartBeatResponse::ddl_works));

        if (response.baikal_status_size() != 0) {
            std::vector<pb::BaikalStatus> address_status_vec;
            for (int i = 0 ; i < response.baikal_status_size(); ++i) {
                pb::BaikalStatus db_status = response.baikal_status(i);
                address_status_vec.emplace_back(db_status);
            }
            factory->update_valiable_addresses(address_status_vec);
        }
    }

    if (meta_id != 0) {
        DB_NOTICE("for dblink, region_change_info size: %d", response.region_change_info().size());
    }
}

void BaikalHeartBeat::process_heart_beat_response_sync(
        const pb::BaikalHeartBeatResponse& response, const int64_t meta_id) {
    TimeCost cost;
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index(meta_id)) {
        factory->set_last_updated_index(response.last_updated_index(), meta_id);
    }
    factory->update_tables_double_buffer_sync(response.schema_change_info());
    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info(), meta_id);
    }
    if (meta_id == 0) {
        factory->update_show_db(response.db_info());
        for (auto& info : response.privilege_change_info()) {
            factory->update_user(info);
        }
        if (response.statistics().size() > 0) {
            factory->update_statistics(response.statistics());
        }
    }
    factory->update_regions_double_buffer_sync(response.region_change_info());
    if (response.baikal_status_size() != 0) {
        std::vector<pb::BaikalStatus> address_status_vec;
        for (int i = 0 ; i < response.baikal_status_size(); ++i) {
            pb::BaikalStatus db_status = response.baikal_status(i);
            address_status_vec.emplace_back(db_status);
        }
        factory->update_valiable_addresses(address_status_vec);
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
    BaikalHeartBeat::construct_heart_beat_request(request, false, true);
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
        BaikalHeartBeat::construct_heart_beat_request(request, false, true);
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
    factory->update_show_db(response.db_info());
    for (auto& info : response.privilege_change_info()) {
        factory->update_user(info);
    }
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }
    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index()) {
        factory->set_last_updated_index(response.last_updated_index());
    }
    if (response.baikal_status_size() != 0) {
        std::vector<pb::BaikalStatus> address_status_vec;
        for (int i = 0 ; i < response.baikal_status_size(); ++i) {
            pb::BaikalStatus db_status = response.baikal_status(i);
            address_status_vec.emplace_back(db_status);
        }
        factory->update_valiable_addresses(address_status_vec);
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
    std::ostringstream oss;
    for (auto id : all_binlog_ids) {
        bool find = true;
        for (auto& table_ptr : table_ptrs) {
            if(table_ptr != nullptr && table_ptr->binlog_ids.count(id) == 0) {
                oss << "table[" << table_ptr->name << "] binlog id not found: " << id << ", ";
                find = false;
                break;
            }
        }
        if (find) {
            common_binlog_ids.emplace(id);
        }
    }
    if (common_binlog_ids.size() == 0) {
        DB_FATAL("get binlog id error. %s", oss.str().c_str());
        return -1;
    } else if (common_binlog_ids.size() == 1) {
        _binlog_id = *common_binlog_ids.begin();
    } else if (common_binlog_ids.size() > 1) {
        int64_t min_partition_num = INT64_MAX;
        for (int64_t id : common_binlog_ids) {
            auto binlog_info = factory->get_table_info_ptr(id);
            if (binlog_info != nullptr) {
                if (binlog_info->partition_num < min_partition_num) {
                    min_partition_num = binlog_info->partition_num;
                    _binlog_id = id;
                }
            }
        }
        DB_WARNING("selected multi binlog_ids: %ld maybe config error. binlog_id: %ld, partition_num: %ld", 
                common_binlog_ids.size(), _binlog_id, min_partition_num);
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
    factory->update_show_db(response.db_info());
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
