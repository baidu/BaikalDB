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

#include <unordered_set>
#include "meta_server_interact.hpp"
#include "store_interact.hpp"
#include "namespace_manager.h"
#include "database_manager.h"
#include "region_manager.h"
#include "table_manager.h"
#include "cluster_manager.h"
#include "meta_util.h"
#include "meta_rocksdb.h"
#include "ddl_manager.h"
#include "mut_table_key.h"

namespace baikaldb {
DECLARE_int32(concurrency_num);
DEFINE_int32(region_replica_num, 3, "region replica num, default:3"); 
DEFINE_int32(learner_region_replica_num, 1, "learner region replica num, default:1"); 
DEFINE_int32(region_region_size, 100 * 1024 * 1024, "region size, default:100M");
DEFINE_int64(table_tombstone_gc_time_s, 3600 * 24 * 5, "time interval to clear table_tombstone. default(5d)");
DEFINE_uint64(statistics_heart_beat_bytesize, 256 * 1024 * 1024, "default(256M)");
DEFINE_int32(pre_split_threashold, 300, "pre_split_threashold for sync create table");
DEFINE_int32(remove_dropped_partition_region_s, 60 * 1000 * 1000LL, "remove dropped partition region delay seconds, default: 60s");
DEFINE_bool(use_partition_split_key, false, "add dynamic partition use get_partition_split_key");

void TableTimer::run() {
    DB_NOTICE("Table Timer run.");
    std::vector<pb::SchemaInfo> delete_schemas;
    delete_schemas.reserve(10);
    std::vector<pb::SchemaInfo> clear_schemas;
    clear_schemas.reserve(10);
    TableManager::get_instance()->get_delay_delete_index(delete_schemas, clear_schemas);
    for (auto& schema : delete_schemas) {
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_DROP_INDEX);
        request.mutable_table_info()->CopyFrom(schema);
        DB_NOTICE("DDL_LOG drop_index_request req[%s]", request.ShortDebugString().c_str());
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL); 
    }
    for (auto& schema : clear_schemas) {
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_UPDATE_INDEX_STATUS);
        auto& index_info = schema.indexs(0);
        pb::DdlWorkInfo  ddl_work;
        ddl_work.set_deleted(true);
        ddl_work.set_errcode(pb::SUCCESS);
        ddl_work.set_job_state(pb::IS_NONE);
        ddl_work.set_index_id(index_info.index_id());
        ddl_work.set_table_id(schema.table_id());
        ddl_work.set_op_type(pb::OP_DROP_INDEX);
        ddl_work.set_global(index_info.is_global());
        request.mutable_ddlwork_info()->CopyFrom(ddl_work);
        request.mutable_table_info()->CopyFrom(schema);
        DB_NOTICE("DDL_LOG clear local_index_request req[%s]", request.ShortDebugString().c_str());
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL); 
    }

    // Dynamic Partition
    std::vector<pb::SchemaInfo> add_partition_schemas;
    add_partition_schemas.reserve(10);
    std::vector<pb::SchemaInfo> del_partition_schemas;
    del_partition_schemas.reserve(10);
    std::vector<pb::SchemaInfo> cold_partition_schemas;
    cold_partition_schemas.reserve(10);
    TableManager::get_instance()->get_change_partition_schemas(
            add_partition_schemas, del_partition_schemas, cold_partition_schemas);
    for (auto& schema : add_partition_schemas) {
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_ADD_PARTITION);
        request.set_is_dynamic_change(true);
        request.mutable_table_info()->Swap(&schema);
        DB_NOTICE("DDL_LOG add dynamic partition req[%s]", request.ShortDebugString().c_str());
        // brpc::DoNothing()作为done，用于标识raft的leader
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, brpc::DoNothing());
    }
    for (auto& schema : del_partition_schemas) {
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_DROP_PARTITION);
        request.set_is_dynamic_change(true);
        request.mutable_table_info()->Swap(&schema);
        DB_NOTICE("DDL_LOG drop dynamic partition req[%s]", request.ShortDebugString().c_str());
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, brpc::DoNothing());
    }
    for (auto& schema : cold_partition_schemas) {
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_MODIFY_PARTITION);
        request.set_is_dynamic_change(true);
        request.mutable_table_info()->Swap(&schema);
        DB_NOTICE("DDL_LOG modify dynamic partition req[%s]", request.ShortDebugString().c_str());
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, brpc::DoNothing());
    }

    // gc删除很久的partition的drop_ts
    pb::MetaManagerRequest drop_partition_ts_request;
    drop_partition_ts_request.set_op_type(pb::OP_DROP_PARTITION_TS);
    SchemaManager::get_instance()->process_schema_info(NULL, &drop_partition_ts_request, NULL, NULL);
}

void TableManager::update_index_status(const pb::DdlWorkInfo& ddl_work) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    auto table_id = ddl_work.table_id();
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_FATAL("update index table_id [%ld] table_info not exist.", table_id);
        return;
    }

    DB_DEBUG("DDL_LOG update_index_status req[%s]", ddl_work.ShortDebugString().c_str());
    pb::MetaManagerRequest request;
    request.set_op_type(pb::OP_UPDATE_INDEX_STATUS);
    request.mutable_ddlwork_info()->CopyFrom(ddl_work);
    request.mutable_table_info()->CopyFrom(_table_info_map[table_id].schema_pb);
    SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
}

void TableManager::drop_index_request(const pb::DdlWorkInfo& ddl_work) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    auto table_id = ddl_work.table_id();
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_FATAL("update index table_id [%ld] table_info not exist.", table_id);
        return;
    }
    std::string index_name;
    for (const auto& index_info : _table_info_map[table_id].schema_pb.indexs()) {
        if (index_info.index_id() == ddl_work.index_id()) {
            index_name = index_info.index_name();
        }
    }
    if (index_name.empty()) {
        return;
    }
    pb::MetaManagerRequest request;
    request.set_op_type(pb::OP_DROP_INDEX);
    request.mutable_table_info()->CopyFrom(_table_info_map[table_id].schema_pb);
    request.mutable_table_info()->clear_indexs();
    auto index_to_drop_iter = request.mutable_table_info()->add_indexs();
    index_to_drop_iter->set_index_name(index_name);
    DB_DEBUG("DDL_LOG drop_index_request req[%s]", request.ShortDebugString().c_str());
    SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
}

int64_t TableManager::get_row_count(int64_t table_id) {
    std::vector<int64_t> region_ids;
    int64_t byte_size_per_record = 0;
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            return 0;
        }
        byte_size_per_record = _table_info_map[table_id].schema_pb.byte_size_per_record();
        for (auto& partition_regions : _table_info_map[table_id].partition_regions) {
            for (auto& region_id :  partition_regions.second) {
                region_ids.push_back(region_id);    
            }
        }
    }
    if (byte_size_per_record == 0) {
        byte_size_per_record = 1;
    }
    std::vector<SmartRegionInfo> region_infos;
    RegionManager::get_instance()->get_region_info(region_ids, region_infos);
    int64_t total_byte_size = 0;
    for (auto& region : region_infos) {
        total_byte_size += region->used_size();
    }
    int64_t total_row_count = 0;
    for (auto& region : region_infos) {
        total_row_count += region->num_table_lines();
    }
    if (total_row_count == 0) {
        total_row_count = total_byte_size / byte_size_per_record;
    }
    return total_row_count;
}

void TableManager::update_table_internal(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done,
        std::function<void(const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done)> update_callback) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    pb::SchemaInfo mem_schema_pb = _table_info_map[table_id].schema_pb;

    update_callback(request, mem_schema_pb, done);
    if (done != nullptr && ((MetaServerClosure*)done)->response
        && ((MetaServerClosure*)done)->response->errcode() == pb::INPUT_PARAM_ERROR) {
        return;
    }
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);   
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("update table internal success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::create_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    auto& table_info = const_cast<pb::SchemaInfo&>(request.table_info());
    table_info.set_timestamp(time(NULL));
    table_info.set_version(1);
    
    std::string namespace_name = table_info.namespace_name();
    std::string database_name = namespace_name + "\001" + table_info.database();
    std::string table_name = database_name + "\001" + table_info.table_name();
   
    TableMem table_mem;
    table_mem.whether_level_table = false;
    std::string upper_table_name;
    if (table_info.has_upper_table_name()) {
        table_mem.whether_level_table = true;
        upper_table_name = database_name + "\001" + table_info.upper_table_name();
    }
    //校验合法性, 准备数据
    int64_t namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    if (namespace_id == 0) {
        DB_WARNING("request namespace:%s not exist", namespace_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "namespace not exist");
        return;
    }
    table_info.set_namespace_id(namespace_id);

    int64_t database_id = DatabaseManager::get_instance()->get_database_id(database_name);
    if (database_id == 0) {
        DB_WARNING("request database:%s not exist", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database not exist");
        return;
    }
    table_info.set_database_id(database_id);

    if (_table_id_map.find(table_name) != _table_id_map.end()) {
        DB_WARNING("request table_name:%s already exist", table_name.c_str());
        if (table_info.if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table already exist");
        } else {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        }
        return;
    }

    //分配table_id
    int64_t max_table_id_tmp = _max_table_id;
    table_info.set_table_id(++max_table_id_tmp);
    table_mem.main_table_id = max_table_id_tmp;
    table_mem.global_index_id = max_table_id_tmp;
    if (table_mem.whether_level_table) {
        if (_table_id_map.find(upper_table_name) == _table_id_map.end()) {
            DB_WARNING("request upper_table_name:%s not exist", upper_table_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "upper table not exist");
            return;
        }
        int64_t upper_table_id = _table_id_map[upper_table_name];
        table_info.set_upper_table_id(upper_table_id);
        if (table_info.has_partition_num()) {
            DB_WARNING("table: %s is leve, partition num should be equal to upper table",
                        table_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table already exist");
            return;
        }
        table_info.set_partition_num(1);
        //继承上次表的信息
        table_info.set_top_table_id(_table_info_map[upper_table_id].schema_pb.top_table_id());
        table_info.set_region_size(_table_info_map[upper_table_id].schema_pb.region_size());
        table_info.set_replica_num(_table_info_map[upper_table_id].schema_pb.replica_num());
    } else {
        if (!table_info.has_partition_num()) {
            table_info.set_partition_num(1);
        }
        //非层次表的顶层表填自己
        table_info.set_top_table_id(table_info.table_id());
        if (!table_info.has_region_size()) {
           table_info.set_region_size(FLAGS_region_region_size);
        }
        if (!table_info.has_replica_num()) {
           table_info.set_replica_num(FLAGS_region_replica_num);
        }
    }
    //分配field_id
    bool has_auto_increment = false;
    auto ret = alloc_field_id(table_info, has_auto_increment, table_mem);
    if (ret < 0) {
        DB_WARNING("table:%s 's field info not illegal", table_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field not illegal");
        return;
    }
    ret = alloc_index_id(table_info, table_mem, max_table_id_tmp);
    if (ret < 0) {
        DB_WARNING("table:%s 's index info not illegal", table_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "index not illegal");
        return;
    }
    if (table_info.engine() == pb::BINLOG) {
        table_mem.is_binlog = true;
    }
    // partition分区表，设置分区field信息。
    if (table_info.has_partition_info()) {
        table_mem.is_partition = true;
        std::map<std::string, pb::FieldInfo> field_map;
        for (const auto& field_info : table_info.fields()) {
            field_map[field_info.field_name()] = field_info;
        }
        std::string field_name;
        if (table_info.partition_info().has_field_info()) {
            field_name = table_info.partition_info().field_info().field_name();
            auto iter = field_map.find(field_name);
            if (iter != field_map.end()) {
                table_info.mutable_partition_info()->mutable_field_info()->CopyFrom(iter->second);
                table_info.mutable_partition_info()->set_partition_field(iter->second.field_id());
            } else {
                DB_WARNING("paritition field_name:%s not found.", field_name.c_str());
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "partition field not found.");
                return;
            }
        }
        std::set<int32_t> expr_field_ids;
        auto set_expr_type_func = [&field_map, &expr_field_ids, &field_name](pb::Expr& expr) -> int {
            for (size_t i = 0; i < expr.nodes_size(); i++) {
                auto node = expr.mutable_nodes(i);
                if (node->has_derive_node() && node->derive_node().has_field_name()) {
                    field_name = node->derive_node().field_name();
                    auto iter = field_map.find(field_name);
                    if (iter != field_map.end()) {
                        auto& field_info = iter->second;
                        node->set_col_type(field_info.mysql_type());
                        node->mutable_derive_node()->set_field_id(field_info.field_id());
                        node->set_col_flag(field_info.flag());
                        expr_field_ids.insert(field_info.field_id());
                    } else {
                        return -1;
                    }
                }
            }
            return 0;
        };
        if (table_info.partition_info().type() == pb::PT_RANGE) {
            if (table_mem.is_binlog) {
                DB_WARNING("binlog table not support range partition, request:%s", request.DebugString().c_str());
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "binlog table not support range partition");
                return;
            }
            if (table_info.partition_info().has_range_partition_field()) {
                pb::Expr tmp_expr = table_info.partition_info().range_partition_field();
                if (0 != set_expr_type_func(tmp_expr)) {
                    DB_WARNING("paritition init range expr failed.");
                    IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "partition init range expr failed.");
                    return;
                }
                auto partition_info_ptr = table_info.mutable_partition_info();
                partition_info_ptr->clear_range_partition_field();
                partition_info_ptr->mutable_range_partition_field()->CopyFrom(tmp_expr);
            }
        } else if (table_info.partition_info().type() == pb::PT_HASH) {
            if (table_info.partition_info().has_hash_expr_value()) {
                pb::Expr tmp_expr = table_info.partition_info().hash_expr_value();
                if (0 != set_expr_type_func(tmp_expr)) {
                    DB_WARNING("paritition init hash expr failed.");
                    IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "partition init hash expr failed.");
                    return;
                }
                table_info.mutable_partition_info()->clear_hash_expr_value();
                table_info.mutable_partition_info()->mutable_hash_expr_value()->CopyFrom(tmp_expr);
            }
        }
        if (expr_field_ids.size() > 1) {
            DB_WARNING("paritition multiple fields not support.");
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "partition multiple fields not support.");
            return;
        }
    }
    DB_WARNING("paritition %s.", table_info.partition_info().ShortDebugString().c_str());
    for (auto& learner_resource : *table_info.mutable_learner_resource_tags()) {
        table_mem.learner_resource_tag.emplace_back(learner_resource);
    }
    table_mem.schema_pb = table_info;
    //发起交互， 层次表与非层次表区分对待，非层次表需要与store交互，创建第一个region
    //层级表直接继承后父层次的相关信息即可
    if (table_mem.whether_level_table) {
        ret = write_schema_for_level(table_mem, apply_index, done, max_table_id_tmp, has_auto_increment);
    } else {
        ret = write_schema_for_not_level(table_mem, done, max_table_id_tmp, has_auto_increment); 
    }
    if (ret != 0) { 
        DB_WARNING("write rocksdb fail when create table, table:%s", table_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }

    set_max_table_id(max_table_id_tmp);
    table_mem.schema_pb.clear_init_store();
    table_mem.schema_pb.clear_split_keys();
    set_table_info(table_mem);
    std::vector<pb::SchemaInfo> schema_infos{table_info};
    put_incremental_schemainfo(apply_index, schema_infos);
    DatabaseManager::get_instance()->add_table_id(database_id, table_info.table_id());
    table_mem.print();
    if (table_mem.whether_level_table) {
        DB_NOTICE("create table completely, _max_table_id:%ld, table_name:%s", _max_table_id, table_name.c_str());
    }
    if (table_mem.is_binlog) {
        auto call_func = [](TableSchedulingInfo& infos, int64_t table_id) -> int {
            infos.binlog_table_ids.insert(table_id);
            return 1;
        };
        _table_scheduling_infos.Modify(call_func, table_mem.main_table_id);
    }
    if (done) {
        ((MetaServerClosure*)done)->whether_level_table = table_mem.whether_level_table;
        ((MetaServerClosure*)done)->create_table_ret = ret;
    }
}

int TableManager::do_create_table_sync_req(pb::SchemaInfo& schema_pb, 
                            std::shared_ptr<std::vector<pb::InitRegion>> init_regions,
                            bool has_auto_increment,
                            int64_t start_region_id,
                            pb::MetaManagerResponse* response) {
    if (schema_pb.engine() == pb::ROCKSDB
        || schema_pb.engine() == pb::ROCKSDB_CSTORE 
        || schema_pb.engine() == pb::BINLOG) {
        std::string namespace_name = schema_pb.namespace_name();
        std::string database = schema_pb.database();
        std::string table_name = schema_pb.table_name();
        int64_t table_id = schema_pb.table_id();
        uint64_t init_value = 1;
        if (schema_pb.has_auto_increment_increment()) {
            init_value = schema_pb.auto_increment_increment();
        }

        int ret = 0;
        if (has_auto_increment) {
            pb::MetaManagerRequest request;
            request.set_op_type(pb::OP_ADD_ID_FOR_AUTO_INCREMENT);
            pb::AutoIncrementRequest* auto_incr = request.mutable_auto_increment();
            auto_incr->set_table_id(table_id);
            auto_incr->set_start_id(init_value);
            ret = send_auto_increment_request(request);
        }
        if (ret == 0) {
            if (send_create_table_request(namespace_name, database, table_name, init_regions) != 0) {
                send_drop_table_request(namespace_name, database, table_name);
                DB_FATAL("send create_table request fail, table_name: %s", table_name.c_str());
                SET_RESPONSE(response, pb::INTERNAL_ERROR, "create table fail");
                return -1;
            }

            pb::CreateTableResponse create_table_response_tmp;
            auto* schema_info = create_table_response_tmp.mutable_schema_info();
            schema_info->Swap(&schema_pb);
            for (auto& init_region: *init_regions) {
                auto* region_info = create_table_response_tmp.add_region_infos();
                region_info->Swap(init_region.mutable_region_info());
            }
            auto* create_table_response = response->mutable_create_table_response();
            create_table_response->Swap(&create_table_response_tmp);
        } else {
            send_drop_table_request(namespace_name, database, table_name);
            DB_FATAL("send add auto incrment request fail, table_name: %s", table_name.c_str());
            SET_RESPONSE(response, pb::INTERNAL_ERROR, "create table fail");
            return -1;
        }
    }
    SET_RESPONSE(response, pb::SUCCESS, "success");
    DB_WARNING("create table, table_id: %ld, table_name:%s, alloc start_region_id: %ld, end_region_id: %ld", 
                schema_pb.table_id(),
                schema_pb.table_name().c_str(),
                start_region_id, 
                RegionManager::get_instance()->get_max_region_id());
    return 0;
}

void TableManager::drop_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    int64_t namespace_id = 0;
    int64_t database_id = 0;
    int64_t drop_table_id = 0;
    auto ret = check_table_exist(request.table_info(), namespace_id, database_id, drop_table_id);
    if (ret < 0) {
        if (request.table_info().if_exist()) {
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "table not exist");
            return;
        }
        DB_WARNING("input table not exit, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(drop_table_id)) {
        DB_WARNING("table is doing ddl , request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    if (check_table_is_linked(drop_table_id)) {
        DB_WARNING("table is linked, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is linked binlog table");
        return;
    }
    std::vector<std::string> delete_rocksdb_keys;
    std::vector<std::string> write_rocksdb_keys;
    std::vector<std::string> write_rocksdb_values;
    pb::SchemaInfo schema_info = _table_info_map[drop_table_id].schema_pb;
    schema_info.set_deleted(true);
    schema_info.set_timestamp(time(NULL));
    std::string drop_table_value;
    if (!schema_info.SerializeToString(&drop_table_value)) {
        DB_WARNING("request serializeToArray fail, request:%s", 
                request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return ;
    }
    //delete_rocksdb_keys.push_back(construct_table_key(drop_table_id));
    // 删表后保留一个墓碑，帮助region上报时的gc工作
    // TODO 如果后续墓碑残留太多，应该有相应的清理线程
    write_rocksdb_keys.push_back(construct_table_key(drop_table_id));
    write_rocksdb_values.push_back(drop_table_value);
   
    std::vector<int64_t> drop_index_ids;
    drop_index_ids.push_back(drop_table_id);
    for (auto& index_info : _table_info_map[drop_table_id].schema_pb.indexs()) {
        if (!is_global_index(index_info)) {
            continue;
        }
        drop_index_ids.push_back(index_info.index_id());
    } 
    //drop_region_ids用来保存该表的所有region，用来给store发送remove_region
    std::vector<std::int64_t> drop_region_ids;
    //如果table下有region， 直接删除region信息
    for (auto& drop_index_id : drop_index_ids) {
        for (auto& partition_region: _table_info_map[drop_index_id].partition_regions) {
            for (auto& drop_region_id : partition_region.second) {
                std::string drop_region_key = RegionManager::get_instance()->construct_region_key(drop_region_id);
                delete_rocksdb_keys.push_back(drop_region_key);
                drop_region_ids.push_back(drop_region_id);
            }
        }
    }
    //如果是层次表，需要修改顶层表的low_tables信息
    pb::SchemaInfo top_schema_pb;
    int64_t top_table_id = _table_info_map[drop_table_id].schema_pb.top_table_id();
    if (_table_info_map[drop_table_id].schema_pb.has_upper_table_name()
        && _table_info_map.find(top_table_id) != _table_info_map.end()) {
        top_schema_pb = _table_info_map[top_table_id].schema_pb;
        top_schema_pb.clear_lower_table_ids();
        for (auto low_table_id : _table_info_map[top_table_id].schema_pb.lower_table_ids()) {
            if (low_table_id != drop_table_id) {
                top_schema_pb.add_lower_table_ids(low_table_id);
            }
        }
        top_schema_pb.set_version(top_schema_pb.version() + 1);
        std::string top_table_value;
        if (!top_schema_pb.SerializeToString(&top_table_value)) {
            DB_WARNING("request serializeToArray fail when update upper table, request:%s", 
                        top_schema_pb.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return ;
        }
        write_rocksdb_keys.push_back(construct_table_key(top_table_id));
        write_rocksdb_values.push_back(top_table_value);
    }
    ret = MetaRocksdb::get_instance()->write_meta_info(write_rocksdb_keys, 
                                                                    write_rocksdb_values, 
                                                                    delete_rocksdb_keys);
    if (ret < 0) {
        DB_WARNING("drop table fail, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //删除内存中的值
    std::vector<pb::SchemaInfo> schema_infos;
    if (_table_info_map[drop_table_id].schema_pb.has_upper_table_name()
        && _table_info_map.find(top_table_id) != _table_info_map.end()) {
        set_table_pb(top_schema_pb);
        schema_infos.push_back(top_schema_pb);
    }
    erase_table_info(drop_table_id);
    schema_infos.push_back(schema_info);
    put_incremental_schemainfo(apply_index, schema_infos);
    DatabaseManager::get_instance()->delete_table_id(database_id, drop_table_id);
    if (is_table_in_fast_importer(drop_table_id)) {
        // 正在快速导入，删表把快速导入标志清掉，报警，可能需要人工恢复store rocksdb标志
        DB_FATAL("drop table: %ld, drop table is in_fast_import", drop_table_id);
        cancel_in_fast_importer(drop_table_id);
    }
    if (schema_info.is_binlog()) {
        auto call_func = [](TableSchedulingInfo& infos, int64_t table_id) -> int {
            infos.binlog_table_ids.erase(table_id);
            return 1;
        };
        _table_scheduling_infos.Modify(call_func, drop_table_id);
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop table success, request:%s", request.ShortDebugString().c_str());
    if (done) {
        Bthread bth_remove_region(&BTHREAD_ATTR_SMALL);
        std::function<void()> remove_function = [drop_region_ids]() {
                RegionManager::get_instance()->send_remove_region_request(drop_region_ids);
            };
        bth_remove_region.run(remove_function);
    }
}

void TableManager::drop_table_tombstone(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    std::vector<std::string> delete_rocksdb_keys;
    int64_t table_id = request.table_info().table_id();
    // 删除rocksdb
    delete_rocksdb_keys.emplace_back(construct_table_key(table_id));
    delete_rocksdb_keys.emplace_back(construct_statistics_key(table_id));
   
    int ret = MetaRocksdb::get_instance()->delete_meta_info(delete_rocksdb_keys);
    if (ret < 0) {
        DB_WARNING("drop table tombstone fail, request：%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    erase_table_tombstone(table_id);
    std::string database_name = request.table_info().namespace_name() + "\001" + request.table_info().database();
    int64_t database_id = DatabaseManager::get_instance()->get_database_id(database_name);
    if (database_id == 0) {
        DB_WARNING("request database:%s not exist", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database not exist");
        return;
    }
    DatabaseManager::get_instance()->delete_table_id(database_id, table_id);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop table tombstone success,table_id:%ld, request:%s", 
            table_id, request.ShortDebugString().c_str());
    if (done) {
        Bthread bth_drop_auto(&BTHREAD_ATTR_SMALL);
        auto drop_function = [this, table_id]() {
            pb::MetaManagerRequest request;
            request.set_op_type(pb::OP_DROP_ID_FOR_AUTO_INCREMENT);
            pb::AutoIncrementRequest* auto_incr = request.mutable_auto_increment();
            auto_incr->set_table_id(table_id);
            send_auto_increment_request(request);
        };
        bth_drop_auto.run(drop_function);
    }
}

void TableManager::drop_table_tombstone_gc_check() {
    time_t now = time(nullptr);
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& pair : _table_tombstone_map) {
        auto& schema_pb = pair.second.schema_pb;
        if (now - schema_pb.timestamp() > FLAGS_table_tombstone_gc_time_s) {
            Bthread bth;
            bth.run([schema_pb]() {
                pb::MetaManagerRequest request;
                request.set_op_type(pb::OP_DROP_TABLE_TOMBSTONE);
                pb::SchemaInfo *table = request.mutable_table_info();
                *table = schema_pb;
                pb::MetaManagerResponse response;
                MetaServerInteract::get_instance()->send_request("meta_manager", request, response);
                DB_WARNING("send table tombstone gc,table_id:%ld schema_pb:%s",
                    schema_pb.table_id(), schema_pb.ShortDebugString().c_str());
            });
            break;
        }
    }
}

void TableManager::restore_table(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    int64_t table_id = 0;
    if (check_table_exist(request.table_info(), table_id) == 0) {
        DB_WARNING("check table already exist, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table already exist");
        return;
    }
    TableMem table_mem;
    int ret = find_last_table_tombstone(request.table_info(), &table_mem);
    if (ret < 0) {
        DB_WARNING("input table not exit in tombstone, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist in tombstone");
        return;
    }
    std::vector<std::string> delete_rocksdb_keys;
    std::vector<std::string> write_rocksdb_keys;
    std::vector<std::string> write_rocksdb_values;
    pb::SchemaInfo& schema_info = table_mem.schema_pb;
    table_id = schema_info.table_id();
    schema_info.set_deleted(false);
    schema_info.set_timestamp(time(nullptr));
    schema_info.set_version(schema_info.version() + 1);
    std::string table_value;
    if (!schema_info.SerializeToString(&table_value)) {
        DB_WARNING("request serializeToArray fail, request:%s", 
                request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return ;
    }
    // 恢复rocksdb
    write_rocksdb_keys.push_back(construct_table_key(table_id));
    write_rocksdb_values.push_back(table_value);
   
    ret = MetaRocksdb::get_instance()->write_meta_info(write_rocksdb_keys, 
                                                       write_rocksdb_values, 
                                                       delete_rocksdb_keys);
    if (ret < 0) {
        DB_WARNING("restore table fail, request：%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    std::vector<pb::SchemaInfo> schema_infos;
    set_table_info(table_mem);
    schema_infos.push_back(schema_info);
    put_incremental_schemainfo(apply_index, schema_infos);
    std::string database_name = request.table_info().namespace_name() + "\001" + request.table_info().database();
    int64_t database_id = DatabaseManager::get_instance()->get_database_id(database_name);
    if (database_id == 0) {
        DB_WARNING("request database:%s not exist", database_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "database not exist");
        return;
    }
    DatabaseManager::get_instance()->add_table_id(database_id, table_id);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("restore table success, request:%s, info:%s", 
            request.ShortDebugString().c_str(), schema_info.ShortDebugString().c_str());
    
    if (table_mem.is_binlog) {
        auto call_func = [](TableSchedulingInfo& infos, int64_t table_id) -> int {
            infos.binlog_table_ids.insert(table_id);
            return 1;
        };
        _table_scheduling_infos.Modify(call_func, table_mem.main_table_id);
    }
    if (done) {
        std::set<std::string> resource_tag_set {schema_info.resource_tag()};
        for (const auto& dist : schema_info.dists()) {
            if (dist.has_resource_tag()) {
                resource_tag_set.insert(dist.resource_tag());
            }
        }
        std::function<void()> restore_function = [table_id, resource_tag_set]() {
            for (auto& resource_tag : resource_tag_set) {
                std::set<std::string> instances;
                ClusterManager::get_instance()->get_instances(resource_tag, instances);
                DB_WARNING("restore table, resource_tag:%s, instances.size:%lu", 
                        resource_tag.c_str(), instances.size());
                for (auto& instance : instances) {
                    pb::RegionIds request;
                    request.set_table_id(table_id);
                    pb::StoreRes response; 
                    StoreInteract store_interact(instance);
                    store_interact.send_request("restore_region", request, response);
                }
            }
            };
        Bthread bth_restore_region(&BTHREAD_ATTR_SMALL);
        bth_restore_region.run(restore_function);
    }
}

void TableManager::rename_table(const pb::MetaManagerRequest& request, 
                                const int64_t apply_index,
                                braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    
    if (check_table_has_ddlwork(table_id) || check_table_is_linked(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    std::string namespace_name = request.table_info().namespace_name();
    std::string database_name = namespace_name + "\001" + request.table_info().database();
    std::string old_table_name = database_name + "\001" + request.table_info().table_name();
    if (!request.table_info().has_new_table_name()) {
        DB_WARNING("request has no new table_name, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        return;
    }
    std::string new_table_name = database_name + "\001" + request.table_info().new_table_name();
    if (_table_id_map.count(new_table_name) != 0) {
        DB_WARNING("table is existed, table_name:%s", new_table_name.c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "new table name already exist");
        return;
    }
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    //更新数据
    mem_schema_pb.set_table_name(request.table_info().new_table_name());
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //更新内存
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);
    set_new_table_name(old_table_name, new_table_name);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("rename table success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::swap_table(const pb::MetaManagerRequest& request, 
                                const int64_t apply_index,
                                braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    // check new table
    std::string namespace_name = request.table_info().namespace_name();
    std::string database_name = namespace_name + "\001" + request.table_info().database();
    std::string old_table_name = database_name + "\001" + request.table_info().table_name();
    std::string new_table_name = database_name + "\001" + request.table_info().new_table_name();
    int64_t new_table_id = get_table_id(new_table_name);
    if (new_table_id == 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    
    if (check_table_has_ddlwork(table_id) || check_table_is_linked(table_id) ||
            check_table_has_ddlwork(new_table_id) || check_table_is_linked(new_table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    pb::SchemaInfo new_mem_schema_pb =  _table_info_map[new_table_id].schema_pb;
    //更新数据
    mem_schema_pb.set_table_name(request.table_info().new_table_name());
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    new_mem_schema_pb.set_table_name(request.table_info().table_name());
    new_mem_schema_pb.set_version(new_mem_schema_pb.version() + 1);
    std::string table_value;
    if (!mem_schema_pb.SerializeToString(&table_value)) {
        DB_WARNING("request serializeToArray fail, pb:%s", 
                    mem_schema_pb.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return ;
    }
    std::string new_table_value;
    if (!new_mem_schema_pb.SerializeToString(&new_table_value)) {
        DB_WARNING("request serializeToArray fail, pb:%s", 
                    new_mem_schema_pb.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return ;
    }
    std::vector<std::string> rocksdb_keys {construct_table_key(table_id), construct_table_key(new_table_id)};
    std::vector<std::string> rocksdb_values {table_value, new_table_value};
    
    // write date to rocksdb
    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
    if (ret < 0) {                                                                        
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");                  
        return ;                                                             
    }
    //更新内存
    set_table_pb(mem_schema_pb);
    set_table_pb(new_mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb, new_mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);
    swap_table_name(old_table_name, new_table_name);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("swap table success, request:%s", request.ShortDebugString().c_str());
}

bool TableManager::check_and_update_incremental(const pb::BaikalHeartBeatRequest* request,
                         pb::BaikalHeartBeatResponse* response, int64_t applied_index) {
    int64_t last_updated_index = request->last_updated_index();
    auto update_schema_func = [response](const std::vector<pb::SchemaInfo>& schema_infos) { 
        for (auto info : schema_infos) {            
            *(response->add_schema_change_info()) = info;
        }
    };

    bool need_upd = _incremental_schemainfo.check_and_update_incremental(update_schema_func, last_updated_index, applied_index);
    if (need_upd) {
        return true;
    }

    if (response->last_updated_index() < last_updated_index) {
        response->set_last_updated_index(last_updated_index);
    }

    return false;
}

void TableManager::put_incremental_schemainfo(const int64_t apply_index, std::vector<pb::SchemaInfo>& schema_infos) {
    _incremental_schemainfo.put_incremental_info(apply_index, schema_infos);
}

void TableManager::update_byte_size(const pb::MetaManagerRequest& request,
                                    const int64_t apply_index, 
                                    braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            mem_schema_pb.set_byte_size_per_record(request.table_info().byte_size_per_record());
            mem_schema_pb.set_version(mem_schema_pb.version() + 1);
        });
}

void TableManager::update_split_lines(const pb::MetaManagerRequest& request,
                                      const int64_t apply_index,
                                      braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
                mem_schema_pb.set_region_split_lines(request.table_info().region_split_lines());
                mem_schema_pb.set_version(mem_schema_pb.version() + 1);
        });
}

void TableManager::add_partition(const pb::MetaManagerRequest& request,
                                 const int64_t apply_index,
                                 braft::Closure* done) {
    if (!request.has_table_info() || 
            !request.table_info().has_partition_info()) {
        DB_WARNING("request has no valid partition info, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request has no valid partition info");
        return;
    }

    int64_t table_id = -1;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }

    pb::SchemaInfo mem_schema_pb;
    if (get_table_info(table_id, mem_schema_pb) != 0) {
        DB_WARNING("Fail to get_table_info, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Fail to get_table_info");
        return;
    }
    if (!mem_schema_pb.has_partition_info()) {
        DB_WARNING("mem_schema_pb has no partition_info, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table has no partition_info");
        return;
    }
    if (mem_schema_pb.partition_info().type() != pb::PT_RANGE) {
        DB_WARNING("not range partition table, partition type: %d", mem_schema_pb.partition_info().type());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "not range partition table");
        return;
    }
    if (mem_schema_pb.mutable_partition_info() == nullptr) {
        DB_WARNING("partition_info is nullptr, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "table partition_info is nullptr");
        return;
    }

    const int64_t main_table_id = mem_schema_pb.table_id();
    const bool is_binlog = (mem_schema_pb.engine() == pb::BINLOG);
    const pb::PrimitiveType partition_col_type = mem_schema_pb.partition_info().field_info().mysql_type();
    int64_t partition_id = mem_schema_pb.partition_info().max_range_partition_id();
    int64_t tmp_max_region_id = RegionManager::get_instance()->get_max_region_id();

    // 创建region
    std::shared_ptr<std::vector<pb::InitRegion>> init_regions(new std::vector<pb::InitRegion>{});
    if (init_regions == nullptr) {
        DB_WARNING("init_regions is nullptr, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "init_regions is nullptr");
        return;
    }

    std::unordered_map<std::string, int64_t> global_indexes_template;
    for (const auto& index : mem_schema_pb.indexs()) {
        if (index.index_type() == pb::I_PRIMARY || index.is_global()) {
            global_indexes_template[index.index_name()] = index.index_id();
        }
    }

    std::set<std::string> partition_names;
    for (const auto& range_partition_info : mem_schema_pb.partition_info().range_partition_infos()) {
        partition_names.insert(range_partition_info.partition_name());
    }

    for (auto& range_partition_info : request.table_info().partition_info().range_partition_infos()) {
        const std::string& partition_name = range_partition_info.partition_name();
        if (partition_names.find(partition_name) != partition_names.end()) {
            if (request.has_is_dynamic_change() && request.is_dynamic_change()) {
                // 动态创建分区跳过重名分区
                continue;
            }
            DB_WARNING("repeated partition name, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request has repeated partition name");
            return;
        }
        partition_names.insert(partition_name);

        pb::RangePartitionInfo& mut_range_partition_info = const_cast<pb::RangePartitionInfo&>(range_partition_info);
        // 将less value形式转换为range形式
        if (partition_utils::convert_to_partition_range(partition_col_type, mut_range_partition_info) != 0) {
            DB_WARNING("Fail to convert_to_partition_range, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "Fail to convert_to_partition_range");
            return;
        }
        if (!mut_range_partition_info.has_range()) {
            DB_WARNING("range_partition_info has no range, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "range_partition_info has no range");
            return;
        }
        if (partition_utils::set_partition_col_type(partition_col_type, mut_range_partition_info) != 0) {
            DB_WARNING("Fail to set_partition_col_type, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "Fail to set_partition_col_type");
            return;
        }
        if (!partition_utils::check_range_partition_info(mut_range_partition_info)) {
            DB_WARNING("Invalid range_partition_info, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Fail to check_range_partition_info");
            return;
        }

        // 判断新增分区是否与已有分区重叠
        if (partition_utils::check_partition_overlapped(
                mem_schema_pb.partition_info().range_partition_infos(), mut_range_partition_info)) {
            if (request.has_is_dynamic_change() && request.is_dynamic_change()) {
                // 动态创建分区跳过重复分区
                continue;
            }
            DB_WARNING("partition overlapping, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "partition overlapping");
            return;
        }

        ::google::protobuf::RepeatedPtrField<pb::SplitKey> split_keys;
        split_keys.Swap(mut_range_partition_info.mutable_split_keys());
        mut_range_partition_info.clear_split_keys();

        // 将新增分区添加到mem_schema_pb
        ++partition_id;
        int64_t current_partition_id = partition_id;

        pb::RangePartitionInfo* p_range_partition_info = 
                    mem_schema_pb.mutable_partition_info()->add_range_partition_infos();
        if (p_range_partition_info == nullptr) {
            DB_WARNING("p_range_partition_info is nullptr, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "p_range_partition_info is nullptr");
            return;
        }
        p_range_partition_info->CopyFrom(mut_range_partition_info);
        p_range_partition_info->set_partition_id(current_partition_id);

        // 如果partition设置resource_tag，则使用partition的resource_tag
        std::string partition_resource_tag = mem_schema_pb.resource_tag();
        std::string partition_main_logical_room = mem_schema_pb.main_logical_room();
        if (range_partition_info.has_resource_tag()) {
            partition_resource_tag = range_partition_info.resource_tag();
            partition_main_logical_room = "";
        }
        boost::trim(partition_resource_tag);

        std::unordered_map<std::string, int64_t> global_indexes = global_indexes_template;
        std::vector<std::string> processed_index_name;

        // 有split_key的索引先处理
        for (const auto& split_key : split_keys) {
            const std::string& index_name = split_key.index_name();
            if (global_indexes.find(index_name) == global_indexes.end()) {
                continue;
            }
            for (size_t i = 0; i <= split_key.split_keys_size(); ++i) {
                std::string instance;
                int ret = ClusterManager::get_instance()->select_instance_rolling(
                                {partition_resource_tag, partition_main_logical_room, ""}, {}, instance);
                if (ret < 0) {
                    DB_WARNING("select instance fail, request: %s", request.ShortDebugString().c_str());
                    IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "select instance fail");
                    return;
                }
                pb::InitRegion init_region_request;
                pb::RegionInfo* region_info = init_region_request.mutable_region_info();
                if (region_info == nullptr) {
                    DB_WARNING("region_info is nullptr, request: %s", request.ShortDebugString().c_str());
                    IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "region_info is nullptr");
                    return;
                }
                region_info->set_region_id(++tmp_max_region_id);
                region_info->set_table_id(global_indexes[index_name]);
                processed_index_name.emplace_back(index_name);
                region_info->set_main_table_id(main_table_id);
                region_info->set_table_name(mem_schema_pb.table_name());
                construct_common_region(region_info, mem_schema_pb.replica_num());
                region_info->set_partition_id(current_partition_id);
                region_info->add_peers(instance);
                region_info->set_leader(instance);
                region_info->set_can_add_peer(false); // 简化理解，让raft addpeer必须发送snapshot
                region_info->set_is_binlog_region(is_binlog);
                if (i != 0) {
                    region_info->set_start_key(split_key.split_keys(i-1));
                }
                if (i < split_key.split_keys_size()) {
                    region_info->set_end_key(split_key.split_keys(i));
                }
                init_region_request.set_snapshot_times(2);
                init_regions->emplace_back(init_region_request);
            }
        }
        for (const auto& index_name : processed_index_name) {
            global_indexes.erase(index_name);
        }
        for (const auto& index : global_indexes) {
            std::string instance;
            int ret = ClusterManager::get_instance()->select_instance_rolling(
                            {partition_resource_tag, partition_main_logical_room, ""}, {}, instance);
            if (ret < 0) {
                DB_WARNING("select instance failed, request: %s", request.ShortDebugString().c_str());
                IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "select instance failed");
                return;
            }
            pb::InitRegion init_region_request;
            pb::RegionInfo* region_info = init_region_request.mutable_region_info();
            if (region_info == nullptr) {
                DB_WARNING("region_info is nullptr, request: %s", request.ShortDebugString().c_str());
                IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "region_info is nullptr");
                return;
            }
            region_info->set_region_id(++tmp_max_region_id);
            region_info->set_table_id(index.second);
            region_info->set_main_table_id(main_table_id);
            region_info->set_table_name(mem_schema_pb.table_name());
            construct_common_region(region_info, mem_schema_pb.replica_num());
            region_info->set_partition_id(current_partition_id);
            region_info->add_peers(instance);
            region_info->set_leader(instance);
            region_info->set_can_add_peer(false); // 简化理解，让raft addpeer必须发送snapshot
            region_info->set_is_binlog_region(is_binlog);
            init_region_request.set_snapshot_times(2);
            init_regions->emplace_back(init_region_request);
        }
    }

    std::sort(mem_schema_pb.mutable_partition_info()->mutable_range_partition_infos()->pointer_begin(),
              mem_schema_pb.mutable_partition_info()->mutable_range_partition_infos()->pointer_end(),
              partition_utils::PointerRangeComparator());
    mem_schema_pb.mutable_partition_info()->set_max_range_partition_id(partition_id);
    mem_schema_pb.set_partition_num(mem_schema_pb.partition_info().range_partition_infos_size());
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);

    for (auto& init_region_request : *init_regions) {
        pb::RegionInfo* region_info = init_region_request.mutable_region_info();
        if (region_info == nullptr) {
            DB_WARNING("region_info is nullptr, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "region_info is nullptr");
            return;
        }
        // region_info->set_partition_num(mem_schema_pb.partition_num()); // 当前系统未使用region partition_num，range分区不易维护
        *(init_region_request.mutable_schema_info()) = mem_schema_pb;
    }

    std::vector<std::string> rocksdb_keys;
    std::vector<std::string> rocksdb_values;

    // 持久化region_id
    std::string max_region_id_key = RegionManager::get_instance()->construct_max_region_id_key();
    std::string max_region_id_value;
    max_region_id_value.append((char*)&tmp_max_region_id, sizeof(int64_t));
    rocksdb_keys.emplace_back(max_region_id_key);
    rocksdb_values.emplace_back(max_region_id_value);

    // 持久化schema_info
    std::string mem_schema_pb_str;
    if (!mem_schema_pb.SerializeToString(&mem_schema_pb_str)) {
        DB_WARNING("request SerializeToString fail when add_partition, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "SerializeToString fail");
        return;
    }
    rocksdb_keys.emplace_back(construct_table_key(table_id));
    rocksdb_values.emplace_back(mem_schema_pb_str);

    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
    if (ret < 0) {                                                                        
        DB_WARNING("write db fail, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }

    // 更新内存
    RegionManager::get_instance()->set_max_region_id(tmp_max_region_id);
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos { mem_schema_pb };
    put_incremental_schemainfo(apply_index, schema_infos);

    if (done && is_create_table_support_engine(mem_schema_pb.engine())) {
        uint64_t log_id = butil::fast_rand();
        bool success = true;
        std::vector<int64_t> succ_region_ids;
        bthread_mutex_t init_region_mutex;

        // 40个线程并发发送
        bthread_mutex_init(&init_region_mutex, NULL);
        BthreadCond concurrency_cond(-FLAGS_concurrency_num);
        for (const auto& init_region_request : *init_regions) {
            auto send_init_region = 
                    [&init_region_request, &success, &concurrency_cond, &succ_region_ids,
                     &init_region_mutex, log_id, table_id] () {
                std::shared_ptr<BthreadCond> auto_decrease(
                            &concurrency_cond, [](BthreadCond* cond) { cond->decrease_signal(); });
                const int64_t region_id = init_region_request.region_info().region_id();
                StoreInteract store_interact(init_region_request.region_info().leader().c_str());
                pb::StoreRes res;
                int ret = store_interact.send_request(log_id, "init_region", init_region_request, res);
                if (ret < 0) {
                    DB_FATAL("Fail to init_region, address:%s, region_id: %ld", 
                              init_region_request.region_info().leader().c_str(), region_id);
                    success = false;
                    return;
                }
                {
                    BAIDU_SCOPED_LOCK(init_region_mutex);
                    succ_region_ids.emplace_back(region_id);
                }
                DB_NOTICE("new region_id: %ld success, table_id: %ld", region_id, table_id);
            };
            if (!success) {
                break;
            }
            Bthread bth;
            concurrency_cond.increase();
            concurrency_cond.wait();
            bth.run(send_init_region);
        }
        concurrency_cond.wait(-FLAGS_concurrency_num);
        bthread_mutex_destroy(&init_region_mutex);

        if (!success) {
            Bthread bth_remove_region(&BTHREAD_ATTR_SMALL);
            auto remove_function = [succ_region_ids] () {
                RegionManager::get_instance()->send_remove_region_request(succ_region_ids);
            };
            DB_WARNING("Fail to init region, request: %s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "Fail to init region");
            return;
        }
    }

    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_WARNING("add_partition succ, request: %s", request.ShortDebugString().c_str());
}

void TableManager::drop_partition(const pb::MetaManagerRequest& request,
                                  const int64_t apply_index,
                                  braft::Closure* done) {
    if (!request.has_table_info() || 
            !request.table_info().has_partition_info()) {
        DB_WARNING("request has no valid partition info, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request has no valid partition info");
        return;
    }
    
    int64_t table_id = -1;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }

    pb::SchemaInfo mem_schema_pb;
    if (get_table_info(table_id, mem_schema_pb) != 0) {
        DB_WARNING("Fail to get_table_info, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Fail to get_table_info");
        return;
    }
    if (!mem_schema_pb.has_partition_info()) {
        DB_WARNING("mem_schema_pb has no partition_info, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table has no partition_info");
        return;
    }
    if (mem_schema_pb.partition_info().type() != pb::PT_RANGE) {
        DB_WARNING("not range partition table, partition type: %d", mem_schema_pb.partition_info().type());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "not range partition table");
        return;
    }
    if (mem_schema_pb.mutable_partition_info() == nullptr) {
        DB_WARNING("partition_info is nullptr, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table partition_info is nullptr");
        return;
    }

    std::vector<std::string> drop_range_partition_names_vec;
    for (const auto& range_partition_info :request.table_info().partition_info().range_partition_infos()) {
        drop_range_partition_names_vec.emplace_back(range_partition_info.partition_name());
    }
    drop_partition_internal(
        mem_schema_pb, drop_range_partition_names_vec, request.is_dynamic_change(), apply_index, done);
    
    DB_NOTICE("drop_partition, request: %s", request.ShortDebugString().c_str());
}

void TableManager::modify_partition(const pb::MetaManagerRequest& request,
                                    const int64_t apply_index,
                                    braft::Closure* done) {
    if (!request.has_table_info() || 
            !request.table_info().has_partition_info()) {
        DB_WARNING("request has no valid partition info, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "request has no valid partition info");
        return;
    }
    
    int64_t table_id = -1;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }

    pb::SchemaInfo mem_schema_pb;
    if (get_table_info(table_id, mem_schema_pb) != 0) {
        DB_WARNING("Fail to get_table_info, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Fail to get_table_info");
        return;
    }
    if (!mem_schema_pb.has_partition_info()) {
        DB_WARNING("mem_schema_pb has no partition_info, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table has no partition_info");
        return;
    }
    if (mem_schema_pb.mutable_partition_info() == nullptr) {
        DB_WARNING("partition_info is nullptr, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table partition_info is nullptr");
        return;
    }
    if (mem_schema_pb.partition_info().type() != pb::PT_RANGE) {
        DB_WARNING("partition type is not PT_RANGE, request: %s", request.ShortDebugString().c_str());
        return;
    }

    // 获取区间相同的所有分区
    // key: partition_range, value: partition_names vector
    std::map<std::pair<std::string, std::string>, std::vector<std::string>> range_partition_map;
    for (const auto& range_partition_info : mem_schema_pb.partition_info().range_partition_infos()) {
        std::pair<std::string, std::string> range_key;
        if (partition_utils::get_partition_range(range_partition_info, range_key) != 0) {
            DB_FATAL("Fail to get_partition_range");
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Fail to get_partition_range");
            return;
        }
        range_partition_map[range_key].emplace_back(range_partition_info.partition_name());
    }

    std::vector<std::string> drop_range_partition_names_vec;
    for (const auto& req_range_partition_info : request.table_info().partition_info().range_partition_infos()) {
        for (size_t i = 0; i < mem_schema_pb.partition_info().range_partition_infos_size(); ++i) {
            pb::RangePartitionInfo* p_range_partition_info = 
                mem_schema_pb.mutable_partition_info()->mutable_range_partition_infos(i);
            if (p_range_partition_info == nullptr) {
                return;
            }
            if (req_range_partition_info.partition_name() != p_range_partition_info->partition_name()) {
                continue;
            }
            if (req_range_partition_info.has_resource_tag()) {
                p_range_partition_info->set_resource_tag(req_range_partition_info.resource_tag());
            }
            if (req_range_partition_info.has_is_cold()) {
                if (req_range_partition_info.is_cold()) {
                    if (p_range_partition_info->type() == mem_schema_pb.partition_info().primary_range_partition_type()) {
                        // 热转冷，只有主分区才能触发
                        std::pair<std::string, std::string> range_key;
                        if (partition_utils::get_partition_range(*p_range_partition_info, range_key) != 0) {
                            DB_FATAL("Fail to get_partition_range");
                            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Fail to get_partition_range");
                            return;
                        }
                        for (const auto& range_partition_name : range_partition_map[range_key]) {
                            // 主分区保留
                            if (range_partition_name == p_range_partition_info->partition_name()) {
                                continue;
                            }
                            // 备分区删除
                            drop_range_partition_names_vec.emplace_back(range_partition_name);
                        }
                        p_range_partition_info->set_is_cold(req_range_partition_info.is_cold());
                    } else {
                        DB_WARNING("p_range_partition_info->type[%d] is not same to "
                                    "primary_range_partition_type[%d]",
                                    p_range_partition_info->type(), 
                                    mem_schema_pb.partition_info().primary_range_partition_type());
                    }
                } else {
                    p_range_partition_info->set_is_cold(req_range_partition_info.is_cold());
                }
            }
            break;
        }
    }

    drop_partition_internal(
        mem_schema_pb, drop_range_partition_names_vec, request.is_dynamic_change(), apply_index, done);

    DB_NOTICE("modify partition, request: %s", request.ShortDebugString().c_str());
}

void TableManager::convert_partition(const pb::MetaManagerRequest& request,
                                      const int64_t apply_index,
                                      braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
            [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
        if (mem_schema_pb.has_partition_info() && mem_schema_pb.partition_info().type() == pb::PT_RANGE) {
            pb::PartitionInfo* p_partition_info = mem_schema_pb.mutable_partition_info();
            if (p_partition_info == nullptr) {
                DB_WARNING("p_partition_info is nullptr");
                return;
            } 
            if (request.has_table_info() && request.table_info().has_partition_info()) {
                const pb::PartitionInfo& req_partition_info = request.table_info().partition_info();
                const pb::RangePartitionType primary_range_partition_type = req_partition_info.primary_range_partition_type();
                bool found = false;
                if (req_partition_info.gen_range_partition_types_size() > 0) {
                    for (const auto& type : req_partition_info.gen_range_partition_types()) {
                        if (type == primary_range_partition_type) {
                            found = true;
                            break;
                        }
                    }
                } else {
                    for (const auto& type : p_partition_info->gen_range_partition_types()) {
                        if (type == primary_range_partition_type) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    if (done != nullptr && ((MetaServerClosure*)done)->response != nullptr) {
                        ((MetaServerClosure*)done)->response->set_errcode(pb::INPUT_PARAM_ERROR);
                    }
                    DB_WARNING("primary_range_partition_type is not in gen_range_partition_types");
                    return;
                }
                p_partition_info->set_primary_range_partition_type(primary_range_partition_type);
                if (req_partition_info.gen_range_partition_types_size() > 0) {
                    p_partition_info->clear_gen_range_partition_types();
                    for (const auto& type : req_partition_info.gen_range_partition_types()) {
                        p_partition_info->add_gen_range_partition_types(static_cast<pb::RangePartitionType>(type));
                    }
                }
            }
            mem_schema_pb.set_version(mem_schema_pb.version() + 1);
        }
    });
}

void TableManager::update_dynamic_partition_attr(const pb::MetaManagerRequest& request,
                                      const int64_t apply_index,
                                      braft::Closure* done) {
    
    if (!request.has_table_info() ||
            !request.table_info().has_partition_info() ||
            !request.table_info().partition_info().has_dynamic_partition_attr()) {
        return;
    }

    int64_t table_id = -1;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    pb::SchemaInfo mem_schema_pb;
    if (get_table_info(table_id, mem_schema_pb) != 0) {
        DB_WARNING("Fail to get_table_info, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Fail to get_table_info");
        return;
    }
    if (!mem_schema_pb.has_partition_info()) {
        DB_WARNING("mem_schema_pb has no partition_info, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table has no partition_info");
        return;
    }
    if (mem_schema_pb.mutable_partition_info() == nullptr) {
        DB_WARNING("partition_info is nullptr, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table partition_info is nullptr");
        return;
    }
    if (mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr() == nullptr) {
        DB_WARNING("dynamic_partition_attr is nullptr, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table dynamic_partition_attr is nullptr");
        return;
    }
    if (mem_schema_pb.partition_info().type() != pb::PT_RANGE) {
        DB_WARNING("not range partition table, partition type: %d", mem_schema_pb.partition_info().type());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "not range partition table");
        return;
    }

    const pb::DynamicPartitionAttr& dynamic_partition_attr = 
                                    request.table_info().partition_info().dynamic_partition_attr();
    if (dynamic_partition_attr.has_enable()) {
        mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_enable(
                                                                dynamic_partition_attr.enable());
    }
    if (dynamic_partition_attr.has_time_unit()) {
        mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_time_unit(
                                                                dynamic_partition_attr.time_unit());
    }
    if (dynamic_partition_attr.has_start()) {
        mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_start(
                                                                dynamic_partition_attr.start());
    }
    if (dynamic_partition_attr.has_cold()) {
        mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_cold(
                                                                dynamic_partition_attr.cold());
    }
    if (dynamic_partition_attr.has_end()) {
        mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_end(
                                                                dynamic_partition_attr.end());
    }
    if (dynamic_partition_attr.has_prefix()) {
        mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_prefix(
                                                                dynamic_partition_attr.prefix());
    }
    if (dynamic_partition_attr.has_start_day_of_month()) {
        mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_start_day_of_month(
                                                                dynamic_partition_attr.start_day_of_month());
    }

    if (!partition_utils::check_dynamic_partition_attr(mem_schema_pb.partition_info().dynamic_partition_attr())) {
        DB_WARNING("Invalid dynamic_partition_attr, request: %s", request.ShortDebugString().c_str());
        return;
    }

    // 填充动态分区属性默认值
    if (mem_schema_pb.partition_info().dynamic_partition_attr().enable()) {
        if (!mem_schema_pb.partition_info().dynamic_partition_attr().has_start_day_of_month()) {
                mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_start_day_of_month(
                                                                    partition_utils::START_DAY_OF_MONTH);
        }
        if (!mem_schema_pb.partition_info().dynamic_partition_attr().has_prefix()) {
                mem_schema_pb.mutable_partition_info()->mutable_dynamic_partition_attr()->set_prefix(
                                                                    partition_utils::PREFIX);
        }
    }

    mem_schema_pb.set_version(mem_schema_pb.version() + 1);

    // 持久化schema
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }

    // 更新内存
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos { mem_schema_pb };
    put_incremental_schemainfo(apply_index, schema_infos);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");

    if (done) {
        pb::SchemaInfo add_partition_schema;
        pb::SchemaInfo del_partition_schema;
        pb::SchemaInfo cold_partition_schema;
        get_change_partition_schema(mem_schema_pb, add_partition_schema, del_partition_schema, cold_partition_schema);
        if (add_partition_schema.partition_info().range_partition_infos_size() != 0) {
            pb::MetaManagerRequest alter_partition_request;
            alter_partition_request.set_op_type(pb::OP_ADD_PARTITION);
            alter_partition_request.set_is_dynamic_change(true);
            alter_partition_request.mutable_table_info()->Swap(&add_partition_schema);
            DB_NOTICE("DDL_LOG add dynamic partition req[%s]", request.ShortDebugString().c_str());
            SchemaManager::get_instance()->process_schema_info(NULL, &alter_partition_request, NULL, brpc::DoNothing());
        }
        if (del_partition_schema.partition_info().range_partition_infos_size() != 0) {
            pb::MetaManagerRequest alter_partition_request;
            alter_partition_request.set_op_type(pb::OP_DROP_PARTITION);
            alter_partition_request.set_is_dynamic_change(true);
            alter_partition_request.mutable_table_info()->Swap(&del_partition_schema);
            DB_NOTICE("DDL_LOG drop dynamic partition req[%s]", alter_partition_request.ShortDebugString().c_str());
            SchemaManager::get_instance()->process_schema_info(NULL, &alter_partition_request, NULL, brpc::DoNothing());
        }
        if (cold_partition_schema.partition_info().range_partition_infos_size() != 0) {
            pb::MetaManagerRequest alter_partition_request;
            alter_partition_request.set_op_type(pb::OP_MODIFY_PARTITION);
            alter_partition_request.set_is_dynamic_change(true);
            alter_partition_request.mutable_table_info()->Swap(&cold_partition_schema);
            DB_NOTICE("DDL_LOG modify dynamic partition req[%s]", alter_partition_request.ShortDebugString().c_str());
            SchemaManager::get_instance()->process_schema_info(NULL, &alter_partition_request, NULL, brpc::DoNothing());
        }
    }
}

void TableManager::set_main_logical_room(const pb::MetaManagerRequest& request,
                                      const int64_t apply_index,
                                      braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
                mem_schema_pb.set_main_logical_room(request.table_info().main_logical_room());
                mem_schema_pb.set_version(mem_schema_pb.version() + 1);
        });
}

void TableManager::update_schema_conf(const pb::MetaManagerRequest& request,
                                       const int64_t apply_index,
                                       braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
    [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
        const pb::SchemaConf& schema_conf = request.table_info().schema_conf();
        DB_WARNING("request:%s", request.ShortDebugString().c_str());
        pb::SchemaConf* p_conf = mem_schema_pb.mutable_schema_conf();
        if (schema_conf.storage_compute_separate()) {
            for (auto& index : mem_schema_pb.indexs()) {
                DB_WARNING("index:%s", index.ShortDebugString().c_str());
                if (index.index_type() == pb::I_FULLTEXT) {
                    DB_WARNING("table has fulltext index, request:%s", request.ShortDebugString().c_str());
                    IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "fulltext not support kv mode");
                    return;
                }
            }
        }
        update_schema_conf_common(request.table_info().table_name(), schema_conf, p_conf);
        //代价开关操作，需要增加op_version
        if (schema_conf.has_select_index_by_cost()) {
            if (schema_conf.select_index_by_cost()) {
                update_op_version(p_conf, "open cost switch");
            } else {
                update_op_version(p_conf, "close cost switch");
            }
        }
        mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    });
    if (request.table_info().schema_conf().has_pk_prefix_balance()) {
        update_pk_prefix_balance_timestamp(request.table_info().table_id(),
                request.table_info().schema_conf().pk_prefix_balance());
    }
    if (request.table_info().schema_conf().has_in_fast_import())  {
        update_tables_in_fast_importer(request, request.table_info().schema_conf().in_fast_import());
    }
}

void TableManager::update_statistics(const pb::MetaManagerRequest& request,
                                       const int64_t apply_index,
                                       braft::Closure* done) {
    int64_t table_id = 0;
    if (request.has_statistics() && request.statistics().has_table_id()) {
        table_id = request.statistics().table_id();
    } else {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }

    int64_t version = 0;
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        version = _table_info_map[table_id].statistics_version + 1;
    }

    pb::Statistics stat_pb = request.statistics();
    stat_pb.set_version(version);

    auto ret = update_statistics_for_rocksdb(table_id, stat_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        _table_info_map[table_id].statistics_version = version;
    }

    //增加op version
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    update_op_version(mem_schema_pb.mutable_schema_conf(), "update cost statistics");
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    set_table_pb(mem_schema_pb);  
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);   

    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("update table statistics success, request:%s", stat_pb.ShortDebugString().c_str());
}

void TableManager::update_resource_tag(const pb::MetaManagerRequest& request, 
                                       const int64_t apply_index, 
                                       braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    auto resource_tag = request.table_info().resource_tag();
    if (!ClusterManager::get_instance()->check_resource_tag_exist(resource_tag)) {
        DB_WARNING("check resource_tag exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "resource_tag not exist");
        return ;
    }
    mem_schema_pb.set_resource_tag(request.table_info().resource_tag());
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    set_table_pb(mem_schema_pb); 
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);   
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("update table internal success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::update_dists(const pb::MetaManagerRequest& request,
                                const int64_t apply_index, 
                                braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            mem_schema_pb.set_version(mem_schema_pb.version() + 1);
            mem_schema_pb.clear_dists();
            mem_schema_pb.clear_main_logical_room();
            std::string main_logical_room;
            if (request.table_info().has_main_logical_room()) {
                main_logical_room = request.table_info().main_logical_room();
            }
            bool found = false;
            for (auto& dist : request.table_info().dists()) {
                auto dist_ptr = mem_schema_pb.add_dists();
                *dist_ptr = dist;
                if (main_logical_room == dist.logical_room()) {
                    found = true;
                }
            }
            if (found) {
                mem_schema_pb.set_main_logical_room(main_logical_room);
            }
            if (request.table_info().has_replica_num()) {
                mem_schema_pb.set_replica_num(request.table_info().replica_num());
            }
        });
}

void TableManager::update_ttl_duration(const pb::MetaManagerRequest& request,
                                const int64_t apply_index, 
                                braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            if (mem_schema_pb.ttl_duration() > 0 && request.table_info().ttl_duration() > 0) {
                // 只修改ttl
                mem_schema_pb.set_ttl_duration(request.table_info().ttl_duration());
            } else if (mem_schema_pb.ttl_duration() <= 0 && request.table_info().ttl_duration() > 0) {
                // online ttl
                bool can_support_ttl = true;
                for (const auto& index : mem_schema_pb.indexs()) {
                    if (index.index_type() == pb::I_FULLTEXT) {
                        can_support_ttl = false;
                        break;
                    }
                }

                if (mem_schema_pb.engine() == pb::REDIS || mem_schema_pb.engine() == pb::BINLOG) {
                    can_support_ttl = false;
                }

                if (!can_support_ttl) {
                    DB_WARNING("can't support ttl, req: %s", request.ShortDebugString().c_str());
                    return;
                }
                int64_t online_ttl_expire_time_us = butil::gettimeofday_us() + request.table_info().ttl_duration() * 1000000LL;
                mem_schema_pb.set_ttl_duration(request.table_info().ttl_duration());
                mem_schema_pb.set_online_ttl_expire_time_us(online_ttl_expire_time_us);
            } else {
                DB_WARNING("update fail, resuest.ttl_duration:%ld mem_schema_pb.ttl_duration:%ld",
                    request.table_info().ttl_duration(), mem_schema_pb.ttl_duration());
                return;
            }

            mem_schema_pb.set_version(mem_schema_pb.version() + 1);
        });
}

void TableManager::update_table_comment(const pb::MetaManagerRequest& request,
                                const int64_t apply_index,
                                braft::Closure* done) {
    update_table_internal(request, apply_index, done,
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            mem_schema_pb.set_version(mem_schema_pb.version() + 1);
            mem_schema_pb.set_comment(request.table_info().comment());
        });
}

void TableManager::update_charset(const pb::MetaManagerRequest& request,
                                    const int64_t apply_index, 
                                    braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            mem_schema_pb.set_charset(request.table_info().charset());
            mem_schema_pb.set_version(mem_schema_pb.version() + 1);
        });
}

void TableManager::add_field(const pb::MetaManagerRequest& request,
                             const int64_t apply_index,
                             braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }

    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    int32_t tmp_max_field_id = mem_schema_pb.max_field_id();
    std::unordered_map<std::string, int32_t> add_field_id_map;
    for (auto& field : request.table_info().fields()) {
        if (_table_info_map[table_id].field_id_map.count(field.field_name()) != 0) {
            DB_WARNING("field name:%s has already existed, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name already exist");
            return;
        }
        if (field.has_auto_increment() && field.auto_increment()) {
            DB_WARNING("not support auto increment, field name:%s, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field can not be auto_increment");
            return;
        }
        pb::FieldInfo* add_field = mem_schema_pb.add_fields();
        *add_field = field;
        add_field->set_field_id(++tmp_max_field_id);
        add_field_id_map[field.field_name()] = tmp_max_field_id;
    }
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    mem_schema_pb.set_max_field_id(tmp_max_field_id);
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);
    add_field_mem(table_id, add_field_id_map);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add field success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::drop_field(const pb::MetaManagerRequest& request,
                              const int64_t apply_index,
                              braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    std::vector<std::string> drop_field_names;
    for (auto& field : request.table_info().fields()) {
        if (_table_info_map[table_id].field_id_map.count(field.field_name()) == 0) {
            DB_WARNING("field name:%s not existed, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name not exist");
            return;
        }
        auto field_id = _table_info_map[table_id].field_id_map[field.field_name()];
        if (check_field_is_linked(table_id, field_id)) {
            DB_WARNING("field name:%s is binlog link field, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name is binlog link field");
            return;
        }
        if (mem_schema_pb.has_partition_info()) {
            if (mem_schema_pb.partition_info().field_info().field_name() == field.field_name()) {
                DB_WARNING("field name:%s is partitiion field, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name is partitiion field");
                return;
            }
        }
        drop_field_names.push_back(field.field_name());
    }
    for (auto& index : mem_schema_pb.indexs()) {
        if (index.hint_status() == pb::IHS_DISABLE && index.state() == pb::IS_DELETE_LOCAL) {
            continue;
        }
        for (auto field_name : index.field_names()) {
            auto iter = std::find(drop_field_names.begin(),
                              drop_field_names.end(),
                              field_name);
            if (iter != drop_field_names.end()) {
                DB_WARNING("field name:%s is an index column, request:%s",
                        field_name.c_str(), request.ShortDebugString().c_str());
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name is an index column");
                return;
            }
        }
    }
    for (auto& field : *mem_schema_pb.mutable_fields()) {
        auto iter = std::find(drop_field_names.begin(),
                              drop_field_names.end(), 
                              field.field_name());
        if (iter != drop_field_names.end()) {
            field.set_deleted(true);
        }
    }
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }

    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);
    drop_field_mem(table_id, drop_field_names);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop field success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::rename_field(const pb::MetaManagerRequest& request,
                                const int64_t apply_index, 
                                braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    std::unordered_map<int32_t, std::string> id_new_field_map;
    std::vector<std::string> drop_field_names;
    std::unordered_map<std::string, int32_t> add_field_id_map;
    for (auto& field : request.table_info().fields()) {
        if (_table_info_map[table_id].field_id_map.count(field.field_name()) == 0) {
            DB_WARNING("field name:%s not existed, request:%s",
                    field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name not exist");
            return;
        }
        if (check_field_is_linked(table_id,  _table_info_map[table_id].field_id_map[field.field_name()])) {
            DB_WARNING("field name:%s is binlog link field, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name is binlog link field");
            return;
        }
        if (!field.has_new_field_name()) {
            DB_WARNING("request has no new field name, request:%s", request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "new field name is null");
            return;
        }
        if (_table_info_map[table_id].field_id_map.count(field.new_field_name()) != 0) {
            DB_WARNING("new field name:%s already existed, request:%s",
                        field.new_field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "new field name already exist");
            return;
        }
        int32_t field_id = 0;
        for (auto& mem_field : *mem_schema_pb.mutable_fields()) {
            if (mem_field.field_name() == field.field_name()) {
                mem_field.set_field_name(field.new_field_name());
                field_id = mem_field.field_id();
            }
        }
        for (auto& mem_index : *mem_schema_pb.mutable_indexs()) {
            for (auto& mem_field : *mem_index.mutable_field_names()) {
                if (mem_field == field.field_name()) {
                    mem_field = field.new_field_name();
                }
            }
        }
        id_new_field_map[field_id] = field.new_field_name();
        add_field_id_map[field.new_field_name()] = field_id;
        drop_field_names.push_back(field.field_name());
    }
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);
    drop_field_mem(table_id, drop_field_names);
    add_field_mem(table_id, add_field_id_map);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("rename field success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::modify_field(const pb::MetaManagerRequest& request,
                                const int64_t apply_index, 
                                braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    auto& table_mem =  _table_info_map[table_id];
    if (request.has_ddlwork_info() && request.ddlwork_info().op_type() == pb::OP_MODIFY_FIELD) {
        int ret = DDLManager::get_instance()->init_column_ddlwork(table_id, request.ddlwork_info(), table_mem.partition_regions);
        if (ret < 0) {
            DB_WARNING("table_id[%ld] add index init ddlwork failed.", table_id);
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "init index ddlwork failed");
        }
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        return;
    }
    pb::SchemaInfo mem_schema_pb = table_mem.schema_pb;
    std::vector<std::string> drop_field_names;
    for (auto& field : request.table_info().fields()) {
        std::string field_name = field.field_name();
        if (_table_info_map[table_id].field_id_map.count(field_name) == 0) {
            DB_WARNING("field name:%s not existed, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name not exist");
            return;
        }
        auto field_id = _table_info_map[table_id].field_id_map[field_name];
        if (check_field_is_linked(table_id, field_id)) {
            DB_WARNING("field name:%s is binlog link field, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name is binlog link field");
            return;
        }
        for (auto& mem_field : *mem_schema_pb.mutable_fields()) {
            if (mem_field.field_name() == field_name) {
                if (field.has_mysql_type()) {
                    if (!check_field_is_compatible_type(mem_field.mysql_type(), field.mysql_type())) {
                        // TODO 数据类型变更仅支持meta-only, 有损变更待支持
                        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR,
                                             "modify field data type unsupported lossy changes");
                        return;
                    }
                    mem_field.set_mysql_type(field.mysql_type());
                }
                if (field.has_can_null()) {
                    // TODO NULL VALUE CHECK
                    mem_field.set_can_null(field.can_null());
                }
                if (field.auto_increment() != mem_field.auto_increment()) {
                    //可以去除auto increment
                    if (!field.auto_increment() && mem_field.auto_increment()) {
                        mem_field.set_auto_increment(field.auto_increment());
                    } else {
                        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR,
                                "modify field auto_increment unsupported");
                        return;
                    }
                }
                if (field.has_default_value()) {
                    mem_field.set_default_value(field.default_value());
                    if (field.has_default_literal()) {
                        mem_field.set_default_literal(field.default_literal());
                    }
                }
                if (field.has_comment()) {
                    mem_field.set_comment(field.comment());
                }
                if (field.has_on_update_value()) {
                    mem_field.set_on_update_value(field.on_update_value());
                }
                if (field.has_can_null()) {
                    mem_field.set_can_null(field.can_null());
                }
                if (field.has_default_value()) {
                    if (field.default_value() == "__NULL__") {
                        mem_field.clear_default_value();
                    } else {
                        //修改default value需谨慎，会导致存储null数据的值变成新默认值
                        mem_field.set_default_value(field.default_value());
                    }
                } 
            }
        }
    }
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("modify field type success, request:%s", request.ShortDebugString().c_str());
}
void TableManager::process_schema_heartbeat_for_store(
        std::unordered_map<int64_t, int64_t>& store_table_id_version,
        pb::StoreHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& table_info_map : _table_info_map) {
        int64_t table_id = table_info_map.first;
        if (store_table_id_version.count(table_id) == 0
                || store_table_id_version[table_id]
                    < table_info_map.second.schema_pb.version()) {
            pb::SchemaInfo* new_table_info = response->add_schema_change_info();
            *new_table_info = table_info_map.second.schema_pb;
            DB_DEBUG("table_id[%ld] add schema info [%s] ", table_id,
                    new_table_info->ShortDebugString().c_str());
            //DB_WARNING("add or update table_name:%s, table_id:%ld",
            //            new_table_info->table_name().c_str(), new_table_info->table_id());
        }
    }
    for (auto& store_table_id : store_table_id_version) {
        if (_table_info_map.find(store_table_id.first)  == _table_info_map.end()) {
            pb::SchemaInfo* new_table_info = response->add_schema_change_info();
            new_table_info->set_table_id(store_table_id.first);
            new_table_info->set_deleted(true);
            new_table_info->set_table_name("deleted");
            new_table_info->set_database("deleted");
            new_table_info->set_namespace_name("deleted");
            //DB_WARNING("delete table_info:%s, table_id: %ld",
            //        new_table_info->table_name().c_str(), new_table_info->table_id());
        } 
    }
}
void TableManager::check_update_or_drop_table(
        const pb::BaikalHeartBeatRequest* request,
        pb::BaikalHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& schema_heart_beat : request->schema_infos()) {
        int64_t table_id = schema_heart_beat.table_id();
        //表已经删除
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            auto schema_info = response->add_schema_change_info();
            schema_info->set_table_id(table_id);
            schema_info->set_deleted(true);
            schema_info->set_table_name("deleted");
            schema_info->set_database("deleted");
            schema_info->set_namespace_name("deleted");
            //相应的region也删除
            for (auto& region_heart_beat : schema_heart_beat.regions()) {
                auto region_info = response->add_region_change_info();
                region_info->set_region_id(region_heart_beat.region_id());
                region_info->set_deleted(true);
                region_info->set_table_id(table_id);
                region_info->set_table_name("deleted");
                region_info->set_partition_id(0);
                region_info->set_replica_num(0);
                region_info->set_version(0);
                region_info->set_conf_version(0);
            }   
            continue;
        }
        //全局二级索引没有schema信息
        if (_table_info_map[table_id].is_global_index) {
            continue;
        }
        //表更新
        if (_table_info_map[table_id].schema_pb.version() > schema_heart_beat.version()) {
            *(response->add_schema_change_info()) = _table_info_map[table_id].schema_pb;
        }
    }
}

void TableManager::check_update_statistics(const pb::BaikalOtherHeartBeatRequest* request,
        pb::BaikalOtherHeartBeatResponse* response) {
    std::map<int64_t, int64_t> table_version_map;
    // 先加锁获取需要更新的table_id
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& schema_heart_beat : request->schema_infos()) {
            int64_t table_id = schema_heart_beat.table_id();
            //表已经删除
            auto iter = _table_info_map.find(table_id);
            if (iter == _table_info_map.end()) {
                continue;
            }
            
            if (schema_heart_beat.has_statis_version() && 
                    iter->second.statistics_version > schema_heart_beat.statis_version()) {
                table_version_map[table_id] = iter->second.statistics_version;
            }
        }
    }

    if (table_version_map.empty()) {
        return;
    }

    //统计信息更新，如果需要更新直接从rocksdb读，避免占用内存
    int upd_cnt = 0;
    for (const auto& iter : table_version_map) {
        pb::Statistics stat_pb; 
        int ret = get_statistics(iter.first, stat_pb);
        if (ret < 0) {
            continue;
        }
        if (response->ByteSizeLong() + stat_pb.ByteSizeLong() > FLAGS_statistics_heart_beat_bytesize) {
            DB_WARNING("response size: %lu, statistics size: %lu, big than %ld; count: %d", 
                response->ByteSizeLong(), stat_pb.ByteSizeLong(), FLAGS_statistics_heart_beat_bytesize, upd_cnt);
            break;
        }
        upd_cnt++;
        response->add_statistics()->Swap(&stat_pb);
        DB_WARNING("update statistics, table_id:%ld, version:%ld", iter.first, iter.second);
    }
}

int TableManager::get_statistics(const int64_t table_id, pb::Statistics& stat_pb) {

    std::string stat_value;
    int ret = MetaRocksdb::get_instance()->get_meta_info(construct_statistics_key(table_id), &stat_value);    
    if (ret < 0) {
        DB_WARNING("get statistics info from rocksdb fail, table_id: %ld", table_id);
        return -1;
    } 

    if (!stat_pb.ParseFromString(stat_value)) {
        DB_FATAL("parse statistics failed, table_id: %ld", table_id);
        return -1;
    }

    return 0;
}

void TableManager::check_add_table(
        std::set<int64_t>& report_table_ids, 
        std::vector<int64_t>& new_add_region_ids, 
        const pb::BaikalHeartBeatRequest* request,
        pb::BaikalHeartBeatResponse* response, 
        const std::unordered_set<int64_t>& heartbeat_table_ids) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    bool need_heartbeat_table = request->has_need_heartbeat_table() && request->need_heartbeat_table();
    for (auto& table_info_pair : _table_info_map) {
        if (report_table_ids.find(table_info_pair.first) != report_table_ids.end()) {
            continue;
        }
        //如果是全局二级索引, 没有schema信息
        if (!table_info_pair.second.is_global_index) {
            auto schema_info = response->add_schema_change_info();
            *schema_info = table_info_pair.second.schema_pb;
        }
        if (need_heartbeat_table) {
            const int64_t table_id = table_info_pair.first;
            if (heartbeat_table_ids.find(table_id) == heartbeat_table_ids.end()) {
                continue;
            }
        }
        for (auto& partition_region : table_info_pair.second.partition_regions) {
            for (auto& region_id : partition_region.second) {
                //DB_WARNING("new add region id: %ld", region_id);
                new_add_region_ids.push_back(region_id);
            }
        }
    }
}
void TableManager::check_add_region(
                const std::set<std::int64_t>& report_table_ids,
                std::unordered_map<int64_t, std::set<std::int64_t>>& report_region_ids,
                const pb::BaikalHeartBeatRequest* request,
                pb::BaikalHeartBeatResponse* response,
                const std::unordered_set<int64_t>& heartbeat_table_ids) {
    //获得每个表的regincount
    std::unordered_map<int64_t, int64_t> table_region_count;
    get_region_count(report_table_ids, table_region_count);
    
    bool need_heartbeat_table = request->has_need_heartbeat_table() && request->need_heartbeat_table();
    std::vector<int64_t> table_for_add_region; //需要add_region的table_id
    for (auto& region_ids_pair : report_region_ids) {
        int64_t table_id = region_ids_pair.first;
        if (need_heartbeat_table) {
            if (heartbeat_table_ids.find(table_id) == heartbeat_table_ids.end()) {
                continue;
            }
        }
        if (table_region_count[table_id] <= (int64_t)region_ids_pair.second.size()) {
            continue;
        }
        table_for_add_region.push_back(table_id);
    }

    std::unordered_map<int64_t, std::vector<int64_t>> region_ids;
    get_region_ids(table_for_add_region, region_ids);

    std::vector<int64_t> add_region_ids;
    std::vector<SmartRegionInfo> add_region_infos;
    for (auto& region_id_pair : region_ids) {
        int64_t table_id = region_id_pair.first;
        for (auto& region_id : region_id_pair.second) {
            if (report_region_ids[table_id].find(region_id) == report_region_ids[table_id].end()) {
                add_region_ids.push_back(region_id);
            }
        }
    }
    if (add_region_ids.size() > 0) {
        RegionManager::get_instance()->get_region_info(add_region_ids, add_region_infos);
        for (auto& ptr_region : add_region_infos) {
            *(response->add_region_change_info()) = *ptr_region;
        }
    }
}

int TableManager::load_table_snapshot(const std::string& value) {
    pb::SchemaInfo table_pb;
    if (!table_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load table snapshot, key: %s", value.c_str());
        return -1;
    }
    DB_WARNING("table snapshot:%s, size:%lu", table_pb.ShortDebugString().c_str(), value.size());
    TableMem table_mem;
    table_mem.schema_pb = table_pb;
    table_mem.whether_level_table = table_pb.has_upper_table_name();
    table_mem.main_table_id = table_pb.table_id();
    table_mem.global_index_id = table_pb.table_id();
    for (auto& learner_resource : table_pb.learner_resource_tags()) {
        table_mem.learner_resource_tag.emplace_back(learner_resource);
    }
    if (table_pb.has_partition_info()) {
        table_mem.is_partition = true;
    }
    if (table_pb.engine() == pb::BINLOG) {
        table_mem.is_binlog = true;
        auto call_func = [](TableSchedulingInfo& infos, int64_t table_id) -> int {
            infos.binlog_table_ids.insert(table_id);
            return 1;
        };
        _table_scheduling_infos.Modify(call_func, table_pb.table_id());
    }
    if (table_pb.has_binlog_info()) {
        auto& binlog_info = table_pb.binlog_info();
        if (binlog_info.has_binlog_table_id()) {
            table_mem.is_linked = true;
            table_mem.binlog_ids.insert(binlog_info.binlog_table_id());
        }
        for (auto target_id : binlog_info.target_table_ids()) {
            table_mem.binlog_target_ids.insert(target_id);
        }
    }
    if (table_pb.binlog_infos_size() > 0) {
        for (int i = 0; i < table_pb.binlog_infos_size(); i++) {
            auto& binlog_info = table_pb.binlog_infos(i);
            if (binlog_info.has_binlog_table_id()) {
                table_mem.is_linked = true;
                table_mem.binlog_ids.insert(binlog_info.binlog_table_id());
            }
            for (auto target_id : binlog_info.target_table_ids()) {
                table_mem.binlog_target_ids.insert(target_id);
            }
        }
    }
    for (auto& field : table_pb.fields()) {
        if (!field.has_deleted() || !field.deleted()) {
            table_mem.field_id_map[field.field_name()] = field.field_id();
        }
    }
    for (auto& index : table_pb.indexs()) {
        table_mem.index_id_map[index.index_name()] = index.index_id();
        if (index.hint_status() == pb::IHS_VIRTUAL) {
            _just_add_virtual_index_info.insert(index.index_id());
        }
    }
    if (table_pb.deleted()) {
        //on_snapshot_load中不用加锁
        _table_tombstone_map[table_pb.table_id()] = table_mem;
    } else {
        set_table_info(table_mem); 
        DatabaseManager::get_instance()->add_table_id(table_pb.database_id(), table_pb.table_id());
    }
    if (table_pb.has_schema_conf()
        && table_pb.schema_conf().has_pk_prefix_balance()
        && table_pb.schema_conf().pk_prefix_balance() > 0) {
        auto call_func = [](TableSchedulingInfo& infos, int64_t table_id, int32_t dimension) -> int {
            infos.table_pk_prefix_dimension[table_id] = dimension;
            return 1;
        };
        _table_scheduling_infos.Modify(call_func, table_pb.table_id(), table_pb.schema_conf().pk_prefix_balance());
    }
    if (table_pb.has_schema_conf()
        && table_pb.schema_conf().in_fast_import()
        && !table_pb.deleted()) {
        auto call_func = [](TableSchedulingInfo &infos, int64_t table_id, const std::string &resource_tag) -> int {
            infos.table_in_fast_importer[table_id] = resource_tag;
            return 1;
        };
        _table_scheduling_infos.Modify(call_func, table_pb.table_id(), table_pb.resource_tag());
    }
    return 0;
}

int TableManager::load_ddl_snapshot(const std::string& value) {
    pb::DdlWorkInfo work_info_pb;
    if (!work_info_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load ddl snapshot, key: %s", value.c_str());
        return -1;
    }
    DDLManager::get_instance()->load_table_ddl_snapshot(work_info_pb);
    return 0;
}

int TableManager::load_statistics_snapshot(const std::string& value) {
    pb::Statistics stat_pb;
    if (!stat_pb.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when load statistics snapshot, key: %s", value.c_str());
        return -1;
    }
    DB_WARNING("statistics snapshot, tbale_id:%ld, version:%ld", stat_pb.table_id(), stat_pb.version());
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_table_info_map.count(stat_pb.table_id()) < 1) {
            DB_FATAL("cant find table id:%ld", stat_pb.table_id());
            return 0;
        }
        _table_info_map[stat_pb.table_id()].statistics_version = stat_pb.version();
    }
    return 0;
}

int TableManager::write_schema_for_not_level(TableMem& table_mem, 
                                              braft::Closure* done, 
                                              int64_t max_table_id_tmp,
                                              bool has_auto_increment) {
    //如果创建成功，则不需要做任何操作
    //如果失败，则需要报错，手工调用删除table的接口
    std::vector<std::string> rocksdb_keys;
    std::vector<std::string> rocksdb_values;
   
    std::string max_table_id_value;
    max_table_id_value.append((char*)&max_table_id_tmp, sizeof(int64_t));
    rocksdb_keys.push_back(construct_max_table_id_key());
    rocksdb_values.push_back(max_table_id_value);

    //持久化region_info
    //与store交互
    //准备partition_num个数的regionInfo
    int64_t tmp_max_region_id = RegionManager::get_instance()->get_max_region_id();
    int64_t start_region_id = tmp_max_region_id + 1;
   
    std::shared_ptr<std::vector<pb::InitRegion>> init_regions(new std::vector<pb::InitRegion>{});
    init_regions->reserve(table_mem.schema_pb.init_store_size());
    pb::SchemaInfo simple_table_info = table_mem.schema_pb;
    int64_t main_table_id = simple_table_info.table_id();
    simple_table_info.clear_init_store();
    simple_table_info.clear_split_keys();
    //全局索引和主键索引需要建region
    std::unordered_map<std::string, int64_t> global_index;
    for (auto& index : table_mem.schema_pb.indexs()) {
        if (index.index_type() == pb::I_PRIMARY || index.is_global()) {
            DB_WARNING("index_name: %s is global", index.index_name().c_str());
            global_index[index.index_name()] = index.index_id();
        }
    }

    // Range Partition
    std::string main_logical_room;
    get_main_logical_room(table_mem.schema_pb, main_logical_room);
    std::string resource_tag = table_mem.schema_pb.resource_tag();
    boost::trim(resource_tag);
    if (table_mem.schema_pb.has_partition_info() && table_mem.schema_pb.partition_info().type() == pb::PT_RANGE) {
        if (table_mem.schema_pb.partition_num() != table_mem.schema_pb.partition_info().range_partition_infos_size()) {
            DB_WARNING("Invalid partition_num %ld or range_partition_infos_size: %d",
                        table_mem.schema_pb.partition_num(),
                        (int)table_mem.schema_pb.partition_info().range_partition_infos_size());
            return -1;
        }
    }

    //有split_key的索引先处理
    std::vector<std::string> processed_index_name;
    for (auto i = 0; 
        i < table_mem.schema_pb.partition_num() && is_create_table_support_engine(table_mem.schema_pb.engine()); 
        ++i) {
        int64_t partition_id = i;
        std::string partition_resource_tag = resource_tag;
        std::string partition_main_logical_room = main_logical_room;
        if (table_mem.schema_pb.has_partition_info() && table_mem.schema_pb.partition_info().type() == pb::PT_RANGE) {
            if (table_mem.schema_pb.partition_info().range_partition_infos(i).has_resource_tag()) {
                partition_resource_tag = table_mem.schema_pb.partition_info().range_partition_infos(i).resource_tag();
                partition_main_logical_room = "";
            }
            if (table_mem.schema_pb.partition_info().range_partition_infos(i).has_partition_id()) {
                partition_id = table_mem.schema_pb.partition_info().range_partition_infos(i).partition_id();
            }
        }
        for (auto& split_key : table_mem.schema_pb.split_keys()) {
            std::string index_name = split_key.index_name();
            for (auto j = 0; j <= split_key.split_keys_size(); ++j) {
                std::string instance;
                int ret = ClusterManager::get_instance()->select_instance_rolling(
                                    {partition_resource_tag, partition_main_logical_room, ""}, {}, instance);
                if (ret < 0) {
                    DB_WARNING("select instance fail");
                    return -1;
                }
                pb::InitRegion init_region_request;
                pb::RegionInfo* region_info = init_region_request.mutable_region_info();
                region_info->set_region_id(++tmp_max_region_id);
                region_info->set_table_id(global_index[index_name]);
                processed_index_name.push_back(index_name);
                DB_NOTICE("set table id %ld", global_index[index_name]);
                region_info->set_main_table_id(main_table_id);
                region_info->set_table_name(table_mem.schema_pb.table_name());
                construct_common_region(region_info, table_mem.schema_pb.replica_num());
                region_info->set_partition_id(partition_id);
                region_info->add_peers(instance);
                region_info->set_leader(instance);
                region_info->set_can_add_peer(false); // 简化理解，让raft addpeer必须发送snapshot
                // region_info->set_partition_num(table_mem.schema_pb.partition_num()); // 当前系统未使用region partition_num，range分区不易维护
                region_info->set_is_binlog_region(table_mem.is_binlog);
                if (j != 0) {
                    region_info->set_start_key(split_key.split_keys(j-1));        
                }
                if (j < split_key.split_keys_size()) {
                    region_info->set_end_key(split_key.split_keys(j));
                }
                *(init_region_request.mutable_schema_info()) = simple_table_info;
                init_region_request.set_snapshot_times(2);
                init_regions->push_back(init_region_request);
            }
        }
    }
    for (const auto& index_name : processed_index_name) {
        global_index.erase(index_name);
    }
    //没有指定split_key的索引
    for (auto i = 0;
         i < table_mem.schema_pb.partition_num() && is_create_table_support_engine(table_mem.schema_pb.engine()); 
         ++i) {
        int64_t partition_id = i;
        std::string partition_resource_tag = resource_tag;
        std::string partition_main_logical_room = main_logical_room;
        if (table_mem.schema_pb.has_partition_info() && table_mem.schema_pb.partition_info().type() == pb::PT_RANGE) {
            if (table_mem.schema_pb.partition_info().range_partition_infos(i).has_resource_tag()) {
                partition_resource_tag = table_mem.schema_pb.partition_info().range_partition_infos(i).resource_tag();
                partition_main_logical_room = "";
            }
            if (table_mem.schema_pb.partition_info().range_partition_infos(i).has_partition_id()) {
                partition_id = table_mem.schema_pb.partition_info().range_partition_infos(i).partition_id();
            }
        }
        for (auto& index : global_index) {
            std::string instance;
            int ret = ClusterManager::get_instance()->select_instance_rolling(
                                {partition_resource_tag, partition_main_logical_room, ""}, {}, instance);
            if (ret < 0) {
                DB_WARNING("select instance fail");
                return -1;
            }
            pb::InitRegion init_region_request;
            pb::RegionInfo* region_info = init_region_request.mutable_region_info();
            region_info->set_region_id(++tmp_max_region_id);
            region_info->set_table_id(index.second);
            region_info->set_main_table_id(main_table_id);
            region_info->set_table_name(table_mem.schema_pb.table_name());
            construct_common_region(region_info, table_mem.schema_pb.replica_num());
            region_info->set_partition_id(partition_id);
            region_info->add_peers(instance);
            region_info->set_leader(instance);
            region_info->set_can_add_peer(false);// 简化理解，让raft addpeer必须发送snapshot
            // region_info->set_partition_num(table_mem.schema_pb.partition_num()); // 当前系统未使用region partition_num，range分区不易维护
            region_info->set_is_binlog_region(table_mem.is_binlog);
            *(init_region_request.mutable_schema_info()) = simple_table_info;
            init_region_request.set_snapshot_times(2);
            init_regions->push_back(init_region_request);
            DB_WARNING("init_region_request: %s", init_region_request.ShortDebugString().c_str());
        }
    }
    //持久化region_id
    std::string max_region_id_key = RegionManager::get_instance()->construct_max_region_id_key();
    std::string max_region_id_value;
    max_region_id_value.append((char*)&tmp_max_region_id, sizeof(int64_t));
    rocksdb_keys.push_back(max_region_id_key);
    rocksdb_values.push_back(max_region_id_value);

    //持久化schema_info
    int64_t table_id = table_mem.schema_pb.table_id();
    std::string table_value;
    if (!simple_table_info.SerializeToString(&table_value)) {
        DB_WARNING("request serializeToArray fail when create not level table, request:%s",
                    simple_table_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return -1;
    }
    rocksdb_keys.push_back(construct_table_key(table_id));
    rocksdb_values.push_back(table_value);
    
    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
    if (ret < 0) {                                                                        
        DB_WARNING("add new not level table:%s to rocksdb fail",
                        simple_table_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return -1;
    }
    RegionManager::get_instance()->set_max_region_id(tmp_max_region_id);
    if (done) {
        ((MetaServerClosure*)done)->create_table_schema_pb.CopyFrom(table_mem.schema_pb);
        ((MetaServerClosure*)done)->init_regions = init_regions;
        ((MetaServerClosure*)done)->has_auto_increment = has_auto_increment;
        ((MetaServerClosure*)done)->start_region_id = start_region_id;
    }

    if (init_regions->size() > FLAGS_pre_split_threashold) {
        uint64_t init_value = 1;
        if (table_mem.schema_pb.has_auto_increment_increment()) {
            init_value = table_mem.schema_pb.auto_increment_increment();
        }
        
        //leader发送请求
        if (done && is_create_table_support_engine(table_mem.schema_pb.engine())) {
            std::string namespace_name = table_mem.schema_pb.namespace_name();
            std::string database = table_mem.schema_pb.database();
            std::string table_name = table_mem.schema_pb.table_name();
            Bthread bth(&BTHREAD_ATTR_SMALL);
            auto create_table_fun = 
                [this, namespace_name, database, table_name, init_regions, 
                    table_id, init_value, has_auto_increment]() {
                    int ret = 0;
                    if (has_auto_increment) {
                        pb::MetaManagerRequest request;
                        request.set_op_type(pb::OP_ADD_ID_FOR_AUTO_INCREMENT);
                        pb::AutoIncrementRequest* auto_incr = request.mutable_auto_increment();
                        auto_incr->set_table_id(table_id);
                        auto_incr->set_start_id(init_value);
                        ret = send_auto_increment_request(request);
                    }
                    if (ret == 0) {
                        send_create_table_request(namespace_name, database, table_name, init_regions);
                    } else {
                        send_drop_table_request(namespace_name, database, table_name);
                        DB_FATAL("send add auto incrment request fail, table_name: %s", table_name.c_str());
                    }
                };
            bth.run(create_table_fun);
        }
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        DB_WARNING("create table, table_id:%ld, table_name:%s, max_table_id: %ld"
                    " alloc start_region_id:%ld, end_region_id :%ld", 
                    table_mem.schema_pb.table_id(), table_mem.schema_pb.table_name().c_str(), 
                    max_table_id_tmp,
                    start_region_id, 
                    RegionManager::get_instance()->get_max_region_id());
    }
    return 0;
}
int TableManager::send_auto_increment_request(const pb::MetaManagerRequest& request) {
    MetaServerInteract meta_server_interact;
    if (meta_server_interact.init() != 0) {
        DB_FATAL("meta server interact init fail when send auto increment %s", 
                    request.ShortDebugString().c_str());
        return -1;
    }
    pb::MetaManagerResponse response;
    if (meta_server_interact.send_request("meta_manager", request, response) != 0) {
        DB_WARNING("send_auto_increment_request fail, response:%s", 
                    response.ShortDebugString().c_str());
        return -1;
    }
    return 0;
}
int TableManager::send_create_table_request(const std::string& namespace_name,
                               const std::string& database,
                               const std::string& table_name,
                               std::shared_ptr<std::vector<pb::InitRegion>> init_regions) {
    uint64_t log_id = butil::fast_rand();
    //40个线程并发发送
    BthreadCond concurrency_cond(-FLAGS_concurrency_num);
    bool success = true;
    std::string full_table_name = namespace_name + "." + database + "." + table_name;
    for (auto& init_region_request : *init_regions) {
        auto send_init_region = [&init_region_request, &success, &concurrency_cond, log_id, full_table_name] () {
            std::shared_ptr<BthreadCond> auto_decrease(&concurrency_cond, 
                                [](BthreadCond* cond) { cond->decrease_signal();});
            int64_t region_id = init_region_request.region_info().region_id();
            StoreInteract store_interact(init_region_request.region_info().leader().c_str());
            pb::StoreRes res;
            auto ret = store_interact.send_request(log_id, "init_region", init_region_request, res);
            if (ret < 0) {
                DB_FATAL("create table fail, address:%s, region_id: %ld", 
                            init_region_request.region_info().leader().c_str(),
                            region_id);
                success = false;
                return;
            }
            DB_NOTICE("new region_id: %ld success, table_name:%s", region_id, full_table_name.c_str());
        };
        if (!success) {
            break;
        }  
        Bthread bth;
        concurrency_cond.increase();
        concurrency_cond.wait();
        bth.run(send_init_region);
    }
    concurrency_cond.wait(-FLAGS_concurrency_num);
    if (!success) {
        DB_FATAL("create table:%s fail",
                    (namespace_name + "." + database + "." + table_name).c_str());
        send_drop_table_request(namespace_name, database, table_name);
        return -1;
    }
    DB_NOTICE("create table:%s success", 
                    (namespace_name + "." + database + "." + table_name).c_str());
    return 0;
}

int TableManager::write_schema_for_level(const TableMem& table_mem, 
                                          const int64_t apply_index,
                                          braft::Closure* done, 
                                          int64_t max_table_id_tmp,
                                          bool has_auto_increment) {
    if (done && has_auto_increment) {
        int64_t table_id = table_mem.schema_pb.table_id(); 
        uint64_t init_value = 1;
        if (table_mem.schema_pb.has_auto_increment_increment()) {
            init_value = table_mem.schema_pb.auto_increment_increment();
        }
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_ADD_ID_FOR_AUTO_INCREMENT);
        pb::AutoIncrementRequest* auto_incr = request.mutable_auto_increment();
        auto_incr->set_table_id(table_id);
        auto_incr->set_start_id(init_value);
        auto ret = send_auto_increment_request(request);
        if (ret < 0) {
            DB_FATAL("send add auto incrment request fail, table_id: %ld", table_id);
            return -1;
        }
    }
    std::vector<std::string> rocksdb_keys;
    std::vector<std::string> rocksdb_values;

    //持久化表信息
    std::string table_value;
    if (!table_mem.schema_pb.SerializeToString(&table_value)) {
        DB_WARNING("request serializeToArray fail when create table, request:%s",
                        table_mem.schema_pb.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return -1;
    }
    rocksdb_keys.push_back(construct_table_key(table_mem.schema_pb.table_id()));
    rocksdb_values.push_back(table_value);

    //持久化最大table_id
    std::string max_table_id_value;
    max_table_id_value.append((char*)&max_table_id_tmp, sizeof(int64_t));
    rocksdb_keys.push_back(construct_max_table_id_key());
    rocksdb_values.push_back(max_table_id_value);
    
    //更新最顶层表信息
    int64_t top_table_id = table_mem.schema_pb.top_table_id();
    std::string top_table_value;
    pb::SchemaInfo top_table = _table_info_map[top_table_id].schema_pb;
    top_table.add_lower_table_ids(table_mem.schema_pb.table_id());
    top_table.set_version(table_mem.schema_pb.version() + 1);
    if (!top_table.SerializeToString(&top_table_value)) { 
         DB_WARNING("request serializeToArray fail when update upper table, request:%s", 
                     top_table.ShortDebugString().c_str());
         IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
         return -1;
    }
    rocksdb_keys.push_back(construct_table_key(top_table_id));
    rocksdb_values.push_back(top_table_value);
    
    // write date to rocksdb
    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
    if (ret < 0) {                                                                        
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");                  
        return -1;                                                             
    }
    
    //更新顶层表的内存信息
    set_table_pb(top_table);
    std::vector<pb::SchemaInfo> schema_infos{top_table};
    put_incremental_schemainfo(apply_index, schema_infos);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    if (done) {
        auto* response_ptr = ((MetaServerClosure*)done)->response;
        auto* create_table_response = response_ptr->mutable_create_table_response();
        create_table_response->mutable_schema_info()->CopyFrom(top_table);
    }
    return 0;
}


int TableManager::update_schema_for_rocksdb(int64_t table_id,
                                               const pb::SchemaInfo& schema_info,
                                               braft::Closure* done) {
    
    std::string table_value;
    if (!schema_info.SerializeToString(&table_value)) {
        DB_WARNING("request serializeToArray fail when update upper table, request:%s", 
                    schema_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return -1;
    }
    int ret = MetaRocksdb::get_instance()->put_meta_info(construct_table_key(table_id), table_value);    
    if (ret < 0) {
        DB_WARNING("update schema info to rocksdb fail, request：%s", 
                    schema_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return -1;
    }    
    return 0;
}

int TableManager::update_statistics_for_rocksdb(int64_t table_id,
                                               const pb::Statistics& stat_info,
                                               braft::Closure* done) {
    
    std::string stat_value;
    if (!stat_info.SerializeToString(&stat_value)) {
        DB_WARNING("request serializeToArray fail when update upper table, request:%s", 
                    stat_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return -1;
    }
    int ret = MetaRocksdb::get_instance()->put_meta_info(construct_statistics_key(table_id), stat_value);    
    if (ret < 0) {
        DB_WARNING("update statistics info to rocksdb fail, request：%s", 
                    stat_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return -1;
    }    
    return 0;
}

void TableManager::send_drop_table_request(const std::string& namespace_name,
                             const std::string& database,
                             const std::string& table_name) {
    MetaServerInteract meta_server_interact;
    if (meta_server_interact.init() != 0) {
        DB_FATAL("meta server interact init fail when drop table:%s", table_name.c_str());
        return;
    }
    pb::MetaManagerRequest request;
    request.set_op_type(pb::OP_DROP_TABLE);
    pb::SchemaInfo* table_info = request.mutable_table_info();
    table_info->set_table_name(table_name);
    table_info->set_namespace_name(namespace_name);
    table_info->set_database(database);
    pb::MetaManagerResponse response;
    if (meta_server_interact.send_request("meta_manager", request, response) != 0) {
        DB_WARNING("drop table fail, response:%s", response.ShortDebugString().c_str());
        return;
    }
    DB_WARNING("drop table success, namespace:%s, database:%s, table_name:%s",
                namespace_name.c_str(), database.c_str(), table_name.c_str());
}
void TableManager::check_table_exist_for_peer(const pb::StoreHeartBeatRequest* request,
            pb::StoreHeartBeatResponse* response) {
    // TODO:加这么大的锁是否有性能问题
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& peer_info : request->peer_infos()) {
        int64_t global_index_id = peer_info.table_id();
        int64_t main_table_id = peer_info.main_table_id() == 0 ? 
            peer_info.table_id() : peer_info.main_table_id();
        // 通过墓碑来安全gc已删表的region
        auto table_iter = _table_info_map.find(main_table_id);
        if (global_index_id != main_table_id && table_iter != _table_info_map.end()) {
            if (!table_iter->second.exist_global_index(global_index_id)) {
                DB_WARNING("drop global index region %ld", peer_info.region_id());
                response->add_delete_region_ids(peer_info.region_id());
            } 
        }
        if (_table_tombstone_map.find(main_table_id) != _table_tombstone_map.end()) {
            DB_WARNING("table id:%ld has be deleted, drop region_id:%ld not exit, store_address:%s",
                    main_table_id, peer_info.region_id(),
                    request->instance_info().address().c_str());
            response->add_delete_region_ids(peer_info.region_id());
            continue;
        } else if (table_iter != _table_info_map.end()) {
            continue;
        }
        
        // 老逻辑，使用墓碑删除，后续可以删掉这段逻辑
        DB_WARNING("table id:%ld according to region_id:%ld not exit, drop region_id, store_address:%s",
                main_table_id, peer_info.region_id(),
                request->instance_info().address().c_str());
        //为了安全暂时关掉这个删除region的功能，后续稳定再打开，目前先报fatal(todo)
        if (SchemaManager::get_instance()->get_unsafe_decision()) {
            DB_WARNING("store response add delete region according to table id no exist, region_id: %ld", peer_info.region_id());
            response->add_delete_region_ids(peer_info.region_id());
        }
    }
}
int TableManager::check_table_exist(const pb::SchemaInfo& schema_info, 
                                      int64_t& namespace_id,
                                      int64_t& database_id,
                                      int64_t& table_id) { 
    std::string namespace_name = schema_info.namespace_name();
    std::string database_name = namespace_name + "\001" + schema_info.database();
    std::string table_name = database_name + "\001" + schema_info.table_name();
    namespace_id = NamespaceManager::get_instance()->get_namespace_id(namespace_name);
    if (namespace_id == 0) {
        DB_WARNING("namespace not exit, table_name:%s", table_name.c_str());
        return -1;
    }
    database_id = DatabaseManager::get_instance()->get_database_id(database_name);
    if (database_id == 0) {
        DB_WARNING("database not exit, table_name:%s", table_name.c_str());
        return -1;
    }
    table_id = get_table_id(table_name);
    if (table_id == 0) {
        DB_WARNING("table not exit, table_name:%s", table_name.c_str());
        return -1;
    }
    return 0;
}

int TableManager::alloc_field_id(pb::SchemaInfo& table_info, bool& has_auto_increment, TableMem& table_mem) {
    int32_t field_id = 0;
    std::string table_name = table_info.table_name();
    for (auto i = 0; i < table_info.fields_size(); ++i) {
        table_info.mutable_fields(i)->set_field_id(++field_id);
        const std::string& field_name = table_info.fields(i).field_name();
        if (table_mem.field_id_map.count(field_name) == 0) {
            table_mem.field_id_map[field_name] = field_id;
        } else {
            DB_WARNING("table:%s has duplicate field %s", table_name.c_str(), field_name.c_str());
            return -1;
        }
        if (!table_info.fields(i).has_auto_increment() 
                || table_info.fields(i).auto_increment() == false) {
            continue;
        }
        //一个表只能有一个自增列
        if (has_auto_increment == true) {
            DB_WARNING("table:%s has one more auto_increment field, field %s", 
                        table_name.c_str(), field_name.c_str());
            return -1;
        }
        pb::PrimitiveType data_type = table_info.fields(i).mysql_type();
        if (data_type != pb::INT8
                && data_type != pb::INT16
                && data_type != pb::INT32
                && data_type != pb::INT64
                && data_type != pb::UINT8
                && data_type != pb::UINT16
                && data_type != pb::UINT32
                && data_type != pb::UINT64) {
            DB_WARNING("table:%s auto_increment field not interger, field %s", 
                        table_name.c_str(), field_name.c_str());
            return -1;
        }
        if (table_info.fields(i).can_null()) {
            DB_WARNING("table:%s auto_increment field can not null, field %s", 
                        table_name.c_str(), field_name.c_str());
            return -1;
        }
        has_auto_increment = true;
    }
    table_info.set_max_field_id(field_id);
    return 0;
}

int TableManager::alloc_index_id(pb::SchemaInfo& table_info, TableMem& table_mem, int64_t& max_table_id_tmp) {
    bool has_primary_key = false;
    std::string table_name = table_info.table_name();
    //分配index_id， 序列与table_id共享, 必须有primary_key 
    for (auto i = 0; i < table_info.indexs_size(); ++i) {
        std::string index_name = table_info.indexs(i).index_name();
        for (auto j = 0; j < table_info.indexs(i).field_names_size(); ++j) {
            std::string field_name = table_info.indexs(i).field_names(j);
            if (table_mem.field_id_map.find(field_name) == table_mem.field_id_map.end()) {
                DB_WARNING("filed name:%s of index was not exist in table:%s", 
                            field_name.c_str(),
                            table_name.c_str());
                return -1;
            }
            int32_t field_id = table_mem.field_id_map[field_name];
            table_info.mutable_indexs(i)->add_field_ids(field_id);
        }
        if (table_info.indexs(i).index_type() == pb::I_NONE) {
            DB_WARNING("invalid index type: %d", table_info.indexs(i).index_type());
            return -1;
        }

        table_info.mutable_indexs(i)->set_state(pb::IS_PUBLIC);

        if (table_info.indexs(i).index_type() != pb::I_PRIMARY) {
            table_info.mutable_indexs(i)->set_index_id(++max_table_id_tmp);
            table_mem.index_id_map[index_name] = max_table_id_tmp;
            continue;
        } 
        //只能有一个primary key
        if (has_primary_key) {
            DB_WARNING("table:%s has one more primary key", table_name.c_str());
            return -1;
        }
        has_primary_key = true;
        table_info.mutable_indexs(i)->set_index_id(table_info.table_id());
        //有partition的表的主键不能是联合主键
        /*
        if (!table_mem.whether_level_table && table_info.partition_num() != 1) {
            if (table_info.indexs(i).field_names_size() > 1) {
                DB_WARNING("table:%s has partition_num, but not meet our rule", table_name.c_str());
                return -1;
            }
            //而且，带partiton_num的表主键必须是设置了auto_increment属性
            std::string primary_field = table_info.indexs(i).field_names(0);
            for (auto i = 0; i < table_info.fields_size(); ++i) {
                if (table_info.fields(i).field_name() == primary_field
                        && table_info.fields(i).auto_increment() == false) {
                        DB_WARNING("table:%s not auto increment", table_name.c_str());
                        return -1;
                }
            }
        }
        */
        table_mem.index_id_map[index_name] = table_info.table_id();
    }
    if (!has_primary_key) {
        return -1;
    }
    return 0;
}

int64_t TableManager::get_pre_regionid(int64_t table_id, 
                                            const std::string& start_key, int64_t partition) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return -1;
    }
    auto& startkey_regiondesc_map = _table_info_map[table_id].startkey_regiondesc_map[partition];
    if (startkey_regiondesc_map.size() <= 0) {
        DB_WARNING("table_id:%ld map empty", table_id);
        return -1;
    }
    auto iter = startkey_regiondesc_map.lower_bound(start_key);
    if (iter == startkey_regiondesc_map.end()) {
        DB_WARNING("table_id:%ld can`t find region id start_key:%s",
                 table_id, str_to_hex(start_key).c_str()); 
    } else if (iter->first == start_key) {
        DB_WARNING("table_id:%ld start_key:%s exist", table_id, str_to_hex(start_key).c_str());
        return -1;
    }
    
    if (iter == startkey_regiondesc_map.begin()) {
        DB_WARNING("iter is the first");
        return -1;
    }
    --iter;
    DB_WARNING("table_id:%ld start_key:%s region_id:%ld", 
               table_id, str_to_hex(start_key).c_str(), iter->second.region_id);
    return iter->second.region_id;
}

int64_t TableManager::get_startkey_regionid(int64_t table_id, 
                                       const std::string& start_key, int64_t partition) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return -2;
    }
    auto& startkey_regiondesc_map = _table_info_map[table_id].startkey_regiondesc_map[partition];
    if (startkey_regiondesc_map.size() <= 0) {
        DB_WARNING("table_id:%ld map empty", table_id);
        return -2;
    }
    auto iter = startkey_regiondesc_map.find(start_key);
    if (iter == startkey_regiondesc_map.end()) {
        DB_WARNING("table_id:%ld can`t find region id start_key:%s",
                 table_id, str_to_hex(start_key).c_str()); 
        return -1;
    } 
    DB_WARNING("table_id:%ld start_key:%s region_id:%ld", 
               table_id, str_to_hex(start_key).c_str(), iter->second.region_id);    
    return iter->second.region_id;
}

int TableManager::erase_region(int64_t table_id, int64_t region_id, std::string start_key, int64_t partition) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return -1;
    }
    auto& startkey_regiondesc_map = _table_info_map[table_id].startkey_regiondesc_map[partition];
    auto iter = startkey_regiondesc_map.find(start_key);
    if (iter == startkey_regiondesc_map.end()) {
        DB_WARNING("table_id:%ld can`t find region id start_key:%s",
                 table_id, str_to_hex(start_key).c_str()); 
        return -1;
    }
    if (iter->second.region_id != region_id) {
        DB_WARNING("table_id:%ld diff region_id(%ld, %ld)",
                 table_id, iter->second.region_id, region_id); 
        return -1;
    }
    startkey_regiondesc_map.erase(start_key);
    DB_WARNING("table_id:%ld erase region_id:%ld",
               table_id, region_id);
    return 0;
}

int64_t TableManager::get_next_region_id(int64_t table_id, std::string start_key, 
                                        std::string end_key, int64_t partition) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return -1;
    }
    auto& startkey_regiondesc_map = _table_info_map[table_id].startkey_regiondesc_map[partition];
    auto iter = startkey_regiondesc_map.find(start_key);
    if (iter == startkey_regiondesc_map.end()) {
        DB_WARNING("table_id:%ld can`t find region id start_key:%s",
                 table_id, str_to_hex(start_key).c_str()); 
        return -1;
    }
    auto src_iter = iter;
    auto dst_iter = ++iter;
    if (dst_iter == startkey_regiondesc_map.end()) {
        DB_WARNING("table_id:%ld can`t find region id start_key:%s",
                 table_id, str_to_hex(end_key).c_str()); 
        return -1;
    }
    if (dst_iter->first != end_key) {
        DB_WARNING("table_id:%ld start key nonsequence %s vs %s", table_id, 
                 str_to_hex(dst_iter->first).c_str(), str_to_hex(end_key).c_str()); 
        return -1;
    }
    if (src_iter->second.merge_status == MERGE_IDLE
            && dst_iter->second.merge_status == MERGE_IDLE) {
        src_iter->second.merge_status = MERGE_SRC;
        dst_iter->second.merge_status = MERGE_DST;
        DB_WARNING("table_id:%ld merge src region_id:%ld, dst region_id:%ld",
                  table_id, src_iter->second.region_id, dst_iter->second.region_id);
        return dst_iter->second.region_id;
    } else if (src_iter->second.merge_status == MERGE_SRC
            && dst_iter->second.merge_status == MERGE_DST) {
        DB_WARNING("table_id:%ld merge again src region_id:%ld, dst region_id:%ld",
                   table_id, src_iter->second.region_id, dst_iter->second.region_id);
        return dst_iter->second.region_id;
    } else {
        DB_WARNING("table_id:%ld merge get next region fail, src region_id:%ld, "
                   "merge_status:%d; dst region_id:%ld, merge_status:%d",
                   table_id, src_iter->second.region_id, src_iter->second.merge_status,
                   dst_iter->second.region_id, dst_iter->second.merge_status);
        return -1;
    }
}
int TableManager::check_startkey_regionid_map() {
    TimeCost time_cost; 
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto table_info : _table_info_map) {
        int64_t table_id = table_info.first;
        for (const auto& partition_region_map : table_info.second.startkey_regiondesc_map) {
            SmartRegionInfo pre_region;
            bool is_first_region = true;
            auto& startkey_regiondesc_map = partition_region_map.second;
            for (auto iter = startkey_regiondesc_map.begin(); iter != startkey_regiondesc_map.end(); iter++) {
                if (is_first_region == true) {
                    //首个region
                    auto first_region = RegionManager::get_instance()->
                                        get_region_info(iter->second.region_id);
                    if (first_region == nullptr) {
                        DB_FATAL("table_id:%ld, can`t find region_id:%ld start_key:%s, in region info map", 
                                 table_id, iter->second.region_id, str_to_hex(iter->first).c_str());
                        continue;
                    }
                    DB_WARNING("table_id:%ld, first region_id:%ld, version:%ld, key(%s, %s)",
                               table_id, first_region->region_id(), first_region->version(), 
                               str_to_hex(first_region->start_key()).c_str(), 
                               str_to_hex(first_region->end_key()).c_str());
                    pre_region = first_region;
                    is_first_region = false;
                    continue;
                }
                auto cur_region = RegionManager::get_instance()->
                                     get_region_info(iter->second.region_id); 
                if (cur_region == nullptr) {
                    DB_FATAL("table_id:%ld, can`t find region_id:%ld start_key:%s, in region info map", 
                             table_id, iter->second.region_id, str_to_hex(iter->first).c_str());
                    is_first_region = true;
                    continue;
                }
                if (pre_region->end_key() != cur_region->start_key()) {
                    DB_FATAL("table_id:%ld, key nonsequence (region_id, version, "
                             "start_key, end_key) pre vs cur (%ld, %ld, %s, %s) vs "
                             "(%ld, %ld, %s, %s)", table_id, 
                             pre_region->region_id(), pre_region->version(), 
                             str_to_hex(pre_region->start_key()).c_str(), 
                             str_to_hex(pre_region->end_key()).c_str(), 
                             cur_region->region_id(), cur_region->version(), 
                             str_to_hex(cur_region->start_key()).c_str(), 
                             str_to_hex(cur_region->end_key()).c_str());
                    is_first_region = true;
                    continue;
                }
                pre_region = cur_region;
            }   
        }
    }
    DB_WARNING("check finish timecost:%ld", time_cost.get_time());
    return 0;
}
int TableManager::add_startkey_regionid_map(const pb::RegionInfo& region_info) {
    int64_t table_id = region_info.table_id();
    int64_t region_id = region_info.region_id();
    int64_t partition_id = region_info.partition_id();
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return -1;
    }
    if (region_info.start_key() == region_info.end_key() 
            && !region_info.start_key().empty()) {
        DB_WARNING("table_id: %ld, region_id: %ld, start_key: %s is empty",
                   table_id, region_id, str_to_hex(region_info.start_key()).c_str());
        return 0;
    }
    RegionDesc region;
    region.region_id = region_id;
    region.merge_status = MERGE_IDLE;
    auto& key_region_map
        = _table_info_map[table_id].startkey_regiondesc_map;
    if (key_region_map[partition_id].find(region_info.start_key()) == key_region_map[partition_id].end()) {
        key_region_map[partition_id][region_info.start_key()] = region;
    } else {
        int64_t origin_region_id = key_region_map[partition_id][region_info.start_key()].region_id;
        RegionManager* region_manager = RegionManager::get_instance();
        auto origin_region = region_manager->get_region_info(origin_region_id);
        DB_FATAL("table_id:%ld two regions has same start key (%ld, %s, %s) vs (%ld, %s, %s)",
                 table_id, origin_region->region_id(), 
                 str_to_hex(origin_region->start_key()).c_str(),
                 str_to_hex(origin_region->end_key()).c_str(), 
                 region_id, 
                 str_to_hex(region_info.start_key()).c_str(),
                 str_to_hex(region_info.end_key()).c_str());
        return 0;
    }
    return 0;
}

bool TableManager::partition_check_region_when_update(int64_t table_id, 
    std::string min_start_key, 
    std::string max_end_key, std::map<std::string, RegionDesc>& partition_region_map) {
    if (partition_region_map.size() == 0) {
        //首个region
        DB_WARNING("table_id:%ld min_start_key:%s, max_end_key:%s", table_id,
                  str_to_hex(min_start_key).c_str(), str_to_hex(max_end_key).c_str());
        return true;
    }
    auto iter = partition_region_map.find(min_start_key);
    if (iter == partition_region_map.end()) {
        DB_FATAL("table_id:%ld can`t find min_start_key:%s", 
                 table_id, str_to_hex(min_start_key).c_str());
        return false;
    }
    if (!max_end_key.empty()) {
        auto endkey_iter = partition_region_map.find(max_end_key);
        if (endkey_iter == partition_region_map.end()) {
            DB_FATAL("table_id:%ld can`t find max_end_key:%s", 
                     table_id, str_to_hex(max_end_key).c_str());
            return false;
        }
    }
    return true;
}

bool TableManager::check_region_when_update(int64_t table_id, 
                                    std::map<int64_t, std::string>& min_start_key, 
                                    std::map<int64_t, std::string>& max_end_key) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return false;
    }

    for (const auto& start_pid_key : min_start_key) {
        auto partition_id = start_pid_key.first;
        auto& partition_startkey_regiondesc_map = _table_info_map[table_id].startkey_regiondesc_map[partition_id];
        auto max_pid_key = max_end_key.find(partition_id);
        if (max_pid_key == max_end_key.end()) {
            DB_WARNING("not find partition %ld, init", partition_id);
            continue;
        }
        if (!partition_check_region_when_update(table_id, start_pid_key.second, 
            max_pid_key->second, partition_startkey_regiondesc_map)) {
            DB_FATAL("table_id:%ld, min_start_key:%s, max_end_key:%s check fail", 
                     table_id, str_to_hex(start_pid_key.second).c_str(), 
                     str_to_hex(max_pid_key->second).c_str());
            return false;
        }
    }
    return true;
}

void TableManager::update_startkey_regionid_map_old_pb(int64_t table_id, 
                          std::map<int64_t, std::map<std::string, int64_t>>& key_id_map) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return;
    }
    auto& startkey_regiondesc_map = _table_info_map[table_id].startkey_regiondesc_map;
    for (auto& partition_key_id : key_id_map) {
        auto partition = partition_key_id.first;
        for (auto& key_id : partition_key_id.second) {
            RegionDesc region;
            region.region_id = key_id.second;
            region.merge_status = MERGE_IDLE;
            startkey_regiondesc_map[partition][key_id.first] = region;
            DB_WARNING("table_id:%ld, startkey:%s region_id:%ld insert", 
                       table_id, str_to_hex(key_id.first).c_str(), key_id.second);
        }
    }
}
void TableManager::partition_update_startkey_regionid_map(int64_t table_id, std::string min_start_key, 
    std::string max_end_key, 
    std::map<std::string, int64_t>& key_id_map,
    std::map<std::string, RegionDesc>& startkey_regiondesc_map) {

    if (startkey_regiondesc_map.size() == 0) {
        //首个region加入
        for (auto& key_id : key_id_map) {
            RegionDesc region;
            region.region_id = key_id.second;
            region.merge_status = MERGE_IDLE;
            startkey_regiondesc_map[key_id.first] = region;
            DB_WARNING("table_id:%ld, startkey:%s region_id:%ld insert", 
                       table_id, str_to_hex(key_id.first).c_str(), key_id.second);
        }
        return;
    }
    auto iter = startkey_regiondesc_map.find(min_start_key);
    if (iter == startkey_regiondesc_map.end()) {
        DB_FATAL("table_id:%ld can`t find start_key:%s", 
                 table_id, str_to_hex(min_start_key).c_str());
        return;
    }
    int del_count = 0;
    MergeStatus tmp_status = MERGE_IDLE;
    while (iter != startkey_regiondesc_map.end()) {
        if (!max_end_key.empty() && iter->first == max_end_key) {
            break;
        }
        auto delete_iter = iter++;
        DB_WARNING("table_id:%ld startkey:%s region_id:%ld merge_status:%d, erase",
                   table_id, str_to_hex(delete_iter->first).c_str(), 
                   delete_iter->second.region_id, delete_iter->second.merge_status);
        tmp_status = delete_iter->second.merge_status;
        startkey_regiondesc_map.erase(delete_iter->first);
        del_count++;
    }

    // 1个region替换1个时，merge_status不变:改了peers
    if (key_id_map.size() != 1 || del_count != 1) {
        tmp_status = MERGE_IDLE;
    }
    for (auto& key_id : key_id_map) {
        RegionDesc region;
        region.region_id = key_id.second;
        region.merge_status = tmp_status;
        startkey_regiondesc_map[key_id.first] = region;
        DB_WARNING("table_id:%ld, startkey:%s region_id:%ld insert", 
                   table_id, str_to_hex(key_id.first).c_str(), key_id.second);
    }

}
void TableManager::update_startkey_regionid_map(int64_t table_id, std::map<int64_t, std::string>& min_start_key, 
                                  std::map<int64_t, std::string>& max_end_key, 
                                  std::map<int64_t, std::map<std::string, int64_t>>& key_id_map) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return;
    }
    auto& startkey_regiondesc_map = _table_info_map[table_id].startkey_regiondesc_map;

    for (auto& key_id_pair : key_id_map) {
        auto partition_id = key_id_pair.first;
        auto max_end_key_iter = max_end_key.find(partition_id);
        auto min_start_key_iter = min_start_key.find(partition_id);
        if (max_end_key_iter == max_end_key.end() ||
            min_start_key_iter == min_start_key.end()) {                
            DB_WARNING("unknown partition %ld", partition_id);
        } else {
            partition_update_startkey_regionid_map(table_id, min_start_key_iter->second, max_end_key_iter->second, 
                key_id_pair.second, startkey_regiondesc_map[partition_id]);           
        }
    }
}

void TableManager::add_new_region(const pb::RegionInfo& leader_region_info) {
    int64_t table_id  = leader_region_info.table_id();
    int64_t region_id = leader_region_info.region_id();
    int64_t partition_id = leader_region_info.partition_id();
    std::string start_key = leader_region_info.start_key();
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id: %ld not exist", table_id);
        return;
    }
    const pb::SchemaInfo& schema_info = _table_info_map[table_id].schema_pb;
    if (schema_info.has_partition_info() && schema_info.partition_info().type() == pb::PT_RANGE) {
        if (!is_range_partition_exist(schema_info, partition_id)) {
            DB_WARNING("table_id: %ld, partition_id: %ld not exist", table_id, partition_id);
            return;
        }
    }
    _need_apply_raft_table_ids.insert(table_id);
    auto& key_region_map = _table_info_map[table_id].startkey_newregion_map[partition_id];
    auto iter = key_region_map.find(start_key);
    if (iter != key_region_map.end()) {
        auto origin_region_info = iter->second;
        if (region_id != origin_region_info->region_id()) {
            DB_FATAL("two diffrent regions:%ld, %ld has same start_key:%s",
                     region_id, origin_region_info->region_id(),
                     str_to_hex(start_key).c_str());
            return;
        }
        if (leader_region_info.log_index() < origin_region_info->log_index()) {
            DB_WARNING("leader: %s log_index:%ld in heart is less than in "
                       "origin:%ld, region_id:%ld",
                       leader_region_info.leader().c_str(), 
                       leader_region_info.log_index(), 
                       origin_region_info->log_index(),
                       region_id);
            return;
        }
        if (leader_region_info.version() > origin_region_info->version()) {
            if (end_key_compare(leader_region_info.end_key(), origin_region_info->end_key()) > 0) {
                //end_key不可能变大
                DB_FATAL("region_id:%ld, version %ld to %ld, end_key %s to %s",
                         region_id, origin_region_info->version(),
                         leader_region_info.version(), 
                         str_to_hex(origin_region_info->end_key()).c_str(), 
                         str_to_hex(leader_region_info.end_key()).c_str());
                return;
            }
            key_region_map.erase(iter);
            auto ptr_region = std::make_shared<pb::RegionInfo>(leader_region_info);
            key_region_map[start_key] = ptr_region;
            DB_WARNING("region_id:%ld has changed (version, start_key, end_key)"
                       "(%ld, %s, %s) to (%ld, %s, %s)", region_id, 
                       origin_region_info->version(), 
                       str_to_hex(origin_region_info->start_key()).c_str(),
                       str_to_hex(origin_region_info->end_key()).c_str(),
                       leader_region_info.version(), 
                       str_to_hex(leader_region_info.start_key()).c_str(),
                       str_to_hex(leader_region_info.end_key()).c_str());
        }
    } else {
        auto ptr_region = std::make_shared<pb::RegionInfo>(leader_region_info);
        key_region_map[start_key] = ptr_region;
        DB_WARNING("table_id:%ld add new region_id:%ld, key:(%s, %s) version:%ld", 
                  table_id, region_id, str_to_hex(start_key).c_str(), 
                  str_to_hex(leader_region_info.end_key()).c_str(),
                  leader_region_info.version());
    }        
}
void TableManager::add_update_region(const pb::RegionInfo& leader_region_info, bool is_none) {
    int64_t table_id  = leader_region_info.table_id();
    int64_t region_id = leader_region_info.region_id();
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("table_id:%ld not exist", table_id);
        return;
    }
    _need_apply_raft_table_ids.insert(table_id);
    std::map<int64_t, SmartRegionInfo> *id_region_map;
    if (is_none) {
        id_region_map = &_table_info_map[table_id].id_noneregion_map;
    } else {
        id_region_map = &_table_info_map[table_id].id_keyregion_map;
    }
    auto iter = id_region_map->find(region_id);
    if (iter != id_region_map->end()) {
        auto origin_region_info = iter->second;
        if (leader_region_info.log_index() < origin_region_info->log_index()) {
            DB_WARNING("leader: %s log_index:%ld in heart is less than in "
                       "origin:%ld, region_id:%ld",
                       leader_region_info.leader().c_str(), 
                       leader_region_info.log_index(), 
                       origin_region_info->log_index(),
                       region_id);
            return;
        }
        if (leader_region_info.version() > origin_region_info->version()) {
            id_region_map->erase(iter);
            auto ptr_region = std::make_shared<pb::RegionInfo>(leader_region_info);
            id_region_map->insert(std::make_pair(region_id, ptr_region));
            DB_WARNING("table_id:%ld, region_id:%ld has changed (version, start_key, end_key)"
                       "(%ld, %s, %s) to (%ld, %s, %s)", table_id, region_id, 
                       origin_region_info->version(), 
                       str_to_hex(origin_region_info->start_key()).c_str(),
                       str_to_hex(origin_region_info->end_key()).c_str(),
                       leader_region_info.version(), 
                       str_to_hex(leader_region_info.start_key()).c_str(),
                       str_to_hex(leader_region_info.end_key()).c_str());
        }
    } else {
        auto ptr_region = std::make_shared<pb::RegionInfo>(leader_region_info);
        id_region_map->insert(std::make_pair(region_id, ptr_region));
        DB_WARNING("table_id:%ld, region_id:%ld (version, start_key, end_key)"
                   "(%ld, %s, %s)", table_id, region_id, 
                   leader_region_info.version(), 
                   str_to_hex(leader_region_info.start_key()).c_str(),
                   str_to_hex(leader_region_info.end_key()).c_str());
    }        
}
int TableManager::get_merge_regions(int64_t table_id, 
                      std::string new_start_key, std::string origin_start_key, 
                      std::map<int64_t, std::map<std::string, RegionDesc>>& startkey_regiondesc_map,
                      std::map<int64_t, SmartRegionInfo>& id_noneregion_map,
                      std::vector<SmartRegionInfo>& regions, int64_t partition_id) {
    if (new_start_key == origin_start_key) {
        return 0;
    }
    if (new_start_key > origin_start_key) {
        return -1;
    }

    auto& partition_region_map = startkey_regiondesc_map[partition_id];
    for (auto region_iter = partition_region_map.find(new_start_key); 
            region_iter != partition_region_map.end(); region_iter++) {
        if (region_iter->first > origin_start_key) {
            DB_WARNING("table_id:%ld region_id:%ld start_key:%s bigger than end_key:%s",
                    table_id, region_iter->second.region_id, str_to_hex(region_iter->first).c_str(), 
                    str_to_hex(origin_start_key).c_str());
            return -1;
        }
        if (region_iter->first == origin_start_key) {
            return 0;
        }
        int64_t region_id = region_iter->second.region_id;
        auto iter = id_noneregion_map.find(region_id);
        if (iter != id_noneregion_map.end()) {
            regions.push_back(iter->second);
            DB_WARNING("table_id:%ld, find region_id:%ld in id_noneregion_map"
                       "start_key:%s", table_id, region_id,
                       str_to_hex(region_iter->first).c_str());
        } else {
            DB_WARNING("table_id:%ld, can`t find region_id:%ld in id_noneregion_map",
                       table_id, region_id);
            return -1;
        }
    }
    return -1;
}
int TableManager::get_split_regions(int64_t table_id, 
                      std::string new_end_key, std::string origin_end_key, 
                      std::map<std::string, SmartRegionInfo>& key_newregion_map,
                      std::vector<SmartRegionInfo>& regions) {
    if (new_end_key == origin_end_key) {
        return 0;
    }
    if (end_key_compare(new_end_key, origin_end_key) > 0) {
        return -1;
    }
    std::string key = new_end_key;
    for (auto region_iter = key_newregion_map.find(new_end_key);
            region_iter != key_newregion_map.end(); region_iter++) {
        SmartRegionInfo ptr_region = region_iter->second;
        if (key != ptr_region->start_key()) {
            DB_WARNING("table_id:%ld can`t find start_key:%s, in key_region_map", 
                       table_id, str_to_hex(key).c_str());
            return -1;
        }
        DB_WARNING("table_id:%ld, find region_id:%ld in key_region_map"
                   "start_key:%s, end_key:%s", table_id, ptr_region->region_id(),
                   str_to_hex(ptr_region->start_key()).c_str(),
                   str_to_hex(ptr_region->end_key()).c_str());
        regions.push_back(ptr_region);
        if (ptr_region->end_key() == origin_end_key) {
            return 0;
        }
        if (end_key_compare(ptr_region->end_key(), origin_end_key) > 0) {
            DB_FATAL("table_id:%ld region_id:%ld end_key:%s bigger than end_key:%s",
                     table_id, ptr_region->region_id(), 
                     str_to_hex(ptr_region->end_key()).c_str(), 
                     str_to_hex(origin_end_key).c_str());
            return -1;
        }
        key = ptr_region->end_key();
    }
    return -1;
}

int TableManager::get_presplit_regions(int64_t table_id, 
                      std::map<std::string, SmartRegionInfo>& key_newregion_map,
                                    pb::MetaManagerRequest& request) {
    // 保证获取到预分裂的全部Region
    std::string key = "";
    int region_cnt = 0;
    SmartRegionInfo ptr_region = nullptr;
    for (auto region_iter = key_newregion_map.find("");
            region_iter != key_newregion_map.end(); region_iter++) {
        ptr_region = region_iter->second;
        if (key != ptr_region->start_key()) {
            DB_WARNING("table_id:%ld can`t find start_key:%s, in key_region_map", 
                        table_id, str_to_hex(key).c_str());
            return -1;
        }
        region_cnt++;
        pb::RegionInfo* region_info = request.add_region_infos();
        *region_info = *ptr_region;
        if (ptr_region->end_key() == "") {
            break;
        }
        key = ptr_region->end_key();
    }
    DB_WARNING("table_id: %ld, region_cnt: %d", table_id, region_cnt);
    if (ptr_region == nullptr || ptr_region->end_key() != "") {
        return -1;
    }
    if (region_cnt == 0) {
        return -1;
    }
    return 0;
}

void TableManager::get_update_region_requests(int64_t table_id, TableMem& table_info, 
                                std::vector<pb::MetaManagerRequest>& requests) {
    auto& startkey_regiondesc_map  = table_info.startkey_regiondesc_map;
    auto& key_newregion_map = table_info.startkey_newregion_map;
    auto& id_noneregion_map = table_info.id_noneregion_map;
    auto& id_keyregion_map  = table_info.id_keyregion_map;
    //已经没有发生变化的region，startkey_newregion_map和id_noneregion_map可清空
    if (id_keyregion_map.size() == 0) {
        return;
    }
    int ret = 0;
    std::vector<SmartRegionInfo> regions;
    for (auto iter = id_keyregion_map.begin(); iter != id_keyregion_map.end();) {
        auto cur_iter = iter++;
        regions.clear();
        int64_t region_id = cur_iter->first;
        auto ptr_region = cur_iter->second;
        int64_t partition_id = ptr_region->partition_id();
        auto master_region = RegionManager::get_instance()->get_region_info(region_id); 
        if (master_region == nullptr) {
            DB_WARNING("can`t find region_id:%ld in region info map", region_id);
            continue;
        }
        DB_WARNING("table_id:%ld, region_id:%ld key has changed "
                   "(version, start_key, end_key),(%ld, %s, %s)->(%ld, %s, %s)",
                   table_id, region_id, master_region->version(),
                   str_to_hex(master_region->start_key()).c_str(), 
                   str_to_hex(master_region->end_key()).c_str(),
                   ptr_region->version(),
                   str_to_hex(ptr_region->start_key()).c_str(),
                   str_to_hex(ptr_region->end_key()).c_str());
        if (ptr_region->version() <= master_region->version()) {
            DB_WARNING("table_id:%ld, region_id:%ld, version too small need erase",
                    table_id, region_id);
            id_keyregion_map.erase(cur_iter);
            continue;
        }
        if (!ptr_region->end_key().empty() 
                && ptr_region->end_key() < master_region->start_key()) {
            continue;
        }
        // 使用leader region schema替换master region schema时，保留master schema中的learner信息。
        ptr_region->clear_learners();
        for (auto& learner : master_region->learners()) {
            ptr_region->add_learners(learner);
        }

        ret = get_merge_regions(table_id, ptr_region->start_key(), 
                                master_region->start_key(), 
                                startkey_regiondesc_map, id_noneregion_map, regions, partition_id);
        if (ret < 0) {
            DB_WARNING("table_id:%ld, region_id:%ld get merge region failed",
                       table_id, region_id);
            continue;
        }
        regions.push_back(ptr_region);
        ret = get_split_regions(table_id, ptr_region->end_key(), 
                                master_region->end_key(), 
                                key_newregion_map[ptr_region->partition_id()], regions);
        if (ret < 0) {
            DB_WARNING("table_id:%ld, region_id:%ld get split region failed",
                       table_id, region_id);
            continue;
        }
        
        pb::MetaManagerRequest request;
        request.set_op_type(pb::OP_UPDATE_REGION);
        for (auto region : regions) {
            pb::RegionInfo* region_info = request.add_region_infos();
            *region_info = *region;
        }
        requests.push_back(request);
    }
}
void TableManager::recycle_update_region() {
    std::vector<pb::MetaManagerRequest> requests;
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        for (auto& table_info : _table_info_map) {
            auto& key_newregion_map = table_info.second.startkey_newregion_map;
            auto& id_noneregion_map = table_info.second.id_noneregion_map;
            auto& id_keyregion_map  = table_info.second.id_keyregion_map;
            auto& startkey_regiondesc_map  = table_info.second.startkey_regiondesc_map;
            
            for (auto iter = id_keyregion_map.begin(); iter != id_keyregion_map.end(); ) {
                auto cur_iter = iter++;
                int64_t region_id = cur_iter->first;
                auto ptr_region = cur_iter->second;
                auto master_region = RegionManager::get_instance()->get_region_info(region_id); 
                if (master_region == nullptr) {
                    DB_WARNING("can`t find region_id: %ld in region info map", region_id);
                    continue;
                }
                if (ptr_region->version() <= master_region->version()) {
                    id_keyregion_map.erase(cur_iter);
                    DB_WARNING("table_id: %ld, region_id:%ld key has changed "
                               "(version, start_key, end_key),(%ld, %s, %s)->(%ld, %s, %s)",
                               table_info.first, region_id, master_region->version(),
                               str_to_hex(master_region->start_key()).c_str(), 
                               str_to_hex(master_region->end_key()).c_str(),
                               ptr_region->version(),
                               str_to_hex(ptr_region->start_key()).c_str(),
                               str_to_hex(ptr_region->end_key()).c_str());
                    continue;
                }
            }

            for (auto& iter : startkey_regiondesc_map) {
                int64_t partition_key = iter.first;
                if (startkey_regiondesc_map[partition_key].size() == 0 && id_keyregion_map.size() == 0 
                        && key_newregion_map[partition_key].size() != 0 && id_noneregion_map.size() == 0) {
                    //如果该table没有region，但是存在store上报的新region，为预分裂region，特殊处理
                    pb::MetaManagerRequest request;
                    request.set_op_type(pb::OP_UPDATE_REGION);
                    auto ret = get_presplit_regions(table_info.first, key_newregion_map[partition_key], request);
                    if (ret < 0) {
                        continue;
                    }
                    requests.push_back(request);
                    continue;
                }
         
                if (id_keyregion_map.size() == 0) {
                    if (key_newregion_map.size() != 0 || id_noneregion_map.size() != 0) {
                        key_newregion_map[partition_key].clear();
                        id_noneregion_map.clear();
                        DB_WARNING("table_id: %ld partition_key: %ld tmp map clear", table_info.first, partition_key);
                    }
                }
            }
            
        }
    }
    
    for (auto& request : requests) {
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
    } 

}

void TableManager::get_update_regions_apply_raft() {
    //获取可以整体修改的region
    std::vector<pb::MetaManagerRequest> requests;
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        if (_need_apply_raft_table_ids.empty()) {
            return;
        }
        for (auto table_id : _need_apply_raft_table_ids) {
            if (_table_info_map.find(table_id) == _table_info_map.end()) {
                DB_WARNING("table_id: %ld not exist", table_id);
                continue;
            }
            auto& table_info = _table_info_map[table_id];
            auto& id_keyregion_map  = table_info.id_keyregion_map;
            if (id_keyregion_map.size() == 0) {
                continue;
            }

            get_update_region_requests(table_id, table_info, requests);
        }
        _need_apply_raft_table_ids.clear();
    }

    BthreadCond  apply_raft_cond(-40);
    for (auto& request : requests) {
        apply_raft_cond.increase_wait();
        SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, (new ApplyraftClosure(apply_raft_cond)));
    } 
    apply_raft_cond.wait(-40);

    //回收
    recycle_update_region();
}

void TableManager::check_update_region(const pb::LeaderHeartBeat& leader_region,
                         const SmartRegionInfo& master_region_info) {
    const pb::RegionInfo& leader_region_info = leader_region.region();
    if (leader_region_info.start_key() == leader_region_info.end_key()) {
        //空region，加入id_noneregion_map
        add_update_region(leader_region_info, true);
    } else {
        //key范围改变，加入id_keyregion_map
        add_update_region(leader_region_info, false);
    }
}

void TableManager::drop_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    //检查参数有效性
    DB_NOTICE("drop index, request:%s", request.ShortDebugString().c_str());
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (request.table_info().indexs_size() != 1) {
        DB_WARNING("check index info fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "index info fail");
        return;
    }

    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl , request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    
    pb::SchemaInfo& schema_info = _table_info_map[table_id].schema_pb;
    auto index_req = request.table_info().indexs(0);
    auto index_to_del = std::find_if(std::begin(schema_info.indexs()), std::end(schema_info.indexs()), 
        [&index_req](const pb::IndexInfo& info) {
            // 忽略大小写
            return boost::algorithm::iequals(info.index_name(), index_req.index_name()) &&
                (info.index_type() == pb::I_UNIQ || info.index_type() == pb::I_KEY || 
                info.index_type() == pb::I_FULLTEXT);
        });
    if (index_to_del != std::end(schema_info.indexs())) {
        if (index_req.hint_status() == pb::IHS_VIRTUAL || index_to_del->hint_status() == pb::IHS_VIRTUAL) {
            auto& index_name = index_req.index_name();
            auto& database_name = schema_info.database();
            auto& table_name = schema_info.table_name();
            std::string delete_virtual_indx_info = database_name + "," + table_name + "," + index_name;
            {
                //meta内存中虚拟索引影响面记录删除
                BAIDU_SCOPED_LOCK(_load_virtual_to_memory_mutex);
                //将删除的info存入TableManager管理的内存
                DB_NOTICE("DDL_LOG drop_virtual_index_id [%ld], index_name [%s], database_name [%s], table_name[%s]", index_to_del->index_id(), 
                          index_name.c_str(), database_name.c_str(), table_name.c_str());
                _just_add_virtual_index_info.erase(index_to_del->index_id());
                _virtual_index_sql_map.erase(delete_virtual_indx_info);
            }
            drop_virtual_index(request, apply_index, done);
            IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
            return;
        } else {
            int ret = DDLManager::get_instance()->init_del_index_ddlwork(table_id, *index_to_del);
            if (ret != 0) {
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "delete index init error.");
                DB_WARNING("DDL_LOG delete index init error index [%s].", index_to_del->index_name().c_str());
            } else {
                IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
            }
            return;
        }
    } else {
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "index not found");
        DB_WARNING("DDL_LOG drop_index can't find index [%s].", index_req.index_name().c_str());
    }
}

void TableManager::add_index(const pb::MetaManagerRequest& request, 
                             const int64_t apply_index, 
                             braft::Closure* done) {
    int ret = 0;
    DB_DEBUG("DDL_LOG[add_index] add index, request:%s", request.ShortDebugString().c_str());
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0 && 
        request.table_info().table_id() == table_id) {
        DB_WARNING("DDL_LOG[add_index] check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl , request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    //检查field有效性
    if (request.table_info().indexs_size() != 1) {
        DB_WARNING("DDL_LOG[add_index] check index info fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "index info fail");
        return;
    }
    auto&& first_index_fields = request.table_info().indexs(0).field_names();
    auto all_fields_exist = std::all_of(
        std::begin(first_index_fields),
        std::end(first_index_fields),
        [&](const std::string& field_name) -> bool {
            return check_field_exist(field_name, table_id);
        }
    );
    if (!all_fields_exist) {
        DB_WARNING("DDL_LOG[add_index] check fields info fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "fields info fail");
        return;
    }
    DB_DEBUG("DDL_LOG[add_index] check field success.");
    if (_table_info_map.find(table_id) == _table_info_map.end()) {
        DB_WARNING("DDL_LOG[add_index] table not in table_info_map, request:%s", request.DebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not in table_info_map");
        return;
    }

    int64_t index_id;
    int index_ret = check_index(request.table_info().indexs(0), 
        _table_info_map[table_id].schema_pb, index_id);
    
    if (index_ret == -1) {
        DB_WARNING("check index info fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "index info fail");
        return;
    }
    DB_DEBUG("DDL_LOG[add_index] check index info success.");

    if (index_ret == 2) {
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
        return;
    }

    pb::IndexInfo index_info;
    index_info.CopyFrom(request.table_info().indexs(0));
    index_info.set_state(pb::IS_NONE); 
    if (index_ret == 1) {
        index_info.set_index_id(index_id);
    } else {
        int64_t tmp_max_table_id = get_max_table_id();
        index_info.set_index_id(++tmp_max_table_id);
        set_max_table_id(tmp_max_table_id);
        std::string max_table_id_value;
        max_table_id_value.append((char*)&tmp_max_table_id, sizeof(int64_t));
        //RocksDB更新
        ret = MetaRocksdb::get_instance()->put_meta_info(construct_max_table_id_key(), max_table_id_value);    
        if (ret < 0) {
            DB_WARNING("update max_table_id to rocksdb fail.");
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
            return;
        }  
        DB_NOTICE("alloc new index_id[%ld]", tmp_max_table_id);
    }

    for (const auto& field_name : index_info.field_names()) {
        auto field_id_iter = _table_info_map[table_id].field_id_map.find(field_name);
        if (field_id_iter == _table_info_map[table_id].field_id_map.end()) {
            DB_WARNING("field_id not found field_name[%s] in field_id_map.", field_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "get field id error");
            return;
        } else {
            index_info.add_field_ids(field_id_iter->second);
            DB_DEBUG("DDL_LOG add field id[%d] field_name[%s]", field_id_iter->second, field_name.c_str());
        }
    }

    if (request.table_info().indexs(0).hint_status() == pb::IHS_VIRTUAL) {
           BAIDU_SCOPED_LOCK(_load_virtual_to_memory_mutex);
           index_info.set_state(pb::IS_PUBLIC);
           _just_add_virtual_index_info.insert(index_info.index_id());//保存虚拟索引id，后续drop_index的流程中删除相应的id
    } else {
        ret = do_add_index(request, apply_index, done, table_id, index_info);
    } 
    if (ret != 0) {
        DB_WARNING("add global|local index error.");
        return;
    }
    //update schema
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    auto index_iter = mem_schema_pb.mutable_indexs()->begin();
    for (; index_iter != mem_schema_pb.mutable_indexs()->end();) {
        if (index_info.index_id() == index_iter->index_id()) {
            DB_NOTICE("DDL_LOG udpate_index delete index [%ld].", index_iter->index_id());
            mem_schema_pb.mutable_indexs()->erase(index_iter);
        } else {
            index_iter++;
        }
    }
    pb::IndexInfo* add_index = mem_schema_pb.add_indexs();
    add_index->CopyFrom(index_info);
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    if (index_info.index_type() == pb::I_FULLTEXT) {
        // fulltext close kv mode
        auto schema_conf = mem_schema_pb.mutable_schema_conf();
        if (schema_conf->has_storage_compute_separate()) {
            schema_conf->set_storage_compute_separate(false);
        }
    }
    _table_info_map[table_id].index_id_map[add_index->index_name()] = add_index->index_id();
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    
    put_incremental_schemainfo(apply_index, schema_infos);

    ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    DB_DEBUG("DDL_LOG add_index index_info [%s]", add_index->ShortDebugString().c_str());
    DB_NOTICE("DDL_LOG add_index schema_info [%s] apply_index %ld", 
        _table_info_map[table_id].schema_pb.ShortDebugString().c_str(), apply_index);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
}

int TableManager::init_global_index_region(TableMem& table_mem, braft::Closure* done, pb::IndexInfo& index_info) {
    std::vector<std::string> rocksdb_keys;
    std::vector<std::string> rocksdb_values;

    // Range Partition
    std::string main_logical_room;
    if (table_mem.schema_pb.has_main_logical_room()) {
        main_logical_room = table_mem.schema_pb.main_logical_room();
    }
    std::string resource_tag = table_mem.schema_pb.resource_tag();
    boost::trim(resource_tag);

    if (table_mem.schema_pb.has_partition_info() && table_mem.schema_pb.partition_info().type() == pb::PT_RANGE) {
        if (table_mem.schema_pb.partition_num() != table_mem.schema_pb.partition_info().range_partition_infos_size()) {
            DB_WARNING("Invalid partition_num %ld or range_partition_infos_size: %d",
                        table_mem.schema_pb.partition_num(),
                        (int)table_mem.schema_pb.partition_info().range_partition_infos_size());
            return -1;
        }
    }

    //持久化region_info
    //与store交互
    //准备partition_num个数的regionInfo
    int64_t tmp_max_region_id = RegionManager::get_instance()->get_max_region_id();
   
    std::shared_ptr<std::vector<pb::InitRegion>> init_regions(new std::vector<pb::InitRegion>{});
    pb::SchemaInfo simple_table_info = table_mem.schema_pb;
    //没有指定split_key的索引
    for (auto i = 0; 
         i < table_mem.schema_pb.partition_num() && is_create_table_support_engine(table_mem.schema_pb.engine()); 
         ++i) {
        int64_t partition_id = i;
        std::string partition_resource_tag = resource_tag;
        std::string partition_main_logical_room = main_logical_room;
        if (table_mem.schema_pb.has_partition_info() && table_mem.schema_pb.partition_info().type() == pb::PT_RANGE) {
            if (table_mem.schema_pb.partition_info().range_partition_infos(i).has_resource_tag()) {
                partition_resource_tag = table_mem.schema_pb.partition_info().range_partition_infos(i).resource_tag();
                partition_main_logical_room = "";
            }
            if (table_mem.schema_pb.partition_info().range_partition_infos(i).has_partition_id()) {
                partition_id = table_mem.schema_pb.partition_info().range_partition_infos(i).partition_id();
            }
        }
        std::string instance;
        int ret = ClusterManager::get_instance()->select_instance_rolling(
                            {partition_resource_tag, partition_main_logical_room, ""}, {}, instance);
        if (ret < 0) {
            DB_WARNING("select instance fail");
            return -1;
        }
        pb::InitRegion init_region_request;
        pb::RegionInfo* region_info = init_region_request.mutable_region_info();
        region_info->set_region_id(++tmp_max_region_id);
        region_info->set_table_id(index_info.index_id());
        region_info->set_main_table_id(table_mem.main_table_id);
        region_info->set_table_name(table_mem.schema_pb.table_name());
        construct_common_region(region_info, table_mem.schema_pb.replica_num());
        region_info->set_partition_id(partition_id);
        region_info->add_peers(instance);
        region_info->set_leader(instance);
        region_info->set_can_add_peer(false);// 简化理解，让raft addpeer必须发送snapshot
        // region_info->set_partition_num(table_mem.schema_pb.partition_num()); // 当前系统未使用region partition_num，range分区不易维护
        region_info->set_is_binlog_region(table_mem.is_binlog);
        *(init_region_request.mutable_schema_info()) = simple_table_info;
        init_region_request.set_snapshot_times(2);
        init_regions->emplace_back(init_region_request);
        DB_WARNING("init_region_request: %s", init_region_request.DebugString().c_str());
    }
    //持久化region_id
    std::string max_region_id_key = RegionManager::get_instance()->construct_max_region_id_key();
    std::string max_region_id_value;
    max_region_id_value.append((char*)&tmp_max_region_id, sizeof(int64_t));
    rocksdb_keys.emplace_back(max_region_id_key);
    rocksdb_values.emplace_back(max_region_id_value);

    //持久化schema_info
    std::string table_value;
    if (!simple_table_info.SerializeToString(&table_value)) {
        DB_WARNING("request serializeToArray fail when create not level table, request:%s",
                    simple_table_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "serializeToArray fail");
        return -1;
    }
    
    int ret = MetaRocksdb::get_instance()->put_meta_info(rocksdb_keys, rocksdb_values);
    if (ret < 0) {
        DB_WARNING("add new not level table:%s to rocksdb fail",
                        simple_table_info.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return -1;
    }
    RegionManager::get_instance()->set_max_region_id(tmp_max_region_id);
    //leader发送请求
    if (done && is_create_table_support_engine(table_mem.schema_pb.engine())) {
        std::string namespace_name = table_mem.schema_pb.namespace_name();
        std::string database = table_mem.schema_pb.database();
        std::string table_name = table_mem.schema_pb.table_name();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        auto create_table_fun = 
            [this, namespace_name, database, table_name, init_regions]() {
                send_create_table_request(namespace_name, database, table_name, init_regions);
            };
        bth.run(create_table_fun);
    }
    return 0;
}

int TableManager::do_add_index(const pb::MetaManagerRequest& request, 
                             const int64_t apply_index, 
                             braft::Closure* done, const int64_t table_id, pb::IndexInfo& index_info) {
    auto& table_mem =  _table_info_map[table_id];
    int64_t start_region_id = RegionManager::get_instance()->get_max_region_id();
    if (index_info.is_global() && init_global_index_region(table_mem, done, index_info) != 0) {
        DB_WARNING("table_id[%ld] add global index init global region failed.", table_id);
        if (done) {
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "init global region failed");
        }
        return -1;
    }

    int ret = DDLManager::get_instance()->init_index_ddlwork(table_id, index_info, table_mem.partition_regions);
    if (ret < 0) {
        DB_WARNING("table_id[%ld] add index init ddlwork failed.", table_id);
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "init index ddlwork failed");
        return -1;
    }
    if (done) {
        IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    }
    DB_WARNING("create index, table_id:%ld, table_name:%s, "
                " alloc start_region_id:%ld, end_region_id :%ld", 
                table_mem.schema_pb.table_id(), table_mem.schema_pb.table_name().c_str(), 
                start_region_id + 1, 
                RegionManager::get_instance()->get_max_region_id());
    return 0;
}

bool TableManager::check_field_exist(const std::string &field_name,
                        int64_t table_id) {
    auto table_mem_iter = _table_info_map.find(table_id);
    if (table_mem_iter == _table_info_map.end()) {
        DB_WARNING("table_id:[%ld] not exist.", table_id);
        return false;
    }
    auto &&table_mem = table_mem_iter->second;
    if (table_mem.field_id_map.find(field_name) != table_mem.field_id_map.end()) {
        return true;
    }
    return false;
}

int TableManager::check_index(const pb::IndexInfo& index_info_to_check,
                   const pb::SchemaInfo& schema_info, int64_t& index_id) {
    
    /*
    for (const auto& index_info : schema_info.indexs()) {
        if (index_info.storage_type() != index_info_to_check.storage_type()) {
            DB_WARNING("diff fulltext index type.");
            return -1;
        }
    }
    */
    auto same_index = [](const pb::IndexInfo& index_l, const pb::IndexInfo& index_r) -> bool{
        if (index_l.field_names_size() != index_r.field_names_size()) {
            return false;
        }
        for (auto field_index = 0; field_index < index_l.field_names_size(); ++field_index) {
            if (index_l.field_names(field_index) != index_r.field_names(field_index)) {
                   return false;
            }
        }
        return true;
    };

    for (const auto& index_info : schema_info.indexs()) {
        if (try_to_lower(index_info.index_name()) == try_to_lower(index_info_to_check.index_name())) {
            //索引状态为NONE、IS_DELETE_ONLY并且索引的field一致，可以重建。
            if (index_info.state() == pb::IS_NONE || index_info.state() == pb::IS_DELETE_ONLY) {
                if (same_index(index_info, index_info_to_check)) {
                    index_id = index_info.index_id();
                    DB_NOTICE("DDL_LOG rebuild index[%ld]", index_id);
                    return 1;
                } else {
                    DB_WARNING("DDL_LOG same index name, diff fields.");
                    return -1;
                }
            } else {
                if (same_index(index_info, index_info_to_check)) {
                    DB_WARNING("DDL_LOG same index name, same fields.");
                    return 2;
                }
                DB_WARNING("DDL_LOG rebuild index failed, index state not satisfy.");
                return -1;
            }
        } else {
            /*
            if (same_index(index_info, index_info_to_check)) {
               DB_WARNING("DDL_LOG diff index name, same fields.");
                return -1;
            }
            */
        }
    }
    return 0;
}

void TableManager::update_index_status(const pb::MetaManagerRequest& request,
                                       const int64_t apply_index,
                                       braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            auto&& request_index_info = request.ddlwork_info();
            auto index_iter = mem_schema_pb.mutable_indexs()->begin();
            for (; index_iter != mem_schema_pb.mutable_indexs()->end(); index_iter++) {
                if (request_index_info.index_id() == index_iter->index_id()) {
                    if (request_index_info.job_state() != pb::IS_DELETE_LOCAL && request_index_info.deleted()) {
                        //删除索引
                        DB_NOTICE("DDL_LOG udpate_index_status delete index [%s].", request_index_info.ShortDebugString().c_str());
                        update_op_version(mem_schema_pb.mutable_schema_conf(), "drop index " + index_iter->index_name());
                        mem_schema_pb.mutable_indexs()->erase(index_iter);
                    } else {
                        //改变索引状态
                        DB_NOTICE("DDL_LOG set state index state to [%s]", request_index_info.ShortDebugString().c_str());
                        index_iter->set_state(request_index_info.job_state());
                        if (request_index_info.op_type() == pb::OP_DROP_INDEX && index_iter->hint_status() == pb::IHS_NORMAL) {
                            index_iter->set_hint_status(pb::IHS_DISABLE);
                        }
                        if (request_index_info.job_state() == pb::IS_DELETE_LOCAL 
                            && request_index_info.status() == pb::DdlWorkDone) {
                            // 局部索引保留IS_DELETE_LOCAL一段时间，以便store真正删除数据
                            int64_t due_time = butil::gettimeofday_us() + FLAGS_table_tombstone_gc_time_s * 1000 * 1000LL;
                            index_iter->set_drop_timestamp(due_time);
                        }
                        if (request_index_info.job_state() == pb::IS_PUBLIC) {
                            update_op_version(mem_schema_pb.mutable_schema_conf(), "add index " + index_iter->index_name());
                        }
                    }
                    break;
                }
            }
            mem_schema_pb.set_version(mem_schema_pb.version() + 1);
        }); 
}

void TableManager::delete_ddlwork(const pb::MetaManagerRequest& request, braft::Closure* done) {
    DB_NOTICE("delete ddlwork %s is_global[%d]", request.ShortDebugString().c_str(), request.ddlwork_info().global());
    DDLManager::get_instance()->delete_ddlwork(request, done);
}

void TableManager::link_binlog(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    DB_DEBUG("link binlog, request:%s", request.ShortDebugString().c_str());
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (!request.has_binlog_info()) {
        DB_WARNING("check binlog info fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "no binlog info");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl , request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    int64_t binlog_table_id;
    if (check_table_exist(request.binlog_info(), binlog_table_id) != 0) {
        DB_WARNING("check binlog table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "binlog table not exist");
        return;
    }
    if (_table_info_map.find(table_id) == _table_info_map.end() ||
        _table_info_map.find(binlog_table_id) == _table_info_map.end()) {
        DB_WARNING("table not in table_info_map, request:%s", request.DebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not in table_info_map");
        return;
    }
    //check内存，更新内存
    auto& table_mem =  _table_info_map[table_id];
    auto& binlog_table_mem =  _table_info_map[binlog_table_id];
    if (table_mem.binlog_ids.count(binlog_table_id) > 0) {
        DB_WARNING("table already linked, request:%s", request.DebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table already linked");
        return;
    }
    if (!binlog_table_mem.is_binlog) {
        DB_WARNING("table is not binlog, request:%s", request.DebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is not binlog");
        return;
    }
    // 验证普通表使用的分区字段
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    bool get_field_info = false;
    bool partition_is_same_hint = true; // 默认分区方式一样
    pb::FieldInfo link_field;
    if (binlog_table_mem.is_partition) {
        if (mem_schema_pb.has_partition_info() && mem_schema_pb.partition_info().type() == pb::PT_RANGE) {
            DB_WARNING("Range partition table not support partition binlog, request:%s", request.DebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "Range partition table not support partition binlog");
            return;
        }
        if (request.table_info().has_link_field()) {
            for (const auto& field_info : mem_schema_pb.fields()) {
                if (field_info.field_name() == request.table_info().link_field().field_name()) {
                    link_field = field_info;
                    get_field_info = true;
                    break;
                }
            }
            if (!get_field_info) {
                DB_WARNING("link field info error, request:%s", request.DebugString().c_str());
                IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "link field info error");
                return;
            }
        } else {
            DB_WARNING("table no link field info, request:%s", request.DebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "no link field info");
            return;
        }
        if (request.table_info().has_partition_is_same_hint()) {
            partition_is_same_hint = request.table_info().partition_is_same_hint();
        } else {
            DB_WARNING("table no set partition_is_same_hint, request:%s", request.DebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "no set partition_is_same_hint");
            return;
        }
    }
    DB_NOTICE("link binlog tableid[%ld] binlog_table_id[%ld]", table_id, binlog_table_id);
    table_mem.is_linked = true;
    table_mem.binlog_ids.insert(binlog_table_id);
    binlog_table_mem.binlog_target_ids.insert(table_id);
    if (mem_schema_pb.has_binlog_info() && mem_schema_pb.binlog_info().has_binlog_table_id()) {
        auto binlog_info = mem_schema_pb.add_binlog_infos();
        binlog_info->set_binlog_table_id(binlog_table_id);
        if (get_field_info) {
            binlog_info->mutable_link_field()->CopyFrom(link_field);
        }
        binlog_info->set_partition_is_same_hint(partition_is_same_hint);
    } else {
        auto binlog_info = mem_schema_pb.mutable_binlog_info();
        binlog_info->set_binlog_table_id(binlog_table_id);
        if (get_field_info) {
            mem_schema_pb.mutable_link_field()->CopyFrom(link_field);
        }
        mem_schema_pb.set_partition_is_same_hint(partition_is_same_hint);
    }
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};

    pb::SchemaInfo binlog_mem_schema_pb =  _table_info_map[binlog_table_id].schema_pb;
    auto binlog_binlog_info = binlog_mem_schema_pb.mutable_binlog_info();
    binlog_binlog_info->add_target_table_ids(table_id);

    binlog_mem_schema_pb.set_version(binlog_mem_schema_pb.version() + 1);
    set_table_pb(binlog_mem_schema_pb);
    schema_infos.push_back(binlog_mem_schema_pb);

    put_incremental_schemainfo(apply_index, schema_infos);

    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    ret = update_schema_for_rocksdb(binlog_table_id, binlog_mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
}

void TableManager::unlink_binlog(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    DB_DEBUG("link binlog, request:%s", request.ShortDebugString().c_str());
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    if (!request.has_binlog_info()) {
        DB_WARNING("check binlog info fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "no binlog info");
        return;
    }
    if (check_table_has_ddlwork(table_id)) {
        DB_WARNING("table is doing ddl , request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is doing ddl");
        return;
    }
    int64_t binlog_table_id;
    if (check_table_exist(request.binlog_info(), binlog_table_id) != 0) {
        DB_WARNING("check binlog table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "binlog table not exist");
        return;
    }
    if (_table_info_map.find(table_id) == _table_info_map.end() ||
        _table_info_map.find(binlog_table_id) == _table_info_map.end()) {
        DB_WARNING("table not in table_info_map, request:%s", request.DebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not in table_info_map");
        return;
    }
    DB_NOTICE("unlink binlog tableid[%ld] binlog_table_id[%ld]", table_id, binlog_table_id);
    auto& table_mem =  _table_info_map[table_id];
    auto& binlog_table_mem =  _table_info_map[binlog_table_id];
    if (!table_mem.is_linked) {
        DB_WARNING("table not linked, request:%s", request.DebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not linked");
        return;
    }
    if (!binlog_table_mem.is_binlog || binlog_table_mem.binlog_target_ids.count(table_id) == 0) {
        DB_WARNING("table is not binlog or not correct binlog table, request:%s", request.DebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table is not binlog");
        return;
    }
    table_mem.binlog_ids.erase(binlog_table_id);
    binlog_table_mem.binlog_target_ids.erase(table_id);

    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    auto binlog_info = mem_schema_pb.mutable_binlog_info();
    if (binlog_info->binlog_table_id() == binlog_table_id) {
        binlog_info->clear_binlog_table_id();
        mem_schema_pb.clear_link_field();
    }
    auto binlog_iter = mem_schema_pb.mutable_binlog_infos()->begin();
    for (; binlog_iter != mem_schema_pb.mutable_binlog_infos()->end(); ++binlog_iter) {
        if ((*binlog_iter).binlog_table_id() == binlog_table_id) {
            mem_schema_pb.mutable_binlog_infos()->erase(binlog_iter);
            break;
        }
    }
    if (table_mem.binlog_ids.empty()) {
        table_mem.is_linked = false;
    }

    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};

    pb::SchemaInfo binlog_mem_schema_pb =  _table_info_map[binlog_table_id].schema_pb;
    auto binlog_binlog_info = binlog_mem_schema_pb.mutable_binlog_info();
    auto target_iter = binlog_binlog_info->mutable_target_table_ids()->begin();
    for (; target_iter != binlog_binlog_info->mutable_target_table_ids()->end();) {
        if (*target_iter == table_id) {
            binlog_binlog_info->mutable_target_table_ids()->erase(target_iter);
        } else {
            target_iter++;
        }
    }
    binlog_mem_schema_pb.set_version(binlog_mem_schema_pb.version() + 1);
    set_table_pb(binlog_mem_schema_pb);
    schema_infos.push_back(binlog_mem_schema_pb);

    put_incremental_schemainfo(apply_index, schema_infos);

    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    ret = update_schema_for_rocksdb(binlog_table_id, binlog_mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
}

void TableManager::on_leader_start() {
    _table_timer.start();
    auto call_func = [](TableSchedulingInfo& infos) -> int {
        infos.table_pk_prefix_timestamp = butil::gettimeofday_us();
        return 1;
    };
    _table_scheduling_infos.Modify(call_func);
}
void TableManager::on_leader_stop() {
    _table_timer.stop();
}

void TableManager::set_index_hint_status(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            if (request.has_table_info()) {
                for (const auto& index_info : request.table_info().indexs()) {
                    auto index_iter = mem_schema_pb.mutable_indexs()->begin();
                    for (; index_iter != mem_schema_pb.mutable_indexs()->end(); index_iter++) {
                        // 索引匹配不区分大小写
                        if (boost::algorithm::iequals(index_iter->index_name(), index_info.index_name()) &&
                            index_iter->index_type() != pb::I_PRIMARY) {
                            if (index_iter->hint_status() == pb::IHS_VIRTUAL) {
                                continue;
                            }
                            // restore index的索引状态必须是PUBLIC
                            if (index_info.hint_status() == pb::IHS_NORMAL && index_iter->state() != pb::IS_PUBLIC) {
                                continue;
                            }
                            index_iter->set_hint_status(index_info.hint_status());
                            int64_t due_time = 0;
                            if (index_info.hint_status() == pb::IHS_DISABLE) {
                                due_time = butil::gettimeofday_us() + FLAGS_table_tombstone_gc_time_s * 1000 * 1000LL;
                            }
                            index_iter->set_drop_timestamp(due_time);
                            DB_NOTICE("set index hint status schema %s", mem_schema_pb.ShortDebugString().c_str());
                            break;
                        }
                    }
                }
                mem_schema_pb.set_version(mem_schema_pb.version() + 1);
            }
        });
}
void TableManager::drop_virtual_index(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
       update_table_internal(request, apply_index, done, 
        [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
            if (request.has_table_info()) {
                const auto& index_info = request.table_info().indexs(0);
                if (mem_schema_pb.mutable_indexs() != nullptr) {
                auto index_iter = mem_schema_pb.mutable_indexs()->begin();
                    for (; index_iter != mem_schema_pb.mutable_indexs()->end(); index_iter++) {
                        if ( index_iter != mem_schema_pb.mutable_indexs()->end() && index_iter->index_name() == index_info.index_name() &&
                            index_iter->index_type() != pb::I_PRIMARY) {
                            mem_schema_pb.mutable_indexs()->erase(index_iter);
                            DB_NOTICE("set index hint status schema %s", mem_schema_pb.ShortDebugString().c_str());
                            break;
                        }
                    }
                }
                mem_schema_pb.set_version(mem_schema_pb.version() + 1);
            }
        });
}

void TableManager::add_learner(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    int ret = 0;
    DB_DEBUG("request:%s", request.ShortDebugString().c_str());
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0 && 
        request.table_info().table_id() == table_id) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }

    auto& table_mem = _table_info_map[table_id];
    pb::SchemaInfo mem_schema_pb =  table_mem.schema_pb;

    if (request.resource_tags().size() < 1 || request.resource_tags(0) == mem_schema_pb.resource_tag()) {
        DB_WARNING("learner resource tag can`t be the same as origin table resouce %s.", table_mem.schema_pb.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "same resource tag.");
        return;
    }
    for (auto& learner_resource : mem_schema_pb.learner_resource_tags()) {
        if (learner_resource == request.resource_tags(0)) {
            DB_WARNING("already has learner schema %s.", table_mem.schema_pb.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "already has learner");
            return;
        }
    }

    table_mem.learner_resource_tag.emplace_back(request.resource_tags(0));
    mem_schema_pb.add_learner_resource_tags(request.resource_tags(0));
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);

    ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    DB_NOTICE("add_learner schema_info [%s] apply_index %ld", 
        _table_info_map[table_id].schema_pb.ShortDebugString().c_str(), apply_index);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
}

void TableManager::drop_learner(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    int ret = 0;
    DB_DEBUG("request:%s", request.ShortDebugString().c_str());
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0 && 
        request.table_info().table_id() == table_id) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }

    auto& table_mem = _table_info_map[table_id];
    pb::SchemaInfo mem_schema_pb =  table_mem.schema_pb;
    if (table_mem.learner_resource_tag.size() == 0 || request.resource_tags().size() < 1) {
        DB_WARNING("not learner schema %s.", table_mem.schema_pb.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "not learner table");
        return;
    }
    auto iter = std::find(table_mem.learner_resource_tag.begin(), 
        table_mem.learner_resource_tag.end(), request.resource_tags(0));
    if (iter == table_mem.learner_resource_tag.end()) {
        DB_WARNING("can`t find learner resource tag %s.", table_mem.schema_pb.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "can`t find learner resource tag");
        return;
    }

    table_mem.learner_resource_tag.erase(iter);

    auto learner_resource_ptr = mem_schema_pb.mutable_learner_resource_tags();
    for (auto learner_iter = learner_resource_ptr->begin(); learner_iter != learner_resource_ptr->end(); learner_iter++) {
        if (*learner_iter == request.resource_tags(0)) {
            learner_resource_ptr->erase(learner_iter);
            break;
        }
    }
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_infos{mem_schema_pb};
    put_incremental_schemainfo(apply_index, schema_infos);
    ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    DB_NOTICE("drop_learner schema_info [%s] apply_index %ld", 
        _table_info_map[table_id].schema_pb.ShortDebugString().c_str(), apply_index);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
}

void TableManager::remove_global_index_data(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    int ret = 0;
    auto& ddl_work = request.ddlwork_info(); 
    auto drop_index_id = ddl_work.index_id();
    std::vector<std::string> delete_rocksdb_keys;
    delete_rocksdb_keys.reserve(100);
    std::vector<std::string> write_rocksdb_keys;
    std::vector<std::string> write_rocksdb_values;

    if (_table_info_map.find(drop_index_id) == _table_info_map.end()) {
        DB_WARNING("drop table error. table not exist."); 
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "global index not in table info map.");
        return;
    }
    std::vector<std::int64_t> drop_region_ids;
    drop_region_ids.reserve(100);
    for (auto& partition_region: _table_info_map[drop_index_id].partition_regions) {
        for (auto& drop_region_id : partition_region.second) {
            std::string drop_region_key = RegionManager::get_instance()->construct_region_key(drop_region_id);
            delete_rocksdb_keys.emplace_back(drop_region_key);
            drop_region_ids.emplace_back(drop_region_id);
        }
    }

    ret = MetaRocksdb::get_instance()->write_meta_info(write_rocksdb_keys, 
                                                                    write_rocksdb_values, 
                                                                    delete_rocksdb_keys);
    if (ret < 0) {
        DB_WARNING("drop index fail, request：%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    DB_NOTICE("drop index success, request:%s", request.ShortDebugString().c_str());
    if (done) {
        Bthread bth_remove_region(&BTHREAD_ATTR_SMALL);
        std::function<void()> remove_function = [drop_region_ids]() {
                RegionManager::get_instance()->send_remove_region_request(drop_region_ids);
            };
        bth_remove_region.run(remove_function);
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
}

void TableManager::load_virtual_indextosqls_to_memory(const pb::BaikalHeartBeatRequest* request) {
    BAIDU_SCOPED_LOCK(_load_virtual_to_memory_mutex);
    auto& affect_info = request->info_affect();
    for (auto& it1 : affect_info) {
        auto& index_name = it1.virtual_index_name();
        auto& influenced_sql = it1.influenced_sql();
        auto virtual_index_id = it1.virtual_index_id();
        if (_just_add_virtual_index_info.count(virtual_index_id) > 0) {
            std::string row;
            std::string database_name;
            std::string table_name;
            std::string sql;
            uint64_t out[2];
            baikaldb::parse_sample_sql(influenced_sql, database_name, table_name, sql);
            butil::MurmurHash3_x64_128(influenced_sql.c_str(), influenced_sql.size(), 0x1234, out);
            std::string sign = std::to_string(out[0]);
            row = database_name + "," + table_name + "," + index_name ;
            std::pair<std::string,std::string> sign_add_sql = {sign,sql};
            _virtual_index_sql_map[row].insert(sign_add_sql);
        } else {
            continue;
        }
   }
}

VirtualIndexInfo TableManager::get_virtual_index_id_set() {
    BAIDU_SCOPED_LOCK(_load_virtual_to_memory_mutex);
    return _virtual_index_sql_map;
}

void TableManager::get_change_partition_schemas(std::vector<pb::SchemaInfo>& add_partition_schemas, 
                                                std::vector<pb::SchemaInfo>& del_partition_schemas,
                                                std::vector<pb::SchemaInfo>& cold_partition_schemas) {
    std::set<int64_t> table_ids;
    get_table_ids(table_ids);
    for (auto table_id : table_ids) {
        pb::SchemaInfo table_info;
        get_table_info(table_id, table_info);

        pb::SchemaInfo add_partition_schema;
        pb::SchemaInfo del_partition_schema;
        pb::SchemaInfo cold_partition_schema;
        get_change_partition_schema(table_info, add_partition_schema, del_partition_schema, cold_partition_schema);
        if (add_partition_schema.partition_info().range_partition_infos_size() > 0) {
            add_partition_schemas.emplace_back(std::move(add_partition_schema));
        }
        if (del_partition_schema.partition_info().range_partition_infos_size() > 0) {
            del_partition_schemas.emplace_back(std::move(del_partition_schema));
        }
        if (cold_partition_schema.partition_info().range_partition_infos_size() > 0) {
            cold_partition_schemas.emplace_back(std::move(cold_partition_schema));
        }
    }
}

void TableManager::get_change_partition_schema(const pb::SchemaInfo& schema, 
                                               pb::SchemaInfo& add_partition_schema, 
                                               pb::SchemaInfo& del_partition_schema,
                                               pb::SchemaInfo& cold_partition_schema) {
    if (!schema.has_partition_info()) {
        return;
    }
    if (schema.partition_info().type() != pb::PT_RANGE) {
        return;
    }
    if (!schema.partition_info().has_field_info()) {
        return;
    }
    if (!schema.partition_info().has_dynamic_partition_attr()) {
        return;
    }
    if (!schema.partition_info().dynamic_partition_attr().enable()) {
        return;
    }
    const auto& dynamic_partition_attr = schema.partition_info().dynamic_partition_attr();
    const std::string& time_unit = dynamic_partition_attr.time_unit();
    const int32_t start = dynamic_partition_attr.start();
    const int32_t cold = dynamic_partition_attr.cold();
    const int32_t end = dynamic_partition_attr.end();
    const std::string& prefix = dynamic_partition_attr.prefix();
    const int32_t& start_day_of_month = dynamic_partition_attr.start_day_of_month();
    const pb::PrimitiveType& partition_col_type = schema.partition_info().field_info().mysql_type();
    bool has_start = dynamic_partition_attr.has_start();
    bool has_cold = dynamic_partition_attr.has_cold();

    time_t current_ts;
    get_current_timestamp(current_ts);

    time_t normalized_current_ts;
    TimeUnit unit;
    if (boost::algorithm::iequals(time_unit, "DAY")) {
        unit = TimeUnit::DAY;
        get_current_day_timestamp(normalized_current_ts);
    } else {
        unit = TimeUnit::MONTH;
        get_current_month_timestamp(start_day_of_month, normalized_current_ts);
    }

    std::unordered_set<std::string> partition_names;
    for (const auto& range_partition_info : schema.partition_info().range_partition_infos()) {
        partition_names.emplace(range_partition_info.partition_name());
    }

    /// Add partition
    add_partition_schema = schema;
    pb::PartitionInfo* p_add_partition_info = add_partition_schema.mutable_partition_info();
    if (p_add_partition_info == nullptr) {
        DB_WARNING("p_partition_info == nullptr");
        return;
    }
    p_add_partition_info->clear_range_partition_infos();
    for (int32_t i = 0; i <= end; ++i) {
        pb::RangePartitionInfo range_partition_info;
        partition_utils::create_dynamic_range_partition_info(prefix, partition_col_type,
                                                             normalized_current_ts, i, unit, range_partition_info);
        std::unordered_set<pb::RangePartitionType> range_partition_type_set;
        for (const auto& type : schema.partition_info().gen_range_partition_types()) {
            range_partition_type_set.insert(static_cast<pb::RangePartitionType>(type));
        }
        if (range_partition_type_set.empty()) {
            // 兼容旧逻辑
            range_partition_type_set.insert(pb::RPT_DEFAULT);
        }
        for (const pb::RangePartitionType& type : range_partition_type_set) {
            pb::RangePartitionInfo add_range_partition_info;
            add_range_partition_info.CopyFrom(range_partition_info);
            if (type != pb::RPT_DEFAULT) {
                // 兼容旧逻辑
                add_range_partition_info.set_type(type);
                add_range_partition_info.set_partition_name(
                    range_partition_info.partition_name() + "_" + pb::RangePartitionType_Name(type));
            }
            const std::string& add_partition_name = add_range_partition_info.partition_name();
            if (partition_names.find(add_partition_name) != partition_names.end()) {
                continue;
            }
            // 获取预分裂SplitKey
            if (FLAGS_use_partition_split_key) {
                if (!schema.split_keys().empty()) {
                    add_range_partition_info.mutable_split_keys()->CopyFrom(schema.split_keys());
                } else {
                    if (get_partition_split_key(schema, add_range_partition_info, normalized_current_ts, 
                                                i, unit, *add_range_partition_info.mutable_split_keys()) != 0) {
                        DB_WARNING("Fail to get_partition_split_key");
                        add_range_partition_info.clear_split_keys();
                    }
                }
            }
            pb::RangePartitionInfo* p_range_partition_info = p_add_partition_info->add_range_partition_infos();
            if (p_range_partition_info == nullptr) {
                DB_WARNING("p_range_partition_info is nullptr");
                return;
            }
            p_range_partition_info->Swap(&add_range_partition_info);
        }
    }

    /// Del partition
    if (has_start) {
        time_t del_ts;
        std::string del_str;
        get_specified_timestamp(current_ts, start, unit, del_ts);
        if (partition_col_type == pb::DATE) {
            timestamp_to_format_str(del_ts, partition_utils::DATE_FORMAT, del_str);
        } else {
            timestamp_to_format_str(del_ts, partition_utils::DATETIME_FORMAT, del_str);
        }
        pb::Expr del_expr;
        partition_utils::create_partition_expr(partition_col_type, del_str, del_expr);

        del_partition_schema = schema;
        pb::PartitionInfo* p_del_partition_info = del_partition_schema.mutable_partition_info();
        if (p_del_partition_info == nullptr) {
            DB_WARNING("p_del_partition_info is nullptr");
            return;
        }
        p_del_partition_info->clear_range_partition_infos();
        auto* p_del_range_partition_infos = p_del_partition_info->mutable_range_partition_infos();
        if (p_del_range_partition_infos == nullptr) {
            DB_WARNING("p_del_range_partition_infos is nullptr");
            return;
        }
        partition_utils::get_specifed_partitions(
                del_expr, schema.partition_info().range_partition_infos(), *p_del_range_partition_infos);
    }

    /// Cold partition
    // 调整cold值，冷分区不会自动转热
    if (has_cold) {
        time_t cold_ts;
        std::string cold_str;
        get_specified_timestamp(current_ts, cold, unit, cold_ts);
        if (partition_col_type == pb::DATE) {
            timestamp_to_format_str(cold_ts, partition_utils::DATE_FORMAT, cold_str);
        } else {
            timestamp_to_format_str(cold_ts, partition_utils::DATETIME_FORMAT, cold_str);
        }
        pb::Expr cold_expr;
        partition_utils::create_partition_expr(partition_col_type, cold_str, cold_expr);

        cold_partition_schema = schema;
        pb::PartitionInfo* p_cold_partition_info = cold_partition_schema.mutable_partition_info();
        if (p_cold_partition_info == nullptr) {
            DB_WARNING("p_cold_partition_info is nullptr");
            return;
        }
        p_cold_partition_info->clear_range_partition_infos();

        ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo> cold_partition_info;
        partition_utils::get_specifed_partitions(
                cold_expr, schema.partition_info().range_partition_infos(), cold_partition_info, true);

        for (auto& cold_range_partition_info : cold_partition_info) {
            if (cold_range_partition_info.type() == schema.partition_info().primary_range_partition_type()) {
                // 主分区控制转冷
                pb::RangePartitionInfo* p_cold_range_partition_info = p_cold_partition_info->add_range_partition_infos();
                if (p_cold_range_partition_info == nullptr) {
                    DB_WARNING("p_cold_range_partition_info is nullptr");
                    return;
                }
                p_cold_range_partition_info->Swap(&cold_range_partition_info);
                p_cold_range_partition_info->set_is_cold(true);
            }
        }
    }
}

// 如果前一区间存在，则替换前一区间region的start_key/end_key的分区列来作为新区间的split_key
// 如果前一区间不存在，则不使用split_key
int TableManager::get_partition_split_key(
        const pb::SchemaInfo& table_info,
        const pb::RangePartitionInfo& range_partition_info,
        const time_t normalized_current_ts, 
        const int offset,
        TimeUnit unit,
        ::google::protobuf::RepeatedPtrField<pb::SplitKey>& split_key) {
    // 获取前一区间的partition_id
    pb::PrimitiveType partition_col_type = table_info.partition_info().field_info().mysql_type();
    // -1表示前一区间
    int32_t last_partition_offset = -1;
    std::string prefix;
    pb::RangePartitionInfo pre_range_partition_info;
    partition_utils::create_dynamic_range_partition_info(prefix, partition_col_type, normalized_current_ts, 
                                                         last_partition_offset, unit, pre_range_partition_info);
    int64_t pre_partition_id = -1;
    for (auto& range_partition_info_tmp : table_info.partition_info().range_partition_infos()) {
        if (!partition_utils::is_specified_partition(range_partition_info_tmp, range_partition_info)) {
            // 跳过不相同类型的Range分区
            continue;
        }
        if (partition_utils::is_equal(range_partition_info_tmp, pre_range_partition_info)) {
            pre_partition_id = range_partition_info_tmp.partition_id();
            break;
        }
    }
    if (pre_partition_id == -1) {
        DB_WARNING("Fail to find pre_partition_id");
        return 0;
    }
    
    // 获取主表及各全局索引的前一区间的region_ids, 更新分区列作为新分区的split_key
    std::unordered_map<int64_t, pb::PrimitiveType> field_types;
    for(auto& field : table_info.fields()) {
        field_types[field.field_id()] = field.mysql_type();
    }
    const pb::FieldInfo& partition_field = table_info.partition_info().field_info();
    for (const auto& index_info : table_info.indexs()) {
        if (index_info.index_type() != pb::I_PRIMARY && !is_global_index(index_info)) {
            continue;
        }
        pb::SplitKey* p_split_key = split_key.Add();
        if (p_split_key == nullptr) {
            DB_WARNING("p_split_key is nullptr");
            return -1;
        }
        const std::string& index_name = index_info.index_name();
        const int64_t index_id = index_info.index_id();
        p_split_key->set_index_name(index_name);
        
        // 找到分区列在索引中的位置
        int32_t partition_col_idx = -1;
        std::vector<pb::PrimitiveType> index_field_types;
        if (index_info.field_ids_size() != index_info.field_names_size()) {
            DB_WARNING("index_info.field_ids_size[%d] != index_info.field_names_size[%d]",
                        (int)index_info.field_ids_size(), (int)index_info.field_names_size());
            return -1;
        }
        for (size_t i = 0; i < index_info.field_ids_size(); ++i) {
            const std::string& field_name = index_info.field_names(i);
            const int32_t field_id = index_info.field_ids(i);
            if (field_types.find(field_id) == field_types.end()) {
                DB_WARNING("Fail to get index field type");
                return -1;
            }
            if (field_name == partition_field.field_name()) {
                partition_col_idx = i;
            }
            index_field_types.emplace_back(field_types[field_id]);
        }
        if (partition_col_idx == -1) {
            DB_WARNING("Fail to find partition col in index");
            return -1;
        }

        // 获取前一区间的region_id
        std::set<int64_t> pre_partition_region_ids;
        get_region_ids(index_id, pre_partition_id, pre_partition_region_ids);
        for (const auto& region_id : pre_partition_region_ids) {
            SmartRegionInfo p_region_info = RegionManager::get_instance()->get_region_info(region_id);
            if (p_region_info == nullptr) {
                DB_WARNING("p_region_info is nullptr");
                return -1;
            }
            if (!p_region_info->start_key().empty()) {
                MutTableKey mut_table_key(p_region_info->start_key());
                TableKey table_key(mut_table_key);
                // 获取分区列位置
                int partition_col_pos = 0;
                if (is_global_index(index_info)) {
                    // 全局索引分裂时split_key的第一个字节为null字节
                    partition_col_pos = 1;
                }
                table_key.get_partition_col_pos(index_field_types, partition_col_idx, partition_col_pos);
                if (partition_col_pos < 0) {
                    DB_WARNING("Fail to get_partition_col_pos");
                    continue;
                }
                // 替换分区列
                time_t new_ts;
                std::string new_value_str;
                pb::Expr new_partition_expr;
                switch (partition_col_type) {
                case pb::DATE:
                    {
                        uint32_t d = table_key.extract_u32(partition_col_pos);
                        time_t ts = datetime_to_timestamp(date_to_datetime(d));
                        // 前一区间和新建区间相隔(offset+1)个TimeUnit
                        get_specified_timestamp(ts, offset + 1, unit, new_ts);
                        timestamp_to_format_str(new_ts, partition_utils::DATE_FORMAT, new_value_str);
                        partition_utils::create_partition_expr(partition_col_type, new_value_str, new_partition_expr);
                    }
                    break;
                case pb::DATETIME:
                    {
                        uint64_t d = table_key.extract_u64(partition_col_pos);
                        time_t ts = datetime_to_timestamp(d);
                        // 前一区间和新建区间相隔(offset+1)个TimeUnit
                        get_specified_timestamp(ts, offset + 1, unit, new_ts);
                        timestamp_to_format_str(new_ts, partition_utils::DATETIME_FORMAT, new_value_str);
                        partition_utils::create_partition_expr(partition_col_type, new_value_str, new_partition_expr);
                    }
                    break;
                default:
                    DB_WARNING("Invalid partition col type, %d", partition_col_type);
                    return -1;
                }
                // 如果替换后值未落在新建区间，则使用新建区间左端点作为替换值
                if (!(partition_utils::compare(new_partition_expr, range_partition_info.range().left_value()) >= 0 &&
                        partition_utils::compare(new_partition_expr, range_partition_info.range().right_value()) < 0)) {
                    new_partition_expr = range_partition_info.range().left_value();
                }
                ExprValue new_partition_value;
                partition_utils::get_partition_value(new_partition_expr, new_partition_value);
                mut_table_key.replace_partition_col(new_partition_value, partition_col_pos);
                p_split_key->add_split_keys(std::move(mut_table_key.data()));
            }
        }
        if (p_split_key->split_keys_size() > 0) {
            // 对split_key进行排序
            std::sort(
                p_split_key->mutable_split_keys()->pointer_begin(), p_split_key->mutable_split_keys()->pointer_end(),
                [] (const std::string* left, const std::string* right) { 
                    if (left == nullptr || right == nullptr) {
                        return false;
                    }
                    return *left < *right; 
                });
            // 对split_key进行去重
            int remove_num = std::distance(std::unique(p_split_key->mutable_split_keys()->begin(), p_split_key->mutable_split_keys()->end()),
                                           p_split_key->mutable_split_keys()->end());
            for (int i = 0; i < remove_num; ++i) {
                p_split_key->mutable_split_keys()->RemoveLast();
            }
        }

    }

    return 0;
}

void TableManager::check_partition_exist_for_peer(const pb::StoreHeartBeatRequest* request,
            pb::StoreHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& peer_info : request->peer_infos()) {
        int64_t global_index_id = peer_info.table_id();
        int64_t partition_id = peer_info.partition_id();
        if (_table_info_map.find(global_index_id) == _table_info_map.end()) {
            continue;
        }
        const pb::SchemaInfo& schema_pb = _table_info_map[global_index_id].schema_pb;
        if (!schema_pb.has_partition_info() || schema_pb.partition_info().type() != pb::PT_RANGE) {
            continue;
        }
        if (!is_range_partition_exist(schema_pb, partition_id)) {
            // 仅主表TableMem保存drop_partition_ts_map
            // 延迟删除region，避免drop partition时，store region已经删除，但是db还未同步到删除的region信息
            int64_t main_table_id = peer_info.main_table_id() == 0 ? 
                                        peer_info.table_id() : peer_info.main_table_id();
            if (_table_info_map.find(main_table_id) != _table_info_map.end()) {
                continue;
            }
            auto& drop_partition_ts_map = _table_info_map[main_table_id].drop_partition_ts_map;
            if (drop_partition_ts_map.find(partition_id) == drop_partition_ts_map.end()) {
                drop_partition_ts_map[partition_id] = butil::gettimeofday_us();
                continue;
            }
            if (butil::gettimeofday_us() - drop_partition_ts_map[partition_id] < FLAGS_remove_dropped_partition_region_s) {
                continue;
            }
            response->add_delete_region_ids(peer_info.region_id());
        }
    }
}

void TableManager::drop_partition_ts(
    const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& table_info_pair : _table_info_map) {
        auto& drop_partition_ts_map = table_info_pair.second.drop_partition_ts_map;
        for (auto iter = drop_partition_ts_map.begin(); iter != drop_partition_ts_map.end();) {
            if (butil::gettimeofday_us() - iter->second > 60 * FLAGS_remove_dropped_partition_region_s) {
                drop_partition_ts_map.erase(iter++);
            } else {
                iter++;
            }
        }
    }
}

void TableManager::specify_split_keys(const pb::MetaManagerRequest& request, const int64_t apply_index, braft::Closure* done) {
    update_table_internal(request, apply_index, done, 
            [](const pb::MetaManagerRequest& request, pb::SchemaInfo& mem_schema_pb, braft::Closure* done) {
        mem_schema_pb.mutable_split_keys()->CopyFrom(request.table_info().split_keys());
        mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    });
}

// 快慢指针删除指定分区
int TableManager::remove_partitions(
        pb::SchemaInfo& table_info, const std::set<int64_t>& partition_ids) {
    if (table_info.mutable_partition_info() == nullptr || 
            table_info.mutable_partition_info()->mutable_range_partition_infos() == nullptr) {
        DB_WARNING("table_info mutable_partition_info or mutable_range_partition_infos is nullptr");
        return -1;
    }
    
    int slow = 0;
    int fast = 0;
    const size_t range_partition_size = table_info.partition_info().range_partition_infos_size();
    for (;fast < range_partition_size; ++fast) {
        const int64_t partition_id = table_info.partition_info().range_partition_infos(fast).partition_id();
        if (partition_ids.find(partition_id) == partition_ids.end()) {
            table_info.mutable_partition_info()->mutable_range_partition_infos()->SwapElements(slow, fast);
            ++slow;
        }
    }

    int remove_num = range_partition_size - slow;
    for (int i = 0; i < remove_num; ++i) {
        table_info.mutable_partition_info()->mutable_range_partition_infos()->RemoveLast();
    }
    return 0;
}

void TableManager::get_main_logical_room(
        const pb::SchemaInfo& table_info, std::string& main_logical_room) {
    if (table_info.has_main_logical_room()) {
        main_logical_room = table_info.main_logical_room();
        return;
    }
    // 副本数量最多的机房是主机房
    int max_count = 0;
    for (auto& dist : table_info.dists()) {
        if (dist.count() > max_count) {
            max_count = dist.count();
            main_logical_room = dist.logical_room();
        }
    }
    return;
}

void TableManager::drop_partition_internal(pb::SchemaInfo& mem_schema_pb, 
                                           const std::vector<std::string>& range_partition_names_vec,
                                           const bool is_dynamic_change,
                                           const int64_t apply_index, 
                                           braft::Closure* done) {
    std::vector<std::string> write_rocksdb_keys;
    std::vector<std::string> write_rocksdb_values;
    std::vector<std::string> delete_rocksdb_keys;
    std::set<int64_t> partition_ids;
    std::vector<int64_t> drop_region_ids;
    int64_t table_id = mem_schema_pb.table_id();

    if (!range_partition_names_vec.empty()) {
        std::unordered_map<std::string, int64_t> partition_name_id_map;
        for (auto& table_range_partition_info : mem_schema_pb.partition_info().range_partition_infos()) {
            const std::string& partition_name = table_range_partition_info.partition_name();
            const int64_t partition_id = table_range_partition_info.partition_id();
            partition_name_id_map.insert(std::make_pair(partition_name, partition_id));
        }

        for (const auto& partition_name : range_partition_names_vec) {
            int64_t partition_id = -1;
            if (partition_name_id_map.find(partition_name) != partition_name_id_map.end()) {
                partition_id = partition_name_id_map[partition_name];
            }
            if (partition_id != -1) {
                partition_ids.insert(partition_id);
            } else {
                if (is_dynamic_change) {
                    // 动态创建分区跳过不存在分区
                    continue;
                }
                DB_WARNING("Fail to get partition_id");
                IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "Fail to get partition_id");
                return;
            }
        }

        std::set<int64_t> global_index_ids;
        global_index_ids.insert(table_id);
        for (const auto& index_info : mem_schema_pb.indexs()) {
            if (!is_global_index(index_info)) {
                continue;
            }
            global_index_ids.insert(index_info.index_id());
        }

        for (const auto& global_index_id : global_index_ids) {
            for (const auto& partition_id : partition_ids) {
                std::set<int64_t> region_ids;
                get_region_ids(global_index_id, partition_id, region_ids);
                for (const auto& drop_region_id : region_ids) {
                    const std::string& drop_region_key = 
                                RegionManager::get_instance()->construct_region_key(drop_region_id);
                    delete_rocksdb_keys.emplace_back(drop_region_key);
                    drop_region_ids.emplace_back(drop_region_id);
                }
            } 
        }

        if (remove_partitions(mem_schema_pb, partition_ids) != 0) {
            DB_WARNING("fail to remove_partitions");
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "fail to remove_partitions");
            return;
        }

        mem_schema_pb.set_partition_num(mem_schema_pb.partition_info().range_partition_infos_size());

        if (mem_schema_pb.partition_num() == 0) {
            DB_WARNING("mem_schema_pb partition_num is 0");
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "mem_schema_pb partition_num is 0");
            return;
        }
    }
    
    // 检查分区是否有重叠
    for (const auto& range_partition_info : mem_schema_pb.partition_info().range_partition_infos()) {
        if (partition_utils::check_partition_overlapped(
                mem_schema_pb.partition_info().range_partition_infos(), range_partition_info)) {
            // 报警
            DB_FATAL("Partition overlapped range_partition_info: %s", range_partition_info.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "Partition overlapped");
            return;
        }
    }

    mem_schema_pb.set_version(mem_schema_pb.version() + 1);

    // 持久化schema_info
    std::string mem_schema_pb_str;
    if (!mem_schema_pb.SerializeToString(&mem_schema_pb_str)) {
        DB_WARNING("request SerializeToString fail");
        IF_DONE_SET_RESPONSE(done, pb::PARSE_TO_PB_FAIL, "SerializeToString fail");
        return ;
    }
    write_rocksdb_keys.emplace_back(construct_table_key(table_id));
    write_rocksdb_values.emplace_back(mem_schema_pb_str);
    int ret = MetaRocksdb::get_instance()->write_meta_info(
                write_rocksdb_keys, write_rocksdb_values, delete_rocksdb_keys);
    if (ret < 0) {
        DB_WARNING("drop partition fail");
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }

    // 更新内存
    set_table_pb(mem_schema_pb);
    std::vector<pb::SchemaInfo> schema_pbs { mem_schema_pb };
    put_incremental_schemainfo(apply_index, schema_pbs);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");

    if (!range_partition_names_vec.empty()) {
        for (const auto& partition_id : partition_ids) {
            set_drop_partition_timestamp(table_id, partition_id, butil::gettimeofday_us());
        }

        if (done) {
            Bthread bth_remove_region(&BTHREAD_ATTR_SMALL);
            std::function<void()> remove_function = [drop_region_ids]() {
                    // 延迟删除region，避免drop partition时，store region已经删除，但是db还未同步到删除的region信息
                    bthread_usleep(FLAGS_remove_dropped_partition_region_s);
                    RegionManager::get_instance()->send_remove_region_request(drop_region_ids);
                };
            bth_remove_region.run(remove_function);
        }
    }
}

}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
