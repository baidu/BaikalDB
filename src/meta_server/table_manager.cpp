// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include "table_manager.h"
#include "meta_server_interact.hpp"
#include "store_interact.hpp"
#include "namespace_manager.h"
#include "database_manager.h"
#include "region_manager.h"
#include "meta_util.h"
#include "meta_rocksdb.h"

namespace baikaldb {
DECLARE_int32(concurrency_num);
DEFINE_int32(region_replica_num, 3, "region replica num, default:3"); 
DEFINE_int32(region_region_size, 100 * 1024 * 1024, "region size, default:100M");

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
    return total_byte_size / byte_size_per_record;
}

void TableManager::create_table(const pb::MetaManagerRequest& request, braft::Closure* done) {
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
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table already exist");
        return;
    }

    //分配table_id
    int64_t max_table_id_tmp = _max_table_id;
    table_info.set_table_id(++max_table_id_tmp);
     
    if (table_mem.whether_level_table) {
        if (_table_id_map.find(upper_table_name) == _table_id_map.end()) {
            DB_WARNING("request upper_table_name:%s not exist", upper_table_name.c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "upper table not exist");
            return;
        }
        int64_t upper_table_id = _table_id_map[upper_table_name];
        table_info.set_upper_table_id(upper_table_id);
        if (table_info.has_partition_num()) {
            DB_WARNING("table：%s is leve, partition num should be equal to upper table",
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
    table_mem.schema_pb = table_info;
    //发起交互， 层次表与非层次表区分对待，非层次表需要与store交互，创建第一个region
    //层级表直接继承后父层次的相关信息即可
    if (table_mem.whether_level_table) {
        ret = write_schema_for_level(table_mem, done, max_table_id_tmp, has_auto_increment);
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
    DatabaseManager::get_instance()->add_table_id(database_id, table_info.table_id());
    DB_NOTICE("create table completely, _max_table_id:%ld, table_name:%s", _max_table_id, table_name.c_str());
}

void TableManager::drop_table(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t namespace_id;
    int64_t database_id;
    int64_t drop_table_id;
    auto ret = check_table_exist(request.table_info(), namespace_id, database_id, drop_table_id);
    if (ret < 0) {
        DB_WARNING("input table not exit, request: %s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    std::vector<std::string> delete_rocksdb_keys;
    std::vector<std::string> write_rocksdb_keys;
    std::vector<std::string> write_rocksdb_values;
    delete_rocksdb_keys.push_back(construct_table_key(drop_table_id));
    
    //drop_region_ids用来保存该表的所有region，用来给store发送remove_region
    std::vector<std::int64_t> drop_region_ids;
    //如果table下有region， 直接删除region信息
    for (auto& partition_region: _table_info_map[drop_table_id].partition_regions) {
        for (auto& drop_region_id : partition_region.second) {
            std::string drop_region_key = RegionManager::get_instance()->construct_region_key(drop_region_id);
            delete_rocksdb_keys.push_back(drop_region_key);
            drop_region_ids.push_back(drop_region_id);
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
        DB_WARNING("drop table fail, request：%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    //删除内存中的值
    if (_table_info_map[drop_table_id].schema_pb.has_upper_table_name()
        && _table_info_map.find(top_table_id) != _table_info_map.end()) {
        set_table_pb(top_schema_pb);
    }
    erase_table_info(drop_table_id);
    DatabaseManager::get_instance()->delete_table_id(database_id, drop_table_id);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop table success, request:%s", request.ShortDebugString().c_str());
    if (done) {
        Bthread bth_remove_region(&BTHREAD_ATTR_SMALL);
        std::function<void()> remove_function = [drop_region_ids]() {
                RegionManager::get_instance()->send_remove_region_request(drop_region_ids);
            };
        bth_remove_region.run(remove_function);
        Bthread bth_drop_auto(&BTHREAD_ATTR_SMALL);
        auto drop_function = [this, drop_table_id]() {
            pb::MetaManagerRequest request;
            request.set_op_type(pb::OP_DROP_ID_FOR_AUTO_INCREMENT);
            pb::AutoIncrementRequest* auto_incr = request.mutable_auto_increment();
            auto_incr->set_table_id(drop_table_id);
            send_auto_increment_request(request);
        };
        bth_drop_auto.run(drop_function);
    }
}

void TableManager::rename_table(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
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
        DB_WARNING("table not exit, table_name:%s", new_table_name.c_str());
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
    swap_table_name(old_table_name, new_table_name);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("rename table success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::update_byte_size(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    mem_schema_pb.set_byte_size_per_record(request.table_info().byte_size_per_record());
    mem_schema_pb.set_version(mem_schema_pb.version() + 1);
    auto ret = update_schema_for_rocksdb(table_id, mem_schema_pb, done);
    if (ret < 0) {
        IF_DONE_SET_RESPONSE(done, pb::INTERNAL_ERROR, "write db fail");
        return;
    }
    set_table_pb(mem_schema_pb);    
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("update byte size per record success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::add_field(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
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
    add_field_mem(table_id, add_field_id_map);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("add field success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::drop_field(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
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
        drop_field_names.push_back(field.field_name());
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
    drop_field_mem(table_id, drop_field_names);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("drop field success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::rename_field(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
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
    drop_field_mem(table_id, drop_field_names);
    add_field_mem(table_id, add_field_id_map);
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    DB_NOTICE("rename field success, request:%s", request.ShortDebugString().c_str());
}

void TableManager::modify_field(const pb::MetaManagerRequest& request, braft::Closure* done) {
    int64_t table_id;
    if (check_table_exist(request.table_info(), table_id) != 0) {
        DB_WARNING("check table exist fail, request:%s", request.ShortDebugString().c_str());
        IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "table not exist");
        return;
    }
    pb::SchemaInfo mem_schema_pb =  _table_info_map[table_id].schema_pb;
    std::vector<std::string> drop_field_names;
    for (auto& field : request.table_info().fields()) {
        std::string field_name = field.field_name();
        if (_table_info_map[table_id].field_id_map.count(field_name) == 0) {
            DB_WARNING("field name:%s not existed, request:%s",
                        field.field_name().c_str(), request.ShortDebugString().c_str());
            IF_DONE_SET_RESPONSE(done, pb::INPUT_PARAM_ERROR, "field name not exist");
            return;
        }
        for (auto& mem_field : *mem_schema_pb.mutable_fields()) {
            if (mem_field.field_name() == field_name) {
                mem_field.set_mysql_type(field.mysql_type());
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
            DB_WARNING("add or update table_name:%s, table_id:%ld",
                        new_table_info->table_name().c_str(), new_table_info->table_id());
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
            DB_WARNING("delete table_info:%s, table_id: %ld",
                    new_table_info->table_name().c_str(), new_table_info->table_id());
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
        //表更新
        if (_table_info_map[table_id].schema_pb.version() > schema_heart_beat.version()) {
            *(response->add_schema_change_info()) = _table_info_map[table_id].schema_pb;
        }
    }
}
void TableManager::check_add_table(std::set<int64_t>& report_table_ids, 
            std::vector<int64_t>& new_add_region_ids,
            pb::BaikalHeartBeatResponse* response) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& table_info_pair : _table_info_map) {
        if (report_table_ids.find(table_info_pair.first) != report_table_ids.end()) {
            continue;
        }
        auto schema_info = response->add_schema_change_info();
        *schema_info = table_info_pair.second.schema_pb;
        for (auto& partition_region : table_info_pair.second.partition_regions) {
            for (auto& region_id : partition_region.second) {
                DB_WARNING("new add region id: %ld", region_id);
                new_add_region_ids.push_back(region_id);
            }
        }
    }
}
void TableManager::check_add_region(const std::set<std::int64_t>& report_table_ids,
                std::unordered_map<int64_t, std::set<std::int64_t>>& report_region_ids,
                pb::BaikalHeartBeatResponse* response) {
    //获得每个表的regincount
    std::unordered_map<int64_t, int64_t> table_region_count;
    get_region_count(report_table_ids, table_region_count);
    
    std::vector<int64_t> table_for_add_region; //需要add_region的table_id
    for (auto& region_ids_pair : report_region_ids) {
        int64_t table_id = region_ids_pair.first;
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
    DB_WARNING("table snapshot:%s", table_pb.ShortDebugString().c_str());
    TableMem table_mem;
    table_mem.schema_pb = table_pb;
    table_mem.whether_level_table = table_pb.has_upper_table_name();
    for (auto& field : table_pb.fields()) {
        if (!field.has_deleted() || !field.deleted()) {
            table_mem.field_id_map[field.field_name()] = field.field_id();
        }
    }
    for (auto& index : table_pb.indexs()) {
        table_mem.index_id_map[index.index_name()] = index.index_id();
    }
    set_table_info(table_mem); 
    DatabaseManager::get_instance()->add_table_id(table_pb.database_id(), table_pb.table_id());
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
    int64_t instance_count = 0;
    pb::SchemaInfo simple_table_info = table_mem.schema_pb;
    simple_table_info.clear_init_store();
    simple_table_info.clear_split_keys();
    for (auto i = 0; i < table_mem.schema_pb.partition_num() && 
            table_mem.schema_pb.engine() == pb::ROCKSDB; ++i) {
        do {
            pb::InitRegion init_region_request;
            pb::RegionInfo* region_info = init_region_request.mutable_region_info();
            region_info->set_region_id(++tmp_max_region_id);
            region_info->set_table_id(table_mem.schema_pb.table_id());
            region_info->set_table_name(table_mem.schema_pb.table_name());
            region_info->set_partition_id(i);
            region_info->set_version(1);
            region_info->set_conf_version(1);
            region_info->set_replica_num(table_mem.schema_pb.replica_num());
            region_info->add_peers(table_mem.schema_pb.init_store(instance_count));
            region_info->set_leader(table_mem.schema_pb.init_store(instance_count));
            region_info->set_used_size(0);
            region_info->set_log_index(0);
            region_info->set_status(pb::IDLE);
            //region_info.set_can_add_peer(true);
            region_info->set_can_add_peer(false);
            region_info->set_parent(0);
            region_info->set_timestamp(time(NULL));
            if (instance_count != 0) {
                region_info->set_start_key(table_mem.schema_pb.split_keys(instance_count -1));
            }
            if (instance_count < table_mem.schema_pb.split_keys_size()) {
                region_info->set_end_key(table_mem.schema_pb.split_keys(instance_count));
            }
            *(init_region_request.mutable_schema_info()) = simple_table_info;
            init_region_request.set_snapshot_times(2);
            init_regions->push_back(init_region_request);
            ++instance_count;
        } while (instance_count <= table_mem.schema_pb.split_keys_size());
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
    uint64_t init_value = 1;
    if (table_mem.schema_pb.has_auto_increment_increment()) {
        init_value = table_mem.schema_pb.auto_increment_increment();
    }
    
    //leader发送请求
    if (done && table_mem.schema_pb.engine() == pb::ROCKSDB) {
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
void TableManager::send_create_table_request(const std::string& namespace_name,
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
    } else {
        DB_NOTICE("create table:%s success", 
                    (namespace_name + "." + database + "." + table_name).c_str());
    }
}

int TableManager::write_schema_for_level(const TableMem& table_mem, 
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
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
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
    BAIDU_SCOPED_LOCK(_table_mutex);
    for (auto& peer_info : request->peer_infos()) {
        if (_table_info_map.find(peer_info.table_id()) != _table_info_map.end()) {
            continue;
        }
        DB_WARNING("table id:%ld according to region_id:%ld not exit, drop region_id, store_address:%s",
                peer_info.table_id(), peer_info.region_id(),
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
        table_mem.index_id_map[index_name] = table_info.table_id();
    }
    if (!has_primary_key) {
        return -1;
    }
    return 0;
}
}//namespace 

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
