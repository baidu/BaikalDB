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

#include "schema_factory.h"
#include <unordered_set>
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include "table_key.h"
#include "mut_table_key.h"
#include "user_info.h"
#include "password.h"
#include "table_record.h"

using google::protobuf::FileDescriptor;
namespace baikaldb {

int SchemaFactory::init() {
    if (_is_init) {
        return 0;
    }
    int ret = bthread::execution_queue_start(&_table_queue_id, nullptr, 
            update_tables_double_buffer, (void*)this);
    if (ret != 0) {
        DB_FATAL("execution_queue_start error, %d", ret);
        return -1;
    }

    ret = bthread::execution_queue_start(&_region_queue_id, nullptr, 
            update_regions_double_buffer, (void*)this);
    if (ret != 0) {
        DB_FATAL("execution_queue_start error, %d", ret);
        return -1;
    }

    ret = bthread::execution_queue_start(&_idc_queue_id, nullptr, 
                update_idc_double_buffer, (void*)this);
    if (ret != 0) {
        DB_FATAL("execution_queue_start error, %d", ret);
        return -1;
    }

    ret = bthread::execution_queue_start(&_big_sql_queue_id, nullptr, 
                update_big_sql_double_buffer, (void*)this);
    if (ret != 0) {
        DB_FATAL("execution_queue_start error, %d", ret);
        return -1;
    }
    _is_init = true;
    return 0;
}

void SchemaFactory::update_table(const pb::SchemaInfo& table) {
    //DB_NOTICE("update_table");
    bthread::execution_queue_execute(_table_queue_id, table);
}

int SchemaFactory::update_tables_double_buffer(
        void* meta, bthread::TaskIterator<pb::SchemaInfo>& iter) {
    SchemaFactory* factory = (SchemaFactory*)meta;
    factory->update_tables_double_buffer(iter);
    return 0;
}
void SchemaFactory::update_tables_double_buffer(bthread::TaskIterator<pb::SchemaInfo>& iter) {
    for (; iter; ++iter) {
        std::function<int(SchemaMapping& schema_mapping, const pb::SchemaInfo& table)> update_func =
            std::bind(&SchemaFactory::update_table_internal, this, std::placeholders::_1, std::placeholders::_2);
        //DB_NOTICE("update_table double_buffer");
        delete_table_region_map(*iter);
        _double_buffer_table.Modify(update_func, *iter);
    }
}

void SchemaFactory::update_tables_double_buffer_sync(const SchemaVec& tables) {
    for (auto& table : tables) {
        std::function<int(SchemaMapping& schema_mapping, const pb::SchemaInfo& table)> update_func =
            std::bind(&SchemaFactory::update_table_internal, this, std::placeholders::_1, std::placeholders::_2);
        DB_NOTICE("update_table double_buffer_sync");
        delete_table_region_map(table);
        _double_buffer_table.Modify(update_func, table);
    }
}

void SchemaFactory::update_idc(const pb::IdcInfo& idc_info) {
    bthread::execution_queue_execute(_idc_queue_id, idc_info);
}

int SchemaFactory::update_idc_double_buffer(
        void* meta, bthread::TaskIterator<pb::IdcInfo>& iter) {
    SchemaFactory* factory = (SchemaFactory*)meta;
    factory->update_idc_double_buffer(iter);
    return 0;
}

void SchemaFactory::update_idc_double_buffer(bthread::TaskIterator<pb::IdcInfo>& iter) {
    for (; iter; ++iter) {
        std::function<int(IdcMapping&, const pb::IdcInfo&)> call_func = 
            std::bind(&SchemaFactory::update_idc_internal, this, std::placeholders::_1, std::placeholders::_2);
        _double_buffer_idc.Modify(
            call_func,
            *iter
        );
    }
}

int SchemaFactory::update_idc_internal(IdcMapping& ida_mapping, const pb::IdcInfo& idc_info) {
    //double buffer根据该函数返回值确定是否更新ida_mapping。0：不更新，非0：更新
    SELF_TRACE("update_idc idc_info[%s]", idc_info.ShortDebugString().c_str());
    ida_mapping.instance_logical_mapping.clear();
    ida_mapping.physical_logical_mapping.clear();
    for (auto& logical_physical : idc_info.logical_physical_map()) {
        std::string logical_room = logical_physical.logical_room();
        for (auto& physical_room : logical_physical.physical_rooms()) {
            ida_mapping.physical_logical_mapping[physical_room] = logical_room;
        }
    }
    for (auto& instance : idc_info.instance_infos()) {
        std::string address = instance.address();
        std::string physical_room = instance.physical_room();
        if (ida_mapping.physical_logical_mapping.find(physical_room) !=
                    ida_mapping.physical_logical_mapping.end()) {
            ida_mapping.instance_logical_mapping[address] = ida_mapping.physical_logical_mapping[physical_room];
        }    
    }
    return 1;
}

void SchemaFactory::update_big_sql(const std::string& sql) {
    bthread::execution_queue_execute(_big_sql_queue_id, sql);
}

int SchemaFactory::update_big_sql_double_buffer(
        void* meta, bthread::TaskIterator<std::string>& iter) {
    SchemaFactory* factory = (SchemaFactory*)meta;
    factory->update_big_sql_double_buffer(iter);
    return 0;
}

void SchemaFactory::update_big_sql_double_buffer(bthread::TaskIterator<std::string>& iter) {
    for (; iter; ++iter) {
        _double_buffer_big_sql.Modify(set_insert, *iter);
    }
}

bool SchemaFactory::is_big_sql(const std::string& sql) {
    DoubleBufferStringSet::ScopedPtr set_ptr;
    if (_double_buffer_big_sql.Read(&set_ptr) != 0) {
        DB_WARNING("read double_buffer_big_sql error.");
        return false; 
    }
    return set_ptr->count(sql) == 1;
}

//TODO
void SchemaFactory::delete_table(const pb::SchemaInfo& table, SchemaMapping& background) {
    if (!table.has_table_id()) {
        DB_FATAL("missing fields in SchemaInfo");
        return;
    }
    auto& table_info_mapping = background.table_info_mapping;
    auto& table_name_id_mapping = background.table_name_id_mapping;
    auto& index_info_mapping = background.index_info_mapping;
    auto& index_name_id_mapping = background.index_name_id_mapping;
    auto& global_index_id_mapping = background.global_index_id_mapping;
    int64_t table_id = table.table_id();
    if (table_info_mapping.count(table_id) == 0) {
        DB_FATAL("no table found with tableid: %ld", table_id);
        return;
    }
    auto tbl_info_ptr = table_info_mapping[table_id];
    auto& tbl_info = *tbl_info_ptr;
    std::string full_name = tbl_info.namespace_ + "." + tbl_info.name;
    DB_WARNING("full_name: %s, %ld", full_name.c_str(), table_name_id_mapping.size());
    table_name_id_mapping.erase(full_name);
    DB_WARNING("full_name deleted: %ld", table_name_id_mapping.size());

    for (auto index_id : tbl_info.indices) {
        global_index_id_mapping.erase(index_id);
        if (index_info_mapping.count(index_id) == 0) {
            continue;
        }
        IndexInfo& idx_info = *index_info_mapping[index_id];
        std::string full_idx_name = tbl_info.namespace_ + "." + idx_info.name;
        DB_WARNING("full_idx_name: %s, %ld", full_idx_name.c_str(), 
            index_name_id_mapping.size());
        index_name_id_mapping.erase(full_idx_name);
        DB_WARNING("full_idx_name deleted: %ld", index_name_id_mapping.size());
        index_info_mapping.erase(index_id);
    }

    delete tbl_info.file_proto;
    auto _pool = tbl_info.pool;
    auto _factory = tbl_info.factory;
    Bthread bth;
    bth.run([table_id, _pool, _factory]() {
            bthread_usleep(3600 * 1000 * 1000L);
            delete _factory;
            delete _pool; 
            });

    table_info_mapping.erase(table_id);
    return;
}

void SchemaFactory::update_schema_conf(const std::string& table_name, 
                                       const pb::SchemaConf &schema_conf, 
                                       pb::SchemaConf& mem_conf) {
    
    update_schema_conf_common(table_name, schema_conf, &mem_conf);
}

// not thread-safe
int SchemaFactory::update_table_internal(SchemaMapping& background, const pb::SchemaInfo &table) {
    auto& table_info_mapping = background.table_info_mapping;
    auto& table_name_id_mapping = background.table_name_id_mapping;
    auto& db_info_mapping = background.db_info_mapping;
    auto& db_name_id_mapping = background.db_name_id_mapping;
    auto& global_index_id_mapping = background.global_index_id_mapping;

    //DB_WARNING("table:%s", table.ShortDebugString().c_str());
    if (!_is_init) {
        DB_FATAL("SchemaFactory not initialized");
        return -1;
    }
    if (table.has_deleted() && table.deleted()) {
        delete_table(table, background);
        return 1;
    }
    //TODO: change name to id
    if (!table.has_database_id() 
            || !table.has_table_id()
            || !table.has_database()
            || !table.has_table_name()) {
        DB_FATAL("missing fields in SchemaInfo");
        return -1;
    }
    if (table.partition_num() != 1) {
        DB_FATAL("invalid partion_num: %d", table.partition_num());
        return -1;
    }
    int64_t database_id = table.database_id();
    int64_t table_id = table.table_id();

    const std::string& _db_name = table.database();
    const std::string& _tbl_name = table.table_name();
    const std::string& _namespace = table.namespace_name();

    //2. copy the temp DescriptorProto and build the proto
    DatabaseInfo db_info;
    db_info.id = database_id;
    db_info.name = _db_name;
    db_info.namespace_ = _namespace;

    // create table if not exists
    std::string table_name("table_" + std::to_string(table_id));
    //std::string old_tbl_name;
    std::unordered_set<int64_t> last_indics;

    SmartTable tbl_info_ptr = std::make_shared<TableInfo>();
    if (table_info_mapping.count(table_id) == 0) {
        if (nullptr == (tbl_info_ptr->file_proto = new (std::nothrow)FileDescriptorProto)) {
            DB_FATAL("create FileDescriptorProto failed");
            return -1;
        }
    } else {
        *tbl_info_ptr = *table_info_mapping[table_id];
        TableInfo& tbl_info = *tbl_info_ptr;
        std::copy(tbl_info.indices.begin(), tbl_info.indices.end(), std::inserter(last_indics, last_indics.end()));
        // need not  update when version GE
        if (tbl_info.version >= table.version()) {
            //DB_WARNING("need not  update, orgin version:%ld, new version:%ld, table_id:%ld", 
                    //tbl_info.version, table.version(), table_id);
            return 0;
        }
        //old_tbl_name = tbl_info.name;
        // file_proto build完的内容不能删除
        tbl_info.file_proto->Clear();
        tbl_info.fields.clear();
        tbl_info.indices.clear();
        tbl_info.dists.clear();
    }
    TableInfo& tbl_info = *tbl_info_ptr;
    tbl_info.file_proto->mutable_options()->set_cc_enable_arenas(true);
    tbl_info.file_proto->set_name(std::to_string(database_id) + ".proto");
    tbl_info.tbl_proto = tbl_info.file_proto->add_message_type();
    tbl_info.id = table_id;
    tbl_info.db_id = database_id;
    tbl_info.partition_num = table.partition_num(); //TODO
    if (!table.has_byte_size_per_record() || table.byte_size_per_record() < 1) {
        tbl_info.byte_size_per_record = 1;
    } else {
        tbl_info.byte_size_per_record = table.byte_size_per_record();
    }
    if (table.has_region_split_lines() && table.region_split_lines() != 0) {
        tbl_info.region_split_lines = table.region_split_lines();
    } else {
        tbl_info.region_split_lines = table.region_size() / tbl_info.byte_size_per_record;
    }
    if (table.has_schema_conf()) {
        update_schema_conf(_tbl_name, table.schema_conf(), tbl_info.schema_conf);
    } 
    tbl_info.version = table.version();
    tbl_info.name = _db_name + "." + _tbl_name;
    tbl_info.short_name = _tbl_name;
    tbl_info.tbl_proto->set_name(table_name);
    tbl_info.namespace_ = _namespace;
    tbl_info.resource_tag = table.resource_tag();
    tbl_info.charset = table.charset();
    tbl_info.engine = pb::ROCKSDB;
    if (table.has_engine()) {
        tbl_info.engine = table.engine();
    }
    if (table.has_replica_num()) {
        tbl_info.replica_num = table.replica_num(); 
    }
    if (table.has_ttl_duration()) {
        tbl_info.ttl_duration = table.ttl_duration();
        DB_DEBUG("table:%s ttl_duration:%ld", tbl_info.name.c_str(), tbl_info.ttl_duration);
    }
    for (auto& dist : table.dists()) {
        DistInfo dist_info;
        dist_info.logical_room = dist.logical_room();
        dist_info.count = dist.count();
        tbl_info.dists.push_back(dist_info);
    }
    std::unique_ptr<DescriptorPool> tmp_pool(new(std::nothrow)DescriptorPool);
    if (tmp_pool == nullptr) {
        DB_FATAL("create FileDescriptorProto failed");
        return -1;
    }
    std::unique_ptr<DynamicMessageFactory> tmp_factory(
        new(std::nothrow)DynamicMessageFactory(tmp_pool.get()));
    if (tmp_factory == nullptr) {
        DB_FATAL("create DynamicMessageFactory failed");
        return -1;
    }

    //1. create temp DescriptorProto using the input schema
    int field_cnt = table.fields_size();
    std::ostringstream new_fields_sign;
    int pb_idx = 0;
    for (int idx = 0; idx < field_cnt; ++idx) {
        const pb::FieldInfo& field = table.fields(idx);
        if (field.deleted()) {
            continue;
        }
        if (!field.has_field_id() 
                || !field.has_mysql_type() 
                || !field.has_field_name()) {
            DB_FATAL("missing field id (type or name)");
            return -1;
        }
        if (field.auto_increment()) {
            tbl_info.auto_inc_field_id = field.field_id();
        }
        FieldDescriptorProto* field_proto = tbl_info.tbl_proto->add_field();
        if (!field_proto) {
            DB_FATAL("add field failed: %d", field.has_field_id());
            return -1;
        }
        field_proto->set_name(field.field_name());
        //FieldDescriptorProto::Type proto_type;
        int proto_type = primitive_to_proto_type(field.mysql_type());
        if (proto_type == -1) {
            DB_FATAL("mysql_type %d not supported.", field.mysql_type());
            return -1;
        }
        field_proto->set_type((FieldDescriptorProto::Type)proto_type);
        field_proto->set_number(field.field_id());
        field_proto->set_label(FieldDescriptorProto::LABEL_OPTIONAL);
        new_fields_sign << field.field_id() << ":";
        new_fields_sign << proto_type << ";";

        FieldInfo field_info;
        field_info.id = field.field_id();
        field_info.pb_idx = pb_idx++;
        field_info.table_id = table_id;
        field_info.name = tbl_info.name + "." + field.field_name();
        field_info.short_name = field.field_name();
        field_info.type = field.mysql_type();
        field_info.can_null = field.can_null();
        field_info.auto_inc = field.auto_increment();
        field_info.deleted = field.deleted();
        field_info.comment = field.comment();
        field_info.default_value = field.default_value();
        field_info.on_update_value = field.on_update_value();
        if (field.has_default_value()) {
            field_info.default_expr_value.type = pb::STRING;
            field_info.default_expr_value.str_val = field_info.default_value;
            field_info.default_expr_value.cast_to(field_info.type);
        }
        if (field_info.type == pb::STRING || field_info.type == pb::HLL) {
            field_info.size = -1;
        } else {
            field_info.size = get_num_size(field_info.type);
            if (field_info.size == -1) {
                DB_FATAL("get_num_size type %d not supported.", field.mysql_type());
                return -1;
            }
        }
        tbl_info.fields.push_back(field_info);
        //DB_WARNING("field_name:%s, field_id:%d", field_info.name.c_str(), field_info.id);
    }
    bool pb_need_update = tbl_info.fields_sign != new_fields_sign.str();
    DB_NOTICE("double_buffer_write pb_need_update:%d, old:%s new:%s table:%s ", pb_need_update,
            tbl_info.fields_sign.c_str(), new_fields_sign.str().c_str(), table.ShortDebugString().c_str());
    tbl_info.fields_sign = new_fields_sign.str();


    if (pb_need_update) {
        const FileDescriptor* db_desc = tmp_pool->BuildFile(*tbl_info.file_proto);
        if (db_desc == nullptr) {
            DB_FATAL("build proto_file [%ld] failed, %s", database_id, tbl_info.file_proto->DebugString().c_str());
            return -1;
        }
        const Descriptor* descriptor = db_desc->FindMessageTypeByName(table_name);
        if (descriptor == nullptr) {
            DB_FATAL("FindMessageTypeByName [%ld] failed.", table_id);
            return -1;
        }
        auto del_pool = tbl_info.pool;
        auto del_factory = tbl_info.factory;
        tbl_info.pool = tmp_pool.release();
        tbl_info.factory = tmp_factory.release();
        tbl_info.tbl_desc = descriptor;
        tbl_info.msg_proto = tbl_info.factory->GetPrototype(tbl_info.tbl_desc);

        Bthread bth;
        bth.run([table_id, del_pool, del_factory]() {
                // 延迟删除
                bthread_usleep(3600 * 1000 * 1000L);
                delete del_factory;
                delete del_pool; 
                });
    }

    // create name => id mapping
    db_name_id_mapping[_namespace + "." + _db_name] = database_id;

    DB_WARNING("db_name_id_mapping: %s->%ld", std::string(_namespace + "." + _db_name).c_str(), 
        database_id);

    std::string _db_table(_namespace + "." + _db_name + "." + _tbl_name);
    //old_tbl_name = _namespace + "." + old_tbl_name;
    //if (!old_tbl_name.empty() && old_tbl_name != _db_table) {
        //table_name_id_mapping.erase(old_tbl_name);
        //_db_table = _namespace + "." + _db_name + "." + table.new_table_name();
    //}
    table_name_id_mapping[_db_table] = table_id;
    //get all pk fields descriptor
    size_t index_cnt = table.indexs_size();
    const pb::IndexInfo* pk_index = nullptr;
    for (size_t idx = 0; idx < index_cnt; ++idx) {
        const pb::IndexInfo& cur = table.indexs(idx);
        if (cur.index_id() == table_id) {
            pk_index = &cur;
            break;
        }
    }
    if (pk_index == nullptr) {
        DB_FATAL("find pk_index failed: %ld, %ld", database_id, table_id);
        return -1;
    }
    for (size_t idx = 0; idx < index_cnt; ++idx) {
        const pb::IndexInfo& cur = table.indexs(idx);
        int64_t index_id = cur.index_id();
        DB_WARNING("schema_factory_update_index: %ld", index_id);
        last_indics.erase(index_id);
        update_index(tbl_info, cur, pk_index, background);
        if (cur.index_type() == pb::I_PRIMARY
                || cur.is_global() == true) {
            global_index_id_mapping[index_id] = table_id;
        }
        tbl_info.indices.push_back(index_id);
    }
    //删除index索引。
    auto& index_info_mapping = background.index_info_mapping;
    auto& index_name_id_mapping = background.index_name_id_mapping; 
    for (auto index_id : last_indics) {
        auto index_info_iter = index_info_mapping.find(index_id);
        if (index_info_iter != index_info_mapping.end()) {
            std::string fullname = tbl_info.namespace_ + "."  + index_info_iter->second->name;
            index_info_mapping.erase(index_info_iter);
            if (index_name_id_mapping.erase(fullname) != 1) {
                DB_WARNING("delete index_name_id_mapping error.");
            }
            DB_NOTICE("delete index info: index_id[%lld] index_name[%s].", 
                index_id, fullname.c_str());
        }
    }
    

    db_info_mapping[database_id] = db_info;
    table_info_mapping[table_id] = tbl_info_ptr;
    return 1;
}

//TODO, string index type
void SchemaFactory::update_index(TableInfo& table_info, const pb::IndexInfo& index, 
        const pb::IndexInfo* pk_index, SchemaMapping& background) {

    DB_NOTICE("double_buffer_write index_info [%s]", index.ShortDebugString().c_str());
    auto& index_info_mapping = background.index_info_mapping;
    auto& index_name_id_mapping = background.index_name_id_mapping;
    SmartIndex idx_info_ptr = std::make_shared<IndexInfo>();
    std::string old_idx_name;
    //如果表存在，需要清空里面的内容
    if (index_info_mapping.count(index.index_id()) != 0) {
        *idx_info_ptr = *index_info_mapping[index.index_id()];
        IndexInfo& idx_info = *idx_info_ptr;
        old_idx_name = idx_info.name;
        idx_info.fields.clear();
        idx_info.pk_fields.clear();
        idx_info.pk_pos.clear();
    }

    IndexInfo& idx_info = *idx_info_ptr;
    if (index.is_global()) {
        idx_info.is_global = true;
    }
    idx_info.version = table_info.version;
    idx_info.pk   = table_info.id;
    idx_info.id   = index.index_id();
    std::string lower_index_name = index.index_name();
    std::transform(lower_index_name.begin(), lower_index_name.end(), 
                    lower_index_name.begin(), ::tolower);
    idx_info.name = table_info.name + "." + lower_index_name;
    idx_info.short_name = lower_index_name;
    idx_info.type = index.index_type();
    idx_info.state = index.state();
    idx_info.segment_type = index.segment_type();
    if (index.has_storage_type()) {
        idx_info.storage_type = index.storage_type();
    }
    int field_cnt = index.field_ids_size();

    //用于构建 std::vector<std::pair<int,int> > pk_pos;
    std::unordered_map<int32_t, int32_t> id_map;

    bool nullable = false;
    if (idx_info.type == pb::I_KEY || idx_info.type == pb::I_UNIQ) {
        idx_info.length = 1; //nullflag
    } else {
        idx_info.length = 0;
    }
    for (int idx = 0; idx < field_cnt; ++idx) {
        FieldInfo* info = table_info.get_field_ptr(index.field_ids(idx));
        if (info == nullptr) {
            DB_FATAL("table %ld index %ld field %d not exist", 
                    table_info.id, idx_info.id, index.field_ids(idx));
            return;
        }
        idx_info.fields.push_back(*info);

        //记录field_id在index_bytes中的对应位置
        id_map.insert(std::make_pair(info->id, idx_info.length));
        //DB_WARNING("index:%ld, field:%d, length:%d", idx_info.id, info.id, idx_info.length);
        if (info->can_null) {
            nullable = true;
        }
        if (info->size == -1) {
            idx_info.length = -1;
        } else if (idx_info.length != -1) {
            idx_info.length += info->size;
        }
    }
    if (nullable) {
        idx_info.length = -1;
    }
    //DB_WARNING("index:%ld, index_length:%d", idx_info.id, idx_info.length);

    //只有二级索引需要保存pk_fields
    if (idx_info.type == pb::I_KEY || idx_info.type == pb::I_UNIQ) {
        //如果有变长或nullable字段，则不进行压缩
        if (idx_info.length == -1) {
            id_map.clear();
        }
        int32_t pk_length = 0;
        for (int idx = 0; idx < pk_index->field_ids_size(); ++idx) {
            int32_t field_id = pk_index->field_ids(idx);
            FieldInfo* info = table_info.get_field_ptr(field_id);
            if (info == nullptr) {
            DB_FATAL("table %ld index %ld pk field %d not exist", 
                    table_info.id, idx_info.id, field_id);
                return ;
            }
            if (info->size == -1) {
                pk_length = -1;
                break;
            }
            if (id_map.count(field_id) != 0) {
                //重建pk时，该field从二级索引读取
                idx_info.pk_pos.push_back(std::make_pair(1, id_map[field_id]));
            } else {
                //重建pk时，该field从主键读取
                idx_info.pk_fields.push_back(*info);
                idx_info.pk_pos.push_back(std::make_pair(-1, pk_length==-1? 0 : pk_length));
                pk_length += info->size;
            }
        }
        //pk中有变长字段，则不进行主键压缩
        if (pk_length == -1) {
            idx_info.pk_fields.clear();
            idx_info.pk_pos.clear();
            for (int idx = 0; idx < pk_index->field_ids_size(); ++idx) {
                int32_t field_id = pk_index->field_ids(idx);
                FieldInfo* info = table_info.get_field_ptr(field_id);
                if (info != nullptr) {
                    idx_info.pk_fields.push_back(*info);
                }
            }
        }

        //判断index和pk是否有overlap
        //length=-1时overlap一定为false, length>0时overlap仍可能为false）
        if (idx_info.length == -1) {
            idx_info.overlap = false;
        } else if (idx_info.pk_fields.size() == (uint32_t)pk_index->field_ids_size()) {
            idx_info.overlap = false;
        } else {
            idx_info.overlap = true;
        }
    }
    index_info_mapping[idx_info.id] = idx_info_ptr;
    std::string fullname = idx_info.name;
    if (!old_idx_name.empty() && old_idx_name != fullname) {
        index_name_id_mapping.erase(old_idx_name);
    }
    fullname = table_info.namespace_ + "."  + idx_info.name;
    DB_WARNING("index full name:%s, %ld, %d", fullname.c_str(), idx_info.id, idx_info.overlap);
    index_name_id_mapping[fullname] = idx_info.id;
}

void SchemaFactory::update_regions(
        const RegionVec& regions) {
    bthread::execution_queue_execute(_region_queue_id, regions);
}
int SchemaFactory::update_regions_double_buffer(
        void* meta, bthread::TaskIterator<RegionVec>& iter) {
    SchemaFactory* factory = (SchemaFactory*)meta;
    TimeCost cost;
    factory->update_regions_double_buffer(iter);
    SELF_TRACE("update_regions_double_buffer time:%ld", cost.get_time());
    return 0;
}
// 心跳更新时，如果region有分裂，需要保证分裂出来的region与源region同时更新
// todo liuhuicong 更新时判断所有的start/end是否合法（重叠，空洞）
void SchemaFactory::update_regions_double_buffer(bthread::TaskIterator<RegionVec>& iter) {
    // tableid => (partion => (start_key => region))
    std::map<int64_t, std::map<int,
        std::map<std::string, const pb::RegionInfo*>>> table_key_region_map;
    for (; iter; ++iter) {
        for (auto& region : *iter) {
            int64_t table_id = region.table_id();
            int p_id = region.partition_id();
            const std::string& start_key = region.start_key();
            if (!start_key.empty() && start_key == region.end_key()) {
                DB_WARNING("table id:%ld region id:%ld is empty can`t add to map",
                           table_id, region.region_id());
                continue;
            }
            table_key_region_map[table_id][p_id][start_key] = &region;
        }
    }
    for (auto& table_region : table_key_region_map) {
        int64_t table_id = table_region.first;
        std::function<size_t(std::unordered_map<int64_t, TableRegionPtr>&)> update_region_table_func = 
            std::bind(&SchemaFactory::update_regions_table, this, std::placeholders::_1, table_id, table_region.second);

        _table_region_mapping.Modify(
           update_region_table_func
        );
    }
}
void SchemaFactory::update_regions_double_buffer_sync(const RegionVec& regions) {
    // tableid => (partion => (start_key => region))
    std::map<int64_t, std::map<int,
        std::map<std::string, const pb::RegionInfo*>>> table_key_region_map;
    for (auto& region : regions) {
        int64_t table_id = region.table_id();
        int p_id = region.partition_id();
        const std::string& start_key = region.start_key();
        if (!start_key.empty() && start_key == region.end_key()) {
            DB_WARNING("table id:%ld region id:%ld is empty can`t add to map",
                      table_id, region.region_id());
            continue;
        }
        table_key_region_map[table_id][p_id][start_key] = &region;
    }
    for (auto& table_region : table_key_region_map) {
        int64_t table_id = table_region.first;
        std::function<size_t(std::unordered_map<int64_t, TableRegionPtr>&)> update_region_table_func = 
            std::bind(&SchemaFactory::update_regions_table, this, std::placeholders::_1, table_id, table_region.second);

        _table_region_mapping.Modify(
            update_region_table_func
        );
    }
}
void SchemaFactory::update_region(TableRegionPtr table_region_ptr, 
                                  const pb::RegionInfo& region) {
    if (region.has_deleted() && region.deleted()) {
        DB_WARNING("region:%s deleted", region.ShortDebugString().c_str());
        std::vector<StrInt64Map>& vec = table_region_ptr->key_region_mapping;
        if (vec.size() < 1) {
            return;
        }
        StrInt64Map& key_reg_map = vec[0];
        key_reg_map.erase(region.start_key());
        table_region_ptr->region_info_mapping.erase(region.region_id());
        return;
    }
    pb::RegionInfo orgin_region;
    table_region_ptr->get_region_info(region.region_id(), orgin_region);
    //不允许version 回退
    if (orgin_region.version() > region.version()) {
        DB_WARNING("no roll back, region_id: %ld, old_ver:%d, ver:%d", 
                   region.region_id(), orgin_region.version(), region.version());
        return;
    }
    std::vector<StrInt64Map>& vec = table_region_ptr->key_region_mapping;
    if (vec.size() < 1) {
        vec.resize(1);
    }
    StrInt64Map& key_reg_map = vec[0];
    key_reg_map.insert(std::make_pair(region.start_key(), region.region_id()));

    table_region_ptr->insert_region_info(region);
    DB_DEBUG("region_id: %ld ver:%d key:(%s, %s), update success", 
               region.region_id(), region.version(), 
               str_to_hex(region.start_key()).c_str(), 
               str_to_hex(region.end_key()).c_str());
}
void SchemaFactory::clear_region(TableRegionPtr table_region_ptr, 
                                 std::map<std::string, int64_t>& clear_regions) {
    std::vector<StrInt64Map>& vec = table_region_ptr->key_region_mapping;
    if (vec.size() < 1) {
        return;
    }
    StrInt64Map& key_reg_map = vec[0];
    for (auto iter : clear_regions) {
        key_reg_map.erase(iter.first);
        table_region_ptr->region_info_mapping.erase(iter.second);
        DB_WARNING("clear region_id:%ld start_key:%s", iter.second, 
                   str_to_hex(iter.first).c_str());
    }
}
// new_start_key < origin_start_key
void SchemaFactory::get_clear_regions(const std::string& new_start_key, 
                                      const std::string& origin_start_key,
                                     TableRegionPtr table_region_ptr,
                        std::map<std::string, int64_t>& clear_regions) {
    //获取key_region_map中新旧start key之间的所有key，这些key是已经发生merge的，需要删除，
    //包括origin_region
    bool is_over = false;
    std::vector<int64_t> region_ids;
    std::string key = new_start_key;
    std::vector<StrInt64Map>& vec = table_region_ptr->key_region_mapping;
    StrInt64Map& key_reg_map = vec[0];
    auto region_iter = key_reg_map.find(new_start_key);
    while (region_iter != key_reg_map.end()) {
        if (key.empty() || key > origin_start_key) {
            //为空则为最后一个region
            if (is_over) {
                break;
            } else {
                clear_regions.clear();
                return;
            }
        }
        if (key != region_iter->first) {
            clear_regions.clear();
            DB_FATAL("region id:%ld, nonsequence start_key:%s vs %s", 
                    region_iter->second, str_to_hex(key).c_str(), 
                    str_to_hex(region_iter->first).c_str());
            return;
        }
        pb::RegionInfo region;
        int64_t region_id = region_iter->second;
        int ret = table_region_ptr->get_region_info(region_id, region);
        if (ret < 0) {
            DB_FATAL("region_id:%ld is not in map", region_id);
            clear_regions.clear();
            return;
        }
        DB_WARNING("region_id:%ld version:%ld start_key:%s end_key:%s need clear", 
                   region_id, region.version(), 
                   str_to_hex(region.start_key()).c_str(),
                   str_to_hex(region.end_key()).c_str());
        clear_regions[key] = region_id;
        key = region.end_key();
        if (key == origin_start_key) {
            is_over = true;
        }
        region_iter++;
    }
}

size_t SchemaFactory::update_regions_table(
    std::unordered_map<int64_t, TableRegionPtr>& table_region_mapping, int64_t table_id, 
    std::map<int, std::map<std::string, const pb::RegionInfo*>>& key_region_map) {

    if (table_region_mapping.count(table_id) == 0) {
        table_region_mapping[table_id] = std::make_shared<TableRegionInfo>();
    }

    TableRegionPtr table_region_ptr = table_region_mapping[table_id];
    
    DB_NOTICE("double_buffer_write update_regions_table table_id[%ld]", table_id);
    for (auto& start_key_region : key_region_map) {
        auto& start_key_region_map = start_key_region.second;
        std::vector<const pb::RegionInfo*> last_regions;
        std::map<std::string, int64_t> clear_regions;
        //std::string start_key;
        std::string end_key;
        for (auto iter = start_key_region_map.begin(); iter != start_key_region_map.end(); ++iter) {
            const pb::RegionInfo& region = *iter->second;
            DB_DEBUG("double_buffer_write update region_info:%s", region.ShortDebugString().c_str());
            pb::RegionInfo orgin_region;
            int ret = table_region_ptr->get_region_info(region.region_id(), orgin_region);
            if (ret < 0) {
                DB_DEBUG("region:%s", region.ShortDebugString().c_str());
                if (last_regions.size() != 0) {
                    //DB_WARNING("last_region:%s, region:%s",
                    //        last_region->ShortDebugString().c_str(), region.ShortDebugString().c_str());
                    auto last_region = last_regions.back();
                    if (last_region->end_key() != region.start_key()) {
                        last_regions.clear();
                        clear_regions.clear();
                        DB_FATAL("last_region->end_key():%s != region.start_key():%s", 
                                last_region->end_key().c_str(), region.start_key().c_str());
                        continue;
                    }
                    last_regions.push_back(&region);
                    if (region.end_key() == end_key) {
                        clear_region(table_region_ptr, clear_regions);
                        for (auto r : last_regions) {
                            update_region(table_region_ptr, *r);
                            DB_DEBUG("update regions %s", r->ShortDebugString().c_str());
                        }
                        clear_regions.clear();
                        last_regions.clear();
                    } else if (end_key_compare(region.end_key(), end_key) > 0) {
                        DB_FATAL("region.end_key:%s > end_key:%s", 
                                str_to_hex(region.end_key()).c_str(), 
                                str_to_hex(end_key).c_str());
                        clear_regions.clear();
                        last_regions.clear();
                    }
                } else {
                    DB_DEBUG("region:%s", region.ShortDebugString().c_str());
                    // 先判断加入的region是否与现有的region有范围重叠
                    std::vector<StrInt64Map>& vec = table_region_ptr->key_region_mapping;
                    if (vec.size() < 1) {
                        vec.resize(1);
                    }
                    StrInt64Map& key_reg_map = vec[0];
                    auto region_iter = key_reg_map.lower_bound(region.start_key());
                    if (region_iter != key_reg_map.begin()) {
                        int64_t pre_region_id = (--region_iter)->second;
                        pb::RegionInfo pre_info;
                        table_region_ptr->get_region_info(pre_region_id, pre_info);
                        // TODO:应该严格的按照last_end_key == start_key 担心有坑
                        if (end_key_compare(pre_info.end_key(), region.start_key()) > 0) {
                            DB_WARNING("region:%ld %ld is overlapping", 
                                    pre_region_id, region.region_id());
                            continue;
                        }
                    }
                    DB_DEBUG("region:%s", region.ShortDebugString().c_str());
                    update_region(table_region_ptr, region);
                }
            } else if (region.version() > orgin_region.version()) {
                DB_DEBUG("region:%s, orgin_region:%s",
                        region.ShortDebugString().c_str(), orgin_region.ShortDebugString().c_str());
                DB_DEBUG("region id:%ld, new vs origin (%ld, %s, %s) vs (%ld, %s, %s)",
                         region.region_id(), region.version(), str_to_hex(region.start_key()).c_str(), 
                         str_to_hex(region.end_key()).c_str(), orgin_region.version(), 
                         str_to_hex(orgin_region.start_key()).c_str(), 
                         str_to_hex(orgin_region.end_key()).c_str());
                clear_regions.clear();
                last_regions.clear();
                //保证整体更新
                if (region.start_key() < orgin_region.start_key()
                        && end_key_compare(region.end_key(), orgin_region.end_key()) < 0) {
                    //start key变小，end key变小，即发生split又发生merge
                    get_clear_regions(region.start_key(), orgin_region.start_key(), 
                                    table_region_ptr, clear_regions);
                    if (clear_regions.size() < 2) {
                        clear_regions.clear();
                        last_regions.clear();
                        continue;
                    }
                    last_regions.push_back(&region);
                    end_key = orgin_region.end_key();
                } else if (region.start_key() < orgin_region.start_key()
                          && end_key_compare(region.end_key(), orgin_region.end_key()) == 0) {
                    //start key变小， end key不变，发生merge
                    get_clear_regions(region.start_key(), orgin_region.start_key(), 
                                      table_region_ptr, clear_regions);
                    if (clear_regions.size() >= 2) {
                        //包含orgin region和前一个已经发生merge的空region，至少有两个
                        clear_region(table_region_ptr, clear_regions);
                        update_region(table_region_ptr, region);
                    }
                } else if (region.start_key() == orgin_region.start_key()
                           && end_key_compare(region.end_key(), orgin_region.end_key()) < 0) {
                    //start key不变，end key变小，发生split
                    last_regions.push_back(&region);
                    end_key = orgin_region.end_key();
                } else if (region.start_key() == orgin_region.start_key()
                           && region.end_key() == orgin_region.end_key()) {
                    //仅version增大
                    update_region(table_region_ptr, region);
                } else {
                    //其他情况，可能有问题

                }
                continue;
            } else {
                DB_DEBUG("region:%s, orgin_region:%s",
                        region.ShortDebugString().c_str(), orgin_region.ShortDebugString().c_str());
                update_region(table_region_ptr, region);
            }
            //DB_WARNING("region:%s, orgin_region:%s",
            //        region.ShortDebugString().c_str(), orgin_region.ShortDebugString().c_str());
            //last_region = nullptr;
        }
    }
    return 1;
}

void SchemaFactory::update_leader(const pb::RegionInfo& region) {
    int64_t table_id = region.table_id();
    std::function<size_t(std::unordered_map<int64_t, TableRegionPtr>&)> func = 
        std::bind(double_buffer_table_region_update_leader, std::placeholders::_1, 
            table_id, region.region_id(), region.leader());
    _table_region_mapping.Modify(
       func 
    );
}

void SchemaFactory::update_user(const pb::UserPrivilege& user) {
    const std::string& username = user.username();
    const std::string& password = user.password();
    // 每次都新建一个UserInfo，所以内部无需加锁
    std::shared_ptr<UserInfo> user_info(new (std::nothrow)UserInfo);
    BAIDU_SCOPED_LOCK(_update_user_mutex);
    if (_user_info_mapping.count(username) == 1) {
        // need not update when version GE
        if (_user_info_mapping[username]->version >= user.version()) {
            return;
        }
    }
    user_info->username = username;
    user_info->password = password;
    scramble(user_info->scramble_password,
        ("\x26\x4f\x37\x58"
        "\x43\x7a\x6c\x53"
        "\x21\x25\x65\x57"
        "\x62\x35\x42\x66"
        "\x6f\x34\x62\x49"),
        password.c_str());
    user_info->namespace_ = user.namespace_name();
    user_info->namespace_id = user.namespace_id();
    user_info->version = user.version();

    uint32_t db_cnt  = user.privilege_database_size();
    uint32_t tbl_cnt = user.privilege_table_size();

    for (uint32_t idx = 0; idx < db_cnt; ++idx) {
        const pb::PrivilegeDatabase& db = user.privilege_database(idx);
        user_info->database[db.database_id()] = db.database_rw();
        user_info->all_database.insert(db.database_id());
    }
    for (uint32_t idx = 0; idx < tbl_cnt; ++idx) {
        const pb::PrivilegeTable& tbl = user.privilege_table(idx);
        user_info->table[tbl.table_id()] = tbl.table_rw();
        user_info->all_database.insert(tbl.database_id());
    }
    //TODO ip and bns access control
    user_info->need_auth_addr = user.need_auth_addr();
    if (user_info->need_auth_addr) {
        for (auto ip : user.ip()) {
            boost::trim(ip);
            user_info->auth_ip_set.insert(ip);
        }
        for (auto bns : user.bns()) {
            std::vector<std::string> instances;
            int ret = 0;
            boost::trim(bns);
            get_instance_from_bns(&ret, bns, instances, false);
            for (auto& instance : instances) {
                auto pos = instance.find(":");
                user_info->auth_ip_set.insert(instance.substr(0, pos));
            }
        }
    }
    _user_info_mapping[username] = user_info;
}

void SchemaFactory::update_show_db(const DataBaseVec& db_infos) {
    BAIDU_SCOPED_LOCK(_update_show_db_mutex);
    _show_db_info.clear();
    for (auto db_info : db_infos) {
        DatabaseInfo info;
        info.id = db_info.database_id();
        info.version = db_info.version();
        info.name = db_info.database();
        info.namespace_ = db_info.namespace_name();
        _show_db_info[info.id] = info;
    }
}

void SchemaFactory::update_statistics(const StatisticsVec& statistics) {
    std::map<int64_t, SmartStatistics> tmp_mapping;
    for (auto& st : statistics) {
        SmartStatistics ptr = std::make_shared<Statistics>(st);
        tmp_mapping[ptr->table_id()] = ptr;
        DB_WARNING("update statistics, table_id:%ld, version:%ld", ptr->table_id(), ptr->version());
    }

    std::function<int(SchemaMapping& schema_mapping, const std::map<int64_t, SmartStatistics>& mapping)> func =
        std::bind(&SchemaFactory::update_statistics_internal, this, std::placeholders::_1, std::placeholders::_2);
    _double_buffer_table.Modify(func, tmp_mapping);
}

int SchemaFactory::update_statistics_internal(SchemaMapping& background, const std::map<int64_t, SmartStatistics>& mapping) {
    auto& table_statistics_mapping = background.table_statistics_mapping;

    for (auto iter = mapping.begin(); iter != mapping.end(); iter++) {
        auto origin_iter = table_statistics_mapping.find(iter->first);
        if (origin_iter != table_statistics_mapping.end()) {
            if (iter->second->version() > origin_iter->second->version()) {
                table_statistics_mapping[iter->first] = iter->second;
            }
        } else {
            table_statistics_mapping[iter->first] = iter->second;
        }
    }

    return 1;
}


int64_t SchemaFactory::get_statis_version(int64_t table_id) {

    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return 0; 
    }
    auto& table_statistics_mapping = table_ptr->table_statistics_mapping;
    auto iter = table_statistics_mapping.find(table_id);
    if (iter != table_statistics_mapping.end()) {
        return iter->second->version();
    }
    return 0;
}

double SchemaFactory::get_histogram_ratio(int64_t table_id, int field_id, const ExprValue& lower, const ExprValue& upper) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return 1.0; 
    }

    auto& table_statistics_mapping = table_ptr->table_statistics_mapping;
    auto iter = table_statistics_mapping.find(table_id);
    if (iter != table_statistics_mapping.end()) {
        return iter->second->get_histogram_ratio(field_id, lower, upper);
    }

    return 1.0;
}

double SchemaFactory::get_cmsketch_ratio(int64_t table_id, int field_id, const ExprValue& value) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return 1.0; 
    }

    auto& table_statistics_mapping = table_ptr->table_statistics_mapping;
    auto iter = table_statistics_mapping.find(table_id);
    if (iter != table_statistics_mapping.end()) {
        return iter->second->get_cmsketch_ratio(field_id, value);
    }

    return 1.0;
}

SmartStatistics SchemaFactory::get_statistics_ptr(int64_t table_id) {
    if (table_id <= 0) {
        return nullptr;
    }
    
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return nullptr; 
    }

    auto& table_statistics_mapping = table_ptr->table_statistics_mapping;
    auto iter = table_statistics_mapping.find(table_id);
    if (iter != table_statistics_mapping.end()) {
        return iter->second;
    }

    return nullptr;
}

// create a new table record (aka. a table row)
SmartRecord SchemaFactory::new_record(TableInfo& info) {
    Message* message = info.msg_proto->New();
    if (message) {
        return SmartRecord(new (std::nothrow)TableRecord(message));
    } else {
        DB_FATAL("new fail, table_id: %ld", info.id);
        return SmartRecord(nullptr);
    }
}

// create a new table record (aka. a table row)
SmartRecord SchemaFactory::new_record(int64_t tableid) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return nullptr; 
    }
    auto& table_info_mapping = table_ptr->table_info_mapping;
    if (table_info_mapping.count(tableid) == 0) {
        DB_WARNING("no table found: %ld", tableid);
        return nullptr;
    }
    return new_record(*table_info_mapping.at(tableid));
}

DatabaseInfo SchemaFactory::get_database_info(int64_t databaseid) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return DatabaseInfo(); 
    }
    auto& db_info_mapping = table_ptr->db_info_mapping;
    if (db_info_mapping.count(databaseid) == 0) {
        return DatabaseInfo();
    }
    return db_info_mapping.at(databaseid);
}

pb::Engine SchemaFactory::get_table_engine(int64_t tableid) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return pb::ROCKSDB; 
    }
    auto& table_info_mapping = table_ptr->table_info_mapping;
    if (table_info_mapping.count(tableid) == 0) {
        return pb::ROCKSDB;
    }
    return table_info_mapping.at(tableid)->engine;
}

TableInfo SchemaFactory::get_table_info(int64_t tableid) {
    auto ptr = get_table_info_ptr(tableid);
    if (ptr != nullptr) {
        return *ptr;
    } else {
        return TableInfo();
    }
}

SmartTable SchemaFactory::get_table_info_ptr(int64_t tableid) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return nullptr; 
    }
    auto iter = table_ptr->table_info_mapping.find(tableid);
    if (iter == table_ptr->table_info_mapping.end()) {
        return nullptr;
    }
    return iter->second;
}

IndexInfo SchemaFactory::get_index_info(int64_t indexid) {
    auto ptr = get_index_info_ptr(indexid);
    if (ptr != nullptr) {
    return *ptr;
    } else {
        return IndexInfo();
    }
}

SmartIndex SchemaFactory::get_index_info_ptr(int64_t indexid) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return nullptr; 
    }
    auto iter = table_ptr->index_info_mapping.find(indexid);
    if (iter == table_ptr->index_info_mapping.end()) {
        return nullptr;
    }
    return iter->second;
}

std::string SchemaFactory::get_index_name(int64_t index_id) {
    std::string name = "";
    if (index_id == 0) {
        name = "not use index";
        return name;
    }

    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return "failed"; 
    }

    auto iter = table_ptr->index_info_mapping.find(index_id);
    if (iter == table_ptr->index_info_mapping.end()) {
        return "failed";
    }
    return iter->second->short_name;
}

std::shared_ptr<UserInfo> SchemaFactory::get_user_info(const std::string& user) {
    BAIDU_SCOPED_LOCK(_update_user_mutex);
    if (_user_info_mapping.count(user) == 0) {
        return nullptr;
    }
    return _user_info_mapping[user];
}

std::vector<std::string> SchemaFactory::get_db_list(const std::set<int64_t>& db) {
    std::vector<std::string> vec;
    BAIDU_SCOPED_LOCK(_update_show_db_mutex);
    for (auto id : db) {
        if (_show_db_info.count(id) == 1) {
            vec.push_back(_show_db_info[id].name);
        }
    }
    return vec;
}

std::vector<std::string> SchemaFactory::get_table_list(
        std::string namespace_, std::string db_name, UserInfo* user) {
    std::vector<std::string> vec; 
    int64_t db_id = 0;
    auto ret = get_database_id(namespace_ + "." + db_name, db_id);
    if (ret != 0) {
        return vec;
    }
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return vec; 
    }
    for (auto& table_pair : table_ptr->table_info_mapping) {
        auto& table_info = *(table_pair.second);
        if (table_info.db_id == db_id) {
            if (user->database.count(db_id) == 1 ||
                user->table.count(table_info.id) == 1) {
                vec.push_back(table_info.short_name);
            }
        }
    }
    return vec;
}
void SchemaFactory::get_all_table_version(std::unordered_map<int64_t, int64_t>& table_id_version_map) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return;
    }
    for (auto& table_pair : table_ptr->table_info_mapping) {
        table_id_version_map[table_pair.first] = table_pair.second->version;
    }
}

int SchemaFactory::get_region_info(int64_t table_id, int64_t region_id, pb::RegionInfo& info) {
    DoubleBufferedTableRegionInfo::ScopedPtr table_region_mapping_ptr;
    if (_table_region_mapping.Read(&table_region_mapping_ptr) != 0) {
        DB_WARNING("DoubleBufferedTableRegion read scoped ptr error."); 
        return -1; 
    }

    auto it = table_region_mapping_ptr->find(table_id);
    if (it == table_region_mapping_ptr->end()) {
        return -1;
    }
    return it->second->get_region_info(region_id, info);
}

int SchemaFactory::get_region_info(int64_t region_id, pb::RegionInfo& info) {
    // todo
    return 0;
}

int SchemaFactory::get_table_id(const std::string& table_name, int64_t& table_id) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return -1; 
    }
    auto& table_name_id_mapping = table_ptr->table_name_id_mapping;
    if (table_name_id_mapping.count(table_name) == 0) {
        return -1;
    }
    table_id = table_name_id_mapping.at(table_name);
    return 0;
}
int SchemaFactory::get_region_capacity(int64_t global_index_id, int64_t& region_capacity) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return -1;
    }
    auto& global_index_id_mapping = table_ptr->global_index_id_mapping;
    auto& table_info_mapping = table_ptr->table_info_mapping;
    if (global_index_id_mapping.count(global_index_id) == 0) {
        DB_WARNING("index_id: %ld not exist", global_index_id);
        return -1;
    }
    int64_t main_table_id = global_index_id_mapping.at(global_index_id);
    if (table_info_mapping.count(main_table_id) == 0) {
        return -1;
    }
    region_capacity = table_info_mapping.at(main_table_id)->region_split_lines;
    return 0;
}

bool SchemaFactory::get_merge_switch(int64_t table_id) {
    return is_switch_open(table_id, TABLE_SWITCH_MERGE);
}

bool SchemaFactory::get_separate_switch(int64_t table_id) {
    return is_switch_open(table_id, TABLE_SWITCH_SEPARATE);
}

bool SchemaFactory::is_switch_open(const int64_t table_id, const std::string& switch_name) {
        DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return false;
    }
    auto& table_info_mapping = table_ptr->table_info_mapping;
    if (table_info_mapping.count(table_id) == 0) {
        return false;
    }

    auto& pb_conf = table_info_mapping.at(table_id)->schema_conf;
    const google::protobuf::Reflection* reflection = pb_conf.GetReflection();
    const google::protobuf::Descriptor* descriptor = pb_conf.GetDescriptor();
    const google::protobuf::FieldDescriptor* field = nullptr;
    field = descriptor->FindFieldByName(switch_name);
    if (field == nullptr) {
        return false;
    }

    bool has_field = reflection->HasField(pb_conf, field);
    if (!has_field) {
        return false;
    }

    return reflection->GetBool(pb_conf, field);
}

void SchemaFactory::get_schema_conf_op_info(const int64_t table_id, int64_t& op_version, std::string& op_desc) {
    int ret = get_schema_conf_value<int64_t>(table_id, TABLE_OP_VERSION, op_version);
    if (ret < 0) {
        op_version = 0;
        op_desc = "no op";
        return;
    }

    ret = get_schema_conf_str(table_id, TABLE_OP_DESC, op_desc);
    if (ret < 0) {
       op_desc = "";
    }
}

template <class T>
int SchemaFactory::get_schema_conf_value(const int64_t table_id, const std::string& switch_name, T& value) {
        DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return -1;
    }
    auto& table_info_mapping = table_ptr->table_info_mapping;
    if (table_info_mapping.count(table_id) == 0) {
        return -1;
    }

    auto& pb_conf = table_info_mapping.at(table_id)->schema_conf;
    const google::protobuf::Reflection* reflection = pb_conf.GetReflection();
    const google::protobuf::Descriptor* descriptor = pb_conf.GetDescriptor();
    const google::protobuf::FieldDescriptor* field = nullptr;
    field = descriptor->FindFieldByName(switch_name);
    if (field == nullptr) {
        return -1;
    }

    bool has_field = reflection->HasField(pb_conf, field);
    if (!has_field) {
        return -1;
    }

    auto type = field->cpp_type();
    switch (type) {
        case google::protobuf::FieldDescriptor::CPPTYPE_INT32: {
            value = reflection->GetInt32(pb_conf, field);
        } break;
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32: {
            value = reflection->GetUInt32(pb_conf, field);
        } break;
        case google::protobuf::FieldDescriptor::CPPTYPE_INT64: {
            value = reflection->GetInt64(pb_conf, field);
        } break;
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64: {
            value = reflection->GetUInt64(pb_conf, field);
        } break;
        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT: {
            value = reflection->GetFloat(pb_conf, field);
        } break;
        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE: {
            value = reflection->GetDouble(pb_conf, field);
        } break;
        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL: {
            value = reflection->GetBool(pb_conf, field);
        } break;
        default: {
            return -1;
        }
    }

    return 0;
}

int SchemaFactory::get_schema_conf_str(const int64_t table_id, const std::string& switch_name, std::string& value) {
        DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return -1;
    }
    auto& table_info_mapping = table_ptr->table_info_mapping;
    if (table_info_mapping.count(table_id) == 0) {
        return -1;
    }

    auto& pb_conf = table_info_mapping.at(table_id)->schema_conf;
    const google::protobuf::Reflection* reflection = pb_conf.GetReflection();
    const google::protobuf::Descriptor* descriptor = pb_conf.GetDescriptor();
    const google::protobuf::FieldDescriptor* field = nullptr;
    field = descriptor->FindFieldByName(switch_name);
    if (field == nullptr) {
        return -1;
    }

    bool has_field = reflection->HasField(pb_conf, field);
    if (!has_field) {
        return -1;
    }

    if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
        return -1;
    }
    
    value = reflection->GetString(pb_conf, field);

    return 0;
}

int64_t SchemaFactory::get_ttl_duration(int64_t table_id) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return 0;
    }
    auto& table_info_mapping = table_ptr->table_info_mapping;
    if (table_info_mapping.count(table_id) == 0) {
        return 0;
    }
    return table_info_mapping.at(table_id)->ttl_duration;
}

int SchemaFactory::get_database_id(const std::string& db_name, int64_t& db_id) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return -1;
    }
    auto& db_name_id_mapping = table_ptr->db_name_id_mapping;
    if (db_name_id_mapping.count(db_name) == 0) {
        return -1;
    }
    db_id = db_name_id_mapping.at(db_name);
    return 0;
}
bool SchemaFactory::exist_tableid(int64_t table_id) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return false;
    }
    if (table_ptr->global_index_id_mapping.count(table_id) == 0) {
        return false;
    }
    return true;
}
// int SchemaFactory::get_column_id(const std::string& col_name, int32_t col_id) const {
//     if (bsl::HASH_NOEXIST == _column_name_id_mapping.get(col_name, &col_id)) {
//         return -1;
//     }
//     return 0;
// }

int SchemaFactory::get_index_id(int64_t table_id,
                                const std::string& index_name, 
                                int64_t& index_id) {
    DoubleBufferedTable::ScopedPtr table_ptr;
    if (_double_buffer_table.Read(&table_ptr) != 0) {
        DB_WARNING("read double_buffer_table error.");
        return -1;
    }
    auto& table_info_mapping = table_ptr->table_info_mapping;
    auto& index_name_id_mapping = table_ptr->index_name_id_mapping;
    if (table_info_mapping.count(table_id) == 0) {
        return -1;
    }
    std::string lower_index_name = index_name;
    std::transform(lower_index_name.begin(), lower_index_name.end(), lower_index_name.begin(), ::tolower);
    //primary mysql关键词
    if (lower_index_name == "primary") {
        index_id = table_id;
        return 0;
    }
    const TableInfo& tbl_info = *table_info_mapping.at(table_id);
    std::string full_index_name = tbl_info.namespace_ + "." + tbl_info.name + "." + lower_index_name;
    if (index_name_id_mapping.count(full_index_name) == 0) {
        return -1;
    }
    index_id = index_name_id_mapping.at(full_index_name);
    return 0;
}
int SchemaFactory::get_region_by_key(IndexInfo& index, 
        const pb::PossibleIndex* primary,
        std::map<int64_t, pb::RegionInfo>& region_infos,
        std::map<int64_t, pb::PossibleIndex>* region_primary) {
    return get_region_by_key(index.id, index, primary, region_infos, region_primary);
}
int SchemaFactory::get_region_by_key(int64_t main_table_id, 
                                    IndexInfo& index,
                                    const pb::PossibleIndex* primary,
                                    std::map<int64_t, pb::RegionInfo>& region_infos,
                                    std::map<int64_t, pb::PossibleIndex>* region_primary) { 
    region_infos.clear();
    if (region_primary != nullptr) {
        region_primary->clear();
    }
    DoubleBufferedTableRegionInfo::ScopedPtr table_region_mapping_ptr;
    if (_table_region_mapping.Read(&table_region_mapping_ptr) != 0) {
        DB_WARNING("DoubleBufferedTableRegion read scoped ptr error."); 
        return -1; 
    }

    auto it = table_region_mapping_ptr->find(index.id);
    if (it == table_region_mapping_ptr->end()) {
        DB_WARNING("index id[%ld] not in table_region_mapping", index.id);
        return -1;
    }
    auto frontground = it->second;
    auto& key_region_mapping = frontground->key_region_mapping;
    if (primary == nullptr) {
        if (key_region_mapping.size() != 1) {
            DB_WARNING("partion_num not supported:%ld, %lu", index.id, key_region_mapping.size());
            return -1;
        }
        StrInt64Map& map = key_region_mapping[0];
        for (auto& pair : map) {
            int64_t region_id = pair.second;
            frontground->get_region_info(region_id, region_infos[region_id]);
        }
        return 0;
    }
    pb::PossibleIndex template_primary;
    template_primary.set_index_id(primary->index_id());
    if (primary->has_sort_index()) {
        template_primary.mutable_sort_index()->CopyFrom(primary->sort_index());
    }
    template_primary.mutable_index_conjuncts()->CopyFrom(primary->index_conjuncts());

    auto record_template = TableRecord::new_record(main_table_id);
    int range_size = primary->ranges_size();
    for (const auto& range : primary->ranges()) {
        bool like_prefix = range.like_prefix();
        bool left_open = range.left_open();
        bool right_open = range.right_open();
        MutTableKey  start;
        MutTableKey  end;
        if (!range.left_pb_record().empty()) {
            auto left = record_template->clone(false);
            if (left->decode(range.left_pb_record()) != 0) {
                DB_FATAL("Fail to encode pb left, table:%ld", index.id);
                return -1;
            }
            if (left->encode_key(index, start, range.left_field_cnt(), false, like_prefix) != 0) {
                DB_FATAL("Fail to encode_key left, table:%ld", index.id);
                return -1;
            }
        } else {
            left_open = false;
        }
        if (!range.right_pb_record().empty()) {
            auto right = record_template->clone(false);
            if (right->decode(range.right_pb_record()) != 0) {
                DB_FATAL("Fail to encode pb right, table:%ld", index.id);
                return -1;
            }
            if (right->encode_key(index, end, range.right_field_cnt(), false, like_prefix) != 0) {
                DB_FATAL("Fail to encode_key right, table:%ld", index.id);
                return -1;
            }
        } else {
            right_open = false;
        }

        MutTableKey start_sentinel(start.data());
        if (!start.get_full() && left_open) {
            start_sentinel.append_u16(0xFFFF);
        }

        if (key_region_mapping.size() != 1) {
            DB_WARNING("partion_num not supported:%ld, %lu", index.id, key_region_mapping.size());
            return -1;
        }
        StrInt64Map& map = key_region_mapping[0];
        auto region_iter = map.upper_bound(start_sentinel.data());
        
        while (left_open && region_iter != map.end() && 
                boost::starts_with(region_iter->first, start.data())) {
            region_iter++;
        }
        if (region_iter != map.begin()) {
            --region_iter;
        }
        while (region_iter != map.end()) {
            if (end.data().empty() || region_iter->first <= end.data() ||
                    (!right_open && boost::starts_with(region_iter->first, end.data()))) {
                int64_t region_id = region_iter->second;
                frontground->get_region_info(region_id, region_infos[region_id]);
                if (range_size > 1 && region_primary != nullptr) {
                    if (region_primary->count(region_id) == 0) {
                        (*region_primary)[region_id].CopyFrom(template_primary);
                    }
                    (*region_primary)[region_id].add_ranges()->CopyFrom(range);
                }
            } else {
                break;
            }
            region_iter++;
        }
    }
    return 0;
}

// Get a list of new regions given a list of old regions
// used for transaction recovery after baikaldb crash
int SchemaFactory::get_region_by_key(
        const RepeatedPtrField<pb::RegionInfo>& input_regions,
        std::map<int64_t, pb::RegionInfo>& output_regions) {
    for (int idx = 0; idx < input_regions.size(); ++idx) {
        int64_t table_id = input_regions[idx].table_id();

        DoubleBufferedTableRegionInfo::ScopedPtr table_region_mapping_ptr;
        if (_table_region_mapping.Read(&table_region_mapping_ptr) != 0) {
            DB_WARNING("DoubleBufferedTableRegion read scoped ptr error."); 
            continue;
        }
        auto it = table_region_mapping_ptr->find(table_id);
        if (it == table_region_mapping_ptr->end()) {
            DB_WARNING("index id[%ld] not in table_region_mapping", table_id);
            continue;
        }
        //读 
        auto frontground = it->second;
        auto& key_region_mapping = frontground->key_region_mapping;

        const std::string& start = input_regions[idx].start_key();
        const std::string& end = input_regions[idx].end_key();

        if (key_region_mapping.size() != 1) {
            DB_WARNING("partion_num not supported:%ld, %lu", table_id, key_region_mapping.size());
            return -1;
        }
        StrInt64Map& map = key_region_mapping[0];
        auto region_iter = map.upper_bound(start);

        if (region_iter != map.begin()) {
            --region_iter;
        }
        while (region_iter != map.end()) {
            if (end.empty() || region_iter->first < end) {
                int64_t region_id = region_iter->second;
                frontground->get_region_info(region_id, output_regions[region_id]);
            } else {
                break;
            }
            region_iter++;
        }
    }
    return 0;
}

int SchemaFactory::get_region_by_key(IndexInfo& index,
        std::vector<SmartRecord>    records,
        std::map<int64_t, std::vector<SmartRecord>>& region_ids,
        std::map<int64_t, pb::RegionInfo>& region_infos) {
    region_ids.clear();
    region_infos.clear();

    DoubleBufferedTableRegionInfo::ScopedPtr table_region_mapping_ptr;
    if (_table_region_mapping.Read(&table_region_mapping_ptr) != 0) {
        DB_WARNING("DoubleBufferedTableRegion read scoped ptr error."); 
        return -1;
    }
    auto it = table_region_mapping_ptr->find(index.id);
    if (it == table_region_mapping_ptr->end()) {
        DB_WARNING("index id[%ld] not in table_region_mapping", index.id);
        return -1;
    }
    auto frontground = it->second;
    auto& key_region_mapping = frontground->key_region_mapping;
    for (auto& record : records) {
        MutTableKey  key;
        if (0 != key.append_index(index, record.get(), -1, false)) {
            DB_FATAL("Fail to encode_key, table:%ld", index.id);
            return -1;
        }
        if (index.type == pb::I_KEY) {
            if (0 != record->encode_primary_key(index, key, -1)) {
                DB_FATAL("Fail to append_pk_index, tab:%ld", index.id);
                return -1;
            }
        }
        if (key_region_mapping.size() != 1) {
            DB_WARNING("partion_num not supported:%ld, %lu", index.id, key_region_mapping.size());
            return -1;
        }
        StrInt64Map& map = key_region_mapping[0];
        auto region_iter = map.upper_bound(key.data());
        --region_iter;
        int64_t region_id = region_iter->second;
        region_ids[region_id].push_back(record);
        frontground->get_region_info(region_id, region_infos[region_id]);
    }
    //DB_WARNING("region_id:%ld", region_iter->second);
    return 0;
}

int SchemaFactory::get_region_by_key(IndexInfo& index,
         const std::vector<SmartRecord>& insert_records,
         const std::vector<SmartRecord>& delete_records,
         std::map<int64_t, std::vector<SmartRecord>>& insert_region_ids,
         std::map<int64_t, std::vector<SmartRecord>>& delete_region_ids,
         std::map<int64_t, pb::RegionInfo>& region_infos) {
    insert_region_ids.clear();
    delete_region_ids.clear();
    region_infos.clear();

    DoubleBufferedTableRegionInfo::ScopedPtr table_region_mapping_ptr;
    if (_table_region_mapping.Read(&table_region_mapping_ptr) != 0) {
        DB_WARNING("DoubleBufferedTableRegion read scoped ptr error."); 
        return -1; 
    }

    auto it = table_region_mapping_ptr->find(index.id);
    if (it == table_region_mapping_ptr->end()) {
        DB_WARNING("index id[%ld] not in table_region_mapping.", index.id);
        return -1;
    }
    auto frontground = it->second;
    auto& key_region_mapping = frontground->key_region_mapping;
    for (auto& record : insert_records) {
        MutTableKey  key;
        if (0 != key.append_index(index, record.get(), -1, false)) {
            DB_FATAL("Fail to encode_key, table:%ld", index.id);
            return -1;
        }
        if (index.type == pb::I_KEY) {
            if (0 != record->encode_primary_key(index, key, -1)) {
                DB_FATAL("Fail to append_pk_index, tab:%ld", index.id);
                return -1;
            }
        }
        if (key_region_mapping.size() != 1) {
            DB_WARNING("partion_num not supported:%ld, %lu", index.id, key_region_mapping.size());
            return -1;
        }
        StrInt64Map& map = key_region_mapping[0];
        auto region_iter = map.upper_bound(key.data());
        --region_iter;
        int64_t region_id = region_iter->second;
        insert_region_ids[region_id].push_back(record);
        frontground->get_region_info(region_id, region_infos[region_id]); 
    }
    for (auto& record : delete_records) {
        MutTableKey key;
        if (0 != key.append_index(index, record.get(), -1, false)) {
            DB_FATAL("Fail to encode_key, table:%ld", index.id);
            return -1;
        }
        if (index.type == pb::I_KEY) {
            if (0 != record->encode_primary_key(index, key, -1)) {
                DB_FATAL("Fail to append_pk_index, tab:%ld", index.id);
                return -1;
            }
        }
        if (key_region_mapping.size() != 1) {
            DB_WARNING("partion_num not supported:%ld, %lu", index.id, key_region_mapping.size());
            return -1;
        }
        StrInt64Map& map = key_region_mapping[0];
        auto region_iter = map.upper_bound(key.data());
        --region_iter;
        int64_t region_id = region_iter->second;
        delete_region_ids[region_id].push_back(record);
        frontground->get_region_info(region_id, region_infos[region_id]);
    }
    return 0;
}
void SchemaFactory::delete_table_region_map(const pb::SchemaInfo& table) {
    if (table.has_deleted() && table.deleted()) {
        for (const auto& index : table.indexs()) {
            if (index.is_global() || index.index_type() == pb::I_PRIMARY) {
                DB_DEBUG("erase global index_id %lld", index.index_id());
                _table_region_mapping.Modify(double_buffer_table_region_erase, index.index_id());
            }
        }
    }
}

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

