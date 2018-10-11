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
    _is_init = true;
    return 0;
}

void SchemaFactory::update_table(const pb::SchemaInfo& table) {
    bthread::execution_queue_execute(_table_queue_id, table);
}
int SchemaFactory::update_tables_double_buffer(
        void* meta, bthread::TaskIterator<pb::SchemaInfo>& iter) {
    SchemaFactory* factory = (SchemaFactory*)meta;
    factory->update_tables_double_buffer(iter);
    bthread_usleep(100 * 1000); // 更新间隔100ms，双buf不需要引用计数了
    return 0;
}
void SchemaFactory::update_tables_double_buffer(bthread::TaskIterator<pb::SchemaInfo>& iter) {
    SchemaMapping* background = _double_buffer_table.read_background();
    SchemaMapping* frontground = _double_buffer_table.read();
    *background = *frontground;
    for (; iter; ++iter) {
        update_table(*iter, background);
    }
    _double_buffer_table.swap();
}

//TODO
void SchemaFactory::delete_table(const pb::SchemaInfo& table, SchemaMapping* background) {
    if (!table.has_table_id()) {
        DB_FATAL("missing fields in SchemaInfo");
        return;
    }
    auto& _table_info_mapping = background->table_info_mapping;
    auto& _table_name_id_mapping = background->table_name_id_mapping;
    auto& _index_info_mapping = background->index_info_mapping;
    auto& _index_name_id_mapping = background->index_name_id_mapping;
    int64_t table_id = table.table_id();
    if (_table_info_mapping.count(table_id) == 0) {
        DB_FATAL("no table found with tableid: %ld", table_id);
        return;
    }
    const TableInfo& tbl_info = _table_info_mapping[table_id];
    std::string full_name = tbl_info.namespace_ + "." + tbl_info.name;
    DB_WARNING("full_name: %s, %ld", full_name.c_str(), _table_name_id_mapping.size());
    _table_name_id_mapping.erase(full_name);
    DB_WARNING("full_name deleted: %ld", _table_name_id_mapping.size());

    for (auto index_id : tbl_info.indices) {
        if (_index_info_mapping.count(index_id) == 0) {
            continue;
        }
        IndexInfo& idx_info = _index_info_mapping[index_id];
        std::string full_idx_name = tbl_info.namespace_ + "." + idx_info.name;
        DB_WARNING("full_idx_name: %s, %ld", full_idx_name.c_str(), 
            _index_name_id_mapping.size());
        _index_name_id_mapping.erase(full_idx_name);
        DB_WARNING("full_idx_name deleted: %ld", _index_name_id_mapping.size());
        _index_info_mapping.erase(index_id);
    }

    delete tbl_info.file_proto;
    auto _pool = tbl_info.pool;
    auto _factory = tbl_info.factory;
    Bthread bth;
    bth.run([_pool, _factory]() {
            bthread_usleep(3600 * 1000 * 1000L);
            delete _pool; 
            delete _factory;
            });

    {
        BAIDU_SCOPED_LOCK(_update_table_region_mutex);
        _table_region_mapping.erase(table_id);
    }
    _table_info_mapping.erase(table_id);
    return;
}

// not thread-safe
int SchemaFactory::update_table(const pb::SchemaInfo &table, SchemaMapping* background) {
    auto& _table_info_mapping = background->table_info_mapping;
    auto& _table_name_id_mapping = background->table_name_id_mapping;
    auto& _db_info_mapping = background->db_info_mapping;
    auto& _db_name_id_mapping = background->db_name_id_mapping;
    DB_WARNING("table:%s", table.ShortDebugString().c_str());
    if (!_is_init) {
        DB_FATAL("SchemaFactory not initialized");
        return -1;
    }
    if (table.has_deleted() && table.deleted()) {
        delete_table(table, background);
        return 0;
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

    TableInfo tbl_info;
    if (_table_info_mapping.count(table_id) == 0) {
        if (nullptr == (tbl_info.file_proto = new (std::nothrow)FileDescriptorProto)) {
            DB_FATAL("create FileDescriptorProto failed");
            return -1;
        }
    } else {
        tbl_info = _table_info_mapping[table_id];
        // need not  update when version GE
        if (tbl_info.version >= table.version()) {
            DB_WARNING("need not  update, orgin version:%ld, new version:%ld, table_id:%ld", 
                    tbl_info.version, table.version(), table_id);
            return 0;
        }
        //old_tbl_name = tbl_info.name;
        // file_proto build完的内容不能删除
        tbl_info.file_proto->Clear();
        tbl_info.fields.clear();
        tbl_info.indices.clear();
    }
    tbl_info.file_proto->mutable_options()->set_cc_enable_arenas(true);
    tbl_info.file_proto->set_name(std::to_string(database_id) + ".proto");
    tbl_info.tbl_proto = tbl_info.file_proto->add_message_type();
    tbl_info.id = table_id;
    tbl_info.partition_num = table.partition_num(); //TODO
    tbl_info.region_size = table.region_size();
    tbl_info.version = table.version();
    tbl_info.name = _db_name + "." + _tbl_name;
    tbl_info.tbl_proto->set_name(table_name);
    tbl_info.namespace_ = _namespace;
    tbl_info.resource_tag = table.resource_tag();
    tbl_info.charset = table.charset();
    tbl_info.engine = pb::ROCKSDB;
    if (table.has_engine()) {
        tbl_info.engine = table.engine();
    }
    if (!table.has_byte_size_per_record() || table.byte_size_per_record() < 1) {
        tbl_info.byte_size_per_record = 1;
    } else {
        tbl_info.byte_size_per_record = table.byte_size_per_record();
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
        FieldDescriptorProto *field_proto = tbl_info.tbl_proto->add_field();
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

        FieldInfo field_info;
        field_info.id = field.field_id();
        field_info.table_id = table_id;
        field_info.name = tbl_info.name + "." + field.field_name();
        field_info.type = field.mysql_type();
        field_info.can_null = field.can_null();
        field_info.auto_inc = field.auto_increment();
        field_info.deleted = field.deleted();
        field_info.default_value = field.default_value();
        if (field.has_default_value()) {
            field_info.default_expr_value.type = pb::STRING;
            field_info.default_expr_value.str_val = field_info.default_value;
            field_info.default_expr_value.cast_to(field_info.type);
        }
        /*
        if (field_info.default_value == "(current_timestamp())") {
            field_info.default_expr_value = ExprValue::Now();
            field_info.default_expr_value.cast_to(field_info.type);
        } else {
        }
        */
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

    const FileDescriptor *db_desc = tmp_pool->BuildFile(*tbl_info.file_proto);
    if (!db_desc) {
        DB_FATAL("build proto_file [%ld] failed, %s", database_id, tbl_info.file_proto->DebugString().c_str());
        return -1;
    }
    const Descriptor *descriptor = db_desc->FindMessageTypeByName(table_name);
    if (!descriptor) {
        DB_FATAL("FindMessageTypeByName [%ld] failed.", table_id);
        return -1;
    }
    tbl_info.tbl_desc = descriptor;

    // create name => id mapping
    _db_name_id_mapping[_namespace + "." + _db_name] = database_id;

    DB_WARNING("_db_name_id_mapping: %s->%ld", std::string(_namespace + "." + _db_name).c_str(), 
        database_id);

    std::string _db_table(_namespace + "." + _db_name + "." + _tbl_name);
    //old_tbl_name = _namespace + "." + old_tbl_name;
    //if (!old_tbl_name.empty() && old_tbl_name != _db_table) {
        //_table_name_id_mapping.erase(old_tbl_name);
        //_db_table = _namespace + "." + _db_name + "." + table.new_table_name();
    //}
    _table_name_id_mapping[_db_table] = table_id;
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
        //DB_WARNING("update_index: %ld", index_id);
        update_index(tbl_info, cur, pk_index, background);
        tbl_info.indices.push_back(index_id);
    }

    auto del_pool = tbl_info.pool;
    auto del_factory = tbl_info.factory;
    tbl_info.pool = tmp_pool.release();
    tbl_info.factory = tmp_factory.release();
    tbl_info.msg_proto = tbl_info.factory->GetPrototype(tbl_info.tbl_desc);

    Bthread bth;
    bth.run([del_pool, del_factory]() {
        // 延迟删除
            bthread_usleep(3600 * 1000 * 1000L);
            delete del_pool; 
            delete del_factory;
            });

    _db_info_mapping[database_id] = db_info;
    _table_info_mapping[table_id] = tbl_info;
    return 0;
}

//TODO, string index type
void SchemaFactory::update_index(TableInfo& table_info, const pb::IndexInfo& index, 
        const pb::IndexInfo* pk_index, SchemaMapping* background) {
    auto& _index_info_mapping = background->index_info_mapping;
    auto& _index_name_id_mapping = background->index_name_id_mapping;
    IndexInfo idx_info;
    std::string old_idx_name;
    //如果表存在，需要清空里面的内容
    if (_index_info_mapping.count(index.index_id()) != 0) {
        idx_info = _index_info_mapping[index.index_id()];
        old_idx_name = idx_info.name;
        idx_info.fields.clear();
        idx_info.pk_fields.clear();
        idx_info.pk_pos.clear();
    }

    idx_info.version = table_info.version;
    idx_info.pk   = table_info.id;
    idx_info.id   = index.index_id();
    std::string lower_index_name = index.index_name();
    std::transform(lower_index_name.begin(), lower_index_name.end(), 
                    lower_index_name.begin(), ::tolower);
    idx_info.name = table_info.name + "." + lower_index_name;
    idx_info.type = index.index_type();
    idx_info.segment_type = index.segment_type();
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
        FieldInfo& info = table_info.fields[index.field_ids(idx) - 1];
        idx_info.fields.push_back(info);

        //记录field_id在index_bytes中的对应位置
        id_map.insert(std::make_pair(info.id, idx_info.length));
        //DB_WARNING("index:%ld, field:%d, length:%d", idx_info.id, info.id, idx_info.length);
        if (info.can_null) {
            nullable = true;
        }
        if (info.size == -1) {
            idx_info.length = -1;
        } else if (idx_info.length != -1) {
            idx_info.length += info.size;
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
            FieldInfo& info = table_info.fields[field_id - 1];
            if (info.size == -1) {
                pk_length = -1;
                break;
            }
            if (id_map.count(field_id) != 0) {
                //重建pk时，该field从二级索引读取
                idx_info.pk_pos.push_back(std::make_pair(1, id_map[field_id]));
            } else {
                //重建pk时，该field从主键读取
                idx_info.pk_fields.push_back(info);
                idx_info.pk_pos.push_back(std::make_pair(-1, pk_length==-1? 0 : pk_length));
                pk_length += info.size;
            }
        }
        //pk中有变长字段，则不进行主键压缩
        if (pk_length == -1) {
            idx_info.pk_fields.clear();
            idx_info.pk_pos.clear();
            for (int idx = 0; idx < pk_index->field_ids_size(); ++idx) {
                int32_t field_id = pk_index->field_ids(idx);
                FieldInfo& info = table_info.fields[field_id - 1];
                idx_info.pk_fields.push_back(info);
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
    _index_info_mapping[idx_info.id] = idx_info;
    std::string fullname = idx_info.name;
    if (!old_idx_name.empty() && old_idx_name != fullname) {
        _index_name_id_mapping.erase(old_idx_name);
    }
    fullname = table_info.namespace_ + "."  + idx_info.name;
    DB_WARNING("index full name:%s, %ld, %d", fullname.c_str(), idx_info.id, idx_info.overlap);
    _index_name_id_mapping[fullname] = idx_info.id;
}

void SchemaFactory::update_regions(
        const RegionVec& regions) {
    bthread::execution_queue_execute(_region_queue_id, regions);
}
int SchemaFactory::update_regions_double_buffer(
        void* meta, bthread::TaskIterator<RegionVec>& iter) {
    SchemaFactory* factory = (SchemaFactory*)meta;
    factory->update_regions_double_buffer(iter);
    bthread_usleep(100 * 1000); // 更新间隔100ms，双buf不需要引用计数了
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
            table_key_region_map[table_id][p_id][start_key] = &region;
        }
    }
    for (auto& table_region : table_key_region_map) {
        int64_t table_id = table_region.first;
        update_regions_table(table_id, table_region.second);
    }
}

void SchemaFactory::update_regions_table(int64_t table_id, std::map<int,
        std::map<std::string, const pb::RegionInfo*>>& key_region_map) {
    TableRegionPtr double_buffer_ptr = get_table_region(table_id);
    TableRegionInfo* background = double_buffer_ptr->read_background();
    TableRegionInfo* frontground = double_buffer_ptr->read();
    background->region_info_mapping = frontground->region_info_mapping;
    background->key_region_mapping = frontground->key_region_mapping;
    auto _update_region = [background](const pb::RegionInfo& region) {
        if (region.has_deleted() && region.deleted()) {
            DB_WARNING("region:%s deleted", region.ShortDebugString().c_str());
            std::vector<StrInt64Map>& vec = background->key_region_mapping;
            if (vec.size() < 1) {
                return;
            }
            StrInt64Map& key_reg_map = vec[0];
            key_reg_map.erase(region.start_key());
            background->region_info_mapping.erase(region.region_id());
            return;
        }
        pb::RegionInfo orgin_region;
        background->get_region_info(region.region_id(), orgin_region);
        //不允许version 回退
        if (orgin_region.version() > region.version()) {
            DB_WARNING("no roll back, region_id: %ld, old_ver:%d, ver:%d", 
                    region.region_id(), orgin_region.version(), region.version());
            return;
        }
        std::vector<StrInt64Map>& vec = background->key_region_mapping;
        if (vec.size() < 1) {
            vec.resize(1);
        }
        StrInt64Map& key_reg_map = vec[0];
        key_reg_map.insert(std::make_pair(region.start_key(), region.region_id()));

        background->insert_region_info(region);
        DB_WARNING("region_id: %ld ver:%d update success", region.region_id(), region.version());
    };

    for (auto& start_key_region : key_region_map) {
        auto& start_key_region_map = start_key_region.second;
        std::vector<const pb::RegionInfo*> last_regions;
        //std::string start_key;
        std::string end_key;
        for (auto iter = start_key_region_map.begin(); iter != start_key_region_map.end(); ++iter) {
            const pb::RegionInfo& region = *iter->second;
            DB_WARNING("region_info:%s", region.ShortDebugString().c_str());
            pb::RegionInfo orgin_region;
            int ret = background->get_region_info(region.region_id(), orgin_region);
            if (ret < 0) {
                DB_WARNING("region:%s", region.ShortDebugString().c_str());
                if (last_regions.size() != 0) {
                    //DB_WARNING("last_region:%s, region:%s",
                    //        last_region->ShortDebugString().c_str(), region.ShortDebugString().c_str());
                    auto last_region = last_regions.back();
                    if (last_region->end_key() != region.start_key()) {
                        last_regions.clear();
                        DB_FATAL("last_region->end_key():%s != region.start_key():%s", 
                                last_region->end_key().c_str(), region.start_key().c_str());
                        continue;
                    }
                    last_regions.push_back(&region);
                    if (region.end_key() == end_key) {
                        for (auto r : last_regions) {
                            _update_region(*r);
                            DB_WARNING("update regions %s", r->ShortDebugString().c_str());
                        }
                        last_regions.clear();
                    } else if (end_key_compare(region.end_key(), end_key) > 0) {
                        DB_FATAL("region.end_key:%s > end_key:%s", 
                                str_to_hex(region.end_key()).c_str(), 
                                str_to_hex(end_key).c_str());
                        last_regions.clear();
                    }
                } else {
                    DB_WARNING("region:%s", region.ShortDebugString().c_str());
                    // 先判断加入的region是否与现有的region有范围重叠
                    std::vector<StrInt64Map>& vec = background->key_region_mapping;
                    if (vec.size() < 1) {
                        vec.resize(1);
                    }
                    StrInt64Map& key_reg_map = vec[0];
                    auto region_iter = key_reg_map.lower_bound(region.start_key());
                    if (region_iter != key_reg_map.begin()) {
                        int64_t pre_region_id = (--region_iter)->second;
                        pb::RegionInfo pre_info;
                        background->get_region_info(pre_region_id, pre_info);
                        if (end_key_compare(pre_info.end_key(), region.end_key()) >= 0) {
                            DB_WARNING("region:%ld %ld is overlapping", 
                                    pre_region_id, region.region_id());
                            continue;
                        }
                    }
                    DB_WARNING("region:%s", region.ShortDebugString().c_str());
                    _update_region(region);
                }
            } else if (region.version() > orgin_region.version()) {
                DB_WARNING("region:%s, orgin_region:%s",
                        region.ShortDebugString().c_str(), orgin_region.ShortDebugString().c_str());
                last_regions.clear();
                //version变化了说明split了，需要整体更新
                last_regions.push_back(&region);
                //start_key = region.start_key();
                end_key = orgin_region.end_key();
                continue;
            } else {
                DB_WARNING("region:%s, orgin_region:%s",
                        region.ShortDebugString().c_str(), orgin_region.ShortDebugString().c_str());
                _update_region(region);
            }
            //DB_WARNING("region:%s, orgin_region:%s",
            //        region.ShortDebugString().c_str(), orgin_region.ShortDebugString().c_str());
            //last_region = nullptr;
        }
    }
    double_buffer_ptr->swap();
}

TableRegionPtr SchemaFactory::get_table_region(int64_t table_id) {
    {
        BAIDU_SCOPED_LOCK(_update_table_region_mutex);
        if (_table_region_mapping.count(table_id) == 0) {
            _table_region_mapping[table_id] = std::make_shared<DoubleBuffer<TableRegionInfo>>();
        }
        return _table_region_mapping[table_id];
    }
}

void SchemaFactory::update_leader(const pb::RegionInfo& region) {
    int64_t table_id = region.table_id();
    get_table_region(table_id)->read()->update_leader(region.region_id(), region.leader());
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
    }
    for (uint32_t idx = 0; idx < tbl_cnt; ++idx) {
        const pb::PrivilegeTable& tbl = user.privilege_table(idx);
        user_info->table[tbl.table_id()] = tbl.table_rw();
    }
    _user_info_mapping[username] = user_info;
    //TODO ip and bns access control
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
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _table_info_mapping = frontground->table_info_mapping;
    if (_table_info_mapping.count(tableid) == 0) {
        DB_WARNING("no table found: %ld", tableid);
        return nullptr;
    }
    return new_record(_table_info_mapping[tableid]);
}

DatabaseInfo SchemaFactory::get_database_info(int64_t databaseid) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _db_info_mapping = frontground->db_info_mapping;
    if (_db_info_mapping.count(databaseid) == 0) {
        return DatabaseInfo();
    }
    return _db_info_mapping[databaseid];
}

pb::Engine SchemaFactory::get_table_engine(int64_t tableid) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _table_info_mapping = frontground->table_info_mapping;
    if (_table_info_mapping.count(tableid) == 0) {
        return pb::ROCKSDB;
    }
    return _table_info_mapping[tableid].engine;
}

TableInfo SchemaFactory::get_table_info(int64_t tableid) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _table_info_mapping = frontground->table_info_mapping;
    if (_table_info_mapping.count(tableid) == 0) {
        return TableInfo();
    }
    return _table_info_mapping[tableid];
}

IndexInfo SchemaFactory::get_index_info(int64_t indexid) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _index_info_mapping = frontground->index_info_mapping;
    if (_index_info_mapping.count(indexid) == 0) {
        return IndexInfo();
    }
    return _index_info_mapping[indexid];
}

std::shared_ptr<UserInfo> SchemaFactory::get_user_info(const std::string& user) {
    BAIDU_SCOPED_LOCK(_update_user_mutex);
    if (_user_info_mapping.count(user) == 0) {
        return nullptr;
    }
    return _user_info_mapping[user];
}

std::vector<std::string> SchemaFactory::get_db_list(std::string namespace_) {
    std::vector<std::string> vec;
    SchemaMapping* frontground = _double_buffer_table.read();
    for (auto& db_pair : frontground->db_info_mapping) {
        auto& db_info = db_pair.second;
        if (db_info.namespace_ == namespace_) {
            vec.push_back(db_info.name);
        }
    }
    return vec;
}

std::vector<std::string> SchemaFactory::get_table_list(std::string namespace_, std::string db_name) {
    db_name += ".";
    std::vector<std::string> vec; 
    SchemaMapping* frontground = _double_buffer_table.read();
    for (auto& table_pair : frontground->table_info_mapping) {
        auto& table_info = table_pair.second;
        if (table_info.namespace_ == namespace_) {
            if (table_info.name.size() <= db_name.size()) {
                //DB_FATAL("table_info.name:%s less than db_name:%s",
                //        table_info.name.c_str(), db_name.c_str());
                continue;
            }
            if (table_info.name.compare(0, db_name.size(), db_name) == 0) {
                vec.push_back(table_info.name.substr(db_name.size()));
            }
        }
    }
    return vec;
}
void SchemaFactory::get_all_table_version(std::unordered_map<int64_t, int64_t>& table_id_version_map) {
    SchemaMapping* frontground = _double_buffer_table.read();
    for (auto& table_pair : frontground->table_info_mapping) {
        table_id_version_map[table_pair.first] = table_pair.second.version;
    }
}

int SchemaFactory::get_region_info(int64_t table_id, int64_t region_id, pb::RegionInfo& info) {
    return get_table_region(table_id)->read()->get_region_info(region_id, info);
}

int SchemaFactory::get_region_info(int64_t region_id, pb::RegionInfo& info) {
    // todo
    return 0;
}

int SchemaFactory::get_table_id(const std::string& table_name, int64_t& table_id) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _table_name_id_mapping = frontground->table_name_id_mapping;
    if (_table_name_id_mapping.count(table_name) == 0) {
        return -1;
    }
    table_id = _table_name_id_mapping[table_name];
    return 0;
}
int SchemaFactory::get_byte_size_per_record(int64_t table_id, 
        int64_t& byte_size_per_record) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _table_info_mapping = frontground->table_info_mapping;
    if (_table_info_mapping.count(table_id) == 0) {
        return -1;
    }
    byte_size_per_record = _table_info_mapping[table_id].byte_size_per_record;
    return 0;
}
int SchemaFactory::get_region_capacity(int64_t table_id, int64_t& region_capacity) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _table_info_mapping = frontground->table_info_mapping;
    if (_table_info_mapping.count(table_id) == 0) {
        return -1;
    }
    region_capacity = _table_info_mapping[table_id].region_size;
    return 0;
}
int SchemaFactory::get_database_id(const std::string& db_name, int64_t& db_id) {
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _db_name_id_mapping = frontground->db_name_id_mapping;
    if (_db_name_id_mapping.count(db_name) == 0) {
        return -1;
    }
    db_id = _db_name_id_mapping[db_name];
    return 0;
}
int SchemaFactory::whether_exist_tableid(int64_t table_id) {
    SchemaMapping* frontground = _double_buffer_table.read();
    if (frontground->table_info_mapping.count(table_id) == 0) {
        return -1;
    }
    return 0;
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
    SchemaMapping* frontground = _double_buffer_table.read();
    auto& _table_info_mapping = frontground->table_info_mapping;
    auto& _index_name_id_mapping = frontground->index_name_id_mapping;
    if (_table_info_mapping.count(table_id) == 0) {
        return -1;
    }
    std::string lower_index_name = index_name;
    std::transform(lower_index_name.begin(), lower_index_name.end(), lower_index_name.begin(), ::tolower);
    //primary mysql关键词
    if (lower_index_name == "primary") {
        index_id = table_id;
        return 0;
    }
    const TableInfo& tbl_info = _table_info_mapping[table_id];
    std::string full_index_name = tbl_info.namespace_ + "." + tbl_info.name + "." + lower_index_name;
    if (_index_name_id_mapping.count(full_index_name) == 0) {
        return -1;
    }
    index_id = _index_name_id_mapping[full_index_name];
    return 0;
}

int SchemaFactory::get_region_by_key(IndexInfo& index, 
        const pb::PossibleIndex* primary,
        std::map<int64_t, pb::RegionInfo>& region_infos) {
    region_infos.clear();
    TableRegionPtr double_buffer_ptr = get_table_region(index.id);
    TableRegionInfo* frontground = double_buffer_ptr->read();
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

    auto record_template = TableRecord::new_record(index.id);
    for (const auto& range : primary->ranges()) {
        SmartRecord left;// = record_template->clone(false);
        SmartRecord right;// = record_template->clone(false);
        if (range.left_pb_record() != "") {
            left = record_template->clone(false);
            left->decode(range.left_pb_record());
        }
        if (range.right_pb_record() != "") {
            right = record_template->clone(false);
            right->decode(range.right_pb_record());
        }
        bool left_open = range.left_open();
        bool right_open = range.right_open();
        MutTableKey  _start;
        MutTableKey  _end;
        if (left != nullptr) {
            if (0 != _start.append_index(index, left.get(), range.left_field_cnt(), false)) {
                DB_FATAL("Fail to encode_key, table:%ld", index.id);
                return -1;
            }
        } else {
            left_open = false;
        }

        if (right != nullptr) {
            if (0 != _end.append_index(index, right.get(), range.right_field_cnt(), false)) {
                DB_FATAL("Fail to encode_key, table:%ld", index.id);
                return -1;
            }
        } else {
            right_open = false;
        }

        MutTableKey _start_sentinel(_start.data());
        if (!_start.get_full() && left_open) {
            _start_sentinel.append_u16(0xFFFF);
        }

        if (key_region_mapping.size() != 1) {
            DB_WARNING("partion_num not supported:%ld, %lu", index.id, key_region_mapping.size());
            return -1;
        }
        StrInt64Map& map = key_region_mapping[0];
        auto region_iter = map.upper_bound(_start_sentinel.data());
        
        while (left_open && region_iter != map.end() && 
                boost::starts_with(region_iter->first, _start.data())) {
            region_iter++;
        }
        if (region_iter != map.begin()) {
            --region_iter;
        }
        while (region_iter != map.end()) {
            if (_end.data().empty() || region_iter->first <= _end.data() ||
                    (!right_open && boost::starts_with(region_iter->first, _end.data()))) {
                int64_t region_id = region_iter->second;
                frontground->get_region_info(region_id, region_infos[region_id]);
            } else {
                break;
            }
            region_iter++;
        }
    }
    return 0;
}

// Get a list of new regions given a list of old regions
// The in 
int SchemaFactory::get_region_by_key(
        const RepeatedPtrField<pb::RegionInfo>& input_regions,
        std::map<int64_t, pb::RegionInfo>& output_regions) {
    for (int idx = 0; idx < input_regions.size(); ++idx) {
        int64_t table_id = input_regions[idx].table_id();
        TableRegionPtr double_buffer_ptr = get_table_region(table_id);
        TableRegionInfo* frontground = double_buffer_ptr->read();
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
    TableRegionPtr double_buffer_ptr = get_table_region(index.id);
    TableRegionInfo* frontground = double_buffer_ptr->read();
    auto& key_region_mapping = frontground->key_region_mapping;
    for (auto& record : records) {
        MutTableKey  key;
        if (0 != key.append_index(index, record.get(), -1, false)) {
            DB_FATAL("Fail to encode_key, table:%ld", index.id);
            return -1;
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
}
