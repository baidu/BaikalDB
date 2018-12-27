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

#pragma once

#include <cstddef>
#include <mutex>
#include <set>
#include <unordered_map>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <bthread/execution_queue.h>
#include "common.h"
#include "expr_value.h"
#include "proto/meta.interface.pb.h"
#include "proto/plan.pb.h"

using google::protobuf::FileDescriptorProto;
using google::protobuf::DescriptorProto;
using google::protobuf::FieldDescriptorProto;
using google::protobuf::FieldDescriptor;
using google::protobuf::Descriptor;
using google::protobuf::Message;
using google::protobuf::DynamicMessageFactory;
using google::protobuf::DescriptorPool;
using google::protobuf::RepeatedPtrField;

namespace baikaldb {

struct UserInfo;
class TableRecord;
typedef std::shared_ptr<TableRecord> SmartRecord;
typedef std::map<std::string, int64_t> StrInt64Map;

struct RegionInfo {
    mutable std::mutex leader_mutex;
    pb::RegionInfo region_info;
    RegionInfo() {}
    // update leader for all thread
    explicit RegionInfo(const RegionInfo& other) {
        BAIDU_SCOPED_LOCK(other.leader_mutex);
        region_info = other.region_info;
    }
};
struct TableRegionInfo {
    // region_id => RegionInfo
    std::unordered_map<int64_t, RegionInfo> region_info_mapping;
    // partion vector of (start_key => regionid)
    std::vector<StrInt64Map> key_region_mapping;
    void update_leader(int64_t region_id, const std::string& leader) {
        if (region_info_mapping.count(region_id) == 1) {
            BAIDU_SCOPED_LOCK(region_info_mapping[region_id].leader_mutex);
            region_info_mapping[region_id].region_info.set_leader(leader);
        }
    }
    int get_region_info(int64_t region_id, pb::RegionInfo& info) {
        if (region_info_mapping.count(region_id) == 1) {
            BAIDU_SCOPED_LOCK(region_info_mapping[region_id].leader_mutex);
            info = region_info_mapping[region_id].region_info;
            return 0;
        } else {
            return -1;
        }
    }
    void insert_region_info(const pb::RegionInfo& info) {
        if (region_info_mapping.count(info.region_id()) == 1) {
            BAIDU_SCOPED_LOCK(region_info_mapping[info.region_id()].leader_mutex);
            region_info_mapping[info.region_id()].region_info = info;
        } else {
            region_info_mapping[info.region_id()].region_info = info;
        }
    }
};
typedef std::shared_ptr<DoubleBuffer<TableRegionInfo>> TableRegionPtr;

struct FieldInfo {
    int32_t             id;
    int32_t             size = -1;// STRING类型的size为-1，表示变长
    int64_t             table_id;
    std::string         name;   // db.table.field
    std::string         default_value;
    ExprValue           default_expr_value;
    std::string         on_update_value;
    std::string         comment;
    pb::PrimitiveType   type;
    bool                can_null = false;
    bool                auto_inc = false;
    bool                deleted = false;
    //const FieldDescriptor* field;
};

struct DistInfo {
    std::string logical_room;
    int64_t count;
};

// short name
inline int32_t get_field_id_by_name(
        const std::vector<FieldInfo>& fields, const std::string& name) {
    for (auto& field : fields) {
        size_t pos = field.name.find_last_of('.');
        if (pos != std::string::npos) {
            if (field.name.compare(pos + 1, std::string::npos, name) == 0) {
                return field.id;
            }
        }
    }
    return 0;
}

struct TableInfo {
    int64_t                 id = -1;
    int64_t                 version = -1;
    int64_t                 partition_num;
    int64_t                 region_split_lines;
    int64_t                 byte_size_per_record = 1; //默认情况下不分裂，兼容以前的表
    int64_t                 auto_inc_field_id = -1; //自增字段id
    pb::Charset             charset;
    pb::Engine              engine;
    std::string             name;    // db.table
    std::string             namespace_;
    std::string             resource_tag;
    std::vector<int64_t>    indices; // include pk
    //std::set<int64_t>       regions; //TODO
    std::vector<FieldInfo>  fields;
    std::vector<DistInfo>   dists;
    int64_t                 replica_num = 3;

    const Descriptor*       tbl_desc;
    DescriptorProto*        tbl_proto = nullptr;
    FileDescriptorProto*    file_proto = nullptr;
    DynamicMessageFactory*  factory = nullptr;
    DescriptorPool*         pool = nullptr;
    const Message*          msg_proto = nullptr;

    TableInfo() {}
};

struct IndexInfo {
    int64_t                 id = -1;
    int64_t                 version = -1;
    std::string             name;  // db.table.index
    std::string             short_name;
    int64_t                 pk;
    //索引的字节数，正数表示定长且没有nullable字段，-1表示有变长字段或nullable字段
    //length包含nullflag字节
    int32_t                 length = -1;
    pb::IndexType           type;
    pb::SegmentType         segment_type;
    std::vector<FieldInfo>  fields;
    //主键字段和索引字段是否有重叠，如果没有重叠，不需要重建主键
    bool                    overlap = false;

    // all pk fields not in index fields
    // empty if this is a pk index
    std::vector<FieldInfo>  pk_fields;

    // 索引定长(length>0)且overlap时, 主键字段在索引中的位置
    // (用于构造完整的主键使用), 
    // pair.first=1表示在二级索引字节序列中的位置
    // pair.first=-1表示在主键索引字节序列中的位置
    std::vector<std::pair<int,int> > pk_pos;

    //index comments in the create SQL
    std::string             comments;
};

struct DatabaseInfo {
    int64_t                 id = -1;
    int64_t                 version = -1;
    std::string             name; // db
    std::string             namespace_;
};

struct SchemaMapping {
    // namespace.database (db) => database_id
    std::unordered_map<std::string, int64_t> db_name_id_mapping;
    // database_id => IndexInfo
    std::unordered_map<int64_t, DatabaseInfo> db_info_mapping;
    // namespace.database.table_name (db.table) => table_id
    std::unordered_map<std::string, int64_t> table_name_id_mapping;
    // table_id => TableInfo
    std::unordered_map<int64_t, TableInfo> table_info_mapping;
    // index_name (namespace.db.table.index) => index_id
    std::unordered_map<std::string, int64_t> index_name_id_mapping;
    // index_id => IndexInfo
    std::unordered_map<int64_t, IndexInfo> index_info_mapping;
};

struct IdcMapping {
    // store => logical_room
    std::unordered_map<std::string, std::string> instance_logical_mapping;
    // physical_room => logical_room
    std::unordered_map<std::string, std::string> physical_logical_mapping;
};

class SchemaFactory {
    typedef ::google::protobuf::RepeatedPtrField<pb::RegionInfo> RegionVec; 
public:
    virtual ~SchemaFactory() {
        bthread_mutex_destroy(&_update_table_region_mutex);
        bthread_mutex_destroy(&_update_user_mutex);
    }

    static SchemaFactory* get_instance() {
        static SchemaFactory _instance;
        return &_instance;
    }

    //bucket_size
    int init();

    // not thread-safe, should be called in single thread
    // 删除判断deleted, name允许改
    void update_table(const pb::SchemaInfo& table);
    static int update_tables_double_buffer(
            void* meta, bthread::TaskIterator<pb::SchemaInfo>& iter);
    void update_tables_double_buffer(bthread::TaskIterator<pb::SchemaInfo>& iter);

    static int update_idc_double_buffer(
            void* meta, bthread::TaskIterator<pb::IdcInfo>& iter);
    void update_idc_double_buffer(bthread::TaskIterator<pb::IdcInfo>& iter);
    void update_idc(const pb::IdcInfo& idc_info, IdcMapping* background);
    void update_idc(const pb::IdcInfo& idc_info);

    static int update_regions_double_buffer(
            void* meta, bthread::TaskIterator<RegionVec>& iter);
    void update_regions_double_buffer(
            bthread::TaskIterator<RegionVec>& iter);
    void update_regions_table(int64_t table_id, std::map<int,
            std::map<std::string, const pb::RegionInfo*>>& key_region_map);
    // 删除判断deleted
    void update_regions(const RegionVec& regions);
    //void force_update_region(const pb::RegionInfo& region);
    void update_region(const pb::RegionInfo& region);
    void update_leader(const pb::RegionInfo& region);
    TableRegionPtr get_table_region(int64_t table_id);

    //TODO 不考虑删除
    void update_user(const pb::UserPrivilege& user);

    std::unordered_map<int64_t, TableInfo>& table_info_mapping() {
        SchemaMapping* frontground = _double_buffer_table.read();
        return frontground->table_info_mapping;
    }

    ////functions for table info access
    // create a new table record (aka. a table row)
    SmartRecord new_record(TableInfo& info);
    SmartRecord new_record(int64_t tableid);
    
    Message* get_table_message(int64_t tableid);

    DatabaseInfo get_database_info(int64_t databaseid);

    pb::Engine get_table_engine(int64_t tableid);
    TableInfo get_table_info(int64_t tableid);

    IndexInfo get_index_info(int64_t indexid);
    IndexInfo* get_index_info_ptr(int64_t indexid);

    // functions for permission access
    std::shared_ptr<UserInfo> get_user_info(const std::string& user);

    std::vector<std::string> get_db_list(std::string namespace_);
    std::vector<std::string> get_table_list(std::string namespace_, std::string db_name);

    // table_name is full name (namespace.database.table)
    int get_table_id(const std::string& table_name, int64_t& table_id);

    // db_name is full name (namespace.database)
    int get_database_id(const std::string& db_name, int64_t& db_id);

    //int get_column_id(const std::string& col_name, int32_t col_id) const;

    int get_index_id(int64_t table_id, 
                    const std::string& index_name, 
                    int64_t& index_id);

    // functions for region info access
    int get_region_info(int64_t region_id, pb::RegionInfo& info);
    int get_region_info(int64_t table_id, int64_t region_id, pb::RegionInfo& info);

    int get_region_capacity(int64_t table_id, int64_t& region_capacity);
    // only used for pk (not null)
    int get_region_by_key(IndexInfo& index, 
            const pb::PossibleIndex* primary,
            std::map<int64_t, pb::RegionInfo>& region_infos,
            std::map<int64_t, pb::PossibleIndex>* region_primary = nullptr);

    int get_region_by_key(
            const RepeatedPtrField<pb::RegionInfo>& input_regions,
            std::map<int64_t, pb::RegionInfo>& output_regions);

    int get_region_by_key(IndexInfo& index,
            std::vector<SmartRecord> records,
            std::map<int64_t, std::vector<SmartRecord>>& region_ids,
            std::map<int64_t, pb::RegionInfo>& region_infos);

    int whether_exist_tableid(int64_t table_id);

    void get_all_table_version(std::unordered_map<int64_t, int64_t>& table_id_version);
    std::string physical_room() {
        return _physical_room;
    }
    std::string get_logical_room() {
        if (_logical_room.empty()) {
            IdcMapping* frontground = _double_buffer_idc.read();
            auto& physical_logical_mapping = frontground->physical_logical_mapping;
            if (physical_logical_mapping.find(_physical_room) != physical_logical_mapping.end()) {
                _logical_room = physical_logical_mapping[_physical_room];
            }
        }
        return _logical_room;
    }
    std::string logical_room_for_instance(const std::string& store) {
        IdcMapping* frontground = _double_buffer_idc.read();
        auto& instance_logical_mapping = frontground->instance_logical_mapping;
        if (instance_logical_mapping.find(store) != instance_logical_mapping.end()) {
            return instance_logical_mapping[store];
        }
        return "";
    }
private:
    SchemaFactory() {
        _is_init = false;
        bthread_mutex_init(&_update_table_region_mutex, NULL);
        bthread_mutex_init(&_update_user_mutex, NULL);
        butil::EndPoint addr;
        addr.ip = butil::my_ip();
        addr.port = 0;
        std::string address = endpoint2str(addr).c_str(); 
        auto ret = get_physical_room(address, _physical_room);
        if (ret < 0) {
            DB_FATAL("get physical room fail, ip: %s", address.c_str());
        }
    }
    int update_table(const pb::SchemaInfo& table, SchemaMapping* background);
    // 全量更新
    void update_index(TableInfo& info, const pb::IndexInfo& index,
            const pb::IndexInfo* pk_indexi, SchemaMapping* background);
    //delete table和index
    void delete_table(const pb::SchemaInfo& table, SchemaMapping* background);


    bool                    _is_init;
    bthread_mutex_t         _update_table_region_mutex;
    bthread_mutex_t         _update_user_mutex;

    // username => UserPrivilege
    std::unordered_map<std::string, std::shared_ptr<UserInfo>> _user_info_mapping;

    DoubleBuffer<SchemaMapping> _double_buffer_table;
    bthread::ExecutionQueueId<pb::SchemaInfo> _table_queue_id = {0};

    DoubleBuffer<IdcMapping>    _double_buffer_idc;
    bthread::ExecutionQueueId<pb::IdcInfo>  _idc_queue_id = {0};

    std::unordered_map<int64_t, TableRegionPtr> _table_region_mapping;
    bthread::ExecutionQueueId<RegionVec> _region_queue_id = {0};

    std::string _physical_room;
    std::string _logical_room;
};
}
