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

#pragma once

#include <cstddef>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <bthread/execution_queue.h>
#include "common.h"
#include "expr_value.h"
#include "proto/meta.interface.pb.h"
#include "proto/plan.pb.h"
#include "statistics.h"

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
static const std::string TABLE_SWITCH_MERGE    = "need_merge";                //是否开启merge功能
static const std::string TABLE_SWITCH_SEPARATE = "storage_compute_separate";  //是否开启计算存储分离
static const std::string TABLE_SWITCH_COST     = "select_index_by_cost";      //是否开启代价选择索引
static const std::string TABLE_OP_VERSION      = "op_version";                //操作版本号
static const std::string TABLE_OP_DESC         = "op_desc";                   //操作描述信息
static const std::string TABLE_FILTER_RATIO    = "filter_ratio";              //过滤率
struct UserInfo;
class TableRecord;
typedef std::shared_ptr<TableRecord> SmartRecord;
typedef std::map<std::string, int64_t> StrInt64Map;

struct RegionInfo {
    pb::RegionInfo region_info;
    RegionInfo() {}
    // update leader for all thread
    explicit RegionInfo(const RegionInfo& other) {
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
            region_info_mapping[region_id].region_info.set_leader(leader);
            DB_NOTICE("double_buffer_write region_id[%ld] set_leader[%s]", 
                region_id, leader.c_str());
        }
    }
    int get_region_info(int64_t region_id, pb::RegionInfo& info) {
        if (region_info_mapping.count(region_id) == 1) {
            info = region_info_mapping[region_id].region_info;
            return 0;
        } else {
            return -1;
        }
    }
    void insert_region_info(const pb::RegionInfo& info) {
        if (region_info_mapping.count(info.region_id()) == 1) {
            region_info_mapping[info.region_id()].region_info = info;
        } else {
            region_info_mapping[info.region_id()].region_info = info;
        }
        DB_DEBUG("double_buffer_write region_id[%ld] region_info[%s]", 
            info.region_id(), info.ShortDebugString().c_str());
    }
};
typedef std::shared_ptr<TableRegionInfo> TableRegionPtr;
using DoubleBufferedTableRegionInfo = butil::DoublyBufferedData<std::unordered_map<int64_t, TableRegionPtr>>;

inline size_t double_buffer_table_region_erase(std::unordered_map<int64_t, TableRegionPtr>& table_region_map, int64_t table_id) {
    DB_NOTICE("double_buffer_write erase table_id[%ld]", table_id);
    std::unordered_map<int64_t, TableRegionPtr>::iterator it = table_region_map.find(table_id);
    if (it != table_region_map.end()) {
        return table_region_map.erase(table_id);
    }
    return 0;
}

inline size_t double_buffer_table_region_update_leader(
    std::unordered_map<int64_t, TableRegionPtr>& table_region_map, 
    int64_t table_id, int64_t region_id, std::string& leader) {
    
    DB_NOTICE("double_buffer_write table_id[%ld], region_id[%ld], update_leader [%s]",
        table_id, region_id, leader.c_str());
    std::unordered_map<int64_t, TableRegionPtr>::iterator it = table_region_map.find(table_id);
    if (it == table_region_map.end()) {
        table_region_map[table_id] = std::make_shared<TableRegionInfo>();
    }
    table_region_map[table_id]->update_leader(region_id, leader);
    return 1;
}

struct FieldInfo {
    int32_t             id;
    int32_t             size = -1;// STRING类型的size为-1，表示变长
    int64_t             table_id;
    int                 pb_idx = 0; 
    std::string         name;   // db.table.field
    std::string         short_name; // field
    std::string         default_value;
    ExprValue           default_expr_value;
    std::string         on_update_value;
    std::string         comment;
    pb::PrimitiveType   type;
    bool                can_null = false;
    bool                auto_inc = false;
    bool                deleted = false;
};

struct DistInfo {
    std::string logical_room;
    int64_t count;
};

// short name
inline int32_t get_field_id_by_name(
        const std::vector<FieldInfo>& fields, const std::string& name) {
    for (auto& field : fields) {
        if (field.short_name == name) {
            return field.id;
        }
    }
    return 0;
}

struct TableInfo {
    int64_t                 id = -1;
    int64_t                 db_id = -1;
    int64_t                 version = -1;
    int64_t                 partition_num;
    int64_t                 region_split_lines;
    int64_t                 byte_size_per_record = 1; //默认情况下不分裂，兼容以前的表
    int64_t                 auto_inc_field_id = -1; //自增字段id
    pb::SchemaConf          schema_conf;
    pb::Charset             charset;
    pb::Engine              engine;
    std::string             name;    // db.table
    std::string             short_name;
    std::string             namespace_;
    std::string             resource_tag;
    std::vector<int64_t>    indices; // include pk
    std::vector<FieldInfo>  fields;
    std::vector<DistInfo>   dists;
    int64_t                 replica_num = 3;
    //table字段不变的话不需要重新构建动态pb
    std::string             fields_sign;
    //>0表示配置有ttl，单位s
    int64_t                 ttl_duration = 0;

    const Descriptor*       tbl_desc = nullptr;
    DescriptorProto*        tbl_proto = nullptr;
    FileDescriptorProto*    file_proto = nullptr;
    DynamicMessageFactory*  factory = nullptr;
    DescriptorPool*         pool = nullptr;
    const Message*          msg_proto = nullptr;
    bool                    have_statistics = false;
    
    TableInfo() {}
    FieldInfo* get_field_ptr(int32_t field_id) {
        for (auto& info : fields) {
            if (info.id == field_id) {
                return &info;
            }
        }
        return nullptr;
    }
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
    pb::IndexState           state;
    bool                    is_global = false;
    pb::StorageType storage_type = pb::ST_PROTOBUF;
};

struct DatabaseInfo {
    int64_t                 id = -1;
    int64_t                 version = -1;
    std::string             name; // db
    std::string             namespace_;
};
typedef std::shared_ptr<TableInfo> SmartTable;
typedef std::shared_ptr<IndexInfo> SmartIndex;
struct SchemaMapping {
    // namespace.database (db) => database_id
    std::unordered_map<std::string, int64_t> db_name_id_mapping;
    // database_id => IndexInfo
    std::unordered_map<int64_t, DatabaseInfo> db_info_mapping;
    // namespace.database.table_name (db.table) => table_id
    std::unordered_map<std::string, int64_t> table_name_id_mapping;
    // table_id => TableInfo
    std::unordered_map<int64_t, SmartTable> table_info_mapping;
    // index_name (namespace.db.table.index) => index_id
    std::unordered_map<std::string, int64_t> index_name_id_mapping;
    // index_id => IndexInfo
    std::unordered_map<int64_t, SmartIndex> index_info_mapping;
    //全局二级索引与主表id的映射功能
    std::unordered_map<int64_t, int64_t> global_index_id_mapping;
    //table_id => 代价统计信息
    std::map<int64_t, SmartStatistics> table_statistics_mapping;
};

using DoubleBufferedTable = butil::DoublyBufferedData<SchemaMapping>;

struct IdcMapping {
    // store => logical_room
    std::unordered_map<std::string, std::string> instance_logical_mapping;
    // physical_room => logical_room
    std::unordered_map<std::string, std::string> physical_logical_mapping;
};

using DoubleBufferedIdc = butil::DoublyBufferedData<IdcMapping>;

class SchemaFactory {
typedef ::google::protobuf::RepeatedPtrField<pb::RegionInfo> RegionVec; 
typedef ::google::protobuf::RepeatedPtrField<pb::SchemaInfo> SchemaVec;
typedef ::google::protobuf::RepeatedPtrField<pb::DataBaseInfo> DataBaseVec;
typedef ::google::protobuf::RepeatedPtrField<pb::Statistics> StatisticsVec;
public:
    virtual ~SchemaFactory() {
        bthread_mutex_destroy(&_update_user_mutex);
        bthread_mutex_destroy(&_update_show_db_mutex);
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
    //void update_table(DoubleBufferedTable& double_buffered_table, const pb::SchemaInfo& table);

    static int update_tables_double_buffer(
            void* meta, bthread::TaskIterator<pb::SchemaInfo>& iter);
    void update_tables_double_buffer(bthread::TaskIterator<pb::SchemaInfo>& iter);

    // _sync系统初始化的时候调用，防止meta信息获取延迟导致系统不可用
    void update_tables_double_buffer_sync(const SchemaVec& tables);

    static int update_idc_double_buffer(
            void* meta, bthread::TaskIterator<pb::IdcInfo>& iter);
    void update_idc_double_buffer(bthread::TaskIterator<pb::IdcInfo>& iter);
    void update_idc(const pb::IdcInfo& idc_info);
    int update_idc_internal(IdcMapping& background, const pb::IdcInfo& idc_info);

    void update_big_sql(const std::string& sql);
    static int update_big_sql_double_buffer(
            void* meta, bthread::TaskIterator<std::string>& iter);
    void update_big_sql_double_buffer(bthread::TaskIterator<std::string>& iter);

    bool is_big_sql(const std::string& sql);

    static int update_regions_double_buffer(
            void* meta, bthread::TaskIterator<RegionVec>& iter);
    void update_regions_double_buffer(
            bthread::TaskIterator<RegionVec>& iter);
    void update_regions_double_buffer_sync(const RegionVec& regions);
    size_t update_regions_table(std::unordered_map<int64_t, TableRegionPtr>& table_region_mapping, 
        int64_t table_id, std::map<int, std::map<std::string, const pb::RegionInfo*>>& key_region_map);

    void get_clear_regions(const std::string& new_start_key, 
                           const std::string& origin_start_key,
                           TableRegionPtr background,
                           std::map<std::string, int64_t>& clear_regions);
    void clear_region(TableRegionPtr background, 
                      std::map<std::string, int64_t>& clear_regions);
    void update_region(TableRegionPtr background, 
                                     const pb::RegionInfo& region);
    // 删除判断deleted
    void update_regions(const RegionVec& regions);
    //void force_update_region(const pb::RegionInfo& region);
    void update_region(const pb::RegionInfo& region);
    void update_leader(const pb::RegionInfo& region);
    //TableRegionPtr get_table_region(int64_t table_id);

    //TODO 不考虑删除
    void update_user(const pb::UserPrivilege& user);
    void update_show_db(const DataBaseVec& db_infos);
    void update_statistics(const StatisticsVec& statistics);
    int update_statistics_internal(SchemaMapping& background, const std::map<int64_t, SmartStatistics>& mapping);
    int64_t get_statis_version(int64_t table_id);
    // 从直方图中计算取值区间占比，如果计算小于某值的比率，则lower填null；如果计算大于某值的比率，则upper填null
    double get_histogram_ratio(int64_t table_id, int field_id, const ExprValue& lower, const ExprValue& upper);
    // 计算单个值占比
    double get_cmsketch_ratio(int64_t table_id, int field_id, const ExprValue& value);
    SmartStatistics get_statistics_ptr(int64_t table_id);
    void schema_info_scope_read(std::function<void(const SchemaMapping&)> callback) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return;
        }
        callback(*table_ptr);
    }
    ////functions for table info access
    // create a new table record (aka. a table row)
    SmartRecord new_record(TableInfo& info);
    SmartRecord new_record(int64_t tableid);
    
    Message* get_table_message(int64_t tableid);

    DatabaseInfo get_database_info(int64_t databaseid);

    pb::Engine get_table_engine(int64_t tableid);
    // 复制的函数适合长期占用的，ptr适合很短使用情况
    TableInfo get_table_info(int64_t tableid);
    SmartTable get_table_info_ptr(int64_t tableid);

    IndexInfo get_index_info(int64_t indexid);
    SmartIndex get_index_info_ptr(int64_t indexid);

    std::string get_index_name(int64_t index_id);

    // functions for permission access
    std::shared_ptr<UserInfo> get_user_info(const std::string& user);

    std::vector<std::string> get_db_list(const std::set<int64_t>& db);
    std::vector<std::string> get_table_list(
            std::string namespace_, std::string db_name, UserInfo* user);

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

    int get_region_capacity(int64_t global_index_id, int64_t& region_capacity);
    bool get_merge_switch(int64_t table_id);
    bool get_separate_switch(int64_t table_id);
    bool is_switch_open(const int64_t table_id, const std::string& switch_name);
    void get_schema_conf_op_info(const int64_t table_id, int64_t& op_version, std::string& op_desc);
    template <class T>
    int get_schema_conf_value(const int64_t table_id, const std::string& switch_name, T& value);
    int get_schema_conf_str(const int64_t table_id, const std::string& switch_name, std::string& value);
    int64_t get_ttl_duration(int64_t table_id);
    
    int get_region_by_key(int64_t main_table_id, 
            IndexInfo& index,
            const pb::PossibleIndex* primary,
            std::map<int64_t, pb::RegionInfo>& region_infos,
            std::map<int64_t, pb::PossibleIndex>* region_primary = nullptr);
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

    int get_region_by_key(IndexInfo& index,
            const std::vector<SmartRecord>& insert_records,
            const std::vector<SmartRecord>& delete_records,
            std::map<int64_t, std::vector<SmartRecord>>& insert_region_ids,
            std::map<int64_t, std::vector<SmartRecord>>& delete_region_ids,
            std::map<int64_t, pb::RegionInfo>& region_infos);

    bool exist_tableid(int64_t table_id);
    
    void get_all_table_version(std::unordered_map<int64_t, int64_t>& table_id_version);
    std::string physical_room() {
        return _physical_room;
    }
    std::string get_logical_room() {
        if (_logical_room.empty()) {
            DoubleBufferedIdc::ScopedPtr idc_ptr;
            if (_double_buffer_idc.Read(&idc_ptr) == 0) {
                auto& physical_logical_mapping = idc_ptr->physical_logical_mapping;
                if (physical_logical_mapping.find(_physical_room) != physical_logical_mapping.end()) {
                    _logical_room = physical_logical_mapping.at(_physical_room);
                }
            } else {
                DB_WARNING("read double_buffer_idc error.");
            }
        }
        //DB_NOTICE("get_logical_room [%s]", _logical_room.c_str());
        return _logical_room;
    }
    std::string logical_room_for_instance(const std::string& store) {
        DoubleBufferedIdc::ScopedPtr idc_ptr;
        if (_double_buffer_idc.Read(&idc_ptr) == 0) {
            auto& instance_logical_mapping = idc_ptr->instance_logical_mapping;
            if (instance_logical_mapping.find(store) != instance_logical_mapping.end()) {
                return instance_logical_mapping.at(store);
            }
        } else {
            DB_WARNING("read double_buffer_idc error.");
        }
        return "";
    }
    bool is_global_index(const int64_t& table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto& global_index_id_mapping = table_ptr->global_index_id_mapping;
        if (global_index_id_mapping.find(table_id) != global_index_id_mapping.end()
                && global_index_id_mapping.at(table_id) != table_id) {
            return true;
        }
        return false;
    }
    bool has_global_index(const int64_t& main_table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(main_table_id) == table_info_mapping.end()) {
            DB_WARNING("main_table_id: %ld not exist", main_table_id);
            return false;
        }
        auto table_info = table_info_mapping.at(main_table_id);
        std::vector<int64_t> indices = table_info->indices;
        auto& global_index_id_mapping = table_ptr->global_index_id_mapping;
        for (auto& index_id : indices) {
            if (global_index_id_mapping.find(index_id) != global_index_id_mapping.end()
                    && global_index_id_mapping.at(index_id) != index_id) {
                return true;
            }
        }
        return false;
    }

    int get_table_state(int64_t table_id, pb::IndexState& state) {
        auto table_ptr = get_table_info_ptr(table_id);
        if (table_ptr == nullptr) {
            DB_FATAL("table_id[%lld] not in schema", table_id);
            return -1;
        }
        for (auto index_id : table_ptr->indices) {
            auto index_ptr = get_index_info_ptr(index_id);
            if (index_ptr != nullptr) {
                if (index_ptr->state != pb::IS_PUBLIC) {
                    state = index_ptr->state;
                    break;
                }
            } else {
                DB_FATAL("index_id[%lld] table_id[%lld] not in schema", index_id, table_id);
                return -1;
            }
        }
        return 0;
    }

    int64_t last_updated_index() {
        return _last_updated_index;
    }
    void set_last_updated_index(const int64_t index) {
        _last_updated_index = index;
    }

    int get_index_storage_type(int64_t index_id, pb::StorageType& type) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto& index_info_mapping = table_ptr->index_info_mapping;
        if (index_info_mapping.find(index_id) == index_info_mapping.end()) {
            DB_WARNING("index_id: %ld not exist", index_id);
            return -1;
        }
        auto index_info_ptr = index_info_mapping.at(index_id);
        if (index_info_ptr->type == pb::I_FULLTEXT) {
            type = index_info_ptr->storage_type;
            return 0;
        } else {
            return -1;
        }
    }

private:
    SchemaFactory() {
        _is_init = false;
        bthread_mutex_init(&_update_user_mutex, NULL);
        bthread_mutex_init(&_update_show_db_mutex, NULL);
        butil::EndPoint addr;
        addr.ip = butil::my_ip();
        addr.port = 0;
        std::string address = endpoint2str(addr).c_str(); 
        auto ret = get_physical_room(address, _physical_room);
        if (ret < 0) {
            DB_FATAL("get physical room fail, ip: %s", address.c_str());
        }
    }
    int update_table_internal(SchemaMapping& schema_mapping, const pb::SchemaInfo& table);
    void update_schema_conf(const std::string& table_name, 
                            const pb::SchemaConf &schema_conf, 
                                           pb::SchemaConf& mem_conf);
    // 全量更新
    void update_index(TableInfo& info, const pb::IndexInfo& index,
            const pb::IndexInfo* pk_indexi, SchemaMapping& background);
    //delete table和index
    void delete_table(const pb::SchemaInfo& table, SchemaMapping& background);

    void delete_table_region_map(const pb::SchemaInfo& table);
    bool                    _is_init;
    bthread_mutex_t         _update_user_mutex;
    bthread_mutex_t         _update_show_db_mutex;

    // use for show databases
    std::map<int64_t, DatabaseInfo> _show_db_info;

    // username => UserPrivilege
    std::unordered_map<std::string, std::shared_ptr<UserInfo>> _user_info_mapping;
    
    DoubleBufferedTable _double_buffer_table;
    bthread::ExecutionQueueId<pb::SchemaInfo> _table_queue_id = {0};

    DoubleBufferedIdc _double_buffer_idc;
    bthread::ExecutionQueueId<pb::IdcInfo>  _idc_queue_id = {0};

    DoubleBufferedTableRegionInfo _table_region_mapping;
    bthread::ExecutionQueueId<RegionVec> _region_queue_id = {0};

    DoubleBufferStringSet _double_buffer_big_sql;
    bthread::ExecutionQueueId<std::string> _big_sql_queue_id = {0};

    std::string _physical_room;
    std::string _logical_room;
    int64_t     _last_updated_index = 0;
};
}

