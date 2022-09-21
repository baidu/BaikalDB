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
#include "expr_node.h"
#include "literal.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

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
static const std::string VIRTUAL_DATABASE_NAME = "__virtual_db";              // 临时表数据库名
static const std::string TABLE_IN_FAST_IMPORTER= "in_fast_import";
struct UserInfo;
class TableRecord;
typedef std::shared_ptr<TableRecord> SmartRecord;
typedef std::map<std::string, int64_t> StrInt64Map;

enum PartitionRegionSelect {
    PRS_RANDOM = 0
};
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
    // partion map of (partition_id => (start_key => regionid))
    std::unordered_map<int64_t, StrInt64Map> key_region_mapping;
    void update_leader(int64_t region_id, const std::string& leader) {
        if (region_info_mapping.count(region_id) == 1) {
            region_info_mapping[region_id].region_info.set_leader(leader);
            DB_DEBUG("double_buffer_write region_id[%ld] set_leader[%s]", 
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
    
    DB_DEBUG("double_buffer_write table_id[%ld], region_id[%ld], update_leader [%s]",
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
    std::string         lower_name;   // db.table.(lower)field
    std::string         short_name; // field
    std::string         lower_short_name;   // (lower)field
    std::string         default_value;
    ExprValue           default_expr_value;
    std::string         on_update_value;
    std::string         comment;
    pb::PrimitiveType   type;
    bool                can_null = false;
    bool                auto_inc = false;
    bool                deleted = false;
    bool                noskip = false;
    uint32_t            flag   = 0;
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
class Partition {
public:
    virtual int init(const pb::PartitionInfo& partition_info, int64_t table_id, int64_t partition_num) = 0;
    virtual int64_t calc_partition(SmartRecord record) = 0;
    virtual int64_t calc_partition(const ExprValue& field_value) = 0;
    virtual std::string to_str() = 0;
};

class HashPartition : public Partition {
public: 
    int init(const pb::PartitionInfo& partition_info, int64_t table_id, 
        int64_t partition_num) {
        _table_id = table_id;
        _partition_num = partition_num;
        _partition_info.CopyFrom(partition_info);
        if (_partition_num == 0) {
            DB_FATAL("table_id[%ld] partition_num is zero", _table_id);
            return -1;
        }
        return 0;
    };
    int64_t calc_partition(SmartRecord record);
    int64_t calc_partition(const ExprValue& field_value) {
        if (field_value.is_numberic()) {
            return field_value.get_numberic<int64_t>() % _partition_num; 
        } else {
            return field_value.hash() % _partition_num;
        }
    }

    std::string to_str() {
        return ""; 
    }
private:
    pb::PartitionInfo _partition_info;
    int64_t _table_id;
    int64_t _partition_num;
};

class RangePartition : public Partition {
public: 
    int init(const pb::PartitionInfo& partition_info, int64_t table_id, int64_t partition_num) {
        std::lock_guard<std::mutex> lock(_mutex);
        _table_id = table_id;
        _partition_num = partition_num;
        _range_expr.clear();
        _partition_info.CopyFrom(partition_info);
        for (const auto& range : partition_info.range_partition_values()) {
            ExprNode* node_ptr = nullptr;
            if (0 != ExprNode::create_expr_node(range.nodes(0), &node_ptr)) {
                DB_WARNING("create expr node error.");
                return -1;
            }
            _range_expr.push_back(node_ptr->get_value(nullptr));
        }
        if (_partition_num == 0 || _range_expr.size() == 0) {
            DB_WARNING("partition_num [%ld] or range_expr size is zero", _partition_num);
            return -1;
        }
        if (_partition_info.partition_names_size() != _range_expr.size()) {
            _partition_info.clear_partition_names();
            for (uint32_t i = 0; i < _range_expr.size(); i++) {
                _partition_info.add_partition_names("p" + std::to_string(i));
            }
        }
        return 0;
    };
    int64_t calc_partition(SmartRecord record);
    int64_t calc_partition(const ExprValue& field_value) {
        size_t range_index = 0;
        for (; range_index < _range_expr.size(); ++range_index) {
            if (field_value.compare(_range_expr[range_index]) < 0) {
                return range_index;
            }
        }
        return range_index;
    }

    std::string to_str() {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<std::string> exprs;
        exprs.reserve(5);
        std::string ret;
        for (uint32_t i = 0; i < _range_expr.size(); i++) {
            ret += "\nPARTITION ";
            ret += _partition_info.partition_names(i);
            ret += " VALUES LESS THAN (";
            ret += _range_expr[i].get_string();
            ret += "),";
        }
        if (!ret.empty()) {
            ret.pop_back();
        }
        return ret;
    }
private:
    std::vector<ExprValue> _range_expr;
    pb::PartitionInfo _partition_info;
    int64_t _table_id;
    int64_t _partition_num;
    std::mutex _mutex;
};

struct TTLInfo {
    TTLInfo() { }
    int64_t ttl_duration_s           = 0; // >0表示配置有ttl，单位s
    int64_t online_ttl_expire_time_us = 0; // online ttl 过期时间
};

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
    std::string             main_logical_room;
    std::vector<int64_t>    indices; // include pk
    std::vector<FieldInfo>  fields;
    std::vector<DistInfo>   dists;
    int64_t                 replica_num = 3;
    std::string             comment;
    //table字段不变的话不需要重新构建动态pb
    std::string             fields_sign;
    TTLInfo                 ttl_info;
    int32_t                 max_field_id = 0;
    int32_t                 region_num = 0;

    const Descriptor*       tbl_desc = nullptr;
    DescriptorProto*        tbl_proto = nullptr;
    FileDescriptorProto*    file_proto = nullptr;
    DynamicMessageFactory*  factory = nullptr;
    DescriptorPool*         pool = nullptr;
    const Message*          msg_proto = nullptr;
    uint32_t                timestamp = 0;
    bool                    have_statistics = false;
    bool                    have_backup = false;
    bool                    need_read_backup = false;
    bool                    need_write_backup = false;
    bool                    need_learner_backup = false;
    bool                    has_global_not_none = false;
    bool                    has_index_write_only_or_write_local = false;
    bool                    has_fulltext = false;
    // 该表是否已和 binlog 表关联
    bool is_linked = false;
    pb::PartitionInfo partition_info;
    // 该binlog表关联的普通表集合
    std::set<uint64_t> binlog_target_ids;
    // 该表关联的 binlog 表id
    int64_t binlog_id = 0;
    std::shared_ptr<Partition> partition_ptr = nullptr;
    // 普通表 使用该字段和 binlog 表进行关联
    std::vector<FieldInfo> link_field;
    std::unordered_map<int64_t, int64_t>    reverse_fields;
    std::unordered_map<int64_t, int64_t>    arrow_reverse_fields;
    std::vector<std::string> learner_resource_tags;
    std::set<uint64_t> sign_blacklist;
    std::set<uint64_t> sign_forcelearner;
    std::set<std::string> sign_forceindex;
    
    TableInfo() {}
    FieldInfo* get_field_ptr(int32_t field_id) {
        for (auto& info : fields) {
            if (info.id == field_id) {
                return &info;
            }
        }
        return nullptr;
    }

    int32_t get_field_id_by_short_name(const std::string& short_name) {
        for (auto& info : fields) {
            if (info.short_name == short_name) {
                return info.id;
            }
        }
        return -1;
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
    bool                    has_nullable = false;

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
    pb::IndexState          state;
    bool                    is_global = false;
    pb::StorageType storage_type = pb::ST_PROTOBUF_OR_FORMAT1;
    pb::IndexHintStatus     index_hint_status = pb::IHS_NORMAL;
    int64_t                 write_only_time = -1;
    int64_t                 restore_time = -1;
    int64_t                 disable_time = -1;
    int32_t                 max_field_id = 0;
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

struct InstanceDBStatus {
    // NORMAL 正常
    // FAULTY 故障
    // DEAD hang住假死，需要做rpc cancel
    pb::Status status = pb::NORMAL;
    // 只cancel一次，cancel操作后设置false
    bool need_cancel = true;
    std::string logical_room;
    // 正常探测CHECK_COUNT次后才置NORMAL
    int64_t normal_count = 0;
    // 业务请求探测CHECK_COUNT次后才置FAULTY
    int64_t faulty_count = 0;
    TimeCost last_update_time;
    static const int64_t CHECK_COUNT = 10;
};
struct IdcMapping {
    // store => logical_room
    std::unordered_map<std::string, InstanceDBStatus> instance_info_mapping;
    // physical_room => logical_room
    std::unordered_map<std::string, std::string> physical_logical_mapping;
};

using DoubleBufferedIdc = butil::DoublyBufferedData<IdcMapping>;
using DoubleBufferedUser = butil::DoublyBufferedData<std::unordered_map<std::string, std::shared_ptr<UserInfo>>>;

struct SqlStatistics {
    static const int64_t TOTAL_COUNT = 1000000;
    static const int64_t SQL_COUNTS_RANGE = 3;
    double qps = 0.0;
    int64_t avg_scan_rows = 0;
    int64_t scan_rows_9999 = 0;
    int64_t avg_scan_rows_per_region = 0;
    int64_t latency_us = 0;
    int64_t latency_us_9999 = 0;
    int64_t times_avg_and_9999 = 20; // latency_us_9999 / latency_us，应对平响上升后的突发响应
    int64_t counter = 0;
    bool first = true;
    Heap<int64_t> latency_heap{100};
    Heap<int64_t> scan_rows_heap{100};
    std::mutex mutex;
    int64_t dynamic_timeout_ms() const {
        if (latency_us > 1000000) {
            return -1;
        }
        //有sql扫描量很大的并且不稳定的
        if (scan_rows_9999 > 10000 && avg_scan_rows > 0 && scan_rows_9999 / avg_scan_rows > 100) {
            return -1;
        }
        if (latency_us_9999 > 0 || latency_us > 0) {
            return latency_us_9999 / 1000;
        }
        return -1;
    }
    int64_t latency_heap_top() {
        if (!latency_heap.empty()) {
            return latency_heap.top();
        }
        return -1;
    }
    void update(int64_t cost, int64_t scan_rows) {
        std::unique_lock<std::mutex> lock(mutex);
        if (++counter > TOTAL_COUNT) {
            latency_us_9999 = latency_heap.top();
            scan_rows_9999 = scan_rows_heap.top();
            if (latency_us > 0) {
                times_avg_and_9999 = latency_us_9999 / latency_us + 1;
            }
            counter = 0;
            latency_heap.clear();
            latency_heap.resize(100);
            scan_rows_heap.clear();
            scan_rows_heap.resize(100);
            first = false;
            return;
        }
        if (first) {
            //第一轮计算，快速计算出非精确的值
            if (counter < 1000) {
                latency_us_9999 = std::max(latency_us_9999, cost);
                scan_rows_9999 = std::max(scan_rows_9999, scan_rows);
            } else {
                latency_us_9999 = latency_heap.top();
                scan_rows_9999 = scan_rows_heap.top();
            }
            if (latency_us > 0) {
                times_avg_and_9999 = latency_us_9999 / latency_us + 1;
            }
        }
        if (cost > latency_heap.top()) {
            latency_heap.replace_top(cost);
        }
        if (scan_rows > scan_rows_heap.top()) {
            scan_rows_heap.replace_top(scan_rows);
        }
    }
};

using SqlStatMap = std::unordered_map<uint64_t, std::shared_ptr<SqlStatistics>>;
using DoubleBufferedSql = butil::DoublyBufferedData<SqlStatMap>;

class SchemaFactory {
typedef ::google::protobuf::RepeatedPtrField<pb::RegionInfo> RegionVec;
typedef ::google::protobuf::RepeatedPtrField<pb::SchemaInfo> SchemaVec;
typedef ::google::protobuf::RepeatedPtrField<pb::DataBaseInfo> DataBaseVec;
typedef ::google::protobuf::RepeatedPtrField<pb::Statistics> StatisticsVec;
public:
    virtual ~SchemaFactory() {
        bthread_mutex_destroy(&_update_show_db_mutex);
    }

    static BthreadLocal<bool> use_backup;
    static SchemaFactory* get_instance() {
        bool* bk = use_backup.get_bthread_local();
        if (bk != nullptr && *bk) {
            return get_backup_instance();
        }
        static SchemaFactory _instance;
        return &_instance;
    }

    static SchemaFactory* get_backup_instance() {
        static SchemaFactory _instance;
        return &_instance;
    }
    bool is_inited() {
        return _is_inited;
    }
    //bucket_size
    int init();

    // not thread-safe, should be called in single thread
    // 删除判断deleted, name允许改
    void update_table(const pb::SchemaInfo& table);
    //void update_table(DoubleBufferedTable& double_buffered_table, const pb::SchemaInfo& table);

    // _sync系统初始化的时候调用，防止meta信息获取延迟导致系统不可用
    void update_tables_double_buffer_sync(const SchemaVec& tables);

    void update_instance_canceled(const std::string& addr);
    void update_instance(const std::string& addr, pb::Status s, bool user_check, bool cover_dead);
    int update_instance_internal(IdcMapping& idc_mapping, const std::string& addr, pb::Status s, bool user_check);
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
                           std::map<std::string, int64_t>& clear_regions, int64_t partition);
    void clear_region(TableRegionPtr background, 
                      std::map<std::string, int64_t>& clear_regions, int64_t partition);
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
    int64_t get_total_rows(int64_t table_id);
    // 从直方图中计算取值区间占比，如果计算小于某值的比率，则lower填null；如果计算大于某值的比率，则upper填null
    double get_histogram_ratio(int64_t table_id, int field_id, const ExprValue& lower, const ExprValue& upper);
    // 计算单个值占比
    double get_cmsketch_ratio(int64_t table_id, int field_id, const ExprValue& value);
    SmartStatistics get_statistics_ptr(int64_t table_id);
    int64_t get_histogram_sample_cnt(int64_t table_id);
    int64_t get_histogram_distinct_cnt(int64_t table_id, int field_id); 
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
    // split使用的index_info，只加不删
    IndexInfo* get_split_index_info(int64_t indexid) {
        auto iter = _split_index_map.read()->seek(indexid);
        if (iter != nullptr) {
            return *iter;
        }
        return nullptr;
    }

    std::string get_index_name(int64_t index_id);

    // functions for permission access
    std::shared_ptr<UserInfo> get_user_info(const std::string& user);
    std::shared_ptr<SqlStatistics> get_sql_stat(int64_t sign);
    std::shared_ptr<SqlStatistics> create_sql_stat(int64_t sign);
    std::vector<std::string> get_db_list(const std::set<int64_t>& db);
    std::vector<std::string> get_table_list(
            std::string namespace_, std::string db_name, UserInfo* user);
    std::vector<SmartTable> get_table_list(std::string namespace_, UserInfo* user);

    // table_name is full name (namespace.database.table)
    int get_table_id(const std::string& table_name, int64_t& table_id);

    // db_name is full name (namespace.database)
    int get_database_id(const std::string& db_name, int64_t& db_id);

    //int get_column_id(const std::string& col_name, int32_t col_id) const;

    int get_index_id(int64_t table_id, 
                    const std::string& index_name, 
                    int64_t& index_id);

    // functions for region info access
    int get_region_info(int64_t table_id, int64_t region_id, pb::RegionInfo& info);

    int get_region_capacity(int64_t global_index_id, int64_t& region_capacity);
    bool get_merge_switch(int64_t table_id);
    bool get_separate_switch(int64_t table_id);
    bool is_switch_open(const int64_t table_id, const std::string& switch_name);
    bool is_in_fast_importer(int64_t table_id);
    void get_cost_switch_open(std::vector<std::string>& database_table);
    void get_schema_conf_open(const std::string& conf_name, std::vector<std::string>& database_table);
    void get_table_by_filter(std::vector<std::string>& database_table, std::vector<std::string>& link_table,
            const std::function<bool(const SmartTable&)>& select_table);
    void table_with_statistics_info(std::vector<std::string>& database_table);
    int sql_force_learner_read(int64_t table_id, uint64_t sign);
    void get_schema_conf_op_info(const int64_t table_id, int64_t& op_version, std::string& op_desc);
    template <class T>
    int get_schema_conf_value(const int64_t table_id, const std::string& switch_name, T& value);
    int get_schema_conf_str(const int64_t table_id, const std::string& switch_name, std::string& value);
    TTLInfo get_ttl_duration(int64_t table_id);
    
    int get_all_region_by_table_id(int64_t table_id, 
            std::map<std::string, pb::RegionInfo>* region_infos,
            const std::vector<int64_t>& partitions = std::vector<int64_t>{0});
    int check_region_ranges_consecutive(int64_t table_id);
    int get_region_by_key(int64_t main_table_id, 
            IndexInfo& index,
            const pb::PossibleIndex* primary,
            std::map<int64_t, pb::RegionInfo>& region_infos,
            std::map<int64_t, std::string>* region_primary = nullptr,
            const std::vector<int64_t>& partitions = std::vector<int64_t>{0},
            bool is_full_export = false);
    // only used for pk (not null)
    int get_region_by_key(IndexInfo& index, 
            const pb::PossibleIndex* primary,
            std::map<int64_t, pb::RegionInfo>& region_infos,
            std::map<int64_t, std::string>* region_primary = nullptr);

    int get_region_by_key(
            const RepeatedPtrField<pb::RegionInfo>& input_regions,
            std::map<int64_t, pb::RegionInfo>& output_regions);

    int get_region_by_key(IndexInfo& index,
            const std::vector<SmartRecord>& records,
            std::map<int64_t, std::vector<SmartRecord>>& region_ids,
            std::map<int64_t, pb::RegionInfo>& region_infos);

    int get_region_by_key(IndexInfo& index,
            const std::vector<SmartRecord>& insert_records,
            const std::vector<SmartRecord>& delete_records,
            std::map<int64_t, std::vector<SmartRecord>>& insert_region_ids,
            std::map<int64_t, std::vector<SmartRecord>>& delete_region_ids,
            std::map<int64_t, pb::RegionInfo>& region_infos);

    int get_region_ids_by_key(IndexInfo& index,
                              const std::vector<SmartRecord>&  records,
                              std::vector<int64_t>& region_ids);

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
    InstanceDBStatus get_instance_status(const std::string& store) {
        DoubleBufferedIdc::ScopedPtr idc_ptr;
        if (_double_buffer_idc.Read(&idc_ptr) == 0) {
            auto& instance_info_mapping = idc_ptr->instance_info_mapping;
            auto iter = instance_info_mapping.find(store);
            if (iter != instance_info_mapping.end()) {
                return iter->second;
            }
        } else {
            DB_WARNING("read double_buffer_idc error.");
        }
        return InstanceDBStatus();
    }
    int get_all_instance_status(std::unordered_map<std::string, InstanceDBStatus>* info_map) {
        DoubleBufferedIdc::ScopedPtr idc_ptr;
        if (_double_buffer_idc.Read(&idc_ptr) == 0) {
            *info_map = idc_ptr->instance_info_mapping;
            return 0;
        } else {
            DB_WARNING("read double_buffer_idc error.");
            return -1;
        }
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
        const auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(main_table_id) == table_info_mapping.end()) {
            DB_WARNING("main_table_id: %ld not exist", main_table_id);
            return false;
        }
        const auto& table_info = table_info_mapping.at(main_table_id);
        return table_info->has_global_not_none;
    }

    bool has_fulltext_index(const int64_t& main_table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        const auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(main_table_id) == table_info_mapping.end()) {
            DB_WARNING("main_table_id: %ld not exist", main_table_id);
            return false;
        }
        const auto& table_info = table_info_mapping.at(main_table_id);
        return table_info->has_fulltext;
    }

    bool need_begin_txn(const int64_t& main_table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        const auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(main_table_id) == table_info_mapping.end()) {
            DB_WARNING("main_table_id: %ld not exist", main_table_id);
            return false;
        }
        const auto& table_info = table_info_mapping.at(main_table_id);
        return table_info->has_global_not_none || table_info->has_index_write_only_or_write_local;
    }

    int get_table_state(int64_t table_id, pb::IndexState& state) {
        auto table_ptr = get_table_info_ptr(table_id);
        if (table_ptr == nullptr) {
            DB_FATAL("table_id[%ld] not in schema", table_id);
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
                DB_FATAL("index_id[%ld] table_id[%ld] not in schema", index_id, table_id);
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

    int get_disable_indexs(std::vector<std::string>& index_names) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto& index_info_mapping = table_ptr->index_info_mapping;
        for (auto& index_info : index_info_mapping) {
            if (index_info.second->index_hint_status == pb::IHS_DISABLE) {
                index_names.emplace_back(index_info.second->name);
            }
        }
        return 0;
    }

    bool is_table_partitioned(int64_t table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(table_id) == table_info_mapping.end()) {
            //DB_WARNING("table_id: %ld not exist", table_id);
            return false;
        }
        auto table_info = table_info_mapping.at(table_id);
        return table_info->partition_num > 1;
    }

    int get_partition_index(int64_t table_id, const ExprValue& value, int64_t& partition_index) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(table_id) == table_info_mapping.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return -1;
        }
        auto table_info = table_info_mapping.at(table_id);
        if (table_info->partition_num == 1) {
            //非分区表，返回0分区
            partition_index = 0;
            return 0;
        }
        if (table_info->partition_ptr != nullptr) {
            partition_index = table_info->partition_ptr->calc_partition(value);
            if (partition_index < 0) {
                DB_WARNING("get partition number error, value:%s", value.get_string().c_str());
                return -1;
            }
        } else {
            DB_WARNING("get partition number error, value:%s", value.get_string().c_str());
            return -1;
        }
        return 0;
    }

    int get_binlog_id(int64_t table_id, int64_t& binlog_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(table_id) == table_info_mapping.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return -1;
        }
        auto table_info = table_info_mapping.at(table_id);
        if (table_info->is_linked) {
            binlog_id = table_info->binlog_id;
            return 0;
        }
        return -1;
    }

    int get_binlog_regions(int64_t binlog_id, int64_t partition_index, std::map<int64_t, pb::RegionInfo>& region_infos);

    int get_partition_binlog_regions(const std::string& db_table_name, int64_t partition_input_value, 
        std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<pb::RegionInfo>>>& table_id_partition_binlogs);

    bool has_open_binlog(int64_t table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        if (table_info_mapping.find(table_id) == table_info_mapping.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return false;
        }
        auto table_info = table_info_mapping.at(table_id);
        if (table_info->is_linked) {
            return true;
        }
        return false;
    }

    int get_binlog_regions(int64_t table_id, pb::RegionInfo& region_info, const ExprValue& value = ExprValue{}, 
        PartitionRegionSelect prs = PRS_RANDOM);

    bool is_region_info_exist(int64_t table_id) {
        DoubleBufferedTableRegionInfo::ScopedPtr table_region_mapping_ptr;
        if (_table_region_mapping.Read(&table_region_mapping_ptr) != 0) {
            DB_WARNING("DoubleBufferedTableRegion read scoped ptr error."); 
            return false;
        }
        auto it = table_region_mapping_ptr->find(table_id);
        if (it == table_region_mapping_ptr->end()) {
            DB_WARNING("index id[%ld] not in table_region_mapping", table_id);
            return false;
        }
        return true;
    }

    void update_virtual_index_info(const int64_t virtual_index_id, const std::string& virtual_index_name, const std::string& sample_sql) {
        _virtual_index_info << VirtualIndexMap(virtual_index_id, virtual_index_name, sample_sql);
    }

    VirtualIndexMap get_virtual_index_info() {
        return _virtual_index_info.reset();
    }
    int is_unique_field_ids(int64_t table_id, const std::set<int32_t>& field_ids);

    int fill_default_value(SmartRecord record, FieldInfo& field);

    int64_t get_baikaldb_alive_time_us() {
        return _baikaldb_restart_time.get_time();
    }

private:
    SchemaFactory() {
        _is_inited = false;
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
            const pb::IndexInfo* pk_index, SchemaMapping& background);
    //delete table和index
    void delete_table(const pb::SchemaInfo& table, SchemaMapping& background);

    void delete_table_region_map(const pb::SchemaInfo& table);
    bool                    _is_inited;
    bthread_mutex_t         _update_show_db_mutex;

    // use for show databases
    std::map<int64_t, DatabaseInfo> _show_db_info;

    // username => UserPrivilege
    DoubleBufferedUser _user_info_mapping;
    
    DoubleBufferedTable _double_buffer_table;
    // index_id => IndexInfo*
    // 提供给SplitCompactionFilter使用，使用普通双buf，split减少开销
    DoubleBuffer<butil::FlatMap<int64_t, IndexInfo*>> _split_index_map;

    DoubleBufferedIdc _double_buffer_idc;

    DoubleBufferedTableRegionInfo _table_region_mapping;
    bthread::ExecutionQueueId<RegionVec> _region_queue_id = {0};

    DoubleBufferStringSet _double_buffer_big_sql;
    DoubleBufferedSql _double_buffer_sql_stat;

    std::string _physical_room;
    std::string _logical_room;
    int64_t     _last_updated_index = 0;
    bvar::Adder<VirtualIndexMap> _virtual_index_info; // 虚拟索引使用

    //记录baikaldb模块的起始时间用于计算启动时长
    TimeCost _baikaldb_restart_time;
};
}

