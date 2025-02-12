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
#include "user_info.h"
#include "proto_process.hpp"
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
static const std::string TABLE_TAIL_SPLIT_NUM  = "tail_split_num";            //尾分裂数量
static const std::string TABLE_TAIL_SPLIT_STEP = "tail_split_step";           //尾分裂步长
static const std::string TABLE_BINLOG_BACKUP_DAYS = "binlog_backup_days";     //binlog表备份天数

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
    bool                is_unique_indicator = false; // 指标唯一列
    int32_t             float_total_len = -1;
    int32_t             float_precision_len = -1;
};

struct DistInfo {
    std::string logical_room;
    int64_t count;
    std::string resource_tag;
    std::string physical_room;
};

struct TTLInfo {
    TTLInfo() { }
    int64_t ttl_duration_s           = 0; // >0表示配置有ttl，单位s
    int64_t online_ttl_expire_time_us = 0; // online ttl 过期时间
};

class Partition;
struct TableInfo {
    int64_t                 id = -1;
    int64_t                 db_id = -1;
    int64_t                 version = -1;
    int64_t                 partition_num;
    int64_t                 region_split_lines;
    int64_t                 byte_size_per_record = 1; //默认情况下不分裂，兼容以前的表
    int64_t                 auto_inc_field_id = -1; //自增字段id
    int64_t                 auto_inc_rand_max = -1; //meta挂掉后降级到随机id，>0生效
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
    bool                    has_rollup_index = false;
    bool                    has_vector_index = false;
    // 该表是否已和 binlog 表关联
    bool is_linked = false;
    bool is_binlog = false;
    // 是否是range分区表
    bool is_range_partition = false;
    // 是否是视图表
    bool is_view = false;
    std::string view_select_stmt;
    pb::PartitionInfo partition_info;
    // 该binlog表关联的普通表集合
    std::set<uint64_t> binlog_target_ids;
    // 该表关联的 binlog 表id
    int64_t binlog_id = 0;
    // key关联的binlog表id,val: partition_is_same_hint默认分区方式一样
    std::map<int64_t, bool> binlog_ids;
    std::shared_ptr<Partition> partition_ptr = nullptr;
    // 普通表 使用该字段和 binlog 表进行关联
    std::map<int64_t, FieldInfo> link_field_map;
    std::unordered_map<int64_t, int64_t>    reverse_fields;
    std::unordered_map<int64_t, int64_t>    arrow_reverse_fields;
    std::vector<std::string> learner_resource_tags;
    std::set<uint64_t> sign_blacklist;
    std::set<uint64_t> sign_forcelearner;
    std::set<uint64_t> sign_rolling;
    std::set<std::string> sign_forceindex;
    std::set<std::string> sign_exec_type;
    std::set<uint64_t> snapshot_blacklist;
    // use for olap merge operator
    std::vector<FieldInfo> fields_need_sum;
    FieldInfo version_field;
    bool has_version = false;

    // DBLINK表
    pb::DBLinkInfo dblink_info;
    
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
    bool                    is_partitioned = false;
    pb::StorageType storage_type = pb::ST_PROTOBUF_OR_FORMAT1;
    pb::IndexHintStatus     index_hint_status = pb::IHS_NORMAL;
    int64_t                 write_only_time = -1;
    int64_t                 restore_time = -1;
    int64_t                 disable_time = -1;
    int32_t                 max_field_id = 0;
    //vector index
    int32_t                 dimension = 0;
    int32_t                 nprobe = 5;
    pb::RollupType          rollup_type = pb::SUM;
    int32_t                 efsearch = 16;
    int32_t                 efconstruction = 40;
    std::string             vector_description;
    pb::MetricType          metric_type = pb::METRIC_L2;
    int32_t                 publish_timestamp;
};

struct DatabaseInfo {
    int64_t                 id = -1;
    int64_t                 version = -1;
    std::string             name; // db
    std::string             namespace_;

    // infomration_schema使用
    bool                        has_engine = false;
    pb::Engine                  engine;
    bool                        has_charset = false;
    pb::Charset                 charset;
    std::string                 resource_tag;
    std::vector<DistInfo>       dists;
    std::string                 main_logical_room;
    int64_t                     replica_num = -1;
    int64_t                     byte_size_per_record = -1;
    int64_t                     region_split_lines = -1;
    std::vector<std::string>    learner_resource_tags;
    std::vector<pb::BinlogInfo> binlog_infos;
    std::string                 partition_info_str;
};

typedef std::shared_ptr<TableInfo> SmartTable;
typedef std::shared_ptr<IndexInfo> SmartIndex;
struct SchemaMapping {
    // namespace.database (db) => database_id
    std::unordered_map<std::string, int64_t> db_name_id_mapping;
    // database_id => DatabaseInfo
    std::unordered_map<int64_t, DatabaseInfo> db_info_mapping;
    // namespace.database.table_name (db.table) => table_id
    std::unordered_map<std::string, int64_t> table_name_id_mapping;
    // table_id => TableInfo
    std::unordered_map<int64_t, SmartTable> table_info_mapping;
    // index_id => table_id
    std::unordered_map<int64_t, int64_t> index_table_info_mapping;
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
    std::string resource_tag;
    // 正常探测CHECK_COUNT次后才置NORMAL
    int64_t normal_count = 0;
    // 业务请求探测CHECK_COUNT次后才置FAULTY
    int64_t faulty_count = 0;
    TimeCost last_update_time;
    static const int64_t CHECK_COUNT = 10;
};
struct IdcMapping {
    // meta_id => store，记录每个Meta对应的store实例信息，用于全量更新时删除对应Meta的store实例信息
    std::unordered_map<int64_t, std::unordered_set<std::string>> meta_store_mapping;
    // store => logical_room
    std::unordered_map<std::string, InstanceDBStatus> instance_info_mapping;
    // physical_room => logical_room
    std::unordered_map<std::string, std::string> physical_logical_mapping;
};

// short name
inline FieldInfo* get_field_info_by_name(
        std::vector<FieldInfo>& fields, const std::string& name) {
    for (auto& field : fields) {
        if (field.short_name == name) {
            return &field;
        }
    }
    return nullptr;
}

class Partition {
public:
    virtual int init(const pb::PartitionInfo& partition_info, SmartTable& table_ptr, int64_t partition_num) = 0;
    virtual int64_t calc_partition(
            const std::shared_ptr<UserInfo>& user_info, SmartRecord record, 
            bool is_read = false, int64_t* p_another_partition_id = nullptr) = 0;
    virtual int64_t calc_partition(
            const std::shared_ptr<UserInfo>& user_info, const ExprValue& field_value, 
            bool is_read = false, int64_t* p_another_partition_id = nullptr) = 0;
    virtual std::string to_str() = 0;
    virtual int64_t partition_field_id() const = 0;
    virtual pb::PartitionType partition_type() const = 0;
    virtual bool get_partition_id_by_name(const std::string& partition_name, int64_t& partition_id) = 0;
    virtual std::string get_partition_uniq_str(int64_t partition_id) = 0;
    virtual ~Partition() {}
};

class HashPartition : public Partition {
public:
    ~HashPartition() {
        if (_hash_expr != nullptr) {
            _hash_expr->close();
            delete _hash_expr;
            _hash_expr = nullptr;
        }
    }
    int init(const pb::PartitionInfo& partition_info, SmartTable& table_ptr, int64_t partition_num);
    int64_t calc_partition(
            const std::shared_ptr<UserInfo>& user_info, SmartRecord record,
            bool is_read = false, int64_t* p_another_partition_id = nullptr) override;
    int64_t calc_partition(
            const std::shared_ptr<UserInfo>& user_info, const ExprValue& field_value, 
            bool is_read = false, int64_t* p_another_partition_id = nullptr) override;

    bool get_partition_id_by_name(const std::string& partition_name, int64_t& partition_id) {
        auto iter = _partition_name_map.find(partition_name);
        if (iter != _partition_name_map.end()) {
            partition_id = iter->second;
            return true;
        }
        return false;
    }

    int64_t partition_field_id() const {
        return _partition_field_id;
    }
    
    pb::PartitionType partition_type() const {
        return _partition_info.type();
    }

    std::string to_str() {
        std::string partition_s = "\nPARTITION BY HASH (";
        if (_partition_info.has_expr_string()) {
            partition_s += _partition_info.expr_string();
        } else {
            partition_s += _partition_info.field_info().field_name();
        }
        partition_s += ") \nPARTITIONS ";
        partition_s += std::to_string(_partition_num);
        return partition_s;
    }
    std::string get_partition_uniq_str(int64_t partition_id) {
        return std::to_string(partition_id);
    }
private:
    int64_t _table_id;
    int64_t _partition_num;
    int32_t _partition_field_id;
    ExprNode* _hash_expr = nullptr;
    SmartTable _table_ptr;
    FieldInfo* _field_info = nullptr;
    pb::PartitionInfo _partition_info;
    std::map<std::string, int64_t> _partition_name_map;
};

class RangePartition : public Partition {
public: 
    struct Range {
        pb::RangePartitionType partition_type;
        std::string partition_name;
        int64_t partition_id;
        ExprValue left_value;
        ExprValue right_value;
        bool is_cold;
        bool is_isolation;
    };

    struct RangeComparator {
        bool operator() (const Range& range1, const Range& range2) {
            int64_t res = range1.right_value.compare(range2.right_value);
            if (res == 0) {
                return range1.partition_type <= range2.partition_type;
            } else {
                return res < 0;
            }
        }
    };

    ~RangePartition() {
        if (_range_expr != nullptr) {
            _range_expr->close();
            delete _range_expr;
            _range_expr = nullptr;
        }
    }
    int init(const pb::PartitionInfo& partition_info, SmartTable& table_ptr, int64_t partition_num);
    int64_t calc_partition(
            const std::shared_ptr<UserInfo>& user_info, SmartRecord record,
            bool is_read = false, int64_t* p_another_partition_id = nullptr) override;
    int64_t calc_partition(
            const std::shared_ptr<UserInfo>& user_info, const ExprValue& value, 
            bool is_read = false, int64_t* p_another_partition_id = nullptr) override {
        if (_range_expr == nullptr) {
            return -1;
        }

        bool is_request_additional = false;
        bool is_switch_table = false;
        pb::RangePartitionType req_range_partition_type = pb::RPT_DEFAULT;
        if (user_info != nullptr) {
            is_request_additional = user_info->is_request_additional;
            req_range_partition_type = user_info->request_range_partition_type;
            // 如果未在切换表中，使用账户粒度的访问分区类型；
            // 如果在切换表中，根据账户粒度的访问分区类型取指定的分区类型，RPT_DEFAULT<->RPT_NEW_StatsEngine
            if (user_info->switch_tables.find(_table_id) != user_info->switch_tables.end()) {
                is_switch_table = true;
                if (!is_read) {
                    return -1; // switch table 不允许写
                }
                if (req_range_partition_type == pb::RPT_DEFAULT) {
                    req_range_partition_type = pb::RPT_NEW_StatsEngine;
                } else {
                    req_range_partition_type = pb::RPT_DEFAULT;
                }
            }
        }
        auto field_value = _range_expr->get_value(value);
        int64_t partition_id = -1;

        // 账号设置is_request_additional为true时，只读写外挂分区
        if (is_request_additional) {
            for (auto it = _additional_ranges.rbegin(); it != _additional_ranges.rend(); ++it) {
                if (field_value.compare(it->left_value) >= 0 &&
                        field_value.compare(it->right_value) < 0) {
                    partition_id = it->partition_id;
                    break;
                }
            }
            return partition_id;
        }
        
        // 账号设置is_request_additional为false时，写只访问正常分区，读访问正常分区及外挂分区
        for (auto it = _ranges.rbegin(); it != _ranges.rend(); ++it) {
            if (!is_specified_range(*it, req_range_partition_type, is_switch_table)) {
                continue;
            }
            if (field_value.compare(it->left_value) >= 0 &&
                    field_value.compare(it->right_value) < 0) {
                partition_id = it->partition_id;
                break;
            }
        }
        if (is_read) {
            // 读场景需要同时读外挂分区
            int another_partition_id = -1;
            for (auto it = _additional_ranges.rbegin(); it != _additional_ranges.rend(); ++it) {
                if (field_value.compare(it->left_value) >= 0 &&
                        field_value.compare(it->right_value) < 0) {
                    another_partition_id = it->partition_id;
                    break;
                }
            }
            if (partition_id == -1) {
                partition_id = another_partition_id;
            } else {
                if (p_another_partition_id != nullptr) {
                    *p_another_partition_id = another_partition_id;
                }
            }
        }
        return partition_id;
    }
    int64_t calc_partitions(
            const std::shared_ptr<UserInfo>& user_info,
            const ExprValue& left_value, const bool left_open,
            const ExprValue& right_value, const bool right_open, 
            std::set<int64_t>& partition_ids,
            bool is_read = false) {
        if (_range_expr == nullptr) {
            return -1;
        }
        // 升级成支持所有的range_expr类型 OLAPTODO
        if (_range_expr->node_type() != pb::SLOT_REF) {
            for (const auto& range : _ranges) {
                partition_ids.emplace(range.partition_id);
            }
            return 0;
        }
        bool is_request_additional = false;
        bool is_switch_table = false;
        pb::RangePartitionType req_range_partition_type = pb::RPT_DEFAULT;
        if (user_info != nullptr) {
            is_request_additional = user_info->is_request_additional;
            req_range_partition_type = user_info->request_range_partition_type;
            // 如果未在切换表中，使用账户粒度的访问分区类型；
            // 如果在切换表中，根据账户粒度的访问分区类型取指定的分区类型，RPT_DEFAULT<->RPT_NEW_StatsEngine
            if (user_info->switch_tables.find(_table_id) != user_info->switch_tables.end()) {
                is_switch_table = true;
                if (!is_read) {
                    return -1; // switch table 不允许写
                }
                if (req_range_partition_type == pb::RPT_DEFAULT) {
                    req_range_partition_type = pb::RPT_NEW_StatsEngine;
                } else {
                    req_range_partition_type = pb::RPT_DEFAULT;
                }
            }
        }

        ExprValue left_value_tmp = _range_expr->get_value(left_value);
        ExprValue right_value_tmp = _range_expr->get_value(right_value);

        auto range_match_fn = [right_open] (const ExprValue& left_value_tmp, const ExprValue& right_value_tmp,
                                  const ExprValue& range_left_value, const ExprValue& range_right_value) -> bool {
            // 分区右端点为开区间
            if (!left_value_tmp.is_null()) {
                if (left_value_tmp.compare(range_right_value) >= 0) {
                    return false;
                }
            }
            // 分区左端点为闭区间
            if (!right_value_tmp.is_null()) {
                if (right_open) {
                    if (right_value_tmp.compare(range_left_value) <= 0) {
                        return false;
                    }
                } else {
                    if (right_value_tmp.compare(range_left_value) < 0) {
                        return false;
                    }
                }
            }
            return true;
        };

        // 账号设置is_request_additional为true时，只读写外挂分区
        if (is_request_additional) {
            for (const auto& range : _additional_ranges) {
                if (!range_match_fn(left_value_tmp, right_value_tmp, range.left_value, range.right_value)) {
                    continue;
                }
                partition_ids.emplace(range.partition_id);
            }
            return 0;
        }

        // 账号设置is_request_additional为false时，写只访问正常分区，读访问正常分区及外挂分区
        for (const auto& range : _ranges) {
            if (!is_specified_range(range, req_range_partition_type, is_switch_table)) {
                continue;
            }
            if (!range_match_fn(left_value_tmp, right_value_tmp, range.left_value, range.right_value)) {
                continue;
            }
            partition_ids.emplace(range.partition_id);
        }
        if (is_read) {
            // 读场景需要同时读外挂分区
            for (const auto& range : _additional_ranges) {
                if (!range_match_fn(left_value_tmp, right_value_tmp, range.left_value, range.right_value)) {
                    continue;
                }
                partition_ids.emplace(range.partition_id);
            }
        }

        return 0;
    }

    int64_t partition_field_id() const {
        return _partition_field_id;
    }

    pb::PartitionType partition_type() const {
        return _partition_info.type();
    }

    bool get_partition_id_by_name(const std::string& partition_name, int64_t& partition_id) {
        auto iter = _partition_name_map.find(partition_name);
        if (iter != _partition_name_map.end()) {
            partition_id = iter->second;
            return true;
        }
        return false;
    }

    const std::vector<Range>& ranges() {
        return _ranges;
    }
    const std::vector<Range>& additional_ranges() {
        return _additional_ranges;
    }

    int get_specified_partition_ids(
            const std::shared_ptr<UserInfo>& user_info, std::set<int64_t>& partition_ids, bool is_read = false) {
        bool is_request_additional = false;
        bool is_switch_table = false;
        pb::RangePartitionType req_range_partition_type = pb::RPT_DEFAULT;
        if (user_info != nullptr) {
            is_request_additional = user_info->is_request_additional;
            req_range_partition_type = user_info->request_range_partition_type;
            // 如果未在切换表中，使用账户粒度的访问分区类型；
            // 如果在切换表中，根据账户粒度的访问分区类型取指定的分区类型，RPT_DEFAULT<->RPT_NEW_StatsEngine
            if (user_info->switch_tables.find(_table_id) != user_info->switch_tables.end()) {
                is_switch_table = true;
                if (!is_read) {
                    return -1; // switch table 不允许写
                }
                if (req_range_partition_type == pb::RPT_DEFAULT) {
                    req_range_partition_type = pb::RPT_NEW_StatsEngine;
                } else {
                    req_range_partition_type = pb::RPT_DEFAULT;
                }
            }
        }

        // 账号设置is_request_additional为true时，只读写外挂分区
        if (is_request_additional) {
            for (const auto& range : _additional_ranges) {
                partition_ids.emplace(range.partition_id);
            }
            return 0;
        }

        // 账号设置is_request_additional为false时，写只访问正常分区，读访问正常分区及外挂分区
        for (const auto& range : _ranges) {
            if (!is_specified_range(range, req_range_partition_type, is_switch_table)) {
                continue;
            }
            partition_ids.emplace(range.partition_id);
        }
        if (is_read) {
            for (const auto& range : _additional_ranges) {
                partition_ids.emplace(range.partition_id);
            }
        }
        return 0;
    } 

    std::string to_str() {
        std::string partition_s = "\nPARTITION BY RANGE (";
        if (_partition_info.has_expr_string()) {
            partition_s += _partition_info.expr_string();
        } else {
            partition_s += _partition_info.field_info().field_name();
        }
        partition_s += ") (\n";
        std::vector<Range> ranges;
        ranges.reserve(_ranges.size() + _additional_ranges.size());
        ranges.insert(ranges.end(), _ranges.begin(), _ranges.end());
        ranges.insert(ranges.end(), _additional_ranges.begin(), _additional_ranges.end());
        std::sort(ranges.begin(), ranges.end(), RangeComparator());
        for (size_t i = 0; i < ranges.size(); ++i) {
            partition_s += "PARTITION ";
            partition_s += ranges[i].partition_name;
            partition_s += " VALUES [";
            if (ranges[i].left_value.type != pb::MAXVALUE_TYPE) {
                partition_s += "'";
                partition_s += ranges[i].left_value.get_string();
                partition_s += "'";
            } else {
                partition_s += ranges[i].left_value.get_string();
            }
            partition_s += ", ";
            if (ranges[i].right_value.type != pb::MAXVALUE_TYPE) {
                partition_s += "'";
                partition_s += ranges[i].right_value.get_string();
                partition_s += "'";
            } else {
                partition_s += ranges[i].right_value.get_string();
            }
            partition_s += ")";
            if (_partition_info.range_partition_infos(i).has_resource_tag() ||
                    _partition_info.range_partition_infos(i).has_is_cold()  ||
                    _partition_info.range_partition_infos(i).has_type()) {
                partition_s += " COMMENT '{";
                if (_partition_info.range_partition_infos(i).has_resource_tag()) {
                    partition_s += "\"resource_tag\":\"";
                    partition_s += _partition_info.range_partition_infos(i).resource_tag();
                    partition_s += "\",";
                }
                if (_partition_info.range_partition_infos(i).has_is_cold()) {
                    partition_s += "\"is_cold\":";
                    partition_s += _partition_info.range_partition_infos(i).is_cold() ? "true" : "false";
                    partition_s += ",";
                }
                if (_partition_info.range_partition_infos(i).has_type()) {
                    partition_s += "\"type\":\"";
                    partition_s += pb::RangePartitionType_Name(_partition_info.range_partition_infos(i).type());
                    partition_s += "\",";
                }
                partition_s.pop_back();
                partition_s += "}'";
            }
            if (i != ranges.size() - 1) { 
                partition_s += ",\n";
            } else {
                partition_s += "\n";
            }
        }
        partition_s += ")";
        return partition_s;
    }

    // 生成afs路径使用
    std::string get_partition_uniq_str(int64_t partition_id) {
        if (_field_info == nullptr || _field_info->type != pb::DATE) {
            return std::to_string(partition_id);
        }

        bool find = false;
        std::string uniq_str;
        for (auto it = _ranges.rbegin(); it != _ranges.rend(); ++it) {
            if (it->partition_id == partition_id) {
                uniq_str = it->left_value.get_string();
                uniq_str += "_";
                uniq_str += it->right_value.get_string();
                find = true;
                break;
            }
        }
        if (find) {
            return uniq_str;
        } else {
            return std::to_string(partition_id);
        }
    }

    // show active_range工具使用
    void get_active_range(std::map<std::string, std::string>& active_ranges, pb::RangePartitionType type, const std::string& cold_str) {
        active_ranges.clear();
        std::vector<Range>* range_ptr = nullptr;
        if (type == pb::RPT_ADDITIONAL) {
            range_ptr = &_additional_ranges;
        } else {
            range_ptr = &_ranges;
        }

        for (const auto& range : *range_ptr) {
            if (!range.is_cold && type == range.partition_type) {
                if (!cold_str.empty() && cold_str >= range.right_value.get_string()) {
                    continue;
                }
                std::string left = range.left_value.get_string();
                std::string right = range.right_value.get_string();
                if (active_ranges.empty()) {
                    active_ranges[left] = right;
                } else {
                    auto iter = active_ranges.rbegin();
                    if (iter->second == left) {
                        iter->second = right;
                    } else {
                        active_ranges[left] = right;
                    }
                }
            }
        }
    }

private:
    bool is_specified_range(
            const Range& range, const pb::RangePartitionType req_range_partition_type, bool is_switch_table) {
        // 处理正常分区
        if (!range.is_cold) {
            // 切换表，读请求，isolation之前的访问主分区
            if (is_switch_table && range.is_isolation) { 
                if (range.partition_type == _partition_info.primary_range_partition_type()) {
                    return true;
                } else {
                    return false;
                }
            }
            // 访问指定类型热分区
            if (req_range_partition_type != range.partition_type) {
                return false;
            }
        }
        return true;
    }

private:
    int64_t _table_id;
    int64_t _partition_num;
    int32_t _partition_field_id;
    pb::PartitionInfo _partition_info;
    ExprNode* _range_expr = nullptr;
    SmartTable _table_ptr;
    FieldInfo* _field_info = nullptr;
    std::vector<Range> _ranges; // 正常分区集合
    std::vector<Range> _additional_ranges; // 外挂分区集合
    std::map<std::string, int64_t> _partition_name_map;
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
        //有sql扫描量很大的并且不稳定的
        if (scan_rows_9999 > 20000 && avg_scan_rows > 0 && scan_rows_9999 / avg_scan_rows > 100) {
            return -1;
        }
        if (latency_us_9999 > 0) {
            // 至少10ms
            return std::max(latency_us_9999 / 1000, 10L);
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

// DBLink表使用，用于db侧存储BaikalMeta和meta_id的映射关系
struct MetaMap {
    int64_t max_meta_id = 0;
    std::unordered_map<int64_t, std::string> meta_id_name_map; // <meta_id, meta_name>
    std::unordered_map<std::string, int64_t> meta_name_id_map; // <meta_name, meta_id>
};

using SqlStatMap = std::unordered_map<uint64_t, std::shared_ptr<SqlStatistics>>;
using DoubleBufferedSql = butil::DoublyBufferedData<SqlStatMap>;
using DoublBufferedMetaMap = butil::DoublyBufferedData<MetaMap>;

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
    int init(bool is_db = false, bool is_backup = false);

    // not thread-safe, should be called in single thread
    // 删除判断deleted, name允许改
    void update_table(const pb::SchemaInfo& table);
    void delete_table(const int64_t table_id);
    //void update_table(DoubleBufferedTable& double_buffered_table, const pb::SchemaInfo& table);

    // _sync系统初始化的时候调用，防止meta信息获取延迟导致系统不可用
    void update_tables_double_buffer_sync(const SchemaVec& tables);

    void update_instance_canceled(const std::string& addr);
    void update_instance(const std::string& addr, pb::Status s, bool user_check, bool cover_dead);
    int update_instance_internal(IdcMapping& idc_mapping, const std::string& addr, pb::Status s, bool user_check);
    void update_idc(const pb::IdcInfo& idc_info, const int64_t meta_id = 0);
    int update_idc_internal(IdcMapping& background, const pb::IdcInfo& idc_info, const int64_t meta_id);

    void update_big_sql(const std::string& sql);
    static int update_big_sql_double_buffer(
            void* meta, bthread::TaskIterator<std::string>& iter);
    void update_big_sql_double_buffer(bthread::TaskIterator<std::string>& iter);

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

    // for information_schema
    DatabaseInfo get_db_info(int64_t database_id);
    int get_db_id(const std::string& full_database_name, int64_t& db_id);

    DatabaseInfo get_database_info(int64_t databaseid);

    pb::Engine get_table_engine(int64_t tableid);
    // 复制的函数适合长期占用的，ptr适合很短使用情况
    TableInfo get_table_info(int64_t tableid);
    SmartTable get_table_info_ptr(int64_t tableid);
    SmartTable get_table_info_ptr_by_name(const std::string& table_name/*namespace.db.table*/);
    SmartTable get_table_info_ptr_by_index(int64_t indexid);
    IndexInfo get_index_info(int64_t indexid);
    SmartIndex get_index_info_ptr(int64_t indexid);
    std::vector<int64_t> get_all_index_info(int64_t tableid);
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
    std::vector<std::string> get_db_list(const std::shared_ptr<UserInfo>& user_info);
    std::set<int64_t> get_db_id_list(const std::shared_ptr<UserInfo>& user_info);
    std::vector<std::string> get_table_list(
            std::string namespace_, std::string db_name, UserInfo* user);
    std::vector<SmartTable> get_table_list(std::string namespace_, UserInfo* user);

    // table_name is full name (namespace.database.table)
    int get_table_id(const std::string& table_name, int64_t& table_id);

    // db_name is full name (namespace.database)
    int get_database_id(const std::string& db_name, int64_t& db_id);
    // contains all databases both have privileges and no privileges
    int get_show_database_id(const std::string& db_name, int64_t& db_id);

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
    bool is_olap_table(int64_t table_id, int64_t partition_id, bool* is_cold);
    int get_tail_split_nums(int64_t table_id);
    int get_tail_split_step(int64_t table_id);
    int get_binlog_backup_days(int64_t table_id);
    void get_cost_switch_open(std::vector<std::string>& database_table);
    void get_schema_conf_open(const std::string& conf_name, std::vector<std::string>& database_table);
    void get_table_by_filter(std::vector<std::string>& database_table,
            const std::function<bool(const SmartTable&)>& select_table);
    void table_with_statistics_info(std::vector<std::string>& database_table);
    int sql_force_learner_read(int64_t table_id, uint64_t sign);
    void get_schema_conf_op_info(const int64_t table_id, int64_t& op_version, std::string& op_desc);
    template <class T>
    int get_schema_conf_value(const int64_t table_id, const std::string& switch_name, T& value);
    int get_schema_conf_str(const int64_t table_id, const std::string& switch_name, std::string& value);
    TTLInfo get_ttl_duration(int64_t table_id);
    
    int get_all_region_by_table_id(int64_t table_id, 
            std::vector<pb::RegionInfo>* region_infos,
            const std::vector<int64_t>& partitions = std::vector<int64_t>{0});
    int get_all_partition_regions(int64_t table_id, 
            std::map<int64_t, pb::RegionInfo>* region_infos);
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

    int get_region_by_key(const std::shared_ptr<UserInfo>& user_info,
            IndexInfo& index,
            const std::vector<SmartRecord>& records,
            std::map<int64_t, std::vector<SmartRecord>>& region_ids,
            std::map<int64_t, pb::RegionInfo>& region_infos,
            std::set<int64_t>& record_partition_ids);

    int get_region_by_key(const std::shared_ptr<UserInfo>& user_info,
            IndexInfo& index,
            const std::vector<SmartRecord>& insert_records,
            const std::vector<SmartRecord>& delete_records,
            std::map<int64_t, std::vector<SmartRecord>>& insert_region_ids,
            std::map<int64_t, std::vector<SmartRecord>>& delete_region_ids,
            std::map<int64_t, pb::RegionInfo>& region_infos);

    int get_region_ids_by_key(const std::shared_ptr<UserInfo>& user_info,
                              IndexInfo& index,
                              const std::vector<SmartRecord>&  records,
                              std::vector<int64_t>& region_ids);

    bool exist_tableid(int64_t table_id);
    void get_all_table_by_db(const std::string& namespace_, const std::string& db_name, std::vector<SmartTable>& table_ptrs);
    void get_all_table_version(std::unordered_map<int64_t, int64_t>& table_id_version);
    void get_all_table_split_lines(std::unordered_map<int64_t, int64_t>& table_id_split_lines_map, 
                                   int64_t max_split_line);
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
    int get_all_instance_by_resource_tag(const std::string& resource_tag, std::vector<std::string>& instances) {
        instances.clear();
        DoubleBufferedIdc::ScopedPtr idc_ptr;
        if (_double_buffer_idc.Read(&idc_ptr) == 0) {
            const auto& map = idc_ptr->instance_info_mapping;
            for (const auto& iter : map) {
                if (iter.second.resource_tag == resource_tag) {
                    instances.emplace_back(iter.first);
                }
            }
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
    bool get_main_table_id(const int64_t global_index_id, int64_t& main_table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto& global_index_id_mapping = table_ptr->global_index_id_mapping;
        if (global_index_id_mapping.find(global_index_id) != global_index_id_mapping.end()) {
            main_table_id = global_index_id_mapping.at(global_index_id);
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

    bool has_rollup_index(const int64_t& main_table_id) {
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
        return table_info->has_rollup_index;
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
        if (table_info->has_rollup_index) {
            // rollup不走事务
            return false;
        }
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

    int64_t last_updated_index(const int64_t meta_id = 0) {
        if (_last_updated_index_map.find(meta_id) == _last_updated_index_map.end()) {
            return 0;
        }
        return _last_updated_index_map[meta_id];
    }
    void set_last_updated_index(const int64_t index, const int64_t meta_id = 0) {
        _last_updated_index_map[meta_id] = index;
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
            const int64_t meta_id = ::baikaldb::get_meta_id(index_info.first);
            if (meta_id != 0) {
                continue;
            }
            if (index_info.second->index_hint_status == pb::IHS_DISABLE) {
                index_names.emplace_back(index_info.second->name);
            }
        }
        return 0;
    }

    int get_partition_ids_by_name(int64_t table_id, const std::vector<std::string>& partition_names,
        std::set<int64_t>& partition_ids) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        auto iter = table_info_mapping.find(table_id);
        if (iter == table_info_mapping.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return -1;
        }
        auto table_info = iter->second;
        if (table_info == nullptr) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return -1;
        }
        if (table_info->partition_ptr != nullptr) {
            for (auto& name : partition_names) {
                int64_t partition_id;
                if (!table_info->partition_ptr->get_partition_id_by_name(name, partition_id)) {
                    DB_WARNING("get partition number error, partition name:%s", name.c_str());
                    return -1;
                }
                partition_ids.emplace(partition_id);
            }
        } else {
            DB_WARNING("non partition table :%ld", table_id);
            return -1;
        }
        return 0;
    }

    bool is_binlog_table(int64_t table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        auto iter = table_info_mapping.find(table_id);
        if (iter == table_info_mapping.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return false;
        }
        auto& table_info = iter->second;
        if (table_info->is_binlog) {
            return true;
        }
        return false;
    }

    // int get_partition_index(int64_t table_id, const ExprValue& value, int64_t& partition_index) {
    //     DoubleBufferedTable::ScopedPtr table_ptr;
    //     if (_double_buffer_table.Read(&table_ptr) != 0) {
    //         DB_WARNING("read double_buffer_table error.");
    //         return -1;
    //     }
    //     auto& table_info_mapping = table_ptr->table_info_mapping;
    //     auto iter = table_info_mapping.find(table_id);
    //     if (iter == table_info_mapping.end()) {
    //         DB_WARNING("table_id: %ld not exist", table_id);
    //         return -1;
    //     }
    //     auto& table_info = iter->second;
    //     if (table_info->partition_num == 1 && !table_info->is_range_partition) {
    //         // 非分区表，返回0分区
    //         partition_index = 0;
    //         return 0;
    //     }
    //     partition_index = table_info->partition_ptr->calc_partition(value);
    //     if (partition_index < 0) {
    //         DB_WARNING("get partition number error, value:%s", value.get_string().c_str());
    //         return -1;
    //     }
    //     return 0;
    // }

    int get_binlog_id(int64_t table_id, int64_t& binlog_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        auto& table_info_mapping = table_ptr->table_info_mapping;
        auto iter = table_info_mapping.find(table_id);
        if (iter == table_info_mapping.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return -1;
        }
        auto& table_info = iter->second;
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
        auto iter = table_info_mapping.find(table_id);
        if (iter == table_info_mapping.end()) {
            DB_WARNING("table_id: %ld not exist", table_id);
            return false;
        }
        auto& table_info = iter->second;
        if (table_info->is_linked) {
            return true;
        }
        return false;
    }
    
    int get_binlog_region_by_partition_id(int64_t table_id, int64_t partition_id, pb::RegionInfo& region_info,
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

    // Range Partition
    // 检查table_id对应表是否为range分区表
    bool is_range_partition_table(const int64_t table_id) {
        DoubleBufferedTable::ScopedPtr table_ptr;
        if (_double_buffer_table.Read(&table_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return false;
        }
        // 获取主表id
        auto& global_index_id_mapping = table_ptr->global_index_id_mapping;
        if (global_index_id_mapping.find(table_id) == global_index_id_mapping.end()) {
            DB_WARNING("table_id: %ld has no main_table_id", table_id);
            return -1;
        }
        const int64_t main_table_id = global_index_id_mapping.at(table_id);
        // 判断是否是range分区表
        auto& table_info_mapping = table_ptr->table_info_mapping;
        auto iter = table_info_mapping.find(main_table_id);
        if (iter == table_info_mapping.end()) {
            DB_WARNING("main_table_id: %ld not exist", main_table_id);
            return false;
        }
        auto& table_info = iter->second;
        if (!table_info->is_range_partition) {
            return false;
        }
        return true;
    }

    // DBLINK表使用，获取主Meta上的所有DBLink表
    void get_all_dblink_infos(std::vector<pb::DBLinkInfo>& dblink_infos) {
        auto func = [&dblink_infos] (const SchemaMapping& schema) {
            for (auto& info_pair : schema.table_info_mapping) {
                const int64_t meta_id = ::baikaldb::get_meta_id(info_pair.first);
                if (meta_id != 0) {
                    continue;
                }
                if (info_pair.second->engine != pb::DBLINK) {
                    continue;
                }
                dblink_infos.emplace_back(info_pair.second->dblink_info);
            }
        };
        schema_info_scope_read(func);
    }

    int get_meta_id(const std::string& meta_name, int64_t& meta_id) {
        DoublBufferedMetaMap::ScopedPtr scoped_ptr;
        if (_doubly_buffer_meta_map.Read(&scoped_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        const auto& meta_name_id_map = scoped_ptr->meta_name_id_map;
        if (meta_name_id_map.find(meta_name) == meta_name_id_map.end()) {
            return -1;
        }
        meta_id = meta_name_id_map.at(meta_name);
        return 0;
    }

    int get_meta_name(const int64_t& meta_id, std::string& meta_name) {
        DoublBufferedMetaMap::ScopedPtr scoped_ptr;
        if (_doubly_buffer_meta_map.Read(&scoped_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        const auto& meta_id_name_map = scoped_ptr->meta_id_name_map;
        if (meta_id_name_map.find(meta_id) == meta_id_name_map.end()) {
            return -1;
        }
        meta_name = meta_id_name_map.at(meta_id);
        return 0;
    }

    int get_all_meta(std::unordered_map<int64_t, std::string>& meta_id_name_map) {
        DoublBufferedMetaMap::ScopedPtr scoped_ptr;
        if (_doubly_buffer_meta_map.Read(&scoped_ptr) != 0) {
            DB_WARNING("read double_buffer_table error.");
            return -1;
        }
        meta_id_name_map = scoped_ptr->meta_id_name_map;
        return 0;
    }

    void update_meta_map(const std::string& meta_name);

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
    int delete_table_internal(SchemaMapping& schema_mapping, const int64_t table_id);
    int update_meta_map_internal(MetaMap& meta_map, const std::string& meta_name);
    void update_schema_conf(const std::string& table_name, 
                            const pb::SchemaConf &schema_conf, 
                                           pb::SchemaConf& mem_conf);
    // 全量更新
    void update_index(TableInfo& info, const pb::IndexInfo& index,
            const pb::IndexInfo* pk_index, SchemaMapping& background);
    //delete table和index
    void delete_table(const pb::SchemaInfo& table, SchemaMapping& background);

    void delete_table_region_map(const pb::SchemaInfo& table);
    void delete_table_region_map(const int64_t table_id);

    bool _is_inited = false;

    // use for show databases
    bthread_mutex_t _update_show_db_mutex;
    std::map<std::string, int64_t> _show_db_id_name_map;
    std::map<int64_t, DatabaseInfo> _show_db_info;
    std::unordered_map<std::string, int64_t> _show_db_name_id_mapping;

    // username => UserPrivilege
    DoubleBufferedUser _user_info_mapping;
    
    DoubleBufferedTable _double_buffer_table;
    // index_id => IndexInfo*
    // 提供给SplitCompactionFilter使用，使用普通双buf，split减少开销
    DoubleBuffer<butil::FlatMap<int64_t, IndexInfo*>> _split_index_map;

    DoubleBufferedIdc _double_buffer_idc;

    DoubleBufferedTableRegionInfo _table_region_mapping;
    bthread::ExecutionQueueId<RegionVec> _region_queue_id = {0};

    DoubleBufferedSql _double_buffer_sql_stat;

    std::string _physical_room;
    std::string _logical_room;
    bvar::Adder<VirtualIndexMap> _virtual_index_info; // 虚拟索引使用

    //记录baikaldb模块的起始时间用于计算启动时长
    TimeCost _baikaldb_restart_time;

    // DBLINK表
    DoublBufferedMetaMap _doubly_buffer_meta_map;
    std::unordered_map<int64_t, int64_t> _last_updated_index_map; // <meta_id, last_updated_index>
};
}

