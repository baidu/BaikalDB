#pragma once

#include <string>

#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
// 新增字段需要判断是否进行meta标识信息的增删!!!

// DBLINK表
constexpr int64_t META_ID_MASK = 0x7FFF000000000000LL;
constexpr int64_t META_ID_SHIFT = 48;
constexpr int64_t MAX_META_ID = 0x7FFF;

inline std::string get_del_meta_name(const std::string& name) {
    std::string del_meta_name = name;
    size_t pos = del_meta_name.find("\001");
    if (pos != std::string::npos) {
        del_meta_name = del_meta_name.substr(pos + 1);
    }
    return del_meta_name;
}

inline std::string get_add_meta_name(const int64_t meta_id, const std::string& name) {
    if (meta_id == 0) {
        return name;   
    }
    return std::to_string(meta_id) + "\001" + name;
}

inline int64_t get_del_meta_id(const int64_t id) {
    return id & (~META_ID_MASK);
}

inline int64_t get_add_meta_id(const int64_t meta_id, const int64_t id) {
    if (meta_id == 0) {
        return id;
    }
    return id | (meta_id << META_ID_SHIFT);
}

inline int64_t get_meta_id(const int64_t id) {
    return (id & META_ID_MASK) >> META_ID_SHIFT;
}

/**
 *  @brief 把request中的meta标识信息删除，涉及内容:
 *         namespace_id、database_id、table_id、index_id、binlog_id、namespace_name等
 */
template <typename Request>
int del_meta_info(Request& request) {
    return -1;
}

int del_meta_info(pb::BaikalHeartBeatRequest& request);
int del_meta_info(pb::MetaManagerRequest& request);
int del_meta_info(pb::StoreReq& request);

int del_meta_info(pb::BaikalSchemaHeartBeat& baikal_schema_heartbeat);
int del_meta_info(pb::BaikalHeartBeatTable& baikal_heartbeat_table);
int del_meta_info(pb::VirtualIndexInfluence& virtual_index_influence);
int del_meta_info(pb::AutoIncrementRequest& auto_increment_request);
int del_meta_info(pb::TupleDescriptor& tuple_descriptor);
int del_meta_info(pb::RegionInfo& region_info);

int del_meta_info(pb::Plan& plan);
int del_meta_info(pb::Plan& plan, int& idx);
int del_meta_info(pb::ScanNode& scan_node);
int del_meta_info(pb::InsertNode& insert_node);
int del_meta_info(pb::DeleteNode& delete_node);
int del_meta_info(pb::UpdateNode& update_node);
int del_meta_info(pb::PossibleIndex& possible_index);
int del_meta_info(pb::FulltextIndex& fulltext_index);

/** 
 *  @brief 将meta表示信息添加到response中，涉及内容:
 *         namespace_id、database_id、table_id、index_id、binlog_id、namespace_name等
 */
template <typename Response>
int add_meta_info(Response& response, const int64_t meta_id) {
    return -1;
}

int add_meta_info(pb::BaikalHeartBeatResponse& response, const int64_t meta_id);
int add_meta_info(pb::MetaManagerResponse& response, const int64_t meta_id);
int add_meta_info(pb::StoreRes& response, const int64_t meta_id);

int add_meta_info(pb::SchemaInfo& schema_info, const int64_t meta_id);
int add_meta_info(pb::RegionInfo& region_info, const int64_t meta_id);
int add_meta_info(pb::IndexInfo& index_info, const int64_t meta_id);
int add_meta_info(pb::BinlogInfo& binlog_info, const int64_t meta_id);
int add_meta_info(pb::IndexRecords& index_record, const int64_t meta_id);

} // namespace baikaldb