#include "proto_process.hpp"
#include "log.h"

namespace baikaldb {

/// 删除meta信息
int del_meta_info(pb::BaikalHeartBeatRequest& request) {
    for (auto& schema_info : *request.mutable_schema_infos()) {
        del_meta_info(schema_info);
    }
    for (auto& info_affect : *request.mutable_info_affect()) {
        del_meta_info(info_affect);
    }
    for (auto& heartbeat_table : *request.mutable_heartbeat_tables()) {
        del_meta_info(heartbeat_table);
    }
    return 0;
}

int del_meta_info(pb::MetaManagerRequest& request) {
    switch (request.op_type()) {
    case pb::OP_UPDATE_FOR_AUTO_INCREMENT: {
        del_meta_info(*request.mutable_auto_increment());
        break;
    }
    case pb::OP_GEN_ID_FOR_AUTO_INCREMENT: {
        del_meta_info(*request.mutable_auto_increment());
        break;
    }
    default: {
        return -1;
    }
    }
    return 0;
}

int del_meta_info(pb::StoreReq& request) {
    switch (request.op_type()) {
    case pb::OP_INSERT:
    case pb::OP_DELETE:
    case pb::OP_UPDATE:
    case pb::OP_SELECT: {
        for (auto& tuple : *request.mutable_tuples()) {
            del_meta_info(tuple);
        }
        if (request.has_plan()) {
            if (del_meta_info(*request.mutable_plan()) != 0) {
                return -1;
            }
        }
        if (request.has_new_region_info()) {
            del_meta_info(*request.mutable_new_region_info());
        }
        for (auto& region_info : *request.mutable_multi_new_region_infos()) {
            del_meta_info(region_info);
        }
        break;
    }
    default: {
        return -1;
    }
    }
    return 0;
}

int del_meta_info(pb::BaikalSchemaHeartBeat& baikal_schema_heartbeat) {
    if (baikal_schema_heartbeat.has_table_id()) {
        baikal_schema_heartbeat.set_table_id(get_del_meta_id(baikal_schema_heartbeat.table_id()));
    }
    return 0;
}

int del_meta_info(pb::BaikalHeartBeatTable& baikal_heartbeat_table) {
    if (baikal_heartbeat_table.has_namespace_name()) {
        baikal_heartbeat_table.set_namespace_name(get_del_meta_name(baikal_heartbeat_table.namespace_name()));
    }
    if (baikal_heartbeat_table.has_table_id()) {
        baikal_heartbeat_table.set_table_id(get_del_meta_id(baikal_heartbeat_table.table_id()));
    }
    return 0;
}

int del_meta_info(pb::VirtualIndexInfluence& virtual_index_influence) {
    if (virtual_index_influence.has_virtual_index_id()) {
        virtual_index_influence.set_virtual_index_id(get_del_meta_id(virtual_index_influence.virtual_index_id()));
    }
    return 0;
}

int del_meta_info(pb::AutoIncrementRequest& auto_increment_request) {
    if (auto_increment_request.has_table_id()) {
        auto_increment_request.set_table_id(get_del_meta_id(auto_increment_request.table_id()));
    }
    return 0;
}

int del_meta_info(pb::TupleDescriptor& tuple_descriptor) {
    if (tuple_descriptor.has_table_id()) {
        tuple_descriptor.set_table_id(get_del_meta_id(tuple_descriptor.table_id()));
    }
    for (auto& slot_descriptor : *tuple_descriptor.mutable_slots()) {
        if (slot_descriptor.has_table_id()) {
            slot_descriptor.set_table_id(get_del_meta_id(slot_descriptor.table_id()));
        }
    }
    return 0;
}

int del_meta_info(pb::RegionInfo& region_info) {
    if (region_info.has_table_id()) {
        region_info.set_table_id(get_del_meta_id(region_info.table_id()));
    }
    if (region_info.has_main_table_id()) {
        region_info.set_main_table_id(get_del_meta_id(region_info.main_table_id()));
    }
    return 0;
}

int del_meta_info(pb::Plan& plan) {
    int idx = 0;
    return del_meta_info(plan, idx);
}

int del_meta_info(pb::Plan& plan, int& idx) {
    if (idx >= plan.nodes_size()) {
        return -1;
    }
    switch (plan.nodes(idx).node_type()) {
    case pb::SCAN_NODE: {
        if (plan.nodes(idx).has_derive_node() && plan.nodes(idx).derive_node().has_scan_node()) {
            pb::ScanNode& node = *plan.mutable_nodes(idx)->mutable_derive_node()->mutable_scan_node();
            del_meta_info(node);
        }
        break;
    }
    case pb::INSERT_NODE: {
        if (plan.nodes(idx).has_derive_node() && plan.nodes(idx).derive_node().has_insert_node()) {
            pb::InsertNode& node = *plan.mutable_nodes(idx)->mutable_derive_node()->mutable_insert_node();
            del_meta_info(node);
        }
        break;
    }
    case pb::DELETE_NODE: {
        if (plan.nodes(idx).has_derive_node() && plan.nodes(idx).derive_node().has_delete_node()) {
            pb::DeleteNode& node = *plan.mutable_nodes(idx)->mutable_derive_node()->mutable_delete_node();
            del_meta_info(node);
        }
        break;
    }
    case pb::UPDATE_NODE: {
        if (plan.nodes(idx).has_derive_node() && plan.nodes(idx).derive_node().has_update_node()) {
            pb::UpdateNode& node = *plan.mutable_nodes(idx)->mutable_derive_node()->mutable_update_node();
            del_meta_info(node);
        }
        break;
    }
    default: {
        break;
    }
    }
    for (int i = 0; i < plan.nodes(idx).num_children(); ++i) {
        ++idx;
        int ret = del_meta_info(plan, idx);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

int del_meta_info(pb::ScanNode& scan_node) {
    if (scan_node.has_table_id()) {
        scan_node.set_table_id(get_del_meta_id(scan_node.table_id()));
    }
    // TODO - 是否有性能问题
    for (int i = 0; i < scan_node.indexes_size(); ++i) {
        pb::PossibleIndex possible_index;
        possible_index.ParseFromString(scan_node.indexes(i));
        del_meta_info(possible_index);
        possible_index.SerializeToString(scan_node.mutable_indexes(i));
    }
    if (scan_node.has_learner_index()) {
        pb::PossibleIndex possible_index;
        possible_index.ParseFromString(scan_node.learner_index());
        del_meta_info(possible_index);
        possible_index.SerializeToString(scan_node.mutable_learner_index());
    }
    for (int i = 0; i < scan_node.use_indexes_size(); ++i) {
        scan_node.set_use_indexes(i, get_del_meta_id(scan_node.use_indexes(i)));
    }
    for (int i = 0; i < scan_node.ignore_indexes_size(); ++i) {
        scan_node.set_ignore_indexes(i, get_del_meta_id(scan_node.ignore_indexes(i)));
    }
    for (int i = 0; i < scan_node.force_indexes_size(); ++i) {
        scan_node.set_force_indexes(i, get_del_meta_id(scan_node.ignore_indexes(i)));
    }
    if (scan_node.has_fulltext_index()) {
        del_meta_info(*scan_node.mutable_fulltext_index());
    }
    if (scan_node.has_ddl_index_id()) {
        scan_node.set_ddl_index_id(get_del_meta_id(scan_node.ddl_index_id()));
    }
    return 0;
}

int del_meta_info(pb::InsertNode& insert_node) {
    if (insert_node.has_table_id()) {
        insert_node.set_table_id(get_del_meta_id(insert_node.table_id()));
    }
    for (auto& slot_descriptor : *insert_node.mutable_update_slots()) {
        if (slot_descriptor.has_table_id()) {
            slot_descriptor.set_table_id(get_del_meta_id(slot_descriptor.table_id()));
        }
    }
    if (insert_node.has_ddl_index_id()) {
        insert_node.set_ddl_index_id(get_del_meta_id(insert_node.ddl_index_id()));
    }
    return 0;
}

int del_meta_info(pb::DeleteNode& delete_node) {
    if (delete_node.has_table_id()) {
        delete_node.set_table_id(get_del_meta_id(delete_node.table_id()));
    }
    for (auto& slot_descriptor : *delete_node.mutable_primary_slots()) {
        if (slot_descriptor.has_table_id()) {
            slot_descriptor.set_table_id(get_del_meta_id(slot_descriptor.table_id()));
        }
    }
    return 0;
}

int del_meta_info(pb::UpdateNode& update_node) {
    if (update_node.has_table_id()) {
        update_node.set_table_id(get_del_meta_id(update_node.table_id()));
    }
    for (auto& slot_descriptor : *update_node.mutable_primary_slots()) {
        if (slot_descriptor.has_table_id()) {
            slot_descriptor.set_table_id(get_del_meta_id(slot_descriptor.table_id()));
        }
    }
    for (auto& slot_descriptor : *update_node.mutable_update_slots()) {
        if (slot_descriptor.has_table_id()) {
            slot_descriptor.set_table_id(get_del_meta_id(slot_descriptor.table_id()));
        }
    }
    return 0;
}

int del_meta_info(pb::PossibleIndex& possible_index) {
    if (possible_index.has_index_id()) {
        possible_index.set_index_id(get_del_meta_id(possible_index.index_id()));
    }
    return 0;
}

int del_meta_info(pb::FulltextIndex& fulltext_index) {
    if (fulltext_index.has_possible_index()) {
        del_meta_info(*fulltext_index.mutable_possible_index());
    }
    for (auto& nested_fulltext_index : *fulltext_index.mutable_nested_fulltext_indexes()) {
        del_meta_info(nested_fulltext_index);
    }
    return 0;
}

/// 添加meta信息
int add_meta_info(pb::BaikalHeartBeatResponse& response, const int64_t meta_id) {
    for (auto& schema_info : *response.mutable_schema_change_info()) {
        add_meta_info(schema_info, meta_id);
    }
    for (auto& region_info : *response.mutable_region_change_info()) {
        add_meta_info(region_info, meta_id);
    }
    return 0;
}

int add_meta_info(pb::MetaManagerResponse& response, const int64_t meta_id) {
    return 0;
}

int add_meta_info(pb::StoreRes& response, const int64_t meta_id) {
    for (auto& region_info : *response.mutable_regions()) {
        add_meta_info(region_info, meta_id);
    }
    for (auto& record : *response.mutable_records()) {
        add_meta_info(record, meta_id);
    }
    return 0;
}

int add_meta_info(pb::SchemaInfo& schema_info, const int64_t meta_id) {
    if (schema_info.has_table_id()) {
        schema_info.set_table_id(get_add_meta_id(meta_id, schema_info.table_id()));
    }
    if (schema_info.has_upper_table_id()) {
        schema_info.set_upper_table_id(get_add_meta_id(meta_id, schema_info.upper_table_id()));
    }
    if (schema_info.has_top_table_id()) {
        schema_info.set_top_table_id(get_add_meta_id(meta_id, schema_info.top_table_id()));
    }
    for (int i = 0; i < schema_info.lower_table_ids_size(); ++i) {
        schema_info.set_lower_table_ids(i, get_add_meta_id(meta_id, schema_info.lower_table_ids(i)));
    }
    if (schema_info.has_database_id()) {
        schema_info.set_database_id(get_add_meta_id(meta_id, schema_info.database_id()));
    }
    if (schema_info.has_namespace_id()) {
        schema_info.set_namespace_id(get_add_meta_id(meta_id, schema_info.namespace_id()));
    }
    if (schema_info.has_namespace_name()) {
        schema_info.set_namespace_name(get_add_meta_name(meta_id, schema_info.namespace_name()));
    }
    for (auto& index_info : *schema_info.mutable_indexs()) {
        add_meta_info(index_info, meta_id);
    }
    if (schema_info.has_binlog_info()) {
        auto& binlog_info = *schema_info.mutable_binlog_info();
        add_meta_info(binlog_info, meta_id);
    }
    for (auto& binlog_info : *schema_info.mutable_binlog_infos()) {
        add_meta_info(binlog_info, meta_id);
    }
    return 0;
}

int add_meta_info(pb::RegionInfo& region_info, const int64_t meta_id) {
    if (region_info.has_table_id()) {
        region_info.set_table_id(get_add_meta_id(meta_id, region_info.table_id()));
    }
    if (region_info.has_main_table_id()) {
        region_info.set_main_table_id(get_add_meta_id(meta_id, region_info.main_table_id()));
    }
    return 0;
}

int add_meta_info(pb::IndexInfo& index_info, const int64_t meta_id) {
    if (index_info.has_index_id()) {
        index_info.set_index_id(get_add_meta_id(meta_id, index_info.index_id()));
    }
    return 0;
}

int add_meta_info(pb::BinlogInfo& binlog_info, const int64_t meta_id) {
    for (int i = 0; i < binlog_info.target_table_ids_size(); ++i) {
        binlog_info.set_target_table_ids(i, get_add_meta_id(meta_id, binlog_info.target_table_ids(i)));
    }
    if (binlog_info.has_binlog_table_id()) {
        binlog_info.set_binlog_table_id(get_add_meta_id(meta_id, binlog_info.binlog_table_id()));
    }
    return 0;
}

int add_meta_info(pb::IndexRecords& index_record, const int64_t meta_id) {
    if (index_record.has_index_id()) {
        index_record.set_index_id(get_add_meta_id(meta_id, index_record.index_id()));
    }
    return 0;
}

} // namespace baikaldb