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

#include "query_context.h"
#include "exec_node.h"

namespace baikaldb {
DEFINE_bool(default_2pc, false, "default enable/disable 2pc for autocommit queries");

QueryContext::~QueryContext() {
    if (need_destroy_tree) {
        ExecNode::destroy_tree(root);
        root = nullptr;
    }
    fragments.clear();
    if (root_fragment != nullptr) {
        root_fragment->close();
    }
}

int QueryContext::create_plan_tree() {
    need_destroy_tree = true;
    return ExecNode::create_tree(plan, &root, CreateExecOptions());
}

int QueryContext::destroy_plan_tree() {
    need_destroy_tree = false;
    ExecNode::destroy_tree(root);
    root = nullptr;
    return 0;
}

void QueryContext::update_ctx_stat_info(RuntimeState* state, int64_t query_total_time) {
    stat_info.num_returned_rows += state->num_returned_rows();
    stat_info.num_affected_rows += state->num_affected_rows();
    stat_info.num_scan_rows += state->num_scan_rows();
    stat_info.read_disk_size += state->read_disk_size();
    stat_info.num_filter_rows += state->num_filter_rows();
    stat_info.region_count += state->region_count;
    stat_info.db_handle_bytes += state->db_handle_bytes();
    stat_info.db_handle_rows += state->db_handle_rows();
    if (stat_info.error_code == 1000 && state->sign != 0) {
        auto sql_info = SchemaFactory::get_instance()->get_sql_stat(state->sign);
        if (sql_info == nullptr) {
            sql_info = SchemaFactory::get_instance()->create_sql_stat(state->sign);
        }
        if (state->need_statistics) {
            sql_info->update(query_total_time, stat_info.num_scan_rows);
        }
        if (state->execute_type == pb::EXEC_ARROW_ACERO) {
            sql_info->update_db_stat(stat_info.db_handle_rows, 
                    stat_info.db_handle_bytes, 
                    state->get_query_time());
        }
    }
}

int64_t QueryContext::get_ctx_total_time() {
    gettimeofday(&(stat_info.end_stamp), NULL);
    stat_info.total_time = timestamp_diff(stat_info.start_stamp, stat_info.end_stamp);
    return stat_info.total_time;
}

int QueryContext::copy_query_context(QueryContext* p_query_ctx) {
    if (p_query_ctx == nullptr) {
        DB_WARNING("p_query_ctx is nullptr");
        return -1;
    }
    if (client_conn == nullptr) {
        DB_WARNING("client_conn is nullptr");
        return -1;
    }

    is_select = p_query_ctx->is_select;
    is_plan_cache = p_query_ctx->is_plan_cache;
    is_straight_join = p_query_ctx->is_straight_join;
    // is_full_export = is_full_export || p_query_ctx->is_full_export; // 缓存不支持全量导出SQL
    stmt_type = p_query_ctx->stmt_type;
    is_complex = p_query_ctx->is_complex;
    root = p_query_ctx->root;
    ref_slot_id_mapping.insert(p_query_ctx->ref_slot_id_mapping.begin(), p_query_ctx->ref_slot_id_mapping.end());
    table_partition_names.insert(p_query_ctx->table_partition_names.begin(), p_query_ctx->table_partition_names.end());
    table_version_map.insert(p_query_ctx->table_version_map.begin(), p_query_ctx->table_version_map.end());
    table_with_clause_mapping.insert(p_query_ctx->table_with_clause_mapping.begin(), p_query_ctx->table_with_clause_mapping.end());
    _tuple_descs.assign(p_query_ctx->tuple_descs().begin(), p_query_ctx->tuple_descs().end());
    stat_info.family = p_query_ctx->stat_info.family;
    stat_info.table = p_query_ctx->stat_info.table;
    stat_info.resource_tag = p_query_ctx->stat_info.resource_tag;
    stat_info.sign = p_query_ctx->stat_info.sign;
    stat_info.sample_sql << p_query_ctx->stat_info.sample_sql.str();
    need_learner_backup = p_query_ctx->need_learner_backup;
    use_backup = p_query_ctx->use_backup;
    need_convert_charset = p_query_ctx->need_convert_charset;
    charset = p_query_ctx->charset;
    table_charset = p_query_ctx->table_charset;

    // runtime state
    // if (p_query_ctx->is_select) {
    //     if (client_conn->txn_id == 0) {
    //         p_query_ctx->get_runtime_state()->set_single_sql_autocommit(true);
    //     } else {
    //         p_query_ctx->get_runtime_state()->set_single_sql_autocommit(false);
    //     }
    //     runtime_state = p_query_ctx->runtime_state;
    // }
    if (is_select) {
        if (client_conn->txn_id == 0) {
            get_runtime_state()->set_single_sql_autocommit(true);
        } else {
            get_runtime_state()->set_single_sql_autocommit(false);
        }
    }
    // sql sign
    sign_blacklist.insert(p_query_ctx->sign_blacklist.begin(), p_query_ctx->sign_blacklist.end());
    sign_forcelearner.insert(p_query_ctx->sign_forcelearner.begin(), p_query_ctx->sign_forcelearner.end());
    sign_rolling.insert(p_query_ctx->sign_rolling.begin(), p_query_ctx->sign_rolling.end());
    sign_forceindex.insert(p_query_ctx->sign_forceindex.begin(), p_query_ctx->sign_forceindex.end());

    has_derived_table = p_query_ctx->has_derived_table;
    derived_table_ctx_mapping.insert(p_query_ctx->derived_table_ctx_mapping.begin(),
                                           p_query_ctx->derived_table_ctx_mapping.end());
    slot_column_mapping.insert(p_query_ctx->slot_column_mapping.begin(),
                                     p_query_ctx->slot_column_mapping.end());


    sign_exec_type.insert(p_query_ctx->sign_exec_type.begin(), p_query_ctx->sign_exec_type.end());
    table_can_use_arrow_vectorize = p_query_ctx->table_can_use_arrow_vectorize;
    return 0;
}

} // namespace baikaldb
