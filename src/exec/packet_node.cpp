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
#include "packet_node.h"
#include "full_export_node.h"
#include "runtime_state.h"
#include "network_socket.h"
#include "scan_node.h"
#include <arrow/type.h>
#include <arrow/acero/options.h>
#include <arrow/stl_iterator.h>
#include "vectorize_helpper.h"
#include "filter_node.h"
#include "ddl_planner.h"

namespace baikaldb {
DEFINE_int32(expect_bucket_count, 100, "expect_bucket_count");
DEFINE_bool(field_charsetnr_set_by_client, false, "set charsetnr by client");
DECLARE_int32(key_point_collector_interval);

int PacketNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _op_type = node.derive_node().packet_node().op_type();
    for (int i = 0; i < node.derive_node().packet_node().projections_size(); i++) {
        ExprNode* projection = nullptr;
        auto& expr = node.derive_node().packet_node().projections(i);
        auto& name = node.derive_node().packet_node().col_names(i);
        ret = ExprNode::create_tree(expr, &projection);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        _projections.push_back(projection);
        ResultField field;
        field.name = name;
        field.org_name = name;
        field.db = expr.database();
        field.table = expr.table();
        _fields.push_back(field);
    }
    return 0;
}
int PacketNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    int i = 0;
    ret = common_expr_optimize(&_projections);
    if (ret < 0) {
        DB_WARNING("common_expr_optimize fail");
        return ret;
    }
    for (auto& expr : _projections) {
        //db table_name先不填，后续有影响再填
        _fields[i].type = to_mysql_type(expr->col_type());
        _fields[i].flags = 1;
        if (_fields[i].type == MYSQL_TYPE_STRING) {
            _fields[i].length = 255;
        }
        if (is_uint(expr->col_type())) {
            _fields[i].flags |= 32;
        }
//        DB_WARNING("col_type: %d, col_flag: %u", expr->col_type(), expr->col_flag());
        if (is_binary(expr->col_flag()) && is_string(expr->col_type())) {
            _fields[i].type = MYSQL_TYPE_BLOB;
//            DB_WARNING("MYSQL_TYPE_BLOB: %s", _fields[i].name.c_str());
            _fields[i].charsetnr = 0x3f;
            _fields[i].flags |= parser::MYSQL_FIELD_FLAG_BINARY;
            _fields[i].flags |= parser::MYSQL_FIELD_FLAG_BLOB;
        }
        ++i;
    }
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail:%d", ret);
        return ret;
    }
    return 0;
}

int PacketNode::handle_explain(RuntimeState* state) {
    _fields.clear();
    std::vector<std::string> names = {
        "id", "select_type", "table", "partitions", "type", "possible_keys",
        "key", "key_len", "ref", "rows", "Extra"
    };
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.push_back(field);
    }
    pack_head();
    pack_fields();
    std::vector<std::map<std::string, std::string>> explains;
    int id = 1;
    show_explain(state->ctx(), explains, id, -1);
    std::stable_sort(explains.begin(), explains.end(), 
            [](std::map<std::string, std::string> left, 
                std::map<std::string, std::string> right) {
                    return strtoll(left["id"].c_str(), NULL, 10) < strtoll(right["id"].c_str(), NULL, 10);
            });
    for (auto& m : explains) {
        std::vector<std::string> row;
        for (auto& name : names) {
            row.push_back(m[name]);
        }
        pack_vector_row(row);
    }
    pack_eof();
    return 0;
}

int PacketNode::show_explain(QueryContext* ctx, std::vector<std::map<std::string, std::string>>& output, int& next_id, int display_id) {
    int return_id = next_id;
    ExecNode::show_explain(ctx, output, next_id, display_id);
    for (auto it: ctx->noncorrelated_subquerys) {
        it->root->show_explain(it.get(), output, next_id, -1);
    }
    return return_id;
}


int PacketNode::handle_show_cost(RuntimeState* state) {
    _fields.clear();
    std::vector<std::map<std::string, std::string>> explains;
    int id = 1;
    show_explain(state->ctx(), explains, id, -1);
    std::vector<std::map<std::string, std::string>> path_infos;
    std::vector<ExecNode*> scan_nodes;
    get_node(pb::SCAN_NODE, scan_nodes);
    for (auto& scan_node_ptr : scan_nodes) {
        ScanNode* scan_node = static_cast<ScanNode*>(scan_node_ptr);
        scan_node->show_cost(path_infos);
    }

    if (path_infos.size() <= 0) {
        std::map<std::string, std::string> path_info;
        path_info["cost"] = "no cost info";
        path_infos.push_back(path_info);
    }

    std::vector<std::vector<std::string>> rows;
    rows.reserve(5);
    bool fill_name = true;
    for (auto& path_info : path_infos) {
        std::vector<std::string> row;
        for (auto& pair : path_info) {
            if (fill_name) {
                ResultField field;
                field.name = pair.first;
                field.type = MYSQL_TYPE_STRING;
                _fields.push_back(field);
            }
            row.emplace_back(pair.second);
        }
        fill_name = false;
        rows.push_back(row);
    }

    pack_head();
    pack_fields();

    for (auto& row : rows) {
        pack_vector_row(row);
    }
    pack_eof();
    return 0;
}

int PacketNode::handle_trace(RuntimeState* state) {
    _fields.clear();
    std::vector<std::string> names = {
        "trace"
    };
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.push_back(field);
    }
    pack_head();
    pack_fields();
    std::vector<std::string> row;
    row.push_back(_trace->DebugString().c_str());
    pack_vector_row(row);
    pack_eof();
    return 0;
}

void PacketNode::pack_trace2(std::vector<std::map<std::string, std::string>>& info, const pb::TraceNode& trace_node,
    int64_t& total_scan_rows, int64_t& total_index_filter, int64_t& total_get_primary, int64_t& total_where_filter) {
    
    if (trace_node.has_instance() && trace_node.has_region_id()) {
        std::map<std::string, std::string> sub_info;
        sub_info["instance"] = trace_node.instance();
        sub_info["region_id"] = std::to_string(trace_node.region_id());
        if (trace_node.has_total_time()) {
            sub_info["total_cost"] = std::to_string(trace_node.total_time());
        }
        if (trace_node.has_partition_id()) {
            sub_info["partition_id"] = std::to_string(trace_node.partition_id());
        }
        info.push_back(sub_info);
    } else if (trace_node.has_node_type() && trace_node.has_open_trace() && trace_node.has_get_next_trace()) {
        if (info.size() > 0) {
            if (trace_node.get_next_trace().has_scan_rows()) {
                info.back()["scan_rows"] = std::to_string(trace_node.get_next_trace().scan_rows());
                total_scan_rows += trace_node.get_next_trace().scan_rows();
            }
            if (trace_node.get_next_trace().has_index_filter_rows()) {
                info.back()["index_filter"] = std::to_string(trace_node.get_next_trace().index_filter_rows());
                total_index_filter += trace_node.get_next_trace().index_filter_rows();
            }
            if (trace_node.get_next_trace().has_get_primary_rows()) {
                info.back()["get_primary"] = std::to_string(trace_node.get_next_trace().get_primary_rows());
                total_get_primary += trace_node.get_next_trace().get_primary_rows();
            }
            if (trace_node.get_next_trace().has_where_filter_rows()) {
                info.back()["where_filter"] = std::to_string(trace_node.get_next_trace().where_filter_rows());
                total_where_filter += trace_node.get_next_trace().where_filter_rows();
            }
            if (trace_node.open_trace().has_index_name()) {
                info.back()["index"] = trace_node.open_trace().index_name();
            }
            if (trace_node.node_type() == pb::SCAN_NODE && trace_node.get_next_trace().has_time_cost_us()) {
                info.back()["scan_cost"] = std::to_string(trace_node.get_next_trace().time_cost_us());
            }            
        }
    }

    for (auto& c : trace_node.child_nodes()) {
        pack_trace2(info, c, total_scan_rows, total_index_filter, total_get_primary, total_where_filter);
    }
}

int PacketNode::handle_trace2(RuntimeState* state) {
    _fields.clear();
    std::vector<std::string> names = {
        "region_id", "partition_id", "instance", "index", "scan_rows", "index_filter", "get_primary",
        "where_filter", "scan_cost", "total_cost"
    };
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.push_back(field);
    }

    std::vector<std::map<std::string, std::string>> info;
    int64_t total_scan_rows = 0;
    int64_t total_index_filter = 0;
    int64_t total_get_primary = 0;
    int64_t total_where_filter = 0;
    pack_trace2(info, *_trace, total_scan_rows, total_index_filter, total_get_primary, total_where_filter);
    
    std::vector<std::vector<std::string>> rows;
    for (auto& sub_info : info) {
        std::vector<std::string> row;
        for (auto& name : names) {
            if (sub_info.count(name) == 0) {
                row.push_back("NULL");
            } else {
                row.push_back(sub_info[name]);
            }
        }
        rows.push_back(row);
    }
    std::vector<std::string> row;
    for (auto& name : names) {
        if (name == "scan_rows") {
            row.push_back(std::to_string(total_scan_rows));
        } else if (name == "index_filter") {
            row.push_back(std::to_string(total_index_filter));
        } else if (name == "get_primary") {
            row.push_back(std::to_string(total_get_primary));
        } else if (name == "where_filter") {
            row.push_back(std::to_string(total_where_filter));
        } else {
            row.push_back("NULL");
        }
    }
    rows.push_back(row);

    pack_head();
    pack_fields();
    for (auto& row : rows) {
        pack_vector_row(row);
    }
    
    pack_eof();
    return 0;
}

int PacketNode::pack_keypoints(RuntimeState* state, 
                               std::map<std::string, std::vector<std::vector<ExprValue>>>& partition_key_pks,
                               int partition_field_id, 
                               int partition_slot_id) {
    int ret = 0;
    bool eos = false;
    do {
        if (_children.empty()) {
            break;
        }
        RowBatch batch;
        ret = _children[0]->get_next(state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            std::vector<ExprValue> values;
            std::string paritition_key = row->get_value(0, partition_slot_id).get_string();
            for (auto expr : _projections) {
                values.emplace_back(expr->get_value(row));
            }
            partition_key_pks[paritition_key].emplace_back(values);
        }
    } while (!eos);
    // 拿keypoint的order by可能被下推干掉导致解析出来的主键部分字段组成的keypoint无序, 必须在这里排序
    for (auto& pair : partition_key_pks) {
        auto& rows = pair.second;
        // 排序, 和multi_lt_value一样
        std::sort(rows.begin(), rows.end(), [](std::vector<ExprValue>& a, std::vector<ExprValue>& b) -> bool {
            int field_size = a.size();
            for (int i = 0; i < field_size; ++i) {
                if (a[i].compare(b[i]) < 0) {
                    return true;
                } else if (a[i].compare(b[i]) > 0) {
                    return false;
                }
            }
            return false;
        });
    }
    return 0;
}

int PacketNode::handle_keypoint(RuntimeState* state) {
    _fields.clear();
    std::vector<std::string> names = {
        "partition_key", "rows", "keypoints"
    };
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.emplace_back(field);
    }

    // pk field types
    int partition_key_field_id = -1;
    int partition_key_slot_id = -1;
    pb::TupleDescriptor* tuple = state->get_tuple_desc(0);
    if (tuple == nullptr) {
        DB_WARNING("get tuple fail");
        return -1;
    }
    int64_t table_id = tuple->table_id();
    auto pk_info = SchemaFactory::get_instance()->get_index_info_ptr(table_id);
    if (pk_info == nullptr || pk_info->fields.empty()) {
        DB_WARNING("get pk info fail");
        return -1;
    }
    partition_key_field_id = pk_info->fields[0].id;
    for (const auto& slot : tuple->slots()) {
        if (slot.field_id() == partition_key_field_id) {
            partition_key_slot_id = slot.slot_id();
            break;
        }
    }
    if (partition_key_field_id < 0 || partition_key_slot_id < 0) {
        DB_WARNING("get partition key field_id or slot_id fail");
        return -1;
    }
    if (FLAGS_key_point_collector_interval <= 0) {
        DB_WARNING("not open key_point_collector");
        return -1;
    }
    int64_t keypoint_range = state->keypoint_range / FLAGS_key_point_collector_interval;
    if (keypoint_range <= 1) {
        keypoint_range = 2;
    }
    int64_t partition_threshold = state->partition_threshold / FLAGS_key_point_collector_interval;
    if (partition_threshold <= 1) {
        partition_threshold = 2;
    }
    int64_t range_count_limit = state->range_count_limit;
    std::map<std::string, std::vector<std::vector<ExprValue>>> partition_key_pks;
    pack_keypoints(state, partition_key_pks, partition_key_field_id, partition_key_slot_id);

    std::vector<std::vector<std::string>> rows;
    rows.reserve(1);
    for (auto& sub_info : partition_key_pks) {
        std::vector<std::string> row;
        const std::string& partition_key = sub_info.first;
        row.emplace_back(partition_key);
        row.emplace_back(std::to_string(sub_info.second.size() * FLAGS_key_point_collector_interval));
        std::string pks;
        std::string last_pk;
        int key_count = 0;
        int keypoint_range_per_user = keypoint_range;
        // 目前只有大户拆分的场景需要通过range_count_limit聚合, 多账户场景不需要聚合
        if (range_count_limit > 0 && sub_info.second.size() > keypoint_range_per_user * range_count_limit) {
            keypoint_range_per_user = sub_info.second.size() / range_count_limit + 1;
        }
        for (auto& pk : sub_info.second) {
            if (++key_count % keypoint_range_per_user == 0) {
                std::string key;
                for (auto& v : pk) {
                    key += v.cast_to(pb::STRING).get_string() + ",";
                }
                if (!key.empty()) {
                    key.pop_back();
                }
                if (last_pk != key) {
                    pks += key + ";";
                    last_pk = key;
                }
            }
        }
        if (key_count < partition_threshold) {
            continue;
        }
        if (!pks.empty()) {
            pks.pop_back();
        }
        row.emplace_back(pks);
        rows.emplace_back(row);
    }
    state->inc_num_returned_rows(rows.size());
    pack_head();
    pack_fields();
    for (auto& row : rows) {
        pack_vector_row(row);
    }
    
    pack_eof();
    return 0;
}

int PacketNode::sample_keypoint(RuntimeState* state) {
    _fields.clear();
    std::vector<std::string> names = {
        "total_rows", "average_rows", "region_cnt", "split_keys"
    };
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.emplace_back(field);
    }

    // pk field types
    int partition_key_field_id = -1;
    int partition_key_slot_id = -1;
    pb::TupleDescriptor* tuple = state->get_tuple_desc(0);
    if (tuple == nullptr) {
        DB_WARNING("get tuple fail");
        return -1;
    }
    int64_t table_id = tuple->table_id();
    auto pk_info = SchemaFactory::get_instance()->get_index_info_ptr(table_id);
    if (pk_info == nullptr || pk_info->fields.empty()) {
        DB_WARNING("get pk info fail");
        return -1;
    }
    auto table_ptr = SchemaFactory::get_instance()->get_table_info_ptr(table_id);
    if (table_ptr == nullptr) {
        DB_WARNING("get table info fail");
        return -1;
    }
    partition_key_field_id = pk_info->fields[0].id;
    for (const auto& slot : tuple->slots()) {
        if (slot.field_id() == partition_key_field_id) {
            partition_key_slot_id = slot.slot_id();
            break;
        }
    }
    if (partition_key_field_id < 0 || partition_key_slot_id < 0) {
        DB_WARNING("get partition key field_id or slot_id fail");
        return -1;
    }
    if (FLAGS_key_point_collector_interval <= 0) {
        DB_WARNING("not open key_point_collector");
        return -1;
    }

    int pre_split_region_cnt = state->pre_split_region_cnt;
    if (pre_split_region_cnt <= 1) {
        DB_WARNING("not set pre split region count");
        return -1;
    }
    std::map<std::string, std::vector<std::vector<ExprValue>>> partition_key_pks;
    pack_keypoints(state, partition_key_pks, partition_key_field_id, partition_key_slot_id);
    if (partition_key_pks.size() < pre_split_region_cnt) {
        DB_WARNING("partition key pks size:%lu < pre split region count:%d", partition_key_pks.size(), pre_split_region_cnt);
        return -1;
    }
    if (!is_int(pk_info->fields[0].type)) {
        return -1;
    }
    int64_t total_rows = 0;
    std::map<int64_t, int64_t> partition_key_rows;
    for (auto& sub_info : partition_key_pks) {
        std::vector<std::string> row;
        int64_t partition_key = ExprValue(pk_info->fields[0].type, sub_info.first).get_numberic<int64_t>();
        total_rows += sub_info.second.size() * FLAGS_key_point_collector_interval;
        partition_key_rows[partition_key] = sub_info.second.size() * FLAGS_key_point_collector_interval;
    }

    int64_t average_rows = total_rows / pre_split_region_cnt;
    int64_t region_rows = 0;
    std::vector<std::string> pre_split_keys;
    pre_split_keys.reserve(pre_split_region_cnt);
    for (const auto& sub_info : partition_key_rows) {
        if (region_rows + sub_info.second > average_rows) {
            pre_split_keys.emplace_back(std::to_string(sub_info.first));
            region_rows = sub_info.second;
        } else {
            region_rows += sub_info.second;
        }
    }
    std::vector<std::string> row;
    row.reserve(names.size());
    row.emplace_back(std::to_string(total_rows));
    row.emplace_back(std::to_string(average_rows));
    row.emplace_back(std::to_string(pre_split_keys.size() + 1));
    row.emplace_back(boost::join(pre_split_keys, " "));

    std::vector<std::string> split_vec;
    boost::split(split_vec, table_ptr->name, boost::is_any_of("."));
    if (split_vec.size() != 2) {
        DB_FATAL("get table_name[%s] fail", table_ptr->name.c_str());
        return -1;
    }
    int ret = DDLPlanner::update_specify_split_keys(table_ptr->namespace_, split_vec[0], split_vec[1], boost::join(pre_split_keys, ","));
    if (ret < 0) {
        DB_WARNING("update split keys fail");
        return -1;
    }

    state->inc_num_returned_rows(1);
    pack_head();
    pack_fields();
    pack_vector_row(row);
    pack_eof();
    return 0;
}

int PacketNode::fatch_expr_subquery_results(RuntimeState* state) {
    if (state->use_mpp) {
        return 0;
    }
    auto subquery_exprs_vec = state->mutable_subquery_exprs();
    bool eos = false;
    int ret = 0;
    do {
        if (_children.empty()) {
            state->execute_type = pb::EXEC_ROW;
            return 0;
        }
        RowBatch batch;
        ret = _children[0]->get_next(state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        set_node_exec_type(_children[0]->node_exec_type());
        if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
            break;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            std::vector<ExprValue> val_row;
            val_row.reserve(_projections.size());
            for (auto expr : _projections) {
                val_row.emplace_back(expr->get_value(row).cast_to(expr->col_type()));
            }
            subquery_exprs_vec->emplace_back(val_row);
        }
    } while (!eos);
    if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
        // ARROW列式执行入口
        ret = start_vectorized_execution(state);
        if (ret < 0) {
            return ret;
        }
    }
    state->execute_type = _node_exec_type;
    return 0;
}

bool PacketNode::can_use_arrow_vector(RuntimeState* state) {
    for (auto& expr : _projections) {
        if (!expr->can_use_arrow_vector()) {
            return false;
        }
    }
    for (auto& c : _children) {
        if (!c->can_use_arrow_vector(state)) {
            return false;
        }
    }
    return true;
}

int PacketNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    for (auto c : _children) {
        if (c->build_arrow_declaration(state) != 0) {
            return -1;
        }
    }
    //  add projection stage
    std::vector<arrow::compute::Expression> exprs;
    for (auto& field : _projections) {
        if (field->transfer_to_arrow_expression() != 0) {
            DB_FATAL_STATE(state, "packetnode projection transfer_to_arrow_expression fail");
            return -1;
        }
        exprs.emplace_back(field->arrow_expr());
    }
    arrow::acero::Declaration dec{"project", arrow::acero::ProjectNodeOptions{exprs}};
    LOCAL_TRACE_ARROW_PLAN(dec);
    state->append_acero_declaration(dec);
    return 0;
}

int PacketNode::start_vectorized_execution(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    int ret = build_arrow_declaration(state);
    if (ret != 0) {
        DB_FATAL_STATE(state, "arrow execute fail: build arrow declaration fail");
        return -1;
    }
    std::shared_ptr<arrow::Table> table;
    // execute
    arrow::Result<std::shared_ptr<arrow::Table>> final_table;
    GlobalArrowExecutor::execute(state, &final_table);
    if (final_table.ok()) {
        table = *final_table;
        if (state->is_expr_subquery()) {
            state->subquery_result_table = table;
            return 0;
        }
    } else {
        DB_FATAL("arrow execute fail: arrow acero run fail, status: %s", final_table.status().ToString().c_str());
        return -1;
    }
    return vectorized_pack_rows(state, table, false);
}

int PacketNode::vectorized_pack_rows(RuntimeState* state, std::shared_ptr<arrow::Table> table, bool need_pack_eof) {
    int ret = 0;
    if (state->explain_type == SHOW_TRACE) {
        return 0;
    } 
    if (_children.empty()) {
        return 0;
    }
    if (table != nullptr) {
        std::vector<std::shared_ptr<arrow::ChunkedArray>> columns = table->columns();
        for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
            if (_binary_protocol) {
                ret = pack_binary_row(nullptr, state, &columns, row_idx);
            } else {
                ret = pack_text_row(nullptr, state, &columns, row_idx);
            }
            state->inc_num_returned_rows(1);
            if (ret < 0) {
                DB_WARNING("pack_row fail:%d", ret);
                return ret;
            }
        }
    }
    if (need_pack_eof) {
        pack_eof();
    }
    return 0;
}

int PacketNode::mpp_pack_rows(RuntimeState* state, std::shared_ptr<arrow::Table> result) {
    if (state->explain_type == SHOW_TRACE) {
        if (state->explain_type == SHOW_TRACE) {
            handle_trace(state);
        } else {
            handle_trace2(state);
        }
    } 
    if (state->is_expr_subquery()) {
        state->subquery_result_table = result;
        return 0;
    }
    return vectorized_pack_rows(state, result, true);
}

int PacketNode::open(RuntimeState* state) {
    int ret = 0;
    if (state->is_from_subquery() || state->is_union_subquery()) {
        ret = ExecNode::open(state);
        if (_children.size() > 0) {
            set_node_exec_type(_children[0]->node_exec_type());
        }
        if (ret < 0) {
            DB_WARNING("ExecNode::open fail:%d", ret);
            return ret;
        }
        return 0;
    }
    _client = state->client_conn();
    if (FLAGS_field_charsetnr_set_by_client) {
        for (auto& field : _fields) {
            if (field.charsetnr == 0) {
                field.charsetnr = _client->charset_num;
            }
        }
    }

    if (state->explain_type == ANALYZE_STATISTICS) {
        state->force_single_rpc = true;
    }
    _send_buf = state->send_buf();

    if (state->explain_type == EXPLAIN_SHOW_COST) {
        handle_show_cost(state);
        return 0;
    }

    if (!_return_empty || op_type() == pb::OP_SELECT) {
        ret = ExecNode::open(state);
        if (ret < 0) {
            DB_WARNING("ExecNode::open fail:%d", ret);
            return ret;
        }
    }
    for (auto expr : _projections) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
        // 设置返回精度
        if (expr->is_slot_ref() && is_precision(expr->col_type())) {
            SlotRef* slot_ref = static_cast<SlotRef*>(expr);
            int64_t table_id = state->get_tuple_desc(slot_ref->tuple_id())->table_id();
            auto table_info = SchemaFactory::get_instance()->get_table_info_ptr(table_id);
            if (table_info == nullptr) {
                continue;
            }
            auto field_info = table_info->get_field_ptr(slot_ref->field_id());
            if (field_info == nullptr) {
                continue;
            }
            slot_ref->set_float_precision_len(field_info->float_precision_len);
        }
    }
    if (state->is_expr_subquery()) {
        return fatch_expr_subquery_results(state);
    }
    if (_is_explain && state->explain_type == EXPLAIN_NULL) {
        handle_explain(state);
        if (state->is_full_export) {
            state->set_eos();
        }
        return 0;
    }
    state->set_num_affected_rows(ret);
    if (op_type() != pb::OP_SELECT && op_type() != pb::OP_UNION) {
        pack_ok(state->num_affected_rows(), _client);
        return 0;
    }
    if (state->explain_type == SHOW_KEYPOINT) {
        return handle_keypoint(state);
    }
    if (state->explain_type == SAMPLE_KEYPOINT) {
        return sample_keypoint(state);
    }
    if (_trace != nullptr) {
        return open_trace(state);
    }

    if (state->explain_type == ANALYZE_STATISTICS) {
        return open_analyze(state);
    } else if (state->explain_type == SHOW_HISTOGRAM) {
        return open_histogram(state);
    } else if (state->explain_type == SHOW_CMSKETCH) {
        return open_cmsketch(state);
    } else if (state->explain_type == SHOW_HLL) {
        return open_hyperloglog(state);
    }

    pack_head();
    pack_fields();

    if (state->is_full_export) {
        return 0;
    }

    bool eos = false;
    int64_t pack_time = 0;
    do {
        if (_children.empty()) {
            break;
        }
        RowBatch batch;
        ret = _children[0]->get_next(state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        set_node_exec_type(_children[0]->node_exec_type());
        if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
            break;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            TimeCost cost;
            if (_binary_protocol) {
                ret = pack_binary_row(batch.get_row().get(), state);
            } else {
                ret = pack_text_row(batch.get_row().get(), state);
            }

            pack_time += cost.get_time();
            cost.reset();
            state->inc_num_returned_rows(1);
            if (ret < 0) {
                DB_WARNING("pack_row fail:%d", ret);
                return ret;
            }
        }
    } while (!eos);
    if (!_return_empty && _node_exec_type == pb::EXEC_ARROW_ACERO) {
        if (state->use_mpp) {
            return 0;
        }
        // ARROW列式执行入口
        ret = start_vectorized_execution(state);
        if (ret < 0) {
            return ret;
        }
    }
    state->execute_type = _node_exec_type;
    //DB_WARNING("txn_id: %lu, pack_time: %ld", state->txn_id, pack_time);
    pack_eof();
    return 0;
}

int PacketNode::open_trace(RuntimeState* state) {
    bool eos = false;
    int ret = 0;

    do {
        if (_children.empty()) {
            break;
        }
        RowBatch batch;
        ret = _children[0]->get_next(state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        set_node_exec_type(_children[0]->node_exec_type());
        if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
            break;
        }
        state->inc_num_returned_rows(batch.size());
    } while (!eos);
    if (_node_exec_type == pb::EXEC_ARROW_ACERO) {
        if (state->use_mpp) {
            return 0;
        }
        // ARROW列式执行入口
        start_vectorized_execution(state);
    }
    state->execute_type = _node_exec_type;

    if (state->explain_type == SHOW_TRACE) {
        handle_trace(state);
    } else {
        handle_trace2(state);
    }
    return 0;
}

int PacketNode::open_histogram(RuntimeState* state) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    SmartStatistics stat_ptr = schema_factory->get_statistics_ptr(state->get_tuple_desc(0)->table_id());
    if (stat_ptr == nullptr) {
        DB_WARNING("can`t find statistics");
        return -1;
    }
    std::vector<std::string> names;
    std::vector<std::vector<std::string>> rows;
    if (state->get_tuple_desc(0)->slots().size() > 1) {
        names = {
            "field_id", "field_name", "distinct_cnt", "null_value_cnt", "buckets_count"
        };
        stat_ptr->histogram_to_string(rows, _fields);
    } else if (state->get_tuple_desc(0)->slots().size() == 1) {
        names = {
            "bucket_idx", "start_key", "end_key", "distinct_cnt", "bucket_size"
        };
        int i = 0;
        int32_t field_id = state->get_tuple_desc(0)->slots(0).field_id();
        auto histogram_ptr = stat_ptr->get_histogram_ptr(field_id);
        auto& bucket_mapping = histogram_ptr->get_bucket_mapping();
        for (auto iter = bucket_mapping.begin(); iter != bucket_mapping.end(); iter++) {
            i++;
            std::vector<std::string> row;
            row.push_back(std::to_string(i));
            row.push_back(iter->second->start.get_string());
            row.push_back(iter->second->end.get_string());
            row.push_back(std::to_string(iter->second->distinct_cnt));
            row.push_back(std::to_string(iter->second->bucket_size));
            rows.push_back(row);  
        }
    }
    if (rows.size() <= 0) {
        return -1;
    }

    _fields.clear();
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.push_back(field);
    }
    pack_head();
    pack_fields();
    for (auto& row : rows) {
        pack_vector_row(row);
    }
    pack_eof();
    return 0;
}

int PacketNode::open_hyperloglog(RuntimeState* state) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    int64_t table_id = state->get_tuple_desc(0)->table_id();
    SmartStatistics stat_ptr = schema_factory->get_statistics_ptr(table_id);
    if (stat_ptr == nullptr) {
        DB_WARNING("can`t find statistics table_id:%ld", table_id);
        return -1;
    }
    std::vector<std::string> row;
    pb::TupleDescriptor* tuple = state->get_tuple_desc(0);
    if (tuple == nullptr) {
        // should never happen
        return -1;
    }
    for(int i = 0; i < tuple->slots_size(); ++i) {
        int field_id = state->get_tuple_desc(0)->slots(i).field_id();
        auto hllcolumn_ptr = stat_ptr->get_hllcolumn_ptr(field_id);
        if (hllcolumn_ptr == nullptr) {
            DB_WARNING("can`t find hll, table_id:%ld, field_id:%d", table_id, field_id);
            row.emplace_back(std::to_string(-1));
        } else {
            row.emplace_back(std::to_string(hllcolumn_ptr->estimate()));
        }
    }

    for (auto& field : _fields) {
        // 类型全部更新为int64
        field.type = MYSQL_TYPE_LONGLONG;
    }
    pack_head();
    pack_fields();
    pack_vector_row(row);
    pack_eof();
    return 0;
}

int PacketNode::open_cmsketch(RuntimeState* state) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    int64_t table_id = state->get_tuple_desc(0)->table_id();
    SmartStatistics stat_ptr = schema_factory->get_statistics_ptr(table_id);
    if (stat_ptr == nullptr) {
        DB_WARNING("can`t find statistics table_id:%ld", table_id);
        return -1;
    }
    std::vector<std::string> names;
    std::vector<std::vector<std::string>> rows;
    int field_id = state->get_tuple_desc(0)->slots(0).field_id();
    auto cmsketch_ptr = stat_ptr->get_cmsketchcolumn_ptr(field_id);
    if (cmsketch_ptr == nullptr) {
        DB_WARNING("can`t find cmsketch, table_id:%ld, field_id:%d", table_id, field_id);
        return -1;
    }
    names.push_back("field_id:" + std::to_string(field_id));
    for (int width_idx = 0; width_idx < cmsketch_ptr->get_width(); width_idx++) {
        names.push_back(std::to_string(width_idx + 1));
    }
    cmsketch_ptr->to_string(rows);

    _fields.clear();
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.push_back(field);
    }
    pack_head();
    pack_fields();
    for (auto& row : rows) {
        pack_vector_row(row);
    }
    pack_eof();
    return 0;
}

int PacketNode::open_analyze(RuntimeState* state) {
    bool eos = false;
    int ret = 0;
    TimeCost time;
    std::vector<std::shared_ptr<RowBatch> > batch_vector;
    do {
        if (_children.empty()) {
            break;
        }
        std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
        ret = _children[0]->get_next(state, batch.get(), &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        state->inc_num_returned_rows(batch->size());
        if (batch->size() > 0) {
            batch_vector.push_back(batch);
        }
    } while (!eos);

    std::vector<ExprNode*> slot_order_exprs;
    for (auto& slot : state->get_tuple_desc(0)->slots()) {
        ExprNode* order_expr = nullptr;
        pb::Expr slot_expr;
        pb::ExprNode* node = slot_expr.add_nodes(); 
        node->set_node_type(pb::SLOT_REF);
        node->set_col_type(slot.slot_type());
        node->set_num_children(0);
        node->mutable_derive_node()->set_tuple_id(0);
        node->mutable_derive_node()->set_slot_id(slot.slot_id());
        ret = ExprNode::create_tree(slot_expr, &order_expr);
        if (ret < 0) {
            //如何释放资源
            return ret;
        }
        slot_order_exprs.push_back(order_expr);
    }

    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_UPDATE_STATISTICS);
    pb::Statistics* statistics = request.mutable_statistics();
    if (state->get_tuple_desc(0)->has_table_id()) {
        statistics->set_table_id(state->get_tuple_desc(0)->table_id());
        statistics->set_version(0);
        statistics->set_total_rows(state->num_scan_rows());
    } else {
        DB_FATAL("can`t find table_id");
        return -1;
    }
    if (state->statistics_types->find(pb::StatisticType::ST_CLEAR) == state->statistics_types->end()) {
        if (state->num_returned_rows() != 0) {
            // 采样不为0才认为需要柱状图
            pb::Histogram* histogram = statistics->mutable_histogram();
            PacketSample packet_sample(batch_vector, slot_order_exprs, state->get_tuple_desc(0));
            histogram->set_sample_rows(state->num_returned_rows());
            histogram->set_total_rows(state->num_scan_rows());
            packet_sample.packet_sample(histogram);
        }
        if (state->cmsketch != nullptr) {
            pb::CMsketch* cmsketch = statistics->mutable_cmsketch();
            state->cmsketch->to_proto(cmsketch);
        }
        if (state->hll != nullptr) {
            pb::HyperLogLog* hll = statistics->mutable_hll();
            state->hll->to_proto(hll, false);
        }
    }
    
    if (MetaServerInteract::get_instance()->send_request("meta_manager", 
                                                          request, 
                                                          response) != 0) {
        DB_FATAL("update statistics from meta_server fail");
        return -1;
    }
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("send_request fail");
        return -1;
    }

    std::vector<std::string> names = {
            "sample_rows", "scan_rows", "time_cost"
    };
    _fields.clear();
    for (auto& name : names) {
        ResultField field;
        field.name = name;
        field.type = MYSQL_TYPE_STRING;
        _fields.push_back(field);
    }
    std::vector<std::string> row;
    row.push_back(std::to_string(state->num_returned_rows()));
    row.push_back(std::to_string(state->num_scan_rows()));
    row.push_back(std::to_string(time.get_time()));
    pack_head();
    pack_fields();
    pack_vector_row(row);
    pack_eof();

    return 0;
}

int PacketNode::get_next(RuntimeState* state) {
    //TraceLocalNode local_node("PacketNode get_next", get_trace(), GET_NEXT_TRACE);
    if (_is_explain) {
        return 0;
    } 
    if (_children.empty()) {
        state->set_eos();
        pack_eof();
        return 0;
    }
    bool eos = false;
    int ret = 0;
    RowBatch batch;
    ret = _children[0]->get_next(state, &batch, &eos);
    if (ret < 0) {
        DB_WARNING("children:get_next fail:%d", ret);
        return ret;
    }
    for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
        TimeCost cost;
        if (_binary_protocol) {
            ret = pack_binary_row(batch.get_row().get(), state);
        } else {
            ret = pack_text_row(batch.get_row().get(), state);
        }
        state->inc_num_returned_rows(1);
        if (ret < 0) {
            DB_WARNING("pack_row fail:%d", ret);
            return ret;
        }
    }

    if (state->is_eos()) {
        pack_eof();
    }
    return 0;
}

int PacketNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (_children.empty()) {
        DB_WARNING("Has no child");
        return -1;
    }
    return _children[0]->get_next(state, batch, eos);
}

void PacketNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto expr : _projections) {
        expr->close();
    }
}

int PacketNode::pack_ok(int num_affected_rows, NetworkSocket* client) {
    if (_send_buf->_size > 0) {
        _send_buf->byte_array_clear();
    }
    int64_t last_insert_id = (op_type() == pb::OP_INSERT || op_type() == pb::OP_UPDATE)? client->last_insert_id : 0;

    DataBuffer tmp_buf;
    tmp_buf.byte_array_append_length_coded_binary(0);
    tmp_buf.byte_array_append_length_coded_binary(num_affected_rows);
    tmp_buf.byte_array_append_length_coded_binary(last_insert_id);
    
    // https://dev.mysql.com/doc/internals/en/status-flags.html
    uint16_t status_flag = 0;
    if (client->txn_id != 0) {
        status_flag |= 0x0001;
    }
    if (client->autocommit) {
        status_flag |= 0x0002;
    }
    uint8_t bytes[2];
    bytes[0] = (status_flag & 0xff);
    bytes[1] = (status_flag >> 8) & 0xff;
    tmp_buf.byte_array_append_len(bytes, 2);

    bytes[0] = 0 & 0xff;
    bytes[1] = (0 >> 8) & 0xff;
    tmp_buf.byte_array_append_len(bytes, 2);

    return _send_buf->network_queue_send_append(tmp_buf._data, tmp_buf._size, ++client->packet_id, 0);
}

int PacketNode::pack_err() {
    return 0;
}

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
int PacketNode::pack_head() {
    //Result Set Header Packet
    int start_pos = _send_buf->_size;
    if (!_send_buf->byte_array_append_len((const uint8_t *)"\x01\x00\x00\x01", 4)) {
        DB_FATAL("byte_array_append_len failed.");
        return -1;
    }
    if (_fields.size() == 0) {
        DB_FATAL("fields size is wrong.size:[0]");
        return -1;
    }
    if (!_send_buf->byte_array_append_length_coded_binary(_fields.size())) {
        DB_FATAL("byte_array_append_len failed. len:[%lu]", _fields.size());
        return -1;
    }
    int packet_body_len = _send_buf->_size - start_pos - 4;
    _send_buf->_data[start_pos] = packet_body_len & 0xFF;
    _send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xFF;
    _send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xFF;
    _send_buf->_data[start_pos + 3] = (++_client->packet_id) & 0xFF;
    return 0;
}

int PacketNode::pack_fields() {
    for (auto& field : _fields) {
        _wrapper->make_field_packet(_send_buf, &field, ++_client->packet_id);
    }
    pack_eof();
    return 0;
}

// use for make_stmt_prepare_ok_packet
int PacketNode::pack_fields(DataBuffer* buffer, int& packet_id) {
    for (auto& field : _fields) {
        ++packet_id;
        _wrapper->make_field_packet(buffer, &field, packet_id);
    }
    ++packet_id;
    _wrapper->make_eof_packet(buffer, packet_id);
    return 0;
}

int PacketNode::pack_vector_row(const std::vector<std::string>& row) {
    int start_pos = _send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = (++_client->packet_id) & 0xFF;
    if (!_send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len. value:[%s], len:[1]", bytes);
        return -1;
    }

    // package body.
    for (auto& item : row) {
        uint64_t length = item.size();
        if (!_send_buf->byte_array_append_length_coded_binary(length)) {
            DB_FATAL("Failed to append length coded binary.length:[%lu]", length);
            return -1;
        }
        if (!_send_buf->byte_array_append_len((const uint8_t *)item.c_str(), item.size())) {
            DB_FATAL("Failed to append table cell.");
            return -1;
        }
    }
    uint32_t packet_body_len = _send_buf->_size - start_pos - 4;
    _send_buf->_data[start_pos] = packet_body_len & 0xff;
    _send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xff;
    _send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xff;
    return 0;
}

int PacketNode::pack_text_row(MemRow* row,  RuntimeState* state, std::vector<std::shared_ptr<arrow::ChunkedArray>>* columns, int row_idx) {
    int start_pos = _send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = (++_client->packet_id) & 0xFF;
    if (!_send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len. value:[%s], len:[1]", bytes);
        return -1;
    }

    // package body.
    int field_idx = 0;
    for (auto expr : _projections) {
        ExprValue expr_value;
        if (row != nullptr) {
            expr_value = expr->get_value(row).cast_to(expr->col_type());
        } else {
            // TODO, 减少一次拷贝
            expr_value = VectorizeHelpper::get_vectorized_value((*columns)[field_idx].get(), row_idx, expr->float_precision_len()).cast_to(expr->col_type());
        }
        if (state->need_convert_charset) {
            int ret = expr_value.convert_charset(state->table_charset, state->connection_charset);
            if (ret < 0) {
                DB_FATAL("Fail to convert_charset, table_charset: %d, connection_charset: %d, value: %s",
                          state->table_charset, state->connection_charset, expr_value.get_string().c_str());
                return -1;
            }
        }
        if (!_send_buf->append_text_value(expr_value)) {
            DB_FATAL("Failed to append table cell.");
            return -1;
        }
        field_idx++;
    }
    uint32_t packet_body_len = _send_buf->_size - start_pos - 4;
    while (packet_body_len >= PACKET_LEN_MAX) {
        _send_buf->_data[start_pos] = PACKET_LEN_MAX & 0xff;
        _send_buf->_data[start_pos + 1] = (PACKET_LEN_MAX >> 8) & 0xff;
        _send_buf->_data[start_pos + 2] = (PACKET_LEN_MAX >> 16) & 0xff;
        start_pos += PACKET_LEN_MAX + 4;
        packet_body_len -= PACKET_LEN_MAX;
        uint8_t bytes[4];
        bytes[0] = '\x01';
        bytes[1] = '\x00';
        bytes[2] = '\x00';
        bytes[3] = (++_client->packet_id) & 0xFF;
        if (!_send_buf->byte_array_insert_len(bytes, start_pos, 4)) {
            DB_FATAL("Failed to insert len. value:[%s], len:[4]", bytes);
            return -1;
        }
    }
    _send_buf->_data[start_pos] = packet_body_len & 0xff;
    _send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xff;
    _send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xff;
    return 0;
}

int PacketNode::pack_binary_row(MemRow* row, RuntimeState* state, std::vector<std::shared_ptr<arrow::ChunkedArray>>* columns, int row_idx) {
    int start_pos = _send_buf->_size;
    uint8_t bytes[4];
    bytes[0] = '\x01';
    bytes[1] = '\x00';
    bytes[2] = '\x00';
    bytes[3] = (++_client->packet_id) & 0xFF;
    if (!_send_buf->byte_array_append_len(bytes, 4)) {
        DB_FATAL("Failed to append len. value:[%s], len:[4]", bytes);
        return -1;
    }

    // row header
    bytes[0] = '\x00';
    if (!_send_buf->byte_array_append_len(bytes, 1)) {
        DB_FATAL("Failed to append row_header");
        return -1;
    }

    int column_count = _fields.size();
    int null_bitmap_len = (column_count + 7 + 2) / 8;
    std::unique_ptr<uint8_t[]> null_map(new uint8_t[null_bitmap_len]);
    memset(null_map.get(), 0, null_bitmap_len);
    int null_map_pos = _send_buf->_size;
    if (!_send_buf->byte_array_append_len(null_map.get(), null_bitmap_len)) {
        DB_FATAL("Failed to append null_map");
        return -1;
    }

    int field_idx = 0;
    // package body.
    for (auto expr : _projections) {
        ExprValue expr_value;
        if (row != nullptr) {
            expr_value = expr->get_value(row).cast_to(expr->col_type());
        } else {
            // TODO, 减少一次拷贝
            expr_value = VectorizeHelpper::get_vectorized_value((*columns)[field_idx].get(), row_idx, expr->float_precision_len()).cast_to(expr->col_type());
        }
        if (state->need_convert_charset) {
            int ret = expr_value.convert_charset(state->table_charset, state->connection_charset);
            if (ret < 0) {
                DB_FATAL("Fail to convert_charset, table_charset: %d, connection_charset: %d, value: %s",
                          state->table_charset, state->connection_charset, expr_value.get_string().c_str());
                return -1;
            }
        }
        if (!_send_buf->append_binary_value(expr_value,
                _fields[field_idx].type, null_map.get(), field_idx, 2)) {
            DB_FATAL("Failed to append table cell.");
            return -1;
        }
        field_idx++;
    }
    // std::string null_map_str((char*)null_map.get(), null_bitmap_len);
    // DB_WARNING("NULL-Bitmap: %s", str_to_hex(null_map_str).c_str());

    // fill the real values of NULL-Bitmap
    for (int idx = 0; idx < null_bitmap_len; ++idx) {
        _send_buf->_data[null_map_pos + idx] = null_map[idx];
    }
    uint32_t packet_body_len = _send_buf->_size - start_pos - 4;
    while (packet_body_len >= PACKET_LEN_MAX) {
        _send_buf->_data[start_pos] = PACKET_LEN_MAX & 0xff;
        _send_buf->_data[start_pos + 1] = (PACKET_LEN_MAX >> 8) & 0xff;
        _send_buf->_data[start_pos + 2] = (PACKET_LEN_MAX >> 16) & 0xff;
        start_pos += PACKET_LEN_MAX + 4;
        packet_body_len -= PACKET_LEN_MAX;
        uint8_t bytes[4];
        bytes[0] = '\x00';
        bytes[1] = '\x00';
        bytes[2] = '\x00';
        bytes[3] = (++_client->packet_id) & 0xFF;
        if (!_send_buf->byte_array_insert_len(bytes, start_pos, 4)) {
            DB_FATAL("Failed to insert len. value:[%s], len:[4]", bytes);
            return -1;
        }
    }
    _send_buf->_data[start_pos] = packet_body_len & 0xff;
    _send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xff;
    _send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xff;
    return 0;
}

int PacketNode::pack_eof() {
    _wrapper->make_eof_packet(_send_buf, ++_client->packet_id);
    return 0;
}

void PacketNode::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    for (auto& expr : _projections) {
        expr->find_place_holder(placeholders);
    }
}

int PacketNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    if (_children.size() > 0) {
        _children[0]->predicate_pushdown(input_exprs);
    }
    if (input_exprs.size() > 0) {
        add_filter_node_as_child(input_exprs);
    }
    input_exprs.clear();
    return 0;
}

int PacketNode::prune_columns(QueryContext* ctx, const std::unordered_set<int32_t>& invalid_column_ids) {
    if (ctx == nullptr) {
        DB_WARNING("ctx is nullptr");
        return -1;
    }
    if (!invalid_column_ids.empty()) {
        // 更新projection
        pb::PlanNode new_pb_node;
        new_pb_node.CopyFrom(_pb_node);
        const pb::PacketNode& packet_node = _pb_node.derive_node().packet_node();
        pb::PacketNode* new_packet_node = new_pb_node.mutable_derive_node()->mutable_packet_node();
        new_packet_node->clear_projections();
        new_packet_node->clear_col_names();
        if (_projections.size() != packet_node.projections().size() || 
                _projections.size() != packet_node.col_names().size()) {
            DB_WARNING("projections.size(): %lu, packet_node.projections_size(): %d, "
                       "packet_node.col_names_size(): %d",
                       _projections.size(), packet_node.projections().size(), 
                       packet_node.col_names().size());
            return -1;
        }
        std::unordered_set<int32_t> change_tuple_ids;
        std::vector<ExprNode*> new_projections;
        new_projections.reserve(_projections.size());
        for (int i = 0; i < _projections.size(); ++i) {
            if (_projections[i] == nullptr) {
                DB_WARNING("projection is nullptr");
                return -1;
            }
            if (invalid_column_ids.find(i) == invalid_column_ids.end()) {
                new_projections.emplace_back(_projections[i]);
                new_packet_node->add_projections()->CopyFrom(packet_node.projections(i));
                new_packet_node->add_col_names(packet_node.col_names(i));
            } else {
                if (!_projections[i]->has_agg() && !_projections[i]->has_window()) {
                    // Agg表达式或者Window表达式不更新Tuple的引用计数，后续有需要可升级
                    std::vector<std::pair<int32_t, int32_t>> tuple_slot_ids;
                    _projections[i]->get_all_tuple_slot_ids(tuple_slot_ids);
                    for (const auto& [tuple_id, slot_id] : tuple_slot_ids) {
                        pb::TupleDescriptor* tuple = ctx->get_tuple_desc(tuple_id);
                        if (tuple == nullptr) {
                            DB_WARNING("tuple is nullptr");
                            return -1;
                        }
                        if (slot_id - 1 < 0 || slot_id - 1 >= tuple->slot_idxes_size()) {
                            DB_WARNING("Invalid slot_id: %d, tuple slot_idxes size: %d", slot_id, tuple->slot_idxes_size());
                            return -1;
                        }
                        int slot_idx = tuple->slot_idxes(slot_id - 1);
                        if (slot_idx < 0 || slot_idx >= tuple->slots_size()) {
                            DB_WARNING("Invalid slot_idx: %d, tuple slots size: %d", slot_id, tuple->slots_size());
                            return -1;
                        }
                        int ref_cnt = tuple->slots(slot_idx).ref_cnt();
                        tuple->mutable_slots(slot_idx)->set_ref_cnt(ref_cnt - 1);
                        change_tuple_ids.insert(tuple_id);
                    }
                }
                _projections[i]->close();
                ExprNode::destroy_tree(_projections[i]);
            }
        }
        std::swap(_projections, new_projections);
        std::swap(_pb_node, new_pb_node);
        // 更新Tuple
        for (auto tuple_id : change_tuple_ids) {
            pb::TupleDescriptor* tuple = ctx->get_tuple_desc(tuple_id);
            if (tuple == nullptr) {
                DB_WARNING("tuple is nullptr");
                return -1;
            }
            int32_t new_slot_idx = 0;
            pb::TupleDescriptor new_tuple;
            new_tuple.CopyFrom(*tuple);
            new_tuple.mutable_slots()->Clear();
            for (int i = 0; i < tuple->slots_size(); ++i) {
                const auto& slot = tuple->slots(i);
                if (slot.ref_cnt() <= 0) {
                    continue;
                }
                if (slot.slot_id() - 1 < 0 || slot.slot_id() - 1 >= tuple->slot_idxes_size()) {
                    DB_WARNING("Invalid slot id: %d, tuple slot idx size: %d", slot.slot_id(), tuple->slot_idxes_size());
                    return -1;
                }
                new_tuple.add_slots()->CopyFrom(slot);
                new_tuple.set_slot_idxes(slot.slot_id() - 1, new_slot_idx++);
            }
            std::swap(*tuple, new_tuple);
        }
    }
    return ExecNode::prune_columns(ctx, invalid_column_ids);
}

}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
