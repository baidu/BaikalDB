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

#include <map>
#include "rocksdb_scan_node.h"
#include "filter_node.h"
#include "join_node.h"
#include "schema_factory.h"
#include "scalar_fn_call.h"
#include "slot_ref.h"
#include "runtime_state.h"
#include "qos.h"
#include <arrow/acero/options.h>
#include "arrow_io_excutor.h"
#include "vectorize_helpper.h"

namespace baikaldb {

DEFINE_bool(reverse_seek_first_level, false, "reverse index seek first level, default(false)");
// DEFINE_bool(scan_use_multi_get, true, "use MultiGet API, default(true)");
DEFINE_int32(in_predicate_check_threshold, 4096, "in predicate threshold to check memory, default(4096)");
DECLARE_int64(print_time_us);
DECLARE_int32(chunk_size);

int RocksdbScanNode::choose_index(RuntimeState* state) {
    // 做完logical plan还没有索引
    auto& scan_pb = _pb_node.derive_node().scan_node();
    if (scan_pb.indexes_size() == 0) {
        DB_FATAL_STATE(state, "no index");
        return -1;
    }
    
    pb::PossibleIndex pos_index;
    pos_index.ParseFromString(scan_pb.indexes(0));
    if (_pb_node.derive_node().scan_node().has_fulltext_index()) {
        _new_fulltext_tree = true;
    }
    if (pos_index.has_range_key_sorted()) {
        _range_key_sorted = pos_index.range_key_sorted();
    }

    _index_id = pos_index.index_id();
    _index_info = _factory->get_index_info_ptr(_index_id);
    if (_index_info == nullptr || _index_info->id == -1) {
        DB_WARNING_STATE(state, "no index_info found for index id: %ld", _index_id);
        return -1;
    }
    bool use_vector = _index_info->type == pb::I_VECTOR;
    bool use_fulltext = _index_info->type == pb::I_FULLTEXT;

    int ret = 0;
    for (auto& expr : pos_index.index_conjuncts()) {
        ExprNode* index_conjunct = nullptr;
        ret = ExprNode::create_tree(expr, &index_conjunct);
        if (ret < 0) {
            DB_WARNING_STATE(state, "ExprNode::create_tree fail, ret:%d", ret);
            return ret;
        }
        if (index_conjunct == nullptr) {
            DB_WARNING_STATE(state, "expr is null");
            return -1;
        }
        _scan_conjuncts.emplace_back(index_conjunct);
    }
    if (pos_index.has_sort_index()) {
        if (pos_index.ranges_size() > 1) {
            bool multi_range_limit = false;
            if (_limit != -1) {
                multi_range_limit = true;
            } else {
                // limit没下推，并且filter条件不为空，不能走_sort_limit_by_range逻辑
                if (get_parent() != nullptr && (get_parent()->node_type() == pb::TABLE_FILTER_NODE ||
                    get_parent()->node_type() == pb::WHERE_FILTER_NODE)) {
                    if (static_cast<FilterNode*>(get_parent())->mutable_conjuncts()->empty()) {
                        multi_range_limit = true;
                    }
                }
            }
            if (multi_range_limit) {
                _sort_use_index_by_range = true;
                _sort_limit_by_range = pos_index.sort_index().sort_limit();
            }
        } else {
            _sort_use_index = true;
        }
        _scan_forward = pos_index.sort_index().is_asc();
    }
    if (use_vector) {
        _sort_use_index = true;
    }

    for (auto& f : _pri_info->fields) {
        auto slot_id = state->get_slot_id(_tuple_id, f.id);
        if (slot_id > 0) {
            _index_slot_field_map[slot_id] = f.id;
        }
    }
    if (_index_info->type == pb::I_KEY || _index_info->type == pb::I_UNIQ) {
        for (auto& f : _index_info->fields) {
            auto slot_id = state->get_slot_id(_tuple_id, f.id);
            if (slot_id > 0) {
                _index_slot_field_map[slot_id] = f.id;
            }
        }
    }
    // 以baikaldb的判断为准
    _is_covering_index = pos_index.is_covering_index();

    if (use_vector) {
        index_condition_pushdown();
        for (auto expr : _scan_conjuncts) {
            ret = expr->open();
            if (ret < 0) {
                DB_WARNING_STATE(state, "Expr::open fail:%d", ret);
                return ret;
            }
        }
        for (auto expr : _vector_filter_conjuncts) {
            ret = expr->open();
            if (ret < 0) {
                DB_WARNING_STATE(state, "Expr::open fail:%d", ret);
                return ret;
            }
        }
        for (auto& raw_index : scan_pb.indexes()) {
            pb::PossibleIndex pos_index;
            pos_index.ParseFromString(raw_index);
            auto index_id = pos_index.index_id();
            auto index_info = _factory->get_index_info_ptr(index_id);
            if (index_info == nullptr|| index_info->id == -1) {
                DB_WARNING_STATE(state, "no index_info found for index id: %ld", index_id);
                return -1;
            }
            for (auto& range : pos_index.ranges()) {
                _vector_word = range.left_key();
                int topk = range.topk();
                if (topk < 0) {
                    // 兼容没有order by的场景
                    // select * from tbl where match(vec) against ('' in vector mode) limit 100;
                    // select * from tbl where match(vec) against ('' in vector mode) and id > 0 limit 100;
                    if (_parent != nullptr) {
                        topk = _parent->get_limit();
                    }
                }
                _topk = std::max(topk, _topk);
                _separate_value = range.separate_value();
                _efsearch = std::max(range.efsearch(), _efsearch);
            }
        }
        _vector_index = state->vector_index_map()[_index_id];
        return 0;
    }

    if (use_fulltext) {
        // 索引条件下推，减少主表查询次数
        index_condition_pushdown();
        for (auto expr : _scan_conjuncts) {
            ret = expr->open();
            if (ret < 0) {
                DB_WARNING_STATE(state, "Expr::open fail:%d", ret);
                return ret;
            }
        }
        for (auto& raw_index : scan_pb.indexes()) {
            pb::PossibleIndex pos_index;
            pos_index.ParseFromString(raw_index);
            auto index_id = pos_index.index_id();
            auto index_info = _factory->get_index_info_ptr(index_id);
            if (index_info == nullptr|| index_info->id == -1) {
                DB_WARNING_STATE(state, "no index_info found for index id: %ld", index_id);
                return -1;
            }
            for (auto& range : pos_index.ranges()) {
                std::string word;
                word = range.left_key();
                _reverse_infos.emplace_back(*index_info);
                _query_words.emplace_back(word);
                _match_modes.emplace_back(range.match_mode());
            }
            _bool_and = pos_index.bool_and();
            //DB_WARNING_STATE(state, "use multi %d", _reverse_infos.size());
        }
        return 0;
    }
    if (pos_index.ranges_size() == 0) {
        return 0;
    }
    //DB_WARNING_STATE(state, "use_index: %ld table_id: %ld left:%d, right:%d", 
    //        _index_id, _table_id, pos_index.ranges(0).left_field_cnt(), pos_index.ranges(0).right_field_cnt());

    int64_t ranges_used_size = 0;
    bool check_memory = false;
    if (pos_index.ranges_size() > FLAGS_in_predicate_check_threshold) {
        check_memory = true;
    }
    bool has_global_param = false;
    if (pos_index.has_left_field_cnt() || pos_index.has_right_field_cnt()) {
        has_global_param = true;
        _is_eq = pos_index.is_eq();
        _like_prefix = pos_index.like_prefix();
        _left_field_cnt = pos_index.left_field_cnt();
        _left_open = pos_index.left_open();
        _right_field_cnt = pos_index.right_field_cnt();
        _right_open = pos_index.right_open();
    }
    for (auto& range : pos_index.ranges()) {
        if (!has_global_param) {
            has_global_param = true;
            if (range.left_key() == range.right_key()) {
                _is_eq = true;
            }
            _left_field_cnt = range.left_field_cnt();
            _right_field_cnt = range.right_field_cnt();
            _left_open = range.left_open();
            _right_open = range.right_open();
            _like_prefix = range.like_prefix();
            if (_is_eq) {
                _right_field_cnt = _left_field_cnt;
                _right_open = _left_open;
            }
        }
        if (!_is_eq) {
            _scan_range_keys.add_key(range.left_key(), range.left_full(), range.right_key(), range.right_full());
        } else {
            _scan_range_keys.add_key(range.left_key(), range.left_full(), range.left_key(), range.left_full());
        }
        if (check_memory) {
            ranges_used_size += range.left_key().size() * 2;
            ranges_used_size += range.right_key().size() * 2;
            ranges_used_size += 100; // 估计值
        }
    }
    _scan_range_keys.set_start_capacity(state->row_batch_capacity());
    if (check_memory && 0 != state->memory_limit_exceeded(std::numeric_limits<int>::max(), ranges_used_size)) {
        return -1;
    }
    if (_index_info->type == pb::I_PRIMARY || _index_info->type == pb::I_UNIQ) {
        if (_left_field_cnt == (int)_index_info->fields.size() && _is_eq && !_like_prefix) {
            //DB_WARNING_STATE(state, "index use get ,index:%ld", _index_info.id);
            _use_get = true;
        }
    }

    // 索引条件下推，减少主表查询次数
    index_condition_pushdown();
    for (auto expr : _scan_conjuncts) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "Expr::open fail:%d", ret);
            return ret;
        }
    }
    //DB_WARNING_STATE(state, "start search");
    return 0;
}

int RocksdbScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ScanNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _factory = SchemaFactory::get_instance();
    _table_info = _factory->get_table_info_ptr(_table_id);
    _pri_info = _factory->get_index_info_ptr(_table_id);

    if (_table_info == nullptr) {
        DB_WARNING("table info not found _table_id:%ld", _table_id);
        return -1;
    }
    if (_pri_info == nullptr) {
        DB_WARNING("pri info not found _table_id:%ld", _table_id);
        return -1;
    }
    if (node.derive_node().scan_node().has_lock() &&
            node.derive_node().scan_node().lock() == pb::LOCK_GET_ONLY_PRIMARY) {
        _get_mode = GET_LOCK;
    }
    _is_ddl_work = node.derive_node().scan_node().is_ddl_work();
    _ddl_work_type = node.derive_node().scan_node().ddl_work_type();
    _ddl_index_id = node.derive_node().scan_node().ddl_index_id();
    _watt_stats_version = node.derive_node().scan_node().watt_stats_version();
    if (_ddl_work_type == pb::DDL_LOCAL_INDEX || _is_ddl_work) {
        _ddl_index_info = _factory->get_index_info_ptr(_ddl_index_id);
        if (_ddl_index_info == nullptr) {
            DB_WARNING("ddl index info not found _index_id:%ld", _ddl_index_id);
            return -1;
        }
    } else if (_ddl_work_type == pb::DDL_COLUMN) {
        for (auto& slot : node.derive_node().scan_node().column_ddl_info().update_slots()) {
            _update_slots.emplace_back(slot);
        }
        for (auto& expr : node.derive_node().scan_node().column_ddl_info().update_exprs()) {
            ExprNode* up_expr = nullptr;
            ret = ExprNode::create_tree(expr, &up_expr);
            if (ret < 0) {
                return ret;
            }
            _update_exprs.emplace_back(up_expr);
        }
        for (auto expr : _update_exprs) {
            ret = expr->open();
            if (ret < 0) {
                DB_WARNING("expr open fail, ret:%d", ret);
                return ret;
            }
        }
        for (auto& expr : node.derive_node().scan_node().column_ddl_info().scan_conjuncts()) {
            ExprNode* scan_conjunct = nullptr;
            ret = ExprNode::create_tree(expr, &scan_conjunct);
            if (ret < 0) {
                return ret;
            }
            _scan_conjuncts.emplace_back(scan_conjunct);
        }
        for (auto expr : _scan_conjuncts) {
            ret = expr->open();
            if (ret < 0) {
                DB_WARNING("Expr::open fail:%d", ret);
                return ret;
            }
        }
    }
    return 0;
}

int RocksdbScanNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    //DB_WARNING("node:%ld is pushdown", this);
    if (_parent->node_type() == pb::WHERE_FILTER_NODE
            || _parent->node_type() == pb::TABLE_FILTER_NODE) {
        //DB_WARNING("parent is filter node,%d", _parent->node_type());
        return 0;
    }
    if (input_exprs.size() > 0) {
        add_filter_node(input_exprs);
    }
    input_exprs.clear();
    return 0;
}

bool RocksdbScanNode::need_pushdown(ExprNode* expr) {
    pb::IndexType index_type = _index_info->type;
    bool is_cstore_table_seek = false;
    if ((!_use_get) && _table_info->engine == pb::ROCKSDB_CSTORE && _index_id == _table_id) {
        is_cstore_table_seek = true;
    }
    // get方式和主键无需下推
    if (_use_get || (index_type == pb::I_PRIMARY && !is_cstore_table_seek)) {
        return false;
    }
    if (index_type == pb::I_KEY || index_type == pb::I_UNIQ || index_type == pb::I_FULLTEXT) {
        // 普通索引只要全包含slot id就可以下推
        // 对于倒排 _index_slot_field_map 里只有pk和 __weight和 __querywords
        std::unordered_set<int32_t> slot_ids;
        expr->get_all_slot_ids(slot_ids);
        for (auto slot_id : slot_ids) {
            if (_index_slot_field_map.count(slot_id) == 0) {
                return false;
            }
        }
        return true;
    }
    // 倒排索引条件比较苛刻
    if (expr->children_size() < 2) {
        return false;
    }
    if (expr->children(0)->node_type() != pb::SLOT_REF) {
        return false;
    }
    if (!is_cstore_table_seek) {
        SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
        if (_index_slot_field_map.count(slot_ref->slot_id()) == 0) {
            return false;
        }
    }
    // 倒排里用field_id识别
    //slot_ref->set_field_id(_index_slot_field_map[slot_ref->slot_id()]);
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: { 
            if (static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EQ) {
                return true;
            }
            break;
        }
        case pb::IN_PREDICATE:
            return true;
        default:
            return false;
    }
    return false;
}

int RocksdbScanNode::index_condition_pushdown() {
    //DB_WARNING("node:%ld is pushdown", this);
    if (_parent == NULL) {
        //DB_WARNING("parent is null");
        return 0;
    }
    if (_parent->node_type() != pb::WHERE_FILTER_NODE &&
            _parent->node_type() != pb::TABLE_FILTER_NODE) {
        DB_WARNING("parent is not filter node:%d", _parent->node_type());
        return 0;
    }
    
    std::vector<ExprNode*>* parent_conditions = _parent->mutable_conjuncts();
    auto iter = parent_conditions->begin();
    while (iter != parent_conditions->end()) {
        if (need_pushdown(*iter)) {
            _scan_conjuncts.emplace_back(*iter);
            iter = parent_conditions->erase(iter);
        } else {
            if (_index_info->type == pb::I_VECTOR) {
                // 向量索引需要将过滤条件下推，用于前过滤
                _vector_filter_conjuncts.emplace_back(*iter);
                iter = parent_conditions->erase(iter);
            } else {
                ++iter;
            }
        }
    }
    return 0;
}

int RocksdbScanNode::index_condition_pullup() {
    if (_parent == NULL) {
        return 0;
    }
    if (_parent->node_type() != pb::WHERE_FILTER_NODE &&
            _parent->node_type() != pb::TABLE_FILTER_NODE) {
        return 0;
    }
    std::vector<ExprNode*>* parent_conditions = _parent->mutable_conjuncts();
    if (parent_conditions == nullptr) {
        DB_WARNING("parent_conditions is nullptr");
        return -1;
    }
    for (auto* conjunct : _vector_filter_conjuncts) {
        parent_conditions->emplace_back(conjunct);
    }
    _vector_filter_conjuncts.clear();
    return 0;
}

int RocksdbScanNode::build_arrow_declaration(RuntimeState* state) {
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    for (auto c : _children) {
        if (c->build_arrow_declaration(state) != 0) {
            return -1;
        }
    }
    // MPP场景下，_arrow_vectorized_reader为nullptr
    if (_arrow_vectorized_reader == nullptr) {
        _arrow_vectorized_reader = std::make_shared<RocksdbVectorizedReader>();
        if (_arrow_vectorized_reader->init(this, state) != 0) {
            DB_WARNING("Fail to init arrow vectorized reader");
            return -1;
        }
    }
    // transfer acero source Declaration
    std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [this] () {
        arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(_arrow_vectorized_reader);
        return batch_it;
    };

    if (state->vectorlized_parallel_execution == false) {
        arrow::acero::Declaration dec{"record_batch_source",
            arrow::acero::RecordBatchSourceNodeOptions{state->arrow_input_schemas[_tuple_id], std::move(iter_maker)}}; 
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, state->arrow_input_schemas[_tuple_id], nullptr);
        state->append_acero_declaration(dec);
    } else {
        auto executor = BthreadArrowExecutor::Make(1);
        _arrow_io_executor = *executor;
        arrow::acero::Declaration dec{"record_batch_source",
            arrow::acero::RecordBatchSourceNodeOptions{state->arrow_input_schemas[_tuple_id], std::move(iter_maker), _arrow_io_executor.get()}}; 
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, state->arrow_input_schemas[_tuple_id], nullptr);
        state->append_acero_declaration(dec);
    }

    std::vector<arrow::compute::Expression> sub_exprs;
    // append scan index filter
    if (!_scan_conjuncts.empty()) {
        sub_exprs.reserve(_scan_conjuncts.size());
        for (int i = 0; i < _scan_conjuncts.size(); ++i) {
            if (_scan_conjuncts[i]->transfer_to_arrow_expression() != 0) {
                DB_FATAL_STATE(state, "expr transfer arrow fail");
                return -1;
            }
            sub_exprs.emplace_back(_scan_conjuncts[i]->arrow_expr());
        }
        _arrow_scan_conjuncts = arrow::compute::and_(sub_exprs);
        arrow::Result<arrow::compute::Expression> bind_expr = _arrow_scan_conjuncts.Bind(*(state->arrow_input_schemas[_tuple_id]));
        if (!bind_expr.ok()) {
            // bind失败的, 无法scan的时候执行filter
            DB_FATAL("bind expr fail:%s", bind_expr.status().ToString().c_str());
            return -1;
        } 
        _arrow_scan_conjuncts = *bind_expr;
        _arrow_vectorized_reader->set_arrow_scan_conjuncts(&_arrow_scan_conjuncts);
        LOCAL_TRACE_ARROW_FILTER(&_arrow_scan_conjuncts, _limit);
    }

    sub_exprs.clear();
    // arrow pushdown all filter into scan, for limit support
    ExecNode* parent = _parent;
    int64_t filter_limit = get_limit();
    for (; parent != nullptr; parent = parent->get_parent()) {
        if (parent->node_type() == pb::WHERE_FILTER_NODE 
            || parent->node_type() == pb::TABLE_FILTER_NODE
            || parent->node_type() == pb::HAVING_FILTER_NODE) {
            if (static_cast<FilterNode*>(parent)->arrow_steal_conjuncts(sub_exprs, filter_limit) != 0) {
                DB_FATAL("arrow steal conjuncts fail");
                return -1;
            }
        }
    }
    arrow::Expression* filter_expr = nullptr;
    if (!sub_exprs.empty()) {
        _arrow_filter_conjuncts = arrow::compute::and_(sub_exprs);
        arrow::Result<arrow::compute::Expression> bind_expr = _arrow_filter_conjuncts.Bind(*(state->arrow_input_schemas[_tuple_id]));
        if (!bind_expr.ok()) {
            // bind失败的, 无法scan的时候执行filter
            DB_FATAL("bind expr fail:%s", bind_expr.status().ToString().c_str());
            return -1;
        }
        _arrow_filter_conjuncts = *bind_expr;
        filter_expr = &_arrow_filter_conjuncts;
    }
    _arrow_vectorized_reader->set_arrow_filter_conjuncts(filter_expr, filter_limit);
    LOCAL_TRACE_ARROW_FILTER(filter_expr, filter_limit);
    _arrow_filter_batch_size = 1024;
    if (get_limit() > 0) {
        _arrow_filter_batch_size = get_limit();
    }
    return 0;
}

int RocksdbScanNode::open(RuntimeState* state) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, ([this](TraceLocalNode& local_node) {
        if (_table_info != nullptr) {
            local_node.append_description() << "table_name:" << _table_info->short_name;
        }
        if (_index_info != nullptr) {
            local_node.append_description() << " index_name:" << _index_info->short_name;
            local_node.set_index_name(_index_info->short_name);
        }
        local_node.append_description() << " index_id:" << _index_id;
    }));
    int ret = 0;
    ret = ScanNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    set_node_exec_type(pb::EXEC_ROW);
    _mem_row_desc = state->mem_row_desc();
    if (_is_explain) {
        return 0;
    }
    if (_table_info == nullptr) {
        DB_WARNING_STATE(state, "table null:%d", ret);
        return -1;
    }
    std::set<int32_t> pri_field_ids;
    for (auto& field_info : _pri_info->fields) {
        pri_field_ids.insert(field_info.id);
    }
    // 用数组映射slot，提升性能
    _field_slot.resize(_table_info->fields.back().id + 1);
    std::vector<int32_t> virtual_cols;
    virtual_cols.reserve(2);
    for (auto& slot : _tuple_desc->slots()) {
        if (slot.field_id() > _field_slot.size() - 1) {
            DB_WARNING("vector out of range, region_id: %ld, field_id: %d", _region_id, slot.field_id());
            continue;
        }
        _field_slot[slot.field_id()] = slot.slot_id();
        if (pri_field_ids.count(slot.field_id()) == 0) {
            auto field = _table_info->get_field_ptr(slot.field_id());
            if (field == nullptr) {
                DB_WARNING("field not found region_id: %ld, field_id: %d", _region_id, slot.field_id());
                return -1;
            }

            _field_ids[slot.field_id()] = field;
            // 这两个倒排的特殊字段
            if (field->short_name == "__weight" ||
                field->short_name == "__querywords") {
                _has__weight = true;
                // 倒排__weight也能index过滤
                virtual_cols.emplace_back(slot.field_id());
                _index_slot_field_map[slot.slot_id()] = field->id;
            }
        }
    }
    if (_ddl_work_type == pb::DDL_COLUMN) {
        for (auto& field_info : _table_info->fields) {
            if (pri_field_ids.count(field_info.id) == 0) {
                // 这两个倒排的特殊字段
                if (field_info.short_name != "__weight" &&
                    field_info.short_name != "__querywords") {
                    _ddl_field_ids[field_info.id] = &field_info;
                }
            }
        }
    }
    ret = choose_index(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "calc index fail:%d", ret);
        return ret;
    }
    if (_index_info == nullptr) {
        DB_WARNING_STATE(state, "index null:%d", ret);
        return -1;
    }
    if (_index_info->type != pb::I_FULLTEXT && _index_info->type != pb::I_VECTOR) {
        // 不命中全文或者向量索引的情况不需要这两个字段
        for (auto it: virtual_cols) {
            _field_ids.erase(it);
        }
    }
    if (_sort_use_index) {
        state->set_sort_use_index();
    }
    _region_id = state->region_id();
    //DB_WARNING_STATE(state, "use_index: %ld table_id: %ld region_id: %ld", _index_id, _table_id, _region_id);
    _region_info = &(state->resource()->region_info);
    if (_region_info->has_main_table_id()
        && _region_info->main_table_id() != _region_info->table_id()) {
        _is_global_index = true;
    }

    auto txn = state->txn();
    auto reverse_index_map = state->reverse_index_map();
    //DB_WARNING_STATE(state, "_is_covering_index:%d", _is_covering_index);
    if (_vector_index != nullptr) {
        int ret = _vector_index->search_vector(txn->get_txn(), 
                                               _separate_value,
                                               _pri_info, 
                                               _table_info, 
                                               _vector_word, 
                                               _topk,
                                               _efsearch,
                                               _left_records, 
                                               _vector_filter_conjuncts,
                                               _scan_conjuncts,
                                               _pre_filter_conjuncts);
        if (ret < 0) {
            DB_FATAL("vector_index fail search, index:%ld, table:%ld", _index_info->id, _table_info->id);
            return -1;
        }
        if (!_vector_filter_conjuncts.empty()) {
            if (index_condition_pullup() != 0) {
                DB_WARNING("Fail to put conjuncts back to parent");
                return -1;
            }
        }
        DB_WARNING("vector_index search, index:%ld, table:%ld, size:%lu", _index_info->id, _table_info->id, _left_records.size());
    } else if (_reverse_infos.size() > 1) {
        //TODO 为不影响原流程暂时保留，后续删除。
        for (auto& info : _reverse_infos) {
            if (reverse_index_map.count(info.id) == 1) {
                _reverse_indexes.emplace_back(reverse_index_map[info.id]);
            } else {
                DB_WARNING_STATE(state, "index:%ld is not FULLTEXT", info.id);
                return -1;
            }
        }

        if (_new_fulltext_tree) {
            if (_factory->get_index_storage_type(_index_id, _storage_type) == -1) {
                DB_FATAL("get index storage type error.");
                return -1;
            }

            if (_storage_type == pb::ST_PROTOBUF_OR_FORMAT1 || _storage_type == pb::ST_ARROW) {
                _m_index.search(txn->get_txn(), _pri_info, _table_info,
                    reverse_index_map, !FLAGS_reverse_seek_first_level, 
                    _pb_node.derive_node().scan_node().fulltext_index());
                //DB_FATAL("fulltext: %s", _pb_node.derive_node().scan_node().fulltext_index().DebugString().c_str());
            } else {
                DB_FATAL("fulltext storage type error");
                return -1;
            }
        } else {
            // 应该不会再走这了
            // 为了性能,多索引倒排查找不seek
            if (_factory->get_index_storage_type(_index_id, _storage_type) == -1) {
                DB_FATAL("get index storage type error.");
                return -1;
            }

            if (_storage_type == pb::ST_PROTOBUF_OR_FORMAT1 || _storage_type == pb::ST_ARROW) {
                _m_index.search(txn->get_txn(), _pri_info, _table_info, 
                    _reverse_indexes, _query_words, _match_modes, !FLAGS_reverse_seek_first_level, !_bool_and);
            } else {
                DB_FATAL("fulltext storage type error");
                return -1;
            }
        }
        
    } else if (_reverse_infos.size() ==1 && reverse_index_map.count(_index_id) == 1) {
        //倒排索引不允许是多字段
        if (_index_info->fields.size() != 1) {
            DB_WARNING_STATE(state, "indexinfo get fail, index_id:%ld", _index_id);
            return -1;
        }
        _reverse_index = reverse_index_map[_index_id];
        //DB_NOTICE("word:%s", str_to_hex(word).c_str());
        // seek性能太差了，倒排索引都不做seek
        ret = _reverse_index->search(txn->get_txn(), _pri_info, _table_info, 
                _query_words[0], _match_modes[0], _scan_conjuncts, !FLAGS_reverse_seek_first_level);
        if (ret < 0) {
            return ret;
        }
    }

    if (!_use_get && _table_info->engine == pb::ROCKSDB_CSTORE && _index_id == _table_id) {
        std::unordered_set<int32_t> filt_field_ids;
        for (auto& expr : _scan_conjuncts) {
            expr->get_all_field_ids(filt_field_ids);
        }
        for (auto& iter : _field_ids) {
            if (filt_field_ids.count(iter.first)) {
                _filt_field_ids.emplace_back(iter.first);
            } else {
                _trivial_field_ids.emplace_back(iter.first);
            }
        }
    }
    return 0;
}

int RocksdbScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {  
    if (_is_explain) {
        // 生成一条临时数据跑通所有流程
        std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
        for (auto slot : _tuple_desc->slots()) {
            ExprValue tmp(pb::INT64);
            row->set_value(slot.tuple_id(), slot.slot_id(), tmp);
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
        *eos = true;
        return 0;
    }
    if (state->is_timeout()) {
        DB_WARNING_STATE(state, "sql exec reach timeout");
        return -1;
    }
    if (state->execute_type == pb::EXEC_ARROW_ACERO 
            && batch->use_memrow() 
            && _arrow_vectorized_reader != nullptr 
            && _scan_rows >= ROW_BATCH_CAPACITY) {
        // 扫描量超过1024才转列执行
        set_node_exec_type(pb::EXEC_ARROW_ACERO);
        return 0;
    }
    ON_SCOPE_EXIT(([this, state]() {
        state->set_num_scan_rows(_scan_rows);
        state->set_read_disk_size(_read_disk_size);
    }));

    // 检查是否需要拒绝
    if (StoreQos::get_instance()->need_reject()) {
        return -1;
    }
    if (_is_get_keypoint) {
        return get_key_points(state, batch, eos);
    } 
    
    int ret = 0;
    if (_index_id == _table_id || _index_info->type == pb::I_ROLLUP) {
        if (_use_get) {
            ret = get_next_by_table_get(state, batch, eos);
        } else {
            ret = get_next_by_table_seek(state, batch, eos);
        }
    } else {
        if (_use_get) {
            ret = get_next_by_index_get(state, batch, eos);
        } else {
            ret = get_next_by_index_seek(state, batch, eos);
        }
    }
    // 更新qos统计信息
    StoreQos::get_instance()->update_statistics();
    if (ret < 0) {
        return ret;
    }
    if (state->execute_type == pb::EXEC_ROW 
            && 0 != state->memory_limit_exceeded(_scan_rows, batch->used_bytes_size())) {
        // [ARROW TODO] 列存不感知上面filter和agg, 先不支持memory_limit
        return -1;
    }
    if (state->execute_type == pb::EXEC_ARROW_ACERO && batch->use_memrow()) {
        // [ARROW TODO] filter+limit ; 第一个batch的eos=false, 可能会在这里多转一次列存但是不使用
        if (*eos == false || state->force_vectorize) {
            // 行数据转列recordbatch
            if (_arrow_vectorized_reader == nullptr) {
                _arrow_vectorized_reader = std::make_shared<RocksdbVectorizedReader>();
                ret = _arrow_vectorized_reader->init(this, state);
                if (ret != 0) {
                    return -1;
                }
            }
            ret = _arrow_vectorized_reader->add_first_row_batch(batch);
            if (ret != 0) {
                return -1;
            }
        }
        if (state->force_vectorize) {
            set_node_exec_type(pb::EXEC_ARROW_ACERO);
        }
    }
    return 0;
}

void RocksdbScanNode::close(RuntimeState* state) {
    ScanNode::close(state);
    for (auto expr : _scan_conjuncts) {
        expr->close();
    }
    for (auto expr : _vector_filter_conjuncts) {
        expr->close();
    }
    for (auto expr : _pre_filter_conjuncts) {
        expr->close();
    }
    _idx = 0;
    _topk = 10;
    _reverse_infos.clear();
    _query_words.clear();
    _match_modes.clear();
    _reverse_indexes.clear();
    _vector_index = nullptr;
    _vector_word.clear();
    _filter_chunk.reset();
    _arrow_vectorized_reader.reset();
    _separate_value = 0;
}

int64_t RocksdbScanNode::copy_multiget_rows(RowBatch* output_batch, std::vector<ExprNode*>* conjuncts) {
    int64_t index_filter_cnt = 0;
    if (output_batch->use_memrow()) {
        while (!_multiget_row_batch.is_traverse_over()) {
            if (output_batch->is_full()) {
                return index_filter_cnt;
            }
            if (reached_limit()) {
                return index_filter_cnt;
            }
            std::unique_ptr<MemRow>& row = _multiget_row_batch.get_row();
            bool do_copy = true;
            if (conjuncts != nullptr) {
                do_copy = need_copy(row.get(), *conjuncts);
            }
            if (do_copy) {
                output_batch->move_row(std::move(row));
                ++_num_rows_returned;
                ++_num_rows_returned_by_range;
            } else {
                ++index_filter_cnt;
            }
            _multiget_row_batch.next();
        }
    } else {
        _multiget_row_batch.add_row_batch_to_chunk({get_tuple()}, output_batch->get_chunk());
    }
    _multiget_row_batch.clear();
    return index_filter_cnt;
}

int RocksdbScanNode::get_next_by_table_get(RuntimeState* state, RowBatch* batch, bool* eos) {
    int64_t index_filter_cnt = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this, &index_filter_cnt](TraceLocalNode& local_node) {
        local_node.set_scan_rows(_scan_rows);
        local_node.add_index_filter_rows(index_filter_cnt);
    }));
    auto txn = state->txn();
    SmartRecord record = _factory->new_record(_table_id);
    bool use_mem_row = batch->use_memrow();
    RowBatch* multiget_row_batch = &_multiget_row_batch;
    if (!use_mem_row) {
        multiget_row_batch = batch;
    }
    while (1) {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            *eos = true;
            return 0;
        }
        int filter_cnt = copy_multiget_rows(batch, &_scan_conjuncts);
        index_filter_cnt += filter_cnt;
        state->inc_num_filter_rows(filter_cnt);
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }

        if (batch->chunk_exceed_max_size()) {
            return 0;
        }
        if (_scan_range_keys.is_traverse_over()) {

            *eos = true;
            return 0;
        }
        if (_get_mode != GET_ONLY) {
            ++_scan_rows;
            auto key_pair = _scan_range_keys.get_next();
            int ret = txn->get_update_primary(_region_id, *_pri_info, key_pair->left_key(), record,
                        _field_ids, _get_mode, state->need_check_region());
            if (ret < 0) {
                continue;
            }
            _read_disk_size += txn->read_disk_size;
            if (use_mem_row) {
                std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                for (auto slot : _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    row->set_value(slot.tuple_id(), slot.slot_id(),
                            record->get_value(field));
                }
                if (!need_copy(row.get(), _scan_conjuncts)) {
                    state->inc_num_filter_rows();
                    ++index_filter_cnt;
                    continue;
                }
                batch->move_row(std::move(row));
                ++_num_rows_returned;
            } else {
                for (auto slot : _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    if (0 != batch->set_chunk_tmp_row_value(slot.tuple_id(), slot.slot_id(), record->get_value(field))) {
                        DB_FATAL_STATE(state, "add chunk row fail");
                        return -1;
                    }
                }
                if (0 != batch->add_chunk_row()) {
                    DB_FATAL_STATE(state, "add chunk row fail");
                    return -1;
                }
            }
        } else {
            auto key_pairs = _scan_range_keys.get_next_batch();
            _scan_rows += key_pairs.size();
            int ret = txn->multiget_primary(_region_id, *_pri_info, key_pairs, _tuple_id, _mem_row_desc, multiget_row_batch,
                                _field_ids, _field_slot, state->need_check_region(), _range_key_sorted);
            _read_disk_size += txn->read_disk_size;
            if (ret < 0) {
                continue;
            }
        }
    }
    return 0;
}

int RocksdbScanNode::get_next_by_index_get(RuntimeState* state, RowBatch* batch, bool* eos) {
    int64_t get_primary_cnt = 0;
    int64_t index_filter_cnt = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, 
    ([this, &get_primary_cnt, &index_filter_cnt](TraceLocalNode& local_node) {
        local_node.add_get_primary_rows(get_primary_cnt);
        local_node.set_scan_rows(_scan_rows);
        local_node.add_index_filter_rows(index_filter_cnt);
    }));
    bool use_mem_row = batch->use_memrow();
    RowBatch* multiget_row_batch = &_multiget_row_batch;
    if (!use_mem_row) {
        multiget_row_batch = batch;
    }
    auto txn = state->txn();
    SmartRecord record = _factory->new_record(_table_id);
    while (1) {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            *eos = true;
            return 0;
        }
        int filter_cnt = copy_multiget_rows(batch, &_scan_conjuncts);
        index_filter_cnt += filter_cnt;
        state->inc_num_filter_rows(filter_cnt);
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }

        if (batch->chunk_exceed_max_size()) {
            return 0;
        }
        if (_scan_range_keys.is_traverse_over()) {
            *eos = true;
            return 0;
        }
        if (_get_mode != GET_ONLY) {
            ++_scan_rows;
            auto key_pair = _scan_range_keys.get_next();
            int ret = txn->get_update_secondary(_region_id, *_pri_info, *_index_info, key_pair->left_key(), record,
                        GET_ONLY, true);
            if (ret < 0) {
                continue;
            }
            _read_disk_size += txn->read_disk_size;
            if (_index_info->type == pb::I_UNIQ) {
                record->decode_key(*_index_info, key_pair->left_key().data());
            }
            if (!_is_covering_index && !_is_global_index) {
                ++get_primary_cnt;
                int ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, _get_mode, false);
                if (ret < 0) {
                    DB_FATAL("get primary:%ld fail, not exist, ret:%d, record: %s", 
                            _table_id, ret, record->to_string().c_str());
                    continue;
                }
                _read_disk_size += txn->read_disk_size;
            }
            if (use_mem_row) {
                std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                for (auto slot : _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    row->set_value(slot.tuple_id(), slot.slot_id(),
                            record->get_value(field));
                }
                if (!need_copy(row.get(), _scan_conjuncts)) {
                    state->inc_num_filter_rows();
                    ++index_filter_cnt;
                    continue;
                }
                batch->move_row(std::move(row));
                ++_num_rows_returned;
            } else {
                for (auto slot : _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    if (0 != batch->set_chunk_tmp_row_value(slot.tuple_id(), slot.slot_id(), record->get_value(field))) {
                        DB_FATAL_STATE(state, "add chunk row fail");
                        return -1;
                    }
                }
                if (0 != batch->add_chunk_row()) {
                    DB_FATAL_STATE(state, "add chunk row fail");
                    return -1;
                }
            }
        } else {
            auto key_pairs = _scan_range_keys.get_next_batch();
            _scan_rows += key_pairs.size();
            int ret = txn->multiget_secondary(_region_id, *_pri_info, *_index_info, key_pairs, record, _multiget_records,
                                    _tuple_id, _mem_row_desc, multiget_row_batch, _field_slot,
                                    !_is_covering_index && !_is_global_index, state->need_check_region(), _range_key_sorted);
            if (ret < 0) {
                DB_FATAL("get secondary:%ld fail, not exist, ret:%d, record: %s",
                            _table_id, ret, record->to_string().c_str());
                continue;
            }
            _read_disk_size += txn->read_disk_size;
            if (_multiget_records.size() == 0) {
                continue;
            }
            if (!_is_covering_index && !_is_global_index) {
                int ret = txn->multiget_primary(_region_id, *_pri_info, _tuple_id, _mem_row_desc,
                                 multiget_row_batch, _multiget_records, _field_ids, _field_slot, false);
                if (ret < 0) {
                    DB_FATAL("get primary:%ld fail, not exist, ret:%d, record: %s",
                            _table_id, ret, record->to_string().c_str());
                    continue;
                }
                _read_disk_size += txn->read_disk_size;
            }
        }
    }
}
    
int RocksdbScanNode::lock_primary(RuntimeState* state, MemRow* row) {
    SmartRecord record = TableRecord::new_record(_table_id);
    for (auto& field : _pri_info->fields) {
        int32_t field_id = field.id;
        int32_t slot_id = _field_slot[field_id];
        record->set_value(record->get_field_by_idx(field.pb_idx), row->get_value(_tuple_id, slot_id));
    }
    int64_t ttl_duration = 0;
    if (state->txn() == nullptr) {
        return -1;
    }
    int ret = state->txn()->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true, ttl_duration);
    if (ret == -3 || ret == -2 || ret == -4) {
        DB_DEBUG("DDL_LOG key is deleted, skip. error[%d]", ret);
        return 0;
    }
    if (ret != 0) {
        DB_FATAL("lock key error.");
        return -1;
    }
    for (auto& pair: _field_ids) {
        int32_t field_id = pair.first;
        int32_t slot_id = _field_slot[field_id];
        row->set_value(_tuple_id, slot_id, record->get_value(record->get_field_by_idx(pair.second->pb_idx)));
    }
    if (ttl_duration > 0) {
        state->ttl_timestamp_vec.emplace_back(ttl_duration);
    }
    return 0;
}

int RocksdbScanNode::column_ddl_work(RuntimeState* state, MemRow* row) {
    SmartRecord record = TableRecord::new_record(_table_id);
    for (auto& field : _pri_info->fields) {
        int32_t field_id = field.id;
        int32_t slot_id = _field_slot[field_id];
        record->set_value(record->get_field_by_idx(field.pb_idx), row->get_value(_tuple_id, slot_id));
    }
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_FATAL("txn is nullptr");
        return -1;
    }
    int64_t ttl_duration = 0;
    int ret = txn->get_update_primary(_region_id, *_pri_info, record, _ddl_field_ids, GET_LOCK, true, ttl_duration);
    if (ret == -3 || ret == -2 || ret == -4) {
        DB_DEBUG("DDL_LOG key is deleted, skip. error[%d]", ret);
        return 0;
    }
    if (ret != 0) {
        DB_FATAL("lock key error.");
        return -1;
    }
    txn->set_write_ttl_timestamp_us(ttl_duration);
    for (size_t i = 0; i < _update_exprs.size(); i++) {
        auto& slot = _update_slots[i];
        auto expr = _update_exprs[i];
        record->set_value(record->get_field_by_tag(slot.field_id()),
                expr->get_value(row).cast_to(slot.slot_type()));
    }

    ret = txn->put_primary(_region_id, *_pri_info, record, nullptr);
    if (ret < 0) {
        DB_WARNING_STATE(state, "put table:%ld fail:%d", _table_id, ret);
        return -1;
    }
    return 0;
}


int RocksdbScanNode::index_ddl_work(RuntimeState* state, MemRow* row) {
    std::set<int32_t> pri_field_ids;
    SmartRecord record = TableRecord::new_record(_table_id);
    for (auto& field : _pri_info->fields) {
        int32_t field_id = field.id;
        int32_t slot_id = _field_slot[field_id];
        pri_field_ids.insert(field_id);
        record->set_value(record->get_field_by_idx(field.pb_idx), row->get_value(_tuple_id, slot_id));
    }
    if (_ddl_index_info->type == pb::I_ROLLUP) {
        for (auto& field_info : _table_info->fields) {
            if (pri_field_ids.count(field_info.id) == 0) {
                _field_ids[field_info.id] = &field_info;
            }
        }
    }
    
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_FATAL("txn is nullptr");
        return -1;
    }
    int64_t ttl_duration = 0;
    int ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_LOCK, true, ttl_duration);
    if (ret == -3 || ret == -2 || ret == -4) {
        DB_DEBUG("DDL_LOG key is deleted, skip. error[%d]", ret);
        return 0;
    }
    if (ret != 0) {
        DB_FATAL("lock key error.");
        return -1;
    }

    txn->set_write_ttl_timestamp_us(ttl_duration);
    if (_ddl_index_info->type == pb::I_FULLTEXT) {
        auto& reverse_index_map = state->reverse_index_map();
        if (reverse_index_map.count(_ddl_index_info->id) == 0) {
            DB_FATAL("DDL_LOG fulltext ddl info not found index_id:%ld.", _ddl_index_info->id);
            return -1;
        }
        MutTableKey pk_key;
        ret = record->encode_key(*_pri_info, pk_key, -1, false, false);
        if (ret < 0) {
            DB_WARNING("DDL_LOG record [%s] encode key failed[%d].", record->to_string().c_str(), ret);
            return -1;
        }
        std::string new_pk_str = pk_key.data();

        auto field = record->get_field_by_idx(_ddl_index_info->fields[0].pb_idx);
        if (record->is_null(field)) {
            DB_DEBUG("DDL_LOG record [%s] record field is_null.", record->to_string().c_str());
            return 0;
        }
        std::string word;
        ret = record->get_reverse_word(*_ddl_index_info, word);
        if (ret < 0) {
            DB_WARNING("DDL_LOG record [%s] get_reverse_word failed[%d], index_id: %ld.", 
                record->to_string().c_str(), ret, _ddl_index_info->id);
            return -1;
        }

        DB_DEBUG("reverse debug, record[%s]", record->to_string().c_str());
        ret = reverse_index_map[_ddl_index_info->id]->insert_reverse(txn, word, new_pk_str, record);
        if (ret < 0) {
            DB_WARNING("DDL_LOG record [%s] insert_reverse failed[%d], index_id: %ld.", 
                record->to_string().c_str(), ret, _ddl_index_info->id);
            return -1;
        }
        return 0;
    }
    if (_ddl_index_info->type != pb::I_ROLLUP) {
        for (auto& pair: _field_ids) {
            int32_t field_id = pair.first;
            int32_t slot_id = _field_slot[field_id];
            row->set_value(_tuple_id, slot_id, record->get_value(record->get_field_by_idx(pair.second->pb_idx)));
        }
        SmartRecord exist_record = record->clone();
        ret = txn->get_update_secondary(_region_id, *_pri_info, *_ddl_index_info, exist_record, GET_LOCK, true);
        if (ret == 0) {
            MutTableKey key;
            MutTableKey exist_key;
            if (record->encode_key(*_pri_info, key, -1, false, false) == 0 && 
                exist_record->encode_key(*_pri_info, exist_key, -1, false, false) == 0) {

                if (key.data().compare(exist_key.data()) == 0) {
                    DB_NOTICE("same pk val.");
                    return 0;
                } else if (_ddl_index_info->type == pb::I_UNIQ) {
                    DB_WARNING("not same pk value record %s exist_record %s.", record->to_string().c_str(), 
                        exist_record->to_string().c_str());
                    state->error_code = ER_DUP_ENTRY;
                    state->error_msg << "Duplicate entry: '" << 
                            record->get_index_value(*_ddl_index_info) << "' for key '" << _ddl_index_info->short_name << "'";
                    return -1;
                }
            } else {
                DB_FATAL("encode key error record %s exist_record %s.", record->to_string().c_str(), 
                    exist_record->to_string().c_str());
                state->error_code = ER_DUP_ENTRY;
                state->error_msg << "Duplicate entry: '" << 
                        record->get_index_value(*_ddl_index_info) << "' for key '" << _ddl_index_info->short_name << "'";
                return -1;
            }
        }
        // ret == -3 means the primary_key returned by get_update_secondary is out of the region
        // (dirty data), this does not affect the insertion
        if (ret != -2 && ret != -3 && ret != -4) {
            DB_WARNING_STATE(state, "insert rocksdb failed, index:%ld, ret:%d", _ddl_index_info->id, ret);
            return -1;
        }
    } else {
        txn->set_leader_merge_in_raft(FLAGS_leader_merge_in_raft);

    } 
    ret = txn->put_secondary(_region_id, *_ddl_index_info, record, _ddl_index_info->type == pb::I_ROLLUP);
    if (ret < 0) {
        DB_WARNING_STATE(state, "put index:%ld fail:%d, table_id:%ld", _ddl_index_info->id, ret, _table_id);
        return ret;
    }
    //DB_WARNING_STATE(state,"put index record:%s", record->debug_string().c_str());
    return 0;
}

int RocksdbScanNode::process_ddl_work(RuntimeState* state, MemRow* row) {
    switch (_ddl_work_type) {
        case pb::DDL_LOCAL_INDEX: {
            // 加局部索引
            if (index_ddl_work(state, row) != 0) {
                return -1;
            }
            break;
        }
        case pb::DDL_GLOBAL_INDEX: {
            // 加全局二级索引
            if (lock_primary(state, row) != 0) {
                return -1;
            }
            break;
        }
        case pb::DDL_COLUMN: {
            if (column_ddl_work(state, row) != 0) {
                return -1;
            }
            break;
        }
        default:
            break;
    }
    return 0;
}

int RocksdbScanNode::get_next_by_table_seek(RuntimeState* state, RowBatch* batch, bool* eos) {
    int64_t index_filter_cnt = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this, &index_filter_cnt](TraceLocalNode& local_node) {
        local_node.add_index_filter_rows(index_filter_cnt);
        local_node.set_scan_rows(_scan_rows);
    }));
    state->ttl_timestamp_vec.clear();
    state->txn()->set_watt_stats_version(_watt_stats_version);
    while (1) {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            *eos = true;
            return 0;
        }
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }
        if (batch->chunk_exceed_max_size()) {
            return 0;
        }
        if (!batch->use_memrow() 
                && _sort_use_index_by_range && _sort_limit_by_range != -1  
                && _index_iter != nullptr && !_index_iter->valid()
                && batch->size() > 0) {
            // 不同range的数据必须拆开来
            return 0;
        }
        if (_table_iter == nullptr || !_table_iter->valid() || range_reach_limit()) {
            if (_scan_range_keys.is_traverse_over()) {
                *eos = true;
                return 0;
            } else {
                IndexRange range;
                auto key_pair = _scan_range_keys.get_next();
                range = IndexRange(key_pair->left_key(),
                        key_pair->right_key(),
                        _index_info.get(),
                        _pri_info.get(),
                        _region_info,
                        _left_field_cnt, 
                        _right_field_cnt, 
                        _left_open, 
                        _right_open,
                        _like_prefix);
                delete _table_iter;
                _table_iter = Iterator::scan_primary(
                        state->txn(), range, _field_ids, _field_slot, state->need_check_region(), _scan_forward);
                if (_table_iter == nullptr) {
                    DB_WARNING_STATE(state, "open TableIterator fail, table_id:%ld", _index_id);
                    return -1;
                }
                if (_is_covering_index) {
                    _table_iter->set_mode(KEY_ONLY);
                }
                _num_rows_returned_by_range = 0;
                _idx++;
                ++_scan_rows;
                continue;
            }
        }
        if (!_table_iter->is_cstore()) {
            ++_scan_rows;
            std::unique_ptr<MemRow> row;
            int ret = 0;
            if (batch->use_memrow()) {
                row = _mem_row_desc->fetch_mem_row();
                ret = _table_iter->get_next(_tuple_id, row);
            } else {
                ret = _table_iter->get_next_for_chunk(_tuple_id, batch->get_chunk());
            }
            if (ret < 0) {
                continue;
            }
            _read_disk_size += _table_iter->last_read_disk_size;
            if (row != nullptr) {
                if (_lock != pb::LOCK_GET) {
                    if (_lock == pb::LOCK_GET_ONLY_PRIMARY) {
                        // select ... for update
                        if (lock_primary(state, row.get()) != 0) {
                            return -1;
                        }
                    }
                    if (!need_copy(row.get(), _scan_conjuncts)) {
                        state->inc_num_filter_rows();
                        ++index_filter_cnt;
                        continue;
                    }
                } else if (need_copy(row.get(), _scan_conjuncts)) {
                    if (_is_ddl_work) {
                        // 加局部索引
                        if (index_ddl_work(state, row.get()) != 0) {
                            return -1;
                        }
                    } else if (_ddl_work_type == pb::DDL_NONE) {
                        // 加全局二级索引
                        if (lock_primary(state, row.get()) != 0) {
                            return -1;
                        }
                    } else {
                        if (process_ddl_work(state, row.get()) !=0) {
                            return -1;
                        }
                    }
                }
                if (_lock == pb::LOCK_GET &&
                    (_ddl_work_type == pb::DDL_COLUMN || _ddl_work_type == pb::DDL_LOCAL_INDEX)) {
                        // local index or column返回最大一条数据
                        batch->replace_row(std::move(row), 0);
                } else {
                    batch->move_row(std::move(row));
                }
                ++_num_rows_returned;
                ++_num_rows_returned_by_range;
            } else {
                ret = batch->add_chunk_row();
                if (ret != 0) {
                    DB_FATAL_STATE(state, "add chunk row fail");
                    return -1;
                }
            }
        } else {
            // scan primary
            RowBatch row_batch;
            std::shared_ptr<FiltBitSet> filter;
            if (_scan_conjuncts.size() > 0) {
                filter.reset(new FiltBitSet());
            }
            _table_iter->reset_primary_keys();
            int32_t num = 0;
            while (_table_iter->valid()) {
                if (_limit != -1 && _num_rows_returned + num >= _limit) {
                    break;
                }
                if (row_batch.size() + num >= row_batch.capacity()) {
                    break;
                }
                std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
                std::string key;
                int ret = _table_iter->get_next(_tuple_id, row);
                if (ret < 0) {
                    break;
                }
                _read_disk_size += _table_iter->last_read_disk_size;
                row_batch.move_row(std::move(row));
                ++num;
            }
            // scan filt column
            for (auto& field_id : _filt_field_ids) {
                FieldInfo* field_info = _field_ids[field_id];
                _table_iter->get_column(_tuple_id, *field_info, nullptr, &row_batch);
                _read_disk_size += _table_iter->last_read_disk_size;
            }
            // filt
            if (filter != nullptr) {
                for (row_batch.reset(); !row_batch.is_traverse_over(); row_batch.next()) {
                    std::unique_ptr<MemRow>& row = row_batch.get_row();
                    if (!need_copy(row.get(), _scan_conjuncts)) {
                        filter->set(row_batch.index());
                    }
                }
            }
            // scan trivial column
            for (auto& field_id : _trivial_field_ids) {
                FieldInfo* field_info = _field_ids[field_id];
                _table_iter->get_column(_tuple_id, *field_info, filter.get(), &row_batch);
                _read_disk_size += _table_iter->last_read_disk_size;
            }

            // move to row batch
            for (row_batch.reset(); !row_batch.is_traverse_over(); row_batch.next()) {
                ++_scan_rows;
                std::unique_ptr<MemRow>& row = row_batch.get_row();
                if (_lock != pb::LOCK_GET) {
                    if (filter != nullptr && filter->test(row_batch.index())) {
                        state->inc_num_filter_rows();
                        ++index_filter_cnt;
                        continue;
                    }
                } else {
                    if (_is_ddl_work) {
                        // 加局部索引
                        if (index_ddl_work(state, row.get()) != 0) {
                            return -1;
                        }
                    } else if (_ddl_work_type == pb::DDL_NONE) {
                        // 加全局二级索引
                        if (lock_primary(state, row.get()) != 0) {
                            return -1;
                        }
                    } else if (filter == nullptr || filter != nullptr && !filter->test(row_batch.index())) {
                        if (process_ddl_work(state, row.get()) !=0) {
                            return -1;
                        }
                    }
                }
                if (_lock == pb::LOCK_GET &&
                    (_ddl_work_type == pb::DDL_COLUMN || _ddl_work_type == pb::DDL_LOCAL_INDEX)) {
                        // local index or column返回最大一条数据
                        batch->replace_row(std::move(row), 0);
                } else {
                    batch->move_row(std::move(row));
                }
                ++_num_rows_returned;
                ++_num_rows_returned_by_range;
            }
        }
    }
}

int RocksdbScanNode::vectorize_filter(RuntimeState* state, std::shared_ptr<Chunk> chunk) {
    // 先做一次filter, 再反查主表
    int filter_cnt = 0;
    if (_scan_conjuncts.empty() || !_vectorized_filtered || chunk ==  nullptr || chunk->size() == 0) {
        return 0;
    }
    std::shared_ptr<arrow::RecordBatch> record_batch;
    int ret = chunk->finish_and_make_record_batch(&record_batch);
    if (ret < 0) {
        DB_FATAL_STATE(state, "chunk finish and make record batch fail");
        return -1;
    }
    arrow::ExecBatch exec_batch(*record_batch);
    arrow::Result<arrow::Datum> filter_result = arrow::compute::ExecuteScalarExpression(_arrow_scan_conjuncts, exec_batch, /*ExecContext* = */nullptr);
    if (!filter_result.ok()) {
        DB_FATAL("filter fail, %s", filter_result.status().ToString().c_str());
        return -1;
    }
    if (filter_result->is_scalar()) {
        const auto& mask_scalar = filter_result->scalar_as<arrow::BooleanScalar>();
        if (mask_scalar.is_valid && mask_scalar.value == true) {
            _multiget_records.insert(_multiget_filter_records);
        } else {
            filter_cnt = _multiget_filter_records.size();
            state->inc_num_filter_rows(_multiget_filter_records.size());
        }
        _multiget_filter_records.clear();
        return filter_cnt;
    } 
    int i = 0;
    auto boolean_arr = filter_result->array_as<arrow::BooleanArray>();
    while (!_multiget_filter_records.is_traverse_over()) {
        auto record = _multiget_filter_records.get_next();
        if (boolean_arr->Value(i++) == false) {
            ++filter_cnt;
            continue;
        }
        _multiget_records.emplace_back(record);
    }
    state->inc_num_filter_rows(filter_cnt);
    _multiget_filter_records.clear();
    _arrow_filter_batch_size = std::min(_arrow_filter_batch_size * 2, 1024);
    return filter_cnt;
}

int RocksdbScanNode::get_next_by_index_seek(RuntimeState* state, RowBatch* batch, bool* eos) {
    int64_t index_filter_cnt = 0;
    int64_t get_primary_cnt = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this, &index_filter_cnt, &get_primary_cnt]
        (TraceLocalNode& local_node) {
        local_node.add_index_filter_rows(index_filter_cnt);
        local_node.add_get_primary_rows(get_primary_cnt);
        local_node.set_scan_rows(_scan_rows);
    }));
    // 是否需要反查主表
    bool need_multiget_primary = true;
    if (_is_covering_index || _is_global_index) {
        need_multiget_primary = false;
    }

    // 只普通索引扫描并且不会反查主表的省略record
    bool use_record = false;
    if (need_multiget_primary ||
            !_reverse_indexes.empty() || _reverse_index != nullptr || _vector_index != nullptr) {
        use_record = true;
    }
    int ret = 0;
    SmartRecord record = _factory->new_record(_table_id);
    _multiget_records.set_capacity(batch->capacity());
    bool multiget_last_records = false;
    auto txn = state->txn();
    bool use_chunk = !(batch->use_memrow());
    RowBatch* multiget_row_batch = nullptr;
    std::shared_ptr<Chunk> index_data_chunk;

    if (!use_chunk) {
        multiget_row_batch = &_multiget_row_batch;
    } else {
        if (_filter_chunk == nullptr) {
            _filter_chunk = std::make_shared<Chunk>();
            _filter_chunk->init({get_tuple()});
        }
        multiget_row_batch = batch;
        if (need_multiget_primary) {
            // 要反查主表, 通过filter_chunk列式filter过滤后, 再反查主表
            index_data_chunk = _filter_chunk;
            _vectorized_filtered = true;
        } else {
            index_data_chunk = batch->get_chunk();
        }
    }
    while (1) {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            *eos = true;
            return 0;
        }
        int filter_cnt = copy_multiget_rows(batch, nullptr);
        index_filter_cnt += filter_cnt;
        state->inc_num_filter_rows(filter_cnt);
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }
        if (batch->chunk_exceed_max_size()) {
            return 0;
        }
        if (multiget_last_records) {
            if (need_multiget_primary) {
                filter_cnt = vectorize_filter(state, index_data_chunk);
                if (filter_cnt < 0) {
                    return -1;
                }
                index_filter_cnt += filter_cnt;
                if (_multiget_records.size() > 0) {
                    get_primary_cnt += _multiget_records.size();
                    int ret = txn->multiget_primary(_region_id, *_pri_info, _tuple_id, _mem_row_desc,
                                    multiget_row_batch, _multiget_records, _field_ids, _field_slot, false);
                    if (ret < 0) {
                        DB_FATAL("get primary:%ld fail, not exist, ret:%d, record: %s",
                                _table_id, ret, record->to_string().c_str());
                    }
                    _read_disk_size += txn->read_disk_size;
                    continue;
                }
            }
            *eos = true;
            return 0;
        }
        if (_vector_index != nullptr) {
            if (_idx >= _left_records.size()) {
                multiget_last_records = true;
                continue;
            }
        } else if (_reverse_indexes.size() > 0) {
            if (!multi_valid(_storage_type)) {
                multiget_last_records = true;
                continue;
            }
        } else if (_reverse_index != nullptr) {
            if (!_reverse_index->valid()) {
                multiget_last_records = true;
                continue;
            }
        } else {
            if (use_chunk 
                    && _sort_use_index_by_range && _sort_limit_by_range != -1  
                    && _index_iter != nullptr && !_index_iter->valid()
                    && batch->size() > 0) {
                // 不同range的数据必须拆开
                return 0;
            }
            if (_index_iter == nullptr || !_index_iter->valid() || range_reach_limit()) {
                if (_scan_range_keys.is_traverse_over()) {
                    multiget_last_records = true;
                    continue;
                } else {
                    IndexRange range;
                    auto key_pair = _scan_range_keys.get_next();
                    range = IndexRange(key_pair->left_key(),
                            key_pair->right_key(),
                            _index_info.get(),
                            _pri_info.get(),
                            _region_info,
                            _left_field_cnt, 
                            _right_field_cnt, 
                            _left_open, 
                            _right_open,
                            _like_prefix);
                    delete _index_iter;
                    _index_iter = Iterator::scan_secondary(state->txn(), range, _field_slot, true, _scan_forward);
                    if (_index_iter == nullptr) {
                        DB_WARNING_STATE(state, "open IndexIterator fail, index_id:%ld", _index_id);
                        return -1;
                    }
                    _num_rows_returned_by_range = 0;
                    _idx++;
                    ++_scan_rows;
                    continue;
                }
            }
        }
        //TimeCost cost;
        ++_scan_rows;
        if (use_record) {
            record->clear();
        } 
        std::unique_ptr<MemRow> row = nullptr;
        if (!use_chunk) {
            row = _mem_row_desc->fetch_mem_row();
        } 
        if (_vector_index != nullptr) {
            record = _left_records[_idx++];
        } else if (_reverse_indexes.size() > 0) {
            ret = multi_get_next(_storage_type, record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "get index fail, maybe reach end");
                continue;
            }
        } else if (_reverse_index != nullptr) {
            ret = _reverse_index->get_next(record);
            if (ret < 0) {
                DB_WARNING_STATE(state, "get index fail, maybe reach end");
                continue;
            }
        } else {
            if (use_record) {
                // 要反查主表
                ret = _index_iter->get_next(record);
            } else if (!use_chunk) {
                // 索引完全覆盖, 使用memrow
                ret = _index_iter->get_next(_tuple_id, row);
            } else {
                // 索引完全覆盖, 使用chunk
                ret = _index_iter->get_next_for_chunk(_tuple_id, index_data_chunk);
            }
            //DB_DEBUG("rocksdb_scan region_%ld record[%s]", _region_id, record->to_string().c_str());
            if (ret < 0) {
                //DB_WARNING_STATE(state, "get index fail, maybe reach end");
                continue;
            }
            _read_disk_size += _index_iter->last_read_disk_size;
        }
        // 倒排索引直接下推到了布尔引擎，但是主键条件未下推，因此也需要再次过滤
        // toto: 后续可以再次优化，把userid和source的条件干掉
        // 索引谓词过滤
        if (use_record) {
            for (auto& pair : _index_slot_field_map) {
                auto field = record->get_field_by_tag(pair.second);
                if (!use_chunk) {
                    row->set_value(_tuple_id, pair.first, record->get_value(field));
                } else {
                    if (0 != index_data_chunk->set_tmp_field_value(_tuple_id, pair.first, record->get_value(field))) {
                        DB_FATAL_STATE(state, "set tmp field value fail, tuple:%d, slot:%d", _tuple_id, pair.first);
                        return -1;
                    }
                }
            }
        }
        if (row != nullptr && !need_copy(row.get(), _scan_conjuncts)) {
            state->inc_num_filter_rows();
            ++index_filter_cnt;
            continue;
        }
        if (use_record && use_chunk && _get_mode == GET_ONLY) {
            if (need_multiget_primary) {
                // 列式执行, 需要反查主表
                // 攒一批进行列式filter
                if (0 != index_data_chunk->add_tmp_row()) {
                    DB_FATAL_STATE(state, "add filter chunk row fail");
                    return -1;
                }
                if (_scan_conjuncts.empty()) {
                    _multiget_records.emplace_back(record->clone(true));
                } else {
                    _multiget_filter_records.emplace_back(record->clone(true));
                }
                if (index_data_chunk->size() < _arrow_filter_batch_size) {
                    continue;
                } 
                filter_cnt = vectorize_filter(state, index_data_chunk);
                if (filter_cnt < 0) {
                    return -1;
                }
                index_filter_cnt += filter_cnt;
            }
        }
        //DB_NOTICE("get index: %ld", cost.get_time());
        //cost.reset();
        if (_get_mode != GET_ONLY) {
            if (need_multiget_primary) {
                ++get_primary_cnt;
                // todo: 反查直接用encode_key
                ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, _get_mode, false);
                _read_disk_size += txn->read_disk_size;
                if (ret < 0) {
                    if (_reverse_indexes.size() == 0 && _reverse_index == nullptr) {
                        DB_FATAL("get primary:%ld fail, ret:%d, index primary may be not consistency: %s",
                                _table_id, ret, record->to_string().c_str());
                    }
                    continue;
                }
                //DB_NOTICE("record_after:%s", record->debug_string().c_str());
                for (auto slot : _tuple_desc->slots()) {
                    auto field = record->get_field_by_tag(slot.field_id());
                    if (!use_chunk) {
                        row->set_value(slot.tuple_id(), slot.slot_id(), record->get_value(field));
                    } else {
                        batch->set_chunk_tmp_row_value(slot.tuple_id(), slot.slot_id(), record->get_value(field));
                    }
                }
            }
            if (!use_chunk) {
                batch->move_row(std::move(row));
                ++_num_rows_returned;
                ++_num_rows_returned_by_range;
            } else {
                ret = batch->add_chunk_row();
                if (ret != 0) {
                    DB_FATAL_STATE(state, "add chunk row fail");
                    return -1;
                }
            }
        } else {
            if (need_multiget_primary) {
                if (!use_chunk) {
                    _multiget_records.emplace_back(record->clone(true));
                }
                if (_multiget_records.is_full() || will_reach_limit(_multiget_records.size())) {
                    get_primary_cnt += _multiget_records.size();
                    int ret = txn->multiget_primary(_region_id, *_pri_info, _tuple_id, _mem_row_desc,
                                 multiget_row_batch, _multiget_records, _field_ids, _field_slot, false);
                    if (ret < 0) {
                        DB_FATAL("get primary:%ld fail, not exist, ret:%d, record: %s",
                                _table_id, ret, record->to_string().c_str());
                    }
                    _read_disk_size += txn->read_disk_size;
                }

            } else {
                if (!use_chunk) {
                    batch->move_row(std::move(row));
                    ++_num_rows_returned;
                    ++_num_rows_returned_by_range;
                } else {
                    ret = batch->add_chunk_row();
                    if (ret != 0) {
                        DB_FATAL_STATE(state, "add chunk tmp row fail");
                        return -1;
                    }
                }
            }
        }
    }
    return 0;
}
void RocksdbScanNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto scan_pb = pb_node->mutable_derive_node()->mutable_scan_node();
    scan_pb->clear_use_indexes();
    scan_pb->clear_indexes();
    scan_pb->clear_learner_index();

    for (auto& scan_index_info : _scan_indexs) {
        if (_is_explain) {
            scan_pb->add_use_indexes(scan_index_info.index_id);
            scan_pb->add_indexes(scan_index_info.raw_index);
            continue;
        }

        // 记录index_id供store qos使用
        scan_pb->add_use_indexes(scan_index_info.index_id);
        if ((_current_global_backup && scan_index_info.use_for != ScanIndexInfo::U_GLOBAL_LEARNER) || 
            (!_current_global_backup && scan_index_info.use_for == ScanIndexInfo::U_GLOBAL_LEARNER)) {
            continue;
        } 

        if (scan_index_info.index_id == scan_index_info.router_index_id 
                && scan_index_info.region_primary.count(region_id) > 0) {
            if (scan_index_info.use_for == ScanIndexInfo::U_LOCAL_LEARNER) {
                scan_pb->set_learner_index(scan_index_info.region_primary[region_id]);
            } else {
                scan_pb->add_indexes(scan_index_info.region_primary[region_id]);
            }
        } else {
            if (scan_index_info.use_for == ScanIndexInfo::U_LOCAL_LEARNER) {
                scan_pb->set_learner_index(scan_index_info.raw_index);
            } else {
                scan_pb->add_indexes(scan_index_info.raw_index);
            }
        }
    }
}

int RocksdbScanNode::decode_key_points(RuntimeState* state, RowBatch* batch, const rocksdb::Slice& key) {
    SmartRecord record = _factory->new_record(_table_id);
    if (state->is_cancelled() || _pri_info == nullptr || record == nullptr) {
        return 0;
    }
    int ret = record->decode_key(*_pri_info, key);
    if (ret != 0) {
        DB_WARNING("decode primary index failed");
        return -1;
    }
    std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
    for (auto slot : _tuple_desc->slots()) {
        auto field = record->get_field_by_tag(slot.field_id());
        row->set_value(slot.tuple_id(), slot.slot_id(), record->get_value(field));
    }
    batch->move_row(std::move(row));
    return 0;
}

int RocksdbScanNode::get_key_points(RuntimeState* state, RowBatch* batch, bool* eos) {
    auto db = RocksWrapper::get_instance();
    if (db == nullptr) {
        return -1;
    }
    if (_index_id != _pri_info->id) {
        DB_FATAL("get_key_points only support using primary index");
        return -1;
    }
    std::string region_start_key = _region_info->start_key();
    std::string region_end_key = _region_info->end_key();
    MutTableKey rocksdb_start_key;
    MutTableKey rocksdb_end_key;
    rocksdb_start_key.append_i64(_region_id);
    rocksdb_start_key.append_i64(_table_id);
    rocksdb_start_key.append_string(region_start_key);
    rocksdb_end_key.append_i64(_region_id);
    rocksdb_end_key.append_i64(_table_id);
    if (region_end_key.empty()) {
        rocksdb_end_key.append_u64(UINT64_MAX);
    } else {
        rocksdb_end_key.append_string(region_end_key);
    }
    
    ON_SCOPE_EXIT(([this, state]() {
        state->set_num_scan_rows(_scan_rows);
    }));

    rocksdb::TablePropertiesCollection props;
    if (state->txn()->use_cold_db()) {
        db->get_cold_sst_properties(rocksdb_start_key.data(), rocksdb_end_key.data(), props);
    } else {
        db->get_sst_properties(rocksdb_start_key.data(), rocksdb_end_key.data(), props);
    }
    std::vector<rocksdb::Slice> keys;
    db->decode_key_points(region_start_key, region_end_key, rocksdb_start_key.data().substr(0, 16), props, keys);
    _scan_rows += keys.size();
    uint32_t num = 0;
    for (const auto& key : keys) {
        if (decode_key_points(state, batch, key) != 0) {
            continue;
        }
        ++num;
    }
    
    *eos = true;
    return num;
}

int RocksdbVectorizedReader::init(RocksdbScanNode* scan_node, RuntimeState* state) {
    if (scan_node == nullptr) {
        return -1;
    }
    _scan_node = scan_node;
    _state = state;
    
    _batch.init_chunk({scan_node->get_tuple()}, &_schema);
    if (_schema == nullptr) {
        return -1;
    }
    state->arrow_input_schemas[scan_node->tuple_id()] = _schema;
    return 0;
}

int RocksdbVectorizedReader::add_first_row_batch(RowBatch* first_batch) {
    if (first_batch->add_row_batch_to_chunk({_scan_node->get_tuple()}, _batch.get_chunk()) != 0) {
        return -1;
    }
    _first_batch_need_handle = true;
    if (first_batch->size() > 0) {
        _batch.set_capacity(_batch.size());
    }
    return 0;
}

void RocksdbVectorizedReader::set_arrow_filter_conjuncts(arrow::Expression* expr, int64_t limit) {
    _arrow_filter_conjuncts = expr;
    _filter_node_limit = limit;
}

arrow::Status RocksdbVectorizedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    int ret = 0;
    TimeCost t;
    int64_t index_filter_cnt = 0;
    int64_t where_filter_cnt = 0;
    START_LOCAL_TRACE(_scan_node->get_trace(), _state->get_trace_cost(), GET_NEXT_TRACE, ([&](TraceLocalNode& local_node) {
        local_node.add_index_filter_rows(index_filter_cnt);
        local_node.add_where_filter_rows(where_filter_cnt);
    }));
    
    while (1) {
        if (_eos) {
            out->reset();
            return arrow::Status::OK();
        }
        if (_filter_node_limit != -1 && _pass_filter_count >= _filter_node_limit) {
            out->reset();
            return arrow::Status::OK();
        }
        if (!_first_batch_need_handle) {
            _batch.set_capacity(std::min(FLAGS_chunk_size, 2 * (int32_t)_batch.capacity()));
            ret = _scan_node->get_next(_state, &_batch, &_eos);
            if (ret < 0) {
                DB_FATAL("rocksdb scan node get_next fail in vectorize mode");
                return arrow::Status::IOError("RocksdbVectorizedReader read fail");
            }
        }
        // build output arrow recordBatch
        std::shared_ptr<arrow::RecordBatch> record_batch;
        ret = _batch.finish_and_make_record_batch(&record_batch);
        if (ret < 0) {
            DB_FATAL("arrow chunk finish and make record batch fail");
            return arrow::Status::IOError("chunk finish and make record batch fail");
        }
        int64_t in_filter_cnt = record_batch->num_rows();
        if (_first_batch_need_handle) {
            _first_batch_need_handle = false;
            *out = record_batch;
        } else {
            // step1: handle scan conjuncts
            if (_arrow_scan_conjuncts != nullptr && !_scan_node->vectorized_filtered() && !_first_batch_need_handle) {
                // [ARROW TODO] ExecContext 复用
                if (0 != VectorizeHelpper::vectorize_filter(record_batch, _arrow_scan_conjuncts, out)) {
                    DB_FATAL_STATE(_state, "arrow_scan_conjuncts: vectorize filter fail");
                    return arrow::Status::IOError("vectorize filter fail");
                }
            } else {
                *out = record_batch;
            }
            int64_t out_row = (*out)->num_rows();
            _scan_node->inc_num_rows_returned(out_row);
            index_filter_cnt += in_filter_cnt - out_row;
            _state->inc_num_filter_rows(in_filter_cnt - out_row);
            if (out_row == 0) {
                continue;
            }
        }      
        
        // step2: handle filter conjuncts
        if (_arrow_filter_conjuncts != nullptr) {
            in_filter_cnt = (*out)->num_rows();
            if (0 != VectorizeHelpper::vectorize_filter(*out, _arrow_filter_conjuncts, out)) {
                DB_FATAL_STATE(_state, "arrow_filter_conjuncts: vectorize filter fail");
                return arrow::Status::IOError("vectorize filter fail");
            }
            int64_t out_row = (*out)->num_rows();
            where_filter_cnt += in_filter_cnt - out_row;
            _state->inc_num_filter_rows(in_filter_cnt - out_row);
            if (out_row == 0) {
                continue;
            }
        }
        _pass_filter_count += (*out)->num_rows();
        break;
    }
    return arrow::Status::OK();
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
