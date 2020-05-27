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
#include "parser.h"

namespace baikaldb {
// TODO 临时代码，后面删除
// 为了baikalStore能兼容老的baikaldb
int RocksdbScanNode::select_index_for_store() {
    std::multimap<uint32_t, int> prefix_ratio_id_mapping;
    std::set<int32_t> primary_fields;
    for (int i = 0; i < _pb_node.derive_node().scan_node().indexes_size(); i++) {
        auto& pos_index = _pb_node.derive_node().scan_node().indexes(i);
        int64_t index_id = pos_index.index_id();
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            continue;
        }
        IndexInfo& info = *info_ptr;
        auto index_state = info.state;
        if (index_state != pb::IS_PUBLIC) {
            DB_DEBUG("DDL_LOG index_selector skip index [%lld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }

        int field_count = 0;
        for (auto& range : pos_index.ranges()) {
            if (range.has_left_field_cnt() && range.left_field_cnt() > 0) {
                field_count = std::max(field_count, range.left_field_cnt());
            }
            if (range.has_right_field_cnt() && range.right_field_cnt() > 0) {
                field_count = std::max(field_count, range.right_field_cnt());
            }
        }
        if (info.fields.size() == 0) {
            continue;
        }
        uint16_t prefix_ratio_round = field_count * 100 / info.fields.size();
        uint16_t index_priority = 0;
        if (info.type == pb::I_PRIMARY) {
            for (int j = 0; j < field_count; j++) {
                primary_fields.insert(info.fields[j].id);
            }
            index_priority = 300;
        } else if (info.type == pb::I_UNIQ) {
            index_priority = 200;
        } else if (info.type == pb::I_KEY) {
            index_priority = 100 + field_count;
        } else {
            index_priority = 0;
        }
        // 普通索引如果都包含在主键里，则不选
        if (info.type == pb::I_UNIQ || info.type == pb::I_KEY) {
            bool contain_by_primary = true;
            for (int j = 0; j < field_count; j++) {
                if (primary_fields.count(info.fields[j].id) == 0) {
                    contain_by_primary = false;
                    break;
                }
            }
            if (contain_by_primary) {
                continue;
            }
        }
        // sort index 权重调整到全命中unique或primary索引之后
        if (pos_index.has_sort_index() && field_count > 0) {
            prefix_ratio_round = 100;
            index_priority = 190;
        }
        uint32_t prefix_ratio_index_score = (prefix_ratio_round << 16) | index_priority;
        //DB_WARNING("scan node insert prefix_ratio_index_score:%u, i: %d", prefix_ratio_index_score, i);
        prefix_ratio_id_mapping.insert(std::make_pair(prefix_ratio_index_score, i));

        // 优先选倒排，没有就取第一个
        switch (info.type) {
            case pb::I_FULLTEXT:
                _multi_reverse_index.push_back(i);
                break;
            case pb::I_RECOMMEND:
                _multi_reverse_index.push_back(i);
                return i;
            default:
                break;
        }
    }
    if (choose_arrow_pb_reverse_index(_pb_node.derive_node().scan_node()) != 0) {
        DB_WARNING("choose arrow pb reverse index error.");
        return -1;
    }
    // ratio * 10(=0...9)相同的possible index中，按照PRIMARY, UNIQUE, KEY的优先级选择
    //DB_WARNING("prefix_ratio_id_mapping.size: %d", prefix_ratio_id_mapping.size());
    for (auto iter = prefix_ratio_id_mapping.crbegin(); iter != prefix_ratio_id_mapping.crend(); ++iter) {
        //DB_WARNING("prefix_ratio_index_score:%u, i: %d", iter->first, iter->second);
        return iter->second;
    }
    return 0;
}

int RocksdbScanNode::choose_index(RuntimeState* state) {
    // 做完logical plan还没有索引
    if (_pb_node.derive_node().scan_node().indexes_size() == 0) {
        DB_FATAL_STATE(state, "no index");
        return -1;
    }

    //TODO 索引选择已经在baikaldb做了，后续删除store的选择
    int idx = select_index_for_store();
    if (_multi_reverse_index.size() > 0) {
        idx = _multi_reverse_index[0];
    }

    const pb::PossibleIndex& pos_index = _pb_node.derive_node().scan_node().indexes(idx);
    _index_id = pos_index.index_id();
    _index_info = _factory->get_index_info_ptr(_index_id);
    if (_index_info == nullptr || _index_info->id == -1) {
        DB_WARNING_STATE(state, "no index_info found for index id: %ld", _index_id);
        return -1;
    }

    int ret = 0;
    for (auto& expr : pos_index.index_conjuncts()) {
        ExprNode* index_conjunct = nullptr;
        ret = ExprNode::create_tree(expr, &index_conjunct);
        if (ret < 0) {
            DB_WARNING_STATE(state, "ExprNode::create_tree fail, ret:%d", ret);
            return ret;
        }
        _index_conjuncts.push_back(index_conjunct);
    }
    if (pos_index.has_sort_index()) {
        _sort_use_index = true;
        _scan_forward = pos_index.sort_index().is_asc();
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
    } else if (_index_info->type == pb::I_RECOMMEND) {
        int32_t userid_field_id = get_field_id_by_name(_table_info->fields, "userid");
        int32_t source_field_id = get_field_id_by_name(_table_info->fields, "source");
        auto userid_slot_id = state->get_slot_id(_tuple_id, userid_field_id);
        auto source_slot_id = state->get_slot_id(_tuple_id, source_field_id);
        if (userid_slot_id > 0) {
            _index_slot_field_map[userid_slot_id] = userid_field_id;
        }
        if (source_slot_id > 0) {
            _index_slot_field_map[source_slot_id] = source_field_id;
        }
    }
    for (auto& slot : _tuple_desc->slots()) {
        if (_index_slot_field_map.count(slot.slot_id()) == 0) {
            _is_covering_index = false;
            break;
        }
    }
    // 以baikaldb的判断为准
    if (pos_index.is_covering_index()) {
        _is_covering_index = true;
    }

    // 索引条件下推，减少主表查询次数
    index_condition_pushdown();
    for (auto expr : _index_conjuncts) {
        //pb::Expr pb;
        //ExprNode::create_pb_expr(&pb, expr);
        //DB_NOTICE("where:%s", pb.ShortDebugString().c_str());
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "Expr::open fail:%d", ret);
            return ret;
        }
    }

    if (_multi_reverse_index.size() > 0) {
        for (auto id : _multi_reverse_index) {
            const pb::PossibleIndex& pos_index = _pb_node.derive_node().scan_node().indexes(id);
            auto index_id = pos_index.index_id();
            auto index_info = _factory->get_index_info_ptr(index_id);
            if (index_info == nullptr|| index_info->id == -1) {
                DB_WARNING_STATE(state, "no index_info found for index id: %ld", index_id);
                return -1;
            }
            for (auto& range : pos_index.ranges()) {
                SmartRecord record = _factory->new_record(_table_id);
                record->decode(range.left_pb_record());
                std::string word;
                ret = record->get_reverse_word(*index_info, word);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", index_id);
                    return ret;
                }
                _reverse_infos.push_back(*index_info);
                _query_words.push_back(word);
                _match_modes.push_back(range.match_mode());
            }
            _index_ids.push_back(index_id);
            _bool_and = pos_index.bool_and();
            //DB_WARNING_STATE(state, "use multi %d", _reverse_infos.size());
        }
        return 0;
    }
    _index_ids.push_back(_index_id);
    if (pos_index.ranges_size() == 0) {
        return 0;
    }
    //DB_WARNING_STATE(state, "use_index: %ld table_id: %ld left:%d, right:%d", 
    //        _index_id, _table_id, pos_index.ranges(0).left_field_cnt(), pos_index.ranges(0).right_field_cnt());

    bool is_eq = true;
    bool like_prefix = true;
    for (auto& range : pos_index.ranges()) {
        // 空指针容易出错
        SmartRecord left_record = _factory->new_record(_table_id);
        SmartRecord right_record = _factory->new_record(_table_id);
        left_record->decode(range.left_pb_record());
        right_record->decode(range.right_pb_record());
        int left_field_cnt = range.left_field_cnt();
        int right_field_cnt = range.right_field_cnt();
        bool left_open = range.left_open();
        bool right_open = range.right_open();
        like_prefix = range.like_prefix();
        if (range.left_pb_record() != range.right_pb_record()) {
            is_eq = false;
        }
        if (left_field_cnt != right_field_cnt) {
            is_eq = false;
        }
        //DB_WARNING_STATE(state, "left_open:%d right_open:%d", left_open, right_open);
        if (left_open || right_open) {
            is_eq = false;
        }
        _left_records.push_back(left_record);
        _right_records.push_back(right_record);
        _left_field_cnts.push_back(left_field_cnt);
        _right_field_cnts.push_back(right_field_cnt);
        _left_opens.push_back(left_open);
        _right_opens.push_back(right_open);
        _like_prefixs.push_back(like_prefix);
    }
    if (_index_info->type == pb::I_PRIMARY || _index_info->type == pb::I_UNIQ) {
        if (_left_field_cnts[_idx] == (int)_index_info->fields.size() && is_eq && !like_prefix) {
            //DB_WARNING_STATE(state, "index use get ,index:%ld", _index_info.id);
            _use_get = true;
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
    // get方式和主键无需下推
    if (_use_get || index_type == pb::I_PRIMARY) {
        return false;
    }
    // 该条件用于指定索引，在filternode里处理了
    if (expr->contained_by_index(_index_ids)) {
        return false;
    }
    if (index_type == pb::I_KEY || index_type == pb::I_UNIQ) {
        // 普通索引只要全包含slot id就可以下推
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
    SlotRef* slot_ref = static_cast<SlotRef*>(expr->children(0));
    if (_index_slot_field_map.count(slot_ref->slot_id()) == 0) {
        return false;
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
            _index_conjuncts.push_back(*iter);
            iter = parent_conditions->erase(iter);
            //DB_WARNING("expr is push_down")
        } else {
            iter++;
        }
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
    _mem_row_desc = state->mem_row_desc();
    if (_is_explain) {
        return 0;
    }
    ret = choose_index(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "calc index fail:%d", ret);
        return ret;
    }
    if (_table_info == nullptr) {
        DB_WARNING_STATE(state, "table null:%d", ret);
        return -1;
    }
    if (_index_info == nullptr) {
        DB_WARNING_STATE(state, "index null:%d", ret);
        return -1;
    }
    if (_index_info->type == pb::I_RECOMMEND) {
        state->set_sort_use_index();
    } 
    if (_sort_use_index) {
        state->set_sort_use_index();
    }
    std::set<int32_t> pri_field_ids;
    for (auto& field_info : _pri_info->fields) {
        pri_field_ids.insert(field_info.id);
    }
    // 用数组映射slot，提升性能
    _field_slot.resize(_table_info->fields.back().id + 1);
    for (auto& slot : _tuple_desc->slots()) {
        _field_slot[slot.field_id()] = slot.slot_id();
        if (pri_field_ids.count(slot.field_id()) == 0) {
            auto field = _table_info->get_field_ptr(slot.field_id());
            if (field == nullptr) {
                DB_WARNING("field not found id:%d", slot.field_id());
                return -1;
            }
            // 这两个倒排的特殊字段
            if (field->short_name != "__weight" && field->short_name != "__pic_scores") {
                _field_ids[slot.field_id()] = field;
            }
        }
    }

    _region_id = state->region_id();
    //DB_WARNING_STATE(state, "use_index: %ld table_id: %ld region_id: %ld", _index_id, _table_id, _region_id);
    _region_info = &(state->resource()->region_info);
    auto txn = state->txn();
    auto reverse_index_map = state->reverse_index_map();
    //DB_WARNING_STATE(state, "_is_covering_index:%d", _is_covering_index);
    if (_reverse_infos.size() > 1) {
        for (auto& info : _reverse_infos) {
            if (reverse_index_map.count(info.id) == 1) {
                _reverse_indexes.push_back(reverse_index_map[info.id]);
            } else {
                DB_WARNING_STATE(state, "index:%ld is not FULLTEXT", info.id);
                return -1;
            }
        }
        //DB_WARNING_STATE(state, "_m_index search");
        // reverse has in, need not or boolean
        //if (_reverse_indexes.size() > _index_ids.size()) {
        //    or_bool = false;
        //}
        //DB_NOTICE("or_bool:%d", or_bool);
        // 为了性能,多索引倒排查找不seek

        if (_factory->get_index_storage_type(_index_id, _storage_type) == -1) {
            DB_FATAL("get index storage type error.");
            return -1;
        }

        if (_storage_type == pb::ST_PROTOBUF) {
            std::vector<ReverseIndex<CommonSchema>*> common_reverse_indexes;
            common_reverse_indexes.reserve(4);
            for (auto index_ptr : _reverse_indexes) {
                common_reverse_indexes.push_back(static_cast<ReverseIndex<CommonSchema>*>(index_ptr));
            }
            _m_index.search(txn->get_txn(), *_pri_info, *_table_info, 
                common_reverse_indexes, _query_words, _match_modes, true, !_bool_and);
        } else if (_storage_type == pb::ST_ARROW) {
            std::vector<ReverseIndex<ArrowSchema>*> arrow_reverse_indexes;
            arrow_reverse_indexes.reserve(4);
            for (auto index_ptr : _reverse_indexes) {
                arrow_reverse_indexes.push_back(static_cast<ReverseIndex<ArrowSchema>*>(index_ptr));
            }
            _m_arrow_index.search(txn->get_txn(), *_pri_info, *_table_info, 
                arrow_reverse_indexes, _query_words, _match_modes, true, !_bool_and);
        } else {
            DB_FATAL("fulltext storage type error");
            return -1;
        }
    } else if (_reverse_infos.size() ==1 && reverse_index_map.count(_index_id) == 1) {
        //倒排索引不允许是多字段
        if (_index_info->fields.size() != 1) {
            DB_WARNING_STATE(state, "indexinfo get fail, index_id:%ld", _index_id);
            return -1;
        }
        _reverse_index = reverse_index_map[_index_id];
        //DB_NOTICE("word:%s", str_to_hex(word).c_str());
        //bool dont_seek = _index_info->type == pb::I_RECOMMEND;
        // seek性能太差了，倒排索引都不做seek
        bool dont_seek = true;
        ret = _reverse_index->search(txn->get_txn(), *_pri_info, *_table_info, 
                _query_words[0], _match_modes[0], _index_conjuncts, dont_seek);
        if (ret < 0) {
            return ret;
        }
    }
    for (auto id : _index_ids) {
        state->add_scan_index(id);
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
    ON_SCOPE_EXIT(([this, state]() {
        state->set_num_scan_rows(_scan_rows);
    }));
    if (_index_id == _table_id) {
        if (_use_get) {
            return get_next_by_table_get(state, batch, eos);
        } else {
            return get_next_by_table_seek(state, batch, eos);
        }
    } else {
        if (_use_get) {
            return get_next_by_index_get(state, batch, eos);
        } else {
            return get_next_by_index_seek(state, batch, eos);
        }
    }
    return 0;
}

void RocksdbScanNode::close(RuntimeState* state) {
    ScanNode::close(state);
    for (auto expr : _index_conjuncts) {
        expr->close();
    }
    _index_ids.clear();
    _idx = 0;
    _reverse_infos.clear();
    _query_words.clear();
    _match_modes.clear();
    _reverse_indexes.clear();
}

int RocksdbScanNode::get_next_by_table_get(RuntimeState* state, RowBatch* batch, bool* eos) {
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this](TraceLocalNode& local_node) {
        local_node.set_scan_rows(_scan_rows);
    }));
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr");
        return -1;
    }
    SmartRecord record;
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
        if (_idx >= _left_records.size()) {
            *eos = true;
            return 0;
        } else {
            record = _left_records[_idx++];
        }
        ++_scan_rows;
        int ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY, true);
        if (ret < 0) {
            continue;
        }
        std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
        for (auto slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            row->set_value(slot.tuple_id(), slot.slot_id(),
                    record->get_value(field));
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
    }
}

int RocksdbScanNode::get_next_by_index_get(RuntimeState* state, RowBatch* batch, bool* eos) {
    int64_t get_primary_cnt = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this, &get_primary_cnt](TraceLocalNode& local_node) {
        local_node.add_get_primary_rows(get_primary_cnt);
        local_node.set_scan_rows(_scan_rows);
    }));

    bool is_global_index = false;
    if (_region_info->has_main_table_id() 
        && _region_info->main_table_id() != _region_info->table_id()) {
        is_global_index = true;
    }
    SmartRecord record;
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
        if (_idx >= _left_records.size()) {
            *eos = true;
            return 0;
        } else {
            record = _left_records[_idx++];
        }
        auto txn = state->txn();
        ++_scan_rows;
        int ret = txn->get_update_secondary(_region_id, *_pri_info, *_index_info, record, GET_ONLY, true);
        if (ret < 0) {
            //DB_WARNING_STATE(state, "get index:%ld fail, not exist, ret:%d, record: %s", 
            //        _table_id, ret, record->to_string().c_str());
            continue;
        }
        if (!_is_covering_index && !is_global_index) {
            ++get_primary_cnt;
            ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY, false);
            if (ret < 0) {
                DB_FATAL("get primary:%ld fail, not exist, ret:%d, record: %s", 
                        _table_id, ret, record->to_string().c_str());
                continue;
            }
        }

        std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
        for (auto slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            row->set_value(slot.tuple_id(), slot.slot_id(),
                    record->get_value(field));
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
    }
}

int RocksdbScanNode::get_next_by_table_seek(RuntimeState* state, RowBatch* batch, bool* eos) {
    int64_t index_filter_cnt = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this, &index_filter_cnt](TraceLocalNode& local_node) {
        local_node.add_index_filter_rows(index_filter_cnt);
        local_node.set_scan_rows(_scan_rows);
    }));
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
        if (_table_iter == nullptr || !_table_iter->valid()) {
            if (_idx >= _left_records.size()) {
                *eos = true;
                return 0;
            } else {
                IndexRange range(_left_records[_idx].get(), 
                        _right_records[_idx].get(), 
                        _index_info.get(),
                        _pri_info.get(),
                        _region_info,
                        _left_field_cnts[_idx], 
                        _right_field_cnts[_idx], 
                        _left_opens[_idx], 
                        _right_opens[_idx],
                        _like_prefixs[_idx]);
                delete _table_iter;
                _table_iter = Iterator::scan_primary(
                        state->txn(), range, _field_ids, _field_slot, true, _scan_forward);
                if (_table_iter == nullptr) {
                    DB_WARNING_STATE(state, "open TableIterator fail, table_id:%ld", _index_id);
                    return -1;
                }
                if (_is_covering_index) {
                    _table_iter->set_mode(KEY_ONLY);
                }
                _idx++;
                continue;
            }
        }
        ++_scan_rows;
        std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
        int ret = _table_iter->get_next(_tuple_id, row);
        if (ret < 0) {
            continue;
        }
        if (!need_copy(row.get(), _index_conjuncts)) {
            state->inc_num_filter_rows();
            ++index_filter_cnt;
            continue;
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
    }
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

    bool is_global_index = false;
    if (_region_info->has_main_table_id() 
        && _region_info->main_table_id() != _region_info->table_id()) {
        is_global_index = true;
    }
    int ret = 0;
    SmartRecord record = _factory->new_record(_table_id);
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
        if (_reverse_indexes.size() > 0) {
            if (!multi_valid(_storage_type)) {
                *eos = true; 
                return 0;
            }
        } else if (_reverse_index != nullptr) {
            if (!_reverse_index->valid()) {
                *eos = true;
                return 0;
            }
        } else {
            if (_index_iter == nullptr || !_index_iter->valid()) {
                if (_idx >= _left_records.size()) {
                    *eos = true;
                    return 0;
                } else {
                    IndexRange range(_left_records[_idx].get(), 
                            _right_records[_idx].get(), 
                            _index_info.get(),
                            _pri_info.get(),
                            _region_info,
                            _left_field_cnts[_idx], 
                            _right_field_cnts[_idx], 
                            _left_opens[_idx], 
                            _right_opens[_idx],
                            _like_prefixs[_idx]);
                    delete _index_iter;
                    _index_iter = Iterator::scan_secondary(state->txn(), range, _field_slot, true, _scan_forward);
                    if (_index_iter == nullptr) {
                        DB_WARNING_STATE(state, "open IndexIterator fail, index_id:%ld", _index_id);
                        return -1;
                    }
                    _idx++;
                    continue;
                }
            }
        }
        //TimeCost cost;
        ++_scan_rows;
        if (!_is_covering_index && !is_global_index) {
            record->clear();
        }
        std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
        if (_reverse_indexes.size() > 0) {
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
            if (!_is_covering_index && !is_global_index) {
                ret = _index_iter->get_next(record);
            } else {
                ret = _index_iter->get_next(_tuple_id, row);
            }
            //DB_DEBUG("rocksdb_scan region_%lld record[%s]", _region_id, record->to_string().c_str());
            if (ret < 0) {
                //DB_WARNING_STATE(state, "get index fail, maybe reach end");
                continue;
            }
        }
        // 倒排索引直接下推到了布尔引擎，但是主键条件未下推，因此也需要再次过滤
        // toto: 后续可以再次优化，把userid和source的条件干掉
        // 索引谓词过滤
        if (!_is_covering_index && !is_global_index) {
            for (auto& pair : _index_slot_field_map) {
                auto field = record->get_field_by_tag(pair.second);
                row->set_value(_tuple_id, pair.first, record->get_value(field));
            }
        }
        //DB_NOTICE("record_before:%s", record->debug_string().c_str());
        if (!need_copy(row.get(), _index_conjuncts)) {
            state->inc_num_filter_rows();
            ++index_filter_cnt;
            continue;
        }
        //DB_NOTICE("get index: %ld", cost.get_time());
        //cost.reset();
        if (!_is_covering_index && !is_global_index) {
            ++get_primary_cnt;
            auto txn = state->txn();
            ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY, false);
            if (ret < 0) {
                if (_reverse_indexes.size() == 0 && _reverse_index == nullptr) {
                    DB_FATAL("get primary:%ld fail, ret:%d, index primary may be not consistency: %s", 
                            _table_id, ret, record->to_string().c_str());
                }
                continue;
            }
            for (auto slot : _tuple_desc->slots()) {
                auto field = record->get_field_by_tag(slot.field_id());
                row->set_value(slot.tuple_id(), slot.slot_id(),
                        record->get_value(field));
            }
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
        //DB_NOTICE("MemRow set: %ld", cost.get_time());
    }
}
void RocksdbScanNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    bool ignore_primary = false;
    auto scan_pb = pb_node->mutable_derive_node()->mutable_scan_node();
    if (region_id == 0 || _region_primary.count(region_id) == 0) {
        return;
    }
    pb::PossibleIndex* primary = nullptr;
    for (auto& pos_index : *scan_pb->mutable_indexes()) {
        if (pos_index.index_id() == _router_index_id) {
            primary = &pos_index;
            break;
        }
    }
    if (primary != nullptr) {
        primary->CopyFrom(_region_primary[region_id]);
    }
}

int RocksdbScanNode::choose_arrow_pb_reverse_index(const pb::ScanNode& node) {
    if (_multi_reverse_index.size() > 1) {
        int pb_type_num = 0;
        int arrow_type_num = 0;
        std::vector<int> pb_indexs;
        std::vector<int> arrow_indexs;
        pb_indexs.reserve(4);
        arrow_indexs.reserve(4);
        pb::StorageType filter_type = pb::ST_UNKNOWN;
        for (auto id : _multi_reverse_index) {
            const pb::PossibleIndex& pos_index = node.indexes(id);
            auto index_id = pos_index.index_id();
            DB_DEBUG("reverse_filter index [%lld]", index_id);
            pb::StorageType type = pb::ST_UNKNOWN;
            if (SchemaFactory::get_instance()->get_index_storage_type(index_id, type) == -1) {
                DB_FATAL("get index storage type error index [%lld]", index_id);
                return -1;
            }

            if (type == pb::ST_PROTOBUF) {
                pb_indexs.push_back(index_id);
                ++pb_type_num;
            } else if (type == pb::ST_ARROW) {
                arrow_indexs.push_back(index_id);
                ++arrow_type_num;
            }
        }
        filter_type = pb_type_num <= arrow_type_num ? pb::ST_PROTOBUF : pb::ST_ARROW;
        DB_DEBUG("reverse_filter type[%s]", pb::StorageType_Name(filter_type).c_str());
        auto remove_indexs_func = [this](std::vector<int>& to_remove_indexs) {
            _multi_reverse_index.erase(std::remove_if(_multi_reverse_index.begin(), _multi_reverse_index.end(), [&to_remove_indexs](const int& index) {
                return std::find(to_remove_indexs.begin(), to_remove_indexs.end(), index) 
                    != to_remove_indexs.end() ? true : false;
            }), _multi_reverse_index.end());
        };

        if (filter_type == pb::ST_PROTOBUF) {
            remove_indexs_func(pb_indexs);
        } else if (filter_type == pb::ST_ARROW) {
            remove_indexs_func(arrow_indexs);
        }
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
