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

#include <map>
#include "scan_node.h"
#include "filter_node.h"
#include "join_node.h"
#include "schema_factory.h"
#include "scalar_fn_call.h"
#include "slot_ref.h"
#include "runtime_state.h"
#include "parser.h"

namespace baikaldb {
int ScanNode::select_index(RuntimeState* state, 
                           const pb::PlanNode& node, 
                           std::vector<int>& multi_reverse_index) {
    int i = 0;
    //int index_size = node.derive_node().scan_node().indexes_size();
    int sort_index = -1;

    std::multimap<uint32_t, int> prefix_ratio_id_mapping;
    for (auto& pos_index : node.derive_node().scan_node().indexes()) {
        int64_t index_id = pos_index.index_id();
        IndexInfo& info = state->resource()->get_index_info(index_id);
        if (info.id == -1) {
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
        float prefix_ratio = (field_count + 0.0) / info.fields.size();
        uint16_t prefix_ratio_round = prefix_ratio * 10;
        uint16_t index_priority = 0;
        if (info.type == pb::I_PRIMARY) {
            index_priority = 300;
        } else if (info.type == pb::I_UNIQ) {
            index_priority = 200;
        } else if (info.type == pb::I_KEY) {
            index_priority = 100 + field_count;
        } else {
            index_priority = 0;
        }
        uint32_t prefix_ratio_index_score = (prefix_ratio_round << 16) | index_priority;
        DB_WARNING("scan node insert prefix_ratio_index_score:%u, i: %d", prefix_ratio_index_score, i);
        prefix_ratio_id_mapping.insert(std::make_pair(prefix_ratio_index_score, i));

        // 优先选倒排，没有就取第一个
        switch (info.type) {
            case pb::I_FULLTEXT:
                multi_reverse_index.push_back(i);
                break;
            case pb::I_RECOMMEND:
                return i;
                break;
            default:
                break;
        }
        if (pos_index.has_sort_index()) {
            sort_index = i;
        }
        ++i;
    }
    if (sort_index != -1) {
        return sort_index;
    }
    // ratio * 10(=0...9)相同的possible index中，按照PRIMARY, UNIQUE, KEY的优先级选择
    DB_WARNING("prefix_ratio_id_mapping.size: %d", prefix_ratio_id_mapping.size());
    for (auto iter = prefix_ratio_id_mapping.crbegin(); iter != prefix_ratio_id_mapping.crend(); ++iter) {
        DB_WARNING("prefix_ratio_index_score:%u, i: %d", iter->first, iter->second);
        return iter->second;
    }
    return 0;
}

int ScanNode::choose_index(RuntimeState* state) {
    _table_info = &(state->resource()->table_info);
    _pri_info = &(state->resource()->pri_info);

    // 做完logical plan还没有索引
    if (_pb_node.derive_node().scan_node().indexes_size() == 0) {
        return 0;
    }

    std::vector<int> multi_reverse_index;
    int idx = select_index(state, _pb_node, multi_reverse_index);
    if (multi_reverse_index.size() == 1) {
        idx = multi_reverse_index[0];
    }
    const pb::PossibleIndex& pos_index = _pb_node.derive_node().scan_node().indexes(idx);
    _index_id = pos_index.index_id();
    _index_info = &state->resource()->get_index_info(_index_id);
    if (_index_info->id == -1) {
        DB_WARNING_STATE(state, "no index_info found for index id: %ld", _index_id);
        return -1;
    }
    int ret = 0;
    if (multi_reverse_index.size() > 1 || 
            (multi_reverse_index.size() == 1 && pos_index.ranges_size() > 1)) {
        for (auto id : multi_reverse_index) {
            const pb::PossibleIndex& pos_index = _pb_node.derive_node().scan_node().indexes(id);
            auto index_id = pos_index.index_id();
            IndexInfo& index_info = state->resource()->get_index_info(index_id);
            if (index_info.id == -1) {
                DB_WARNING_STATE(state, "no index_info found for index id: %ld", index_id);
                return -1;
            }
            for (auto& range : pos_index.ranges()) {
                SmartRecord record = _factory->new_record(*_table_info);
                record->decode(range.left_pb_record());
                std::string word;
                ret = record->get_reverse_word(index_info, word);
                if (ret < 0) {
                    DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", index_id);
                    return ret;
                }
                _reverse_infos.push_back(index_info);
                _query_words.push_back(word);
            }
            _index_ids.push_back(index_id);
            DB_WARNING_STATE(state, "use multi %d", _reverse_infos.size());
        }
        return 0;
    }
    _index_ids.push_back(_index_id);
    if (pos_index.ranges_size() == 0) {
        return -1;
    }
    DB_WARNING_STATE(state, "use_index: %ld table_id: %ld left:%d, right:%d", 
            _index_id, _table_id, pos_index.ranges(0).left_field_cnt(), pos_index.ranges(0).right_field_cnt());

    bool is_eq = true;
    for (auto& range : pos_index.ranges()) {
        // 空指针容易出错
        SmartRecord left_record = _factory->new_record(*_table_info);
        SmartRecord right_record = _factory->new_record(*_table_info);
        left_record->decode(range.left_pb_record());
        right_record->decode(range.right_pb_record());
        int left_field_cnt = range.left_field_cnt();
        int right_field_cnt = range.right_field_cnt();
        bool left_open = range.left_open();
        bool right_open = range.right_open();
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
    }
    if (_index_info->type == pb::I_PRIMARY || _index_info->type == pb::I_UNIQ) {
        if (_left_field_cnts[_idx] == (int)_index_info->fields.size() && is_eq) {
            //DB_WARNING_STATE(state, "index use get ,index:%ld", _index_info.id);
            _use_get = true;
        }
    }
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
    //DB_WARNING_STATE(state, "start search");
    return 0;
}

int ScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _factory = SchemaFactory::get_instance();
    _tuple_id = node.derive_node().scan_node().tuple_id();
    _table_id = node.derive_node().scan_node().table_id();
    return 0;
}

int ScanNode::predicate_pushdown(std::vector<ExprNode*>& input_exprs) {
    //DB_WARNING("node:%ld is pushdown", this);
    if (_parent->get_node_type() == pb::WHERE_FILTER_NODE
            || _parent->get_node_type() == pb::TABLE_FILTER_NODE) {
        DB_WARNING("parent is filter node,%d", _parent->get_node_type());
        return 0;
    }
    if (input_exprs.size() > 0) {
        add_filter_node(input_exprs);
    }
    input_exprs.clear();
    return 0;
}

bool ScanNode::need_pushdown(ExprNode* expr) {
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
    slot_ref->set_field_id(_index_slot_field_map[slot_ref->slot_id()]);
    switch (expr->node_type()) {
        case pb::FUNCTION_CALL: { 
#ifdef NEW_PARSER
            if (static_cast<ScalarFnCall*>(expr)->fn().fn_op() == parser::FT_EQ) {
                return true;
            }
#else
            if (static_cast<ScalarFnCall*>(expr)->fn().fn_op() == OP_EQ) {
                return true;
            }
#endif
            break;
        }
        case pb::IN_PREDICATE:
            return true;
        default:
            return false;
    }
    return false;
}

int ScanNode::index_condition_pushdown() {
    //DB_WARNING("node:%ld is pushdown", this);
    if (_parent == NULL) {
        DB_WARNING("parent is null");
        return 0;
    }
    if (_parent->get_node_type() != pb::WHERE_FILTER_NODE &&
            _parent->get_node_type() != pb::TABLE_FILTER_NODE) {
        DB_WARNING("parent is not filter node:%d", _parent->get_node_type());
        return 0;
    }
    
    std::vector<ExprNode*>* parent_conditions = _parent->mutable_conjuncts();
    auto iter = parent_conditions->begin();
    while (iter != parent_conditions->end()) {
        if (need_pushdown(*iter)) {
            _index_conjuncts.push_back(*iter);
            iter = parent_conditions->erase(iter);
            DB_WARNING("expr is push_down")
        } else {
            iter++;
        }
    }
    return 0;
}

int ScanNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    ret = choose_index(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "calc index fail:%d", ret);
        return ret;
    }
    if (_index_info->type == pb::I_RECOMMEND) {
        state->set_sort_use_index();
    } 
    if (_sort_use_index) {
        state->set_sort_use_index();
    }
    _tuple_desc = state->get_tuple_desc(_tuple_id);
    for (auto& slot : _tuple_desc->slots()) {
        _field_ids.push_back(slot.field_id());
    }
    std::sort(_field_ids.begin(), _field_ids.end());

    _mem_row_desc = state->mem_row_desc();
    _region_id = state->region_id();
    DB_WARNING_STATE(state, "use_index: %ld table_id: %ld region_id: %ld", _index_id, _table_id, _region_id);
    _region_info = &(state->resource()->region_info);
    auto txn = state->txn();
    auto reverse_index_map = state->reverse_index_map();
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
    // 索引条件下推，减少主表查询次数
    index_condition_pushdown();
    for (auto expr : _index_conjuncts) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "Expr::open fail:%d", ret);
            return ret;
        }
    }
    //DB_WARNING_STATE(state, "_is_covering_index:%d", _is_covering_index);
    if (_reverse_infos.size() > 0) {
        for (auto& info : _reverse_infos) {
            if (reverse_index_map.count(info.id) == 1) {
                _reverse_indexes.push_back(reverse_index_map[info.id]);
            } else {
                DB_WARNING_STATE(state, "index:%ld is not FULLTEXT", info.id);
                return -1;
            }
        }
        bool or_bool = true;
        //DB_WARNING_STATE(state, "_m_index search");
        // reverse has in, need not or boolean
        if (_reverse_indexes.size() > _index_ids.size()) {
            or_bool = false;
        }
        DB_NOTICE("or_bool:%d", or_bool);
        // 为了性能,多索引倒排查找不seek
        _m_index.search(txn->get_txn(), *_pri_info, *_table_info, 
                    _reverse_indexes, _query_words, true, or_bool);
    } else if (reverse_index_map.count(_index_id) == 1) {
        //倒排索引不允许是多字段
        if (_index_info->fields.size() != 1) {
            DB_WARNING_STATE(state, "indexinfo get fail, index_id:%ld", _index_id);
            return -1;
        }
        _reverse_index = reverse_index_map[_index_id];
        std::string word;
        ret = _left_records[_idx]->get_reverse_word(*_index_info, word);
        if (ret < 0) {
            DB_WARNING_STATE(state, "index_info to word fail for index_id: %ld", _index_id);
            return ret;
        }
        //DB_NOTICE("word:%s", str_to_hex(word).c_str());
        bool dont_seek = _index_info->type == pb::I_RECOMMEND;
        ret = _reverse_index->search(txn->get_txn(), *_pri_info, *_table_info, 
                word, _index_conjuncts, dont_seek);
        if (ret < 0) {
            return ret;
        }
    }
    for (auto id : _index_ids) {
        state->add_scan_index(id);
    }
    return 0;
}

int ScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
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

void ScanNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto expr : _index_conjuncts) {
        expr->close();
    }
}

int ScanNode::get_next_by_table_get(RuntimeState* state, RowBatch* batch, bool* eos) {
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr");
        return -1;
    }
    SmartRecord record;
    while (1) {
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
        int ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY, true);
        if (ret < 0) {
            DB_WARNING_STATE(state, "get primary:%ld fail, not exist, ret:%d, record: %s", 
                    _table_id, ret, record->to_string().c_str());
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

int ScanNode::get_next_by_index_get(RuntimeState* state, RowBatch* batch, bool* eos) {
    SmartRecord record;
    while (1) {
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
        int ret = txn->get_update_secondary(_region_id, *_pri_info, *_index_info, record, GET_ONLY, true);
        if (ret < 0) {
            DB_WARNING_STATE(state, "get index:%ld fail, not exist, ret:%d, record: %s", 
                    _table_id, ret, record->to_string().c_str());
            continue;
        }
        if (!_is_covering_index) {
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

int ScanNode::get_next_by_table_seek(RuntimeState* state, RowBatch* batch, bool* eos) {
    SmartRecord record = _factory->new_record(*_table_info);
    int64_t time = 0;
    while (1) {
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
                        _index_info,
                        _pri_info,
                        _region_info,
                        _left_field_cnts[_idx], 
                        _right_field_cnts[_idx], 
                        _left_opens[_idx], 
                        _right_opens[_idx]);
                delete _table_iter;
                _table_iter = Iterator::scan_primary(state->txn(), range, _field_ids, true, _scan_forward);
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
        record->clear();
        int ret = _table_iter->get_next(record);
        if (ret < 0) {
            continue;
        }
        TimeCost cost;
        //DB_WARNING_STATE(state, "get_next:%lu", cost.get_time());
        std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
        for (auto slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            row->set_value(slot.tuple_id(), slot.slot_id(),
                    record->get_value(field));
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
        time += cost.get_time();
    }
}

inline bool ScanNode::need_copy(MemRow* row) {
    for (auto conjunct : _index_conjuncts) {
        ExprValue value = conjunct->get_value(row);
        if (value.is_null() || value.get_numberic<bool>() == false) {
            return false;
        }
    }
    return true;
}

int ScanNode::get_next_by_index_seek(RuntimeState* state, RowBatch* batch, bool* eos) {
    int ret = 0;
    SmartRecord record = _factory->new_record(*_table_info);
    while (1) {
        if (reached_limit()) {
            *eos = true;
            return 0;
        }
        if (batch->is_full()) {
            return 0;
        }
        if (_reverse_indexes.size() > 0) {
            if (!_m_index.valid()) {
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
                            _index_info,
                            _pri_info,
                            _region_info,
                            _left_field_cnts[_idx], 
                            _right_field_cnts[_idx], 
                            _left_opens[_idx], 
                            _right_opens[_idx]);
                    delete _index_iter;
                    _index_iter = Iterator::scan_secondary(state->txn(), range, true, _scan_forward);
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
        record->clear();
        std::unique_ptr<MemRow> row = _mem_row_desc->fetch_mem_row();
        if (_reverse_indexes.size() > 0) {
            ret = _m_index.get_next(record);
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
            ret = _index_iter->get_next(record);
            if (ret < 0) {
                //DB_WARNING_STATE(state, "get index fail, maybe reach end");
                continue;
            }
        }
        // 倒排索引直接下推到了布尔引擎，但是主键条件未下推，因此也需要再次过滤
        // toto: 后续可以再次优化，把userid和source的条件干掉
        // 索引谓词过滤
        for (auto& pair : _index_slot_field_map) {
            auto field = record->get_field_by_tag(pair.second);
            row->set_value(_tuple_id, pair.first, record->get_value(field));
        }
        if (!need_copy(row.get())) {
            continue;
        }
        //DB_NOTICE("get index: %ld", cost.get_time());
        //cost.reset();
        if (!_is_covering_index) {
            auto txn = state->txn();
            ret = txn->get_update_primary(_region_id, *_pri_info, record, _field_ids, GET_ONLY, false);
            if (ret < 0) {
                DB_FATAL("get primary:%ld fail, ret:%d, index primary may be not consistency: %s", 
                        _table_id, ret, record->to_string().c_str());
                continue;
            }
        }
        //DB_NOTICE("get pri: %ld", cost.get_time());
        //cost.reset();
        //row->set_tuple(_tuple_id, _mem_row_desc);
        for (auto slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            row->set_value(slot.tuple_id(), slot.slot_id(),
                    record->get_value(field));
        }
        batch->move_row(std::move(row));
        ++_num_rows_returned;
        //DB_NOTICE("MemRow set: %ld", cost.get_time());
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
