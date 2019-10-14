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

#include "select_manager_node.h"
#include "filter_node.h"
#include "network_socket.h"
#include "rocksdb_scan_node.h"

namespace baikaldb {
int SelectManagerNode::open(RuntimeState* state) {
    int ret = 0;
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    client_conn->seq_id++;
    for (auto expr : _slot_order_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
    }
    _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
    _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
    std::vector<ExecNode*> scan_nodes;
    get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() != 1) {
        DB_WARNING("select manager has more than one scan node, scan_node_size: %d, txn_id: %lu, log_id:%lu",
                    state->txn_id, state->log_id());
        return -1;
    }
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(scan_nodes[0]);
    int64_t index_id = scan_node->index_ids()[0];
    int64_t main_table_id = scan_node->table_id();
    //如果命中的不是全局二级索引，或者全局二级索引是covering_index, 则直接在主表或者索引表上做scan即可
    if (!_factory->is_global_index(index_id) || scan_node->covering_index()) {
        ret = _fetcher_store.run(state, _region_infos, _children[0], client_conn->seq_id, pb::OP_SELECT);
    } else {
        ret = open_global_index(state, scan_node, index_id, main_table_id);
    } 
    if (ret < 0) {
        DB_WARNING("select manager fetcher mnager node open fail, txn_id: %lu, log_id:%lu", 
                state->txn_id, state->log_id());
        return ret;
    }
    for (auto& pair : _fetcher_store.start_key_sort) {
        auto& batch = _fetcher_store.region_batch[pair.second];
        if (batch != NULL && batch->size() != 0) {
            _sorter->add_batch(batch);
        }
    }
    // 无sort节点时不会排序，按顺序输出
    _sorter->merge_sort();
    return _fetcher_store.affected_rows.load();
}

int SelectManagerNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (state->is_cancelled()) {
        DB_WARNING_STATE(state, "cancelled");
        *eos = true;
        return 0;
    }
    if (reached_limit()) {
        *eos = true;
        return 0;
    }
    int ret = 0;
    ret = _sorter->get_next(batch, eos);
    if (ret < 0) {
        DB_WARNING("sort get_next fail");
        return ret;
    }
    _num_rows_returned += batch->size();
    if (reached_limit()) {
        *eos = true;
        _num_rows_returned = _limit;
        return 0;
    }
    return 0;
}

int SelectManagerNode::open_global_index(RuntimeState* state, ExecNode* exec_node, 
        int64_t global_index_id, int64_t main_table_id) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    auto client_conn = state->client_conn();
    std::vector<ExprNode*> conjuncts_copy;
    std::vector<ExprNode*> conjuncts_global;
    std::vector<ExprNode*> conjuncts_others;
    //重构filter 和scan_node
    std::vector<ExecNode*> filter_nodes;
    get_node(pb::WHERE_FILTER_NODE, filter_nodes);
    //能命中全局二级索引，不可能没有filter(不成立，可能有order by)
    if (filter_nodes.size() >0) {
        auto pri_info = _factory->get_index_info_ptr(main_table_id);
        auto index_info = _factory->get_index_info_ptr(global_index_id);
        for (auto& f : pri_info->fields) {
            auto slot_id = state->get_slot_id(scan_node->tuple_id(), f.id);
            if (slot_id > 0) {
                _index_slot_field_map[slot_id] = f.id;
            }
        }
        for (auto& f : index_info->fields) {
            auto slot_id = state->get_slot_id(scan_node->tuple_id(), f.id);
            if (slot_id > 0) {
                _index_slot_field_map[slot_id] = f.id;
            }
        }
        //如果命中二级索引，需要把filter_node中的条件分为两类，一类是可以在索引中过滤掉的， 一类是必须在主表中过滤的
        std::vector<ExprNode*>* conjuncts = static_cast<FilterNode*>(filter_nodes[0])->mutable_conjuncts();
        conjuncts_copy.swap(*conjuncts);
        for (auto& conjunct : conjuncts_copy) {
            std::unordered_set<int32_t> slot_ids;
            conjunct->get_all_slot_ids(slot_ids);
            bool contained = true;
            for (auto& slot_id : slot_ids) {
                if (_index_slot_field_map.count(slot_id) == 0) {
                    contained = false;
                }
            }
            if (contained) {
                conjuncts_global.push_back(conjunct);
            } else {
                conjuncts_others.push_back(conjunct);
            }
        }
        filter_nodes[0]->mutable_conjuncts()->swap(conjuncts_global);
    }
    //二级索引只执行filter 和 scan_node
    ExecNode* store_exec = _children[0];
    //agg or sort 不在二级索引表上执行
    if (_children[0]->node_type() != pb::SCAN_NODE
        && _children[0]->node_type() != pb::WHERE_FILTER_NODE) {
        store_exec = _children[0]->children(0);
    }
    auto ret = _fetcher_store.run(state, _region_infos, store_exec, client_conn->seq_id, pb::OP_SELECT);
    if (ret < 0) {
        DB_WARNING("select manager fetcher mnager node open fail, txn_id: %lu, log_id:%lu", 
                state->txn_id, state->log_id());
        return ret;
    }
    //重新构造scan_node 的possibble_index
    if (filter_nodes.size() > 0) {
        filter_nodes[0]->mutable_conjuncts()->swap(conjuncts_others);
    }
    ret = construct_primary_possible_index(state, scan_node, main_table_id);
    if (ret < 0) {
        DB_WARNING("construct primary possible index failed");
        return ret;
    }
    ret = _fetcher_store.run(state, _region_infos, _children[0], client_conn->seq_id, pb::OP_SELECT);
    if (filter_nodes.size() > 0) {
        filter_nodes[0]->mutable_conjuncts()->swap(conjuncts_copy);
        conjuncts_global.clear();
        conjuncts_others.clear();
    }
    if (ret < 0) {
        DB_WARNING("select manager fetcher mnager node open fail, txn_id: %lu, log_id:%lu", 
                state->txn_id, state->log_id());
        return ret; 
    }
    return ret;
}

int SelectManagerNode::construct_primary_possible_index(
                      RuntimeState* state,
                      ExecNode* exec_node,
                      int64_t main_table_id) {
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(exec_node);
    int32_t tuple_id = scan_node->tuple_id();
    auto pri_info = _factory->get_index_info_ptr(main_table_id);
    scan_node->clear_possible_indexes();
    pb::ScanNode* pb_scan_node = scan_node->mutable_pb_node()->mutable_derive_node()->mutable_scan_node();
    auto pos_index = pb_scan_node->add_indexes();
    pos_index->set_index_id(main_table_id);
    scan_node->set_router_index_id(main_table_id);
    SmartRecord record_template = _factory->new_record(main_table_id);
    for (auto& pair : _fetcher_store.start_key_sort) {
        auto& batch = _fetcher_store.region_batch[pair.second];
        for (batch->reset(); !batch->is_traverse_over(); batch->next()) {
            std::unique_ptr<MemRow>& mem_row = batch->get_row();
            SmartRecord record = record_template->clone(false);
            for (auto& pri_field : pri_info->fields) {
                int32_t field_id = pri_field.id;
                int32_t slot_id = state->get_slot_id(tuple_id, field_id);
                record->set_value(record->get_field_by_tag(field_id), mem_row->get_value(tuple_id, slot_id));
            }
            auto range = pos_index->add_ranges();
            std::string str;
            record->encode(str);
            range->set_left_pb_record(str);
            range->set_right_pb_record(str);
            range->set_left_field_cnt(pri_info->fields.size());
            range->set_right_field_cnt(pri_info->fields.size());
            range->set_left_open(false);
            range->set_right_open(false);
        }
    }
    //重新做路由选择
    _region_infos.clear();
    return _factory->get_region_by_key(*pri_info, pos_index, _region_infos, 
                static_cast<RocksdbScanNode*>(scan_node)->mutable_region_primary());
}
}
