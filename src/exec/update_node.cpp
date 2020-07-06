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

#include "runtime_state.h"
#include "table_record.h"
#include "update_node.h"

namespace baikaldb {
int UpdateNode::init(const pb::PlanNode& node) { 
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _table_id =  node.derive_node().update_node().table_id();
    _global_index_id = _table_id;
    for (auto& slot : node.derive_node().update_node().primary_slots()) {
        _primary_slots.push_back(slot);
    }
    for (auto& slot : node.derive_node().update_node().update_slots()) {
        _update_slots.push_back(slot);
    }
    for (auto& expr : node.derive_node().update_node().update_exprs()) {
        ExprNode* up_expr = nullptr;
        ret = ExprNode::create_tree(expr, &up_expr);
        if (ret < 0) {
            return ret;
        }
        _update_exprs.push_back(up_expr);
    }
    return 0;
}
int UpdateNode::open(RuntimeState* state) { 
    int num_affected_rows = 0;
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, ([&num_affected_rows](TraceLocalNode& local_node) {
        local_node.set_affect_rows(num_affected_rows);
    }));

    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail, ret:%d", ret);
        return ret;
    }
    if (_is_explain) {
        return 0;
    }
    for (auto expr : _update_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING_STATE(state, "expr open fail, ret:%d", ret);
            return ret;
        }
    }
    ret = init_schema_info(state);
    if (ret == -1) {
        DB_WARNING_STATE(state, "init schema failed fail:%d", ret);
        return ret;
    }
    std::set<int32_t> affect_field_ids;
    for (auto& slot : _update_slots) {
        affect_field_ids.insert(slot.field_id());
    }
    _affect_primary = false;
    //临时存放被影响的index_id
    std::vector<int64_t> affected_indices;
    for (auto index_id : _affected_index_ids) {
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("index info not found index_id:%ld", index_id);
            return -1;
        }
        IndexInfo& info = *info_ptr;
        bool has_id = false;
        for (auto& field : info.fields) {
            if (affect_field_ids.count(field.id) == 1) {
                has_id = true;
                break;
            }
        }
        if (has_id) {
            if (info.id == _table_id) {
                _affect_primary = true;
                break;
            } else {
                affected_indices.push_back(index_id);
            }
        }
    }
    // 如果更新主键，那么影响了全部索引
    if (!_affect_primary) {
        _affected_index_ids.swap(affected_indices);
        // cstore下只更新涉及列
        if (_table_info->engine == pb::ROCKSDB_CSTORE) {
            for (size_t i = 0; i < _update_slots.size(); i++) {
                auto field_id = _update_slots[i].field_id();
                if (_pri_field_ids.count(field_id) == 0 &&
                        _update_field_ids.count(field_id) == 0) {
                    _update_field_ids.insert(field_id);
                }
            }
            _field_ids.clear();
            for (auto index_id : _affected_index_ids) {
                auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
                if (index_info == nullptr) {
                    DB_WARNING("get index info failed index_id: %ld", index_id);
                    return -1;
                }
                for (auto& field_info : index_info->fields) {
                    if (_pri_field_ids.count(field_info.id) == 0) {
                        _field_ids[field_info.id] = &field_info;
                    }
                }
            }
        }
    }
    //_region_id = state->region_id();
    //Transaction* txn = state->txn();
    // ScopeGuard auto_rollback([txn]() {
    //     txn->rollback();
    // });
    bool eos = false;
    AtomicManager<std::atomic<long>> ams[state->reverse_index_map().size()];
    int i = 0;
    for (auto& pair : state->reverse_index_map()) {
        pair.second->sync(ams[i]);
        i++;
    }
    SmartRecord record = _factory->new_record(*_table_info);
    do {
        RowBatch batch;
        ret = _children[0]->get_next(state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING_STATE(state, "children:get_next fail:%d", ret);
            return ret;
        }

        // 不在事务模式下，采用小事务提交防止内存暴涨
        // 最后一个batch和原txn放在一起，这样对小事务可以和原来保持一致
        if (state->txn_id == 0 && !eos) {
            _txn = state->create_batch_txn();
        } else {
            _txn = state->txn();
        }

        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            record->clear();
            //SmartRecord record = record_template->clone(false);
            for (auto slot : _primary_slots) {
                record->set_value(record->get_field_by_tag(slot.field_id()), 
                        row->get_value(slot.tuple_id(), slot.slot_id()).cast_to(slot.slot_type()));
            }
            ret = update_row(state, record, row);
            if (ret < 0) {
                DB_WARNING_STATE(state, "insert_row fail");
                return -1;
            }
            num_affected_rows += ret;
        }

        if (state->need_txn_limit) {
            bool is_limit = TxnLimitMap::get_instance()->check_txn_limit(state->txn_id, batch.size());
            if (is_limit) {
                DB_FATAL("Transaction too big, region_id:%ld, txn_id:%ld, txn_size:%d", 
                    state->region_id(), state->txn_id, batch.size());
                return -1;
            }
        }

        if (state->txn_id == 0 && !eos) {
            if (state->is_separate) {
                //batch单独走raft,raft on_apply中commit
                state->raft_func(state, _txn);
            } else {
                _txn->commit();                
            }
        }
    } while (!eos);
    return num_affected_rows;
}

void UpdateNode::close(RuntimeState* state) {
    ExecNode::close(state);
    for (auto expr : _update_exprs) {
        expr->close();
    }
}

void UpdateNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto update_node = pb_node->mutable_derive_node()->mutable_update_node();
    update_node->clear_update_exprs();
    for (auto expr : _update_exprs) {
        ExprNode::create_pb_expr(update_node->add_update_exprs(), expr);
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
