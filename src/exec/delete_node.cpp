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

#include "runtime_state.h"
#include "delete_node.h"

namespace baikaldb {
DECLARE_bool(disable_writebatch_index);
int DeleteNode::init(const pb::PlanNode& node) { 
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _table_id =  node.derive_node().delete_node().table_id();
    for (auto& slot : node.derive_node().delete_node().primary_slots()) {
        _primary_slots.push_back(slot);
    }
    if (nullptr == (_factory = SchemaFactory::get_instance())) {
        DB_WARNING("get record encoder failed");
        return -1;
    }
    return 0;
}

int DeleteNode::open(RuntimeState* state) { 
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING("ExecNode::open fail:%d", ret);
        return ret;
    }
    ret = init_schema_info(state);
    if (ret == -1) {
        DB_WARNING("init schema failed fail:%d", ret);
        return ret;
    }
    Transaction* txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING("txn is nullptr: region:%ld", _region_id);
        return -1;
    }
    if (FLAGS_disable_writebatch_index) {
        txn->get_txn()->DisableIndexing();
    }

    bool eos = false;
    int num_affected_rows = 0;
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
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            //SmartRecord record = record_template->clone(false);
            record->clear();
            //获取主键信息
            for (auto slot : _primary_slots) {
                record->set_value(record->get_field_by_tag(slot.field_id()), 
                        row->get_value(slot.tuple_id(), slot.slot_id()));
            }
            ret = delete_row(state, record);
            if (ret < 0) {
                DB_WARNING("delete_row fail");
                return -1;
            }
            num_affected_rows += ret;
        }
    } while (!eos);
    // auto_rollback.release();
    // txn->commit();
    state->set_num_increase_rows(_num_increase_rows);
    return num_affected_rows;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
