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
#include "ttl_delete_node.h"

namespace baikaldb {

int TTLDeleteNode::init(const pb::PlanNode& node) {
    _table_id =  node.derive_node().delete_node().table_id();
    _global_index_id = _table_id;
    if (nullptr == (_factory = SchemaFactory::get_instance())) {
        DB_WARNING("get record encoder failed");
        return -1;
    }
    _node_type = pb::DELETE_NODE;
    return 0;
}

int TTLDeleteNode::open(RuntimeState* state) {
    int num_affected_rows = 0;
    ScopeGuard clear_guard([this](){ clean_delete_records(); });

    _txn = state->txn();
    bool need_rollback = true;
    ScopeGuard auto_rollback([&need_rollback, this]() {
        if (need_rollback) {
            _txn->rollback();
        }
    });

    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    ret = init_schema_info(state);
    if (ret == -1) {
        DB_WARNING_STATE(state, "init schema failed fail:%d", ret);
        return ret;
    }
    auto txn = state->txn();
    if (txn == nullptr) {
        DB_WARNING_STATE(state, "txn is nullptr: region:%ld", _region_id);
        return -1;
    }

    for (auto& record: _delete_records) {
        int ret = 0;
        MutTableKey pk_key;
        ret = record->encode_key(*_pri_info, pk_key, -1, false);
        if (ret < 0) {
            DB_WARNING_STATE(state, "encode key failed, ret:%d", ret);
            return ret;
        }
        ret = remove_row(state, record, pk_key.data(), true);
        if (ret < 0) {
            DB_WARNING_STATE(state, "delete_row fail");
            return -1;
        }
        num_affected_rows += ret;
    }
    auto s = _txn->commit();
    if (!s.ok()) {
        DB_FATAL("TTL delete rows commit failed, status: %s", s.getState());
        return -1;
    }
    need_rollback = false;
    return num_affected_rows;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
