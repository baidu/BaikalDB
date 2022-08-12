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
// Brief:  update table exec node

#pragma once

#include "dml_manager_node.h"
#include "binlog_context.h"

namespace baikaldb {
class UpdateNode;

class UpdateManagerNode : public DmlManagerNode {
public:
    UpdateManagerNode() {
    }
    virtual ~UpdateManagerNode() {
        for (auto expr : _update_exprs) {
             ExprNode::destroy_tree(expr);
        }
    }

    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;

    int init_update_info(UpdateNode* update_node);
    bool affect_global_index() {
        return _affect_global_index;
    }
    const std::vector<int64_t>& global_affected_index_ids() {
        return _global_affected_index_ids;
    }
    const std::vector<int64_t>& local_affected_index_ids() {
        return _local_affected_index_ids;
    }
    bool affect_primary() {
        return _affect_primary;
    }
    int64_t table_id() {
        return _table_id;
    }
    void set_update_exprs(std::vector<ExprNode*>& update_exprs) {
        _update_exprs = update_exprs;
    }

    int process_binlog(RuntimeState* state, bool is_local);
    void update_record(RuntimeState* state, SmartRecord record);

private:
    int64_t _table_id = -1;
    std::vector<pb::SlotDescriptor> _primary_slots;
    std::vector<pb::SlotDescriptor> _update_slots;
    std::vector<ExprNode*> _update_exprs;
    pb::TupleDescriptor* _tuple_desc = nullptr;
    std::unique_ptr<MemRow> _update_row;
    bool _affect_primary = true;
    std::vector<int64_t>     _global_affected_index_ids;
    std::vector<int64_t>     _local_affected_index_ids;
    SmartTable               _table_info;
    bool _affect_global_index = false;
    pb::TableMutation     _update_binlog;
    SmartRecord _partition_record;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
