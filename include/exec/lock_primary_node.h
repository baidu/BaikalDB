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

// Brief:  truncate table exec node
#pragma once

#include "dml_node.h"

namespace baikaldb {
DECLARE_bool(check_condition_again_for_global_index);

class LockPrimaryNode : public DMLNode {
public:
    LockPrimaryNode() {}
    virtual ~LockPrimaryNode() {
        if(_conjuncts_need_destory) {
            for (auto conjunct : _conjuncts) {
                ExprNode::destroy_tree(conjunct);
            }
        }
    }
    virtual int init(const pb::PlanNode& pb_node);
    virtual int open(RuntimeState* state);
    virtual void reset(RuntimeState* state);
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    bool check_satisfy_condition(MemRow* row) override {
        if (!need_copy(row)) {
            return false;
        }
        return true;
    }
    inline bool need_copy(MemRow* row) {
        for (auto conjunct : _conjuncts) {
            ExprValue value = conjunct->get_value(row);
            if (value.is_null() || value.get_numberic<bool>() == false) {
                return false;
            }
        }
        return true;
    }
    void add_conjunct(ExprNode* conjunct) {
        if (FLAGS_check_condition_again_for_global_index) {
            _conjuncts.push_back(conjunct);
        }
    }

    void set_affected_index_ids(const std::vector<int64_t>& ids) {
        _affected_index_ids = ids;
    }
private:
    int lock_get_main_table(RuntimeState* state, SmartRecord record);
    int put_row(RuntimeState* state, SmartRecord record);
    std::vector<int64_t>     _affected_index_ids;
    std::vector<ExprNode*> _conjuncts;
    bool _conjuncts_need_destory = false;
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
