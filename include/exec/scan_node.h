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

#pragma once

#include "exec_node.h"
#include "table_record.h"

namespace baikaldb {
class ScanNode : public ExecNode {
public:
    ScanNode() {
    }
    virtual ~ScanNode() {
    }
    static ScanNode* create_scan_node(const pb::PlanNode& node);
    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);
    virtual void close(RuntimeState* state);
    int64_t table_id() {
        return _table_id;
    }
    int32_t tuple_id() {
        return _tuple_id;
    }
    pb::Engine engine() {
        return _engine;
    }
    bool has_index() {
        for (auto& pos_index : _pb_node.derive_node().scan_node().indexes()) {
            for (auto& range : pos_index.ranges()) {
                if (range.left_field_cnt() > 0) {
                    return true;
                }
                if (range.right_field_cnt() > 0) {
                    return true;
                }
            }
        }
        return false;
    }
    void clear_possible_indexes() {
        _pb_node.mutable_derive_node()->mutable_scan_node()->clear_indexes();
    }
    bool need_copy(MemRow* row, std::vector<ExprNode*>& conjuncts) {
        for (auto conjunct : conjuncts) {
            ExprValue value = conjunct->get_value(row);
            if (value.is_null() || value.get_numberic<bool>() == false) {
                return false;
            }
        }
        return true;
    }
    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders) {
        ExecNode::find_place_holder(placeholders);
    }

protected:
    pb::Engine _engine = pb::ROCKSDB;
    int32_t _tuple_id = 0;
    int64_t _table_id = -1;
    pb::TupleDescriptor* _tuple_desc;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
