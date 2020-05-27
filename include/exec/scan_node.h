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

#pragma once

#include "exec_node.h"
#include "access_path.h"
#include "table_record.h"

namespace baikaldb {
class ScanNode : public ExecNode {
public:
    ScanNode() {
    }
    ScanNode(pb::Engine engine): _engine(engine) {
    }
    virtual ~ScanNode() {
    }
    static ScanNode* create_scan_node(const pb::PlanNode& node);
    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);
    virtual void close(RuntimeState* state);
    int64_t table_id() const {
        return _table_id;
    }
    int32_t tuple_id() const {
        return _tuple_id;
    }
    void set_router_index_id(int64_t router_index_id) {
        _router_index_id = router_index_id;
    }
    int64_t router_index_id() const {
        return _router_index_id;
    }
    void set_covering_index(bool covering_index) {
        _is_covering_index = covering_index;
    }
    bool covering_index() const {
        return _is_covering_index;
    }
    pb::PossibleIndex* router_index() const {
        return _router_index;
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
                if (pos_index.has_sort_index()) {
                    return true;
                }
            }
        }
        return false;
    }
    void clear_possible_indexes() {
        _paths.clear();
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

    int64_t select_index(); 
    int64_t select_index_by_cost(); 
    int64_t select_index_in_baikaldb();
    int choose_arrow_pb_reverse_index();
    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output);

    void add_access_path(const SmartPath& access_path) {
        _paths[access_path->index_id] = access_path;
    }

    size_t access_path_size() const {
        return _paths.size();
    }
    
protected:
    pb::Engine _engine = pb::ROCKSDB;
    int32_t _tuple_id = 0;
    int64_t _table_id = -1;
    int64_t _router_index_id = -1;
    std::map<int64_t, SmartPath> _paths;
    std::vector<int64_t> _multi_reverse_index;
    pb::TupleDescriptor* _tuple_desc = nullptr;
    pb::PossibleIndex* _router_index = nullptr;
    bool _is_covering_index = true;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
