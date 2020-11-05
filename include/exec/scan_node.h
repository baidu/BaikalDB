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
#include <boost/variant.hpp>

namespace baikaldb {

template<typename NodeType, typename LeafType>
struct FulltextNode {
    struct FulltextChildType {
        std::vector<std::shared_ptr<FulltextNode>> children;
    };
    using LeafNodeType = LeafType;
    boost::variant<FulltextChildType, LeafType> info;
    NodeType type;
};

template<typename NodeType, typename LeafType>
struct FulltextTree {
    std::shared_ptr<FulltextNode<NodeType, LeafType>> root;
};

using IndexFieldRange = std::pair<int64_t, range::FieldRange>;

using FulltextInfoTree = FulltextTree<pb::FulltextNodeType, IndexFieldRange>;
using FulltextInfoNode = FulltextNode<pb::FulltextNodeType, IndexFieldRange>;

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

    void show_cost(std::vector<std::map<std::string, std::string>>& path_infos);
    int64_t select_index(); 
    int64_t select_index_by_cost(); 
    int64_t select_index_in_baikaldb(const std::string& sample_sql);
    int choose_arrow_pb_reverse_index();
    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output);

    void add_access_path(const SmartPath& access_path) {
        //如果使用倒排索引，则不使用代价进行索引选择
        if ((access_path->index_type == pb::I_FULLTEXT || access_path->index_type == pb::I_RECOMMEND) 
            && access_path->is_possible) {
            _use_fulltext = true;
        }
        _paths[access_path->index_id] = access_path;
    }

    size_t access_path_size() const {
        return _paths.size();
    }

    bool full_coverage(const std::unordered_set<int32_t>& smaller, const std::unordered_set<int32_t>& bigger);

    int compare_two_path(SmartPath outer_path, SmartPath inner_path);

    void inner_loop_and_compare(std::map<int64_t, SmartPath>::iterator outer_loop_iter);

    void set_fulltext_index_tree(const FulltextInfoTree& tree) {
        _fulltext_index_tree = tree;
    }

    int create_fulltext_index_tree(FulltextInfoNode* node, pb::FulltextIndex* root);
    int create_fulltext_index_tree();

// 两两比较，根据一些简单规则干掉次优索引
    int64_t pre_process_select_index();
    
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
    bool _use_fulltext = false;
    int32_t _possible_index_cnt = 0;
    int32_t _cover_index_cnt = 0;
    std::map<int32_t, double> _filed_selectiy; //缓存，避免重复的filed_id多次调用代价接口
    FulltextInfoTree _fulltext_index_tree; //目前只有两层，第一层为and，第二层为or。
    std::unique_ptr<pb::FulltextIndex> _fulltext_index_pb;
    bool _fulltext_use_arrow = false;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
