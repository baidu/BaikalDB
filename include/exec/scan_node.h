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

enum class RouterPolicy : int8_t {
    RP_RANGE = 0,
    RP_REGION = 1
};

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

class AccessPathMgr {
public:
    AccessPathMgr() { }
    ~AccessPathMgr() { }

    int init(int64_t table_id) {
        _table_id = table_id;
        return 0;
    }

    void add_access_path(const SmartPath& access_path) {
        //如果使用倒排索引，则不使用代价进行索引选择
        if ((access_path->index_type == pb::I_FULLTEXT) 
            && access_path->is_possible) {
            _use_fulltext = true;
        }

        if (access_path->index_info_ptr->index_hint_status == pb::IHS_DISABLE 
            && access_path->is_possible) {
            _has_disable_index = true;
        }

        if (access_path->hint == AccessPath::FORCE_INDEX 
            && access_path->is_possible) {
            _use_force_index = true;
        }

        if (access_path->is_possible) {
            _possible_indexs.insert(access_path->index_id);
        }

        _paths[access_path->index_id] = access_path;
    }

    void delete_possible_index(int64_t index_id) {
        _possible_indexs.erase(index_id);
    }

    void reset() {
        _use_fulltext = false;
        _has_disable_index = false;
        _use_force_index = false;
        // 重置is_possible状态，只有此状态会在预处理时修改
        for (auto& pair : _paths) {
            auto& path = pair.second;
            path->is_possible = false;
            if (_possible_indexs.count(path->index_id) > 0) {
                path->is_possible = true;
            }

            if ((path->index_type == pb::I_FULLTEXT) 
                && path->is_possible) {
                _use_fulltext = true;
            }

            if (path->index_info_ptr->index_hint_status == pb::IHS_DISABLE 
                && path->is_possible) {
                _has_disable_index = true;
            }

            if (path->hint == AccessPath::FORCE_INDEX 
                && path->is_possible) {
                _use_force_index = true;
            }
        }
        _multi_reverse_index.clear();
        _possible_index_cnt = 0;
        _cover_index_cnt = 0;
        _fulltext_use_arrow = false;
    }

    void clear() {
        _possible_indexs.clear();
        _paths.clear();
        _multi_reverse_index.clear();
        _use_fulltext = false;
        _possible_index_cnt = 0;
        _cover_index_cnt = 0;
        _fulltext_use_arrow = false;
    }

    std::map<int64_t, SmartPath>& paths() {
        return _paths;
    }

    SmartPath path(int64_t index_id) {
        return _paths[index_id];
    }

    std::vector<int64_t> multi_reverse_index() {
        return _multi_reverse_index;
    }

    bool fulltext_use_arrow() const {
        return _fulltext_use_arrow;
    }

    bool has_disable_index() const {
        return _has_disable_index;
    }

    void show_cost(std::vector<std::map<std::string, std::string>>& path_infos);

    int64_t select_index(); 
private:
    int compare_two_path(SmartPath& outer_path, SmartPath& inner_path);
    void inner_loop_and_compare(std::map<int64_t, SmartPath>::iterator outer_loop_iter);
    // 两两比较，根据一些简单规则干掉次优索引
    int64_t pre_process_select_index();

    int choose_arrow_pb_reverse_index();

    int64_t select_index_common(); 
    int64_t select_index_by_cost();
private:
    std::set<int64_t> _possible_indexs; // reset时，重置possible的index
    std::map<int64_t, SmartPath> _paths;
    std::vector<int64_t> _multi_reverse_index;
    std::map<int32_t, double> _filed_selectiy; //缓存，避免重复的filed_id多次调用代价接口
    int64_t _table_id = 0;
    bool _use_fulltext = false;
    bool _has_disable_index = false;
    bool _use_force_index = false;
    int32_t _possible_index_cnt = 0;
    int32_t _cover_index_cnt = 0;
    bool _fulltext_use_arrow = false;
};

struct ScanIndexInfo {
    enum IndexUseFor {
        U_INIT = 0,
        U_GLOBAL_LEARNER, // 
        U_LOCAL_LEARNER
    };
    bool covering_index = false;
    IndexUseFor use_for = U_INIT;
    int64_t router_index_id = 0;
    int64_t index_id = 0;
    pb::PossibleIndex* router_index = nullptr;
    std::string raw_index;
    std::map<int64_t, pb::RegionInfo> region_infos;
    std::map<int64_t, std::string> region_primary;
};

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

    void set_covering_index(bool covering_index) {
        _is_covering_index = covering_index;
    }

    void set_router_policy(RouterPolicy rp) {
        _router_policy = rp;
    }

    RouterPolicy router_policy() const {
        return _router_policy;
    }

    const google::protobuf::RepeatedPtrField<pb::RegionInfo>& old_region_infos() {
        return _old_region_infos;
    }
    void set_old_region_infos(google::protobuf::RepeatedPtrField<pb::RegionInfo>&& region_infos) {
        _old_region_infos = region_infos;
    }
    pb::Engine engine() {
        return _engine;
    }

    bool has_index() const {
        return _has_index;
    }

    std::vector<ScanIndexInfo>& scan_indexs() {
        return _scan_indexs;
    }

    ScanIndexInfo* main_scan_index() {
        if (_scan_indexs.empty()) {
            return nullptr;
        }
        return &_scan_indexs[0];
    }

    ScanIndexInfo* backup_scan_index() {
        if (_scan_indexs.empty()) {
            return nullptr;
        }
        for (auto& scan_index_info : _scan_indexs) {
            if (scan_index_info.use_for == ScanIndexInfo::U_GLOBAL_LEARNER) {
                return &scan_index_info;
            }
        }
        return nullptr;
    }

    void serialize_index_and_set_router_index(const pb::PossibleIndex& pos_index, pb::PossibleIndex* router_index, bool covering_index,
                ScanIndexInfo::IndexUseFor use_for = ScanIndexInfo::U_INIT) {
        ScanIndexInfo index;
        pos_index.SerializeToString(&index.raw_index);
        index.covering_index = covering_index;
        index.router_index_id = router_index->index_id();
        index.router_index = router_index;
        index.index_id = pos_index.index_id();
        index.use_for = use_for;
        _scan_indexs.emplace_back(std::move(index));
        bool has_index = false;
        for (auto& range : pos_index.ranges()) {
            if (range.left_field_cnt() > 0) {
                has_index = true;
                break;
            }
            if (range.right_field_cnt() > 0) {
                has_index = true;
                break;
            }
            if (pos_index.has_sort_index()) {
                has_index = true;
                break;
            }
        }
        _has_index = _has_index || has_index;
    }

    void clear_possible_indexes() {
        _main_path.clear();
        _learner_path.clear();
        _scan_indexs.clear();
        _pb_node.mutable_derive_node()->mutable_scan_node()->clear_indexes();
        _pb_node.mutable_derive_node()->mutable_scan_node()->clear_learner_index();
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

    void show_cost(std::vector<std::map<std::string, std::string>>& path_infos) {
        _main_path.show_cost(path_infos);
    }

    void add_access_path(const SmartPath& access_path) {
        if (access_path->index_info_ptr->index_hint_status != pb::IHS_DISABLE) {
            //disable之后不用于主集群选索引
            _main_path.add_access_path(access_path);
        }
        if (access_path->need_add_to_learner_paths()) {
            _learner_path.add_access_path(access_path);
        }
    }

    int64_t select_index_in_baikaldb(const std::string& sample_sql);
    
    virtual void show_explain(std::vector<std::map<std::string, std::string>>& output);

    void set_fulltext_index_tree(const FulltextInfoTree& tree) {
        _fulltext_index_tree = tree;
    }

    int create_fulltext_index_tree(FulltextInfoNode* node, pb::FulltextIndex* root);
    int create_fulltext_index_tree();
    bool learner_use_diff_index() const {
        return _learner_use_diff_index;
    }

    void set_index_useage_and_lock(bool use_global_backup) {
        // 只有在存在global backup的时候才加锁
        for (auto& scan_index_info : _scan_indexs) {
            if (scan_index_info.use_for == ScanIndexInfo::U_GLOBAL_LEARNER) {
                _current_index_mutex.lock();
                _current_global_backup = use_global_backup;
                break;
            }
        }
    }

    void current_index_unlock() {
        for (auto& scan_index_info : _scan_indexs) {
            if (scan_index_info.use_for == ScanIndexInfo::U_GLOBAL_LEARNER) {
                _current_index_mutex.unlock();
                break;
            }
        }
    }

    bool current_use_global_backup() const {
        return _current_global_backup;
    }

    void set_expr_field_map(std::map<ExprNode*, std::unordered_set<int32_t>> * expr_field_map) {
        _expr_field_map = expr_field_map;
    }
    void calc_index_range() {
        if (_select_idx != -1) {
            _main_path.path(_select_idx)->calc_index_range();
            if (_expr_field_map != nullptr) {
                _main_path.path(_select_idx)->insert_no_cut_condition(*_expr_field_map);
            }
        }
        if (_main_path.path(_select_idx)->index_info_ptr->is_global) {
            _main_path.path(_table_id)->calc_index_range();
        }
    }
protected:
    pb::Engine _engine = pb::ROCKSDB;
    int32_t _tuple_id = 0;
    int64_t _table_id = -1;
    AccessPathMgr _main_path;    //主集群索引选择
    AccessPathMgr _learner_path; //learner集群索引选择
    bool _learner_use_diff_index = false;
    pb::TupleDescriptor* _tuple_desc = nullptr;
    bool _is_covering_index = true; // 只有store会用
    bool _has_index = false;
    pb::LockCmdType _lock = pb::LOCK_NO;
    RouterPolicy _router_policy = RouterPolicy::RP_RANGE;
    google::protobuf::RepeatedPtrField<pb::RegionInfo> _old_region_infos;
    FulltextInfoTree _fulltext_index_tree; //目前只有两层，第一层为and，第二层为or。
    std::unique_ptr<pb::FulltextIndex> _fulltext_index_pb;
    int64_t _select_idx = -1;

    std::vector<ScanIndexInfo> _scan_indexs;
    bthread::Mutex _current_index_mutex;
    bool _current_global_backup = false;
    std::map<ExprNode*, std::unordered_set<int32_t>>* _expr_field_map = nullptr;
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
