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

#include <map>
#include "scan_node.h"
#include "filter_node.h"
#include "join_node.h"
#include "schema_factory.h"
#include "scalar_fn_call.h"
#include "slot_ref.h"
#include "runtime_state.h"
#include "rocksdb_scan_node.h"
#include "information_schema_scan_node.h"
#include "redis_scan_node.h"

namespace baikaldb {
int64_t AccessPathMgr::select_index_common() {
    std::multimap<uint32_t, int64_t> prefix_ratio_id_mapping;
    std::unordered_set<int32_t> primary_fields;
    primary_fields = _paths[_table_id]->hit_index_field_ids;
    if (!_multi_reverse_index.empty()) {
        // join情况下被驱动表可能会多次执行选择索引函数，每次执行前先清空，避免_multi_reverse_index push多次造成可使用multi倒排假象
        _multi_reverse_index.clear();
        _fulltext_use_arrow = false;
    }
    
    for (auto& pair : _paths) {
        int64_t index_id = pair.first;
        const auto& path = pair.second;
        const auto& pos_index = path->pos_index;
        const auto info_ptr = path->index_info_ptr;
        if (info_ptr == nullptr) {
            continue;
        }
        IndexInfo& info = *info_ptr;
        if (!path->is_possible && info.type != pb::I_PRIMARY) {
            continue;
        }
        auto index_state = info.state;
        if (index_state != pb::IS_PUBLIC) {
            DB_DEBUG("DDL_LOG skip index [%ld] state [%s] ", 
                index_id, pb::IndexState_Name(index_state).c_str());
            continue;
        }

        int field_count = path->hit_index_field_ids.size();
        if (info.fields.size() == 0) {
            continue;
        }
        uint16_t prefix_ratio_round = field_count * 100 / info.fields.size();
        uint16_t index_priority = 0;
        if (info.type == pb::I_PRIMARY) {
            index_priority = 300;
        } else if (info.type == pb::I_UNIQ) {
            index_priority = 200;
        } else if (info.type == pb::I_KEY) {
            index_priority = 100 + field_count;
        } else {
            index_priority = 0;
        }
        // sort index 权重调整到全命中unique或primary索引之后
        if (pos_index.has_sort_index() && field_count > 0) {
            prefix_ratio_round = 100;
            index_priority = 150 + field_count;
        } else if (pos_index.has_sort_index() && pos_index.sort_index().sort_limit() != -1) {
            index_priority = 400;
        }
        uint32_t prefix_ratio_index_score = (prefix_ratio_round << 16) | index_priority;
        // ignore index用到最低优先级，其实只有primary会走到这里
        if (path->hint == AccessPath::IGNORE_INDEX) {
            prefix_ratio_index_score = 0;
        }
        // force index情况下非FORCE_INDEX优先级最低, 其实只有primary会走到这里
        if (_use_force_index && path->hint != AccessPath::FORCE_INDEX) {
            prefix_ratio_index_score = 0;
        }
        prefix_ratio_id_mapping.insert(std::make_pair(prefix_ratio_index_score, index_id));
        //DB_NOTICE("index_id:%ld prefix_ratio_index_score:%u", index_id,prefix_ratio_index_score);
        // 优先选倒排，没有就取第一个
        switch (info.type) {
            case pb::I_FULLTEXT:
                _multi_reverse_index.push_back(index_id);
                break;
            default:
                break;
        }
    }
    if (choose_arrow_pb_reverse_index() != 0) {
        DB_WARNING("choose arrow pb reverse index error.");
        return -1;
    }
    // ratio * 10(=0...9)相同的possible index中，按照PRIMARY, UNIQUE, KEY的优先级选择
    for (auto iter = prefix_ratio_id_mapping.crbegin(); iter != prefix_ratio_id_mapping.crend(); ++iter) {
        return iter->second;
    }
    if (_multi_reverse_index.size() > 0) {
        return _multi_reverse_index[0];
    }
    return _table_id;
}

int ScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _tuple_id = node.derive_node().scan_node().tuple_id();
    _table_id = node.derive_node().scan_node().table_id();
    _lock = node.derive_node().scan_node().lock();
    if (node.derive_node().scan_node().has_engine()) {
        _engine = node.derive_node().scan_node().engine();
    }
    _main_path.init(_table_id);
    _learner_path.init(_table_id);
    return 0;
}

int ScanNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    _tuple_desc = state->get_tuple_desc(_tuple_id);
    state->tuple_id = _tuple_id;
    if (_tuple_desc == nullptr) {
        return -1;
    }
    return 0;
}

void ScanNode::close(RuntimeState* state) {
    ExecNode::close(state);
    clear_possible_indexes();
}
void ScanNode::show_explain(std::vector<std::map<std::string, std::string>>& output) {
    std::map<std::string, std::string> explain_info = {
        {"id", "1"},
        {"select_type", "SIMPLE"},
        {"table", "NULL"},
        {"type", "NULL"},
        {"possible_keys", "NULL"},
        {"key", "NULL"},
        {"key_len", "NULL"},
        {"ref", "NULL"},
        {"rows", "NULL"},
        {"Extra", ""},
        {"sort_index", "0"}
    };
    auto factory = SchemaFactory::get_instance();
    explain_info["table"] = factory->get_table_info(_table_id).name;
    if (!has_index() && _scan_indexs.empty()) {
        explain_info["type"] = "ALL";
    } else {
        explain_info["possible_keys"] = "";
        for (auto& pair : _main_path.paths()) {
            auto& path = pair.second;
            if (path->is_possible) {
                int64_t index_id = path->index_id;
                explain_info["possible_keys"] += factory->get_index_info(index_id).short_name;
                explain_info["possible_keys"] += ",";
            }
        }
        if (!explain_info["possible_keys"].empty()) {
            explain_info["possible_keys"].pop_back();
        }
        std::string tmp;
        //int64_t index_id = select_index_in_baikaldb(tmp);
        int64_t index_id = _select_idx;
        auto index_info = factory->get_index_info(index_id);
        auto pri_info = factory->get_index_info(_table_id);
        explain_info["key"] = index_info.short_name;
        explain_info["type"] = "range";
        auto& pos_index = _main_path.path(index_id)->pos_index;
        if (pos_index.ranges_size() == 1) {
            int field_cnt = pos_index.ranges(0).left_field_cnt();
            if (field_cnt == (int)index_info.fields.size() && 
                    pos_index.ranges(0).left_pb_record() == pos_index.ranges(0).right_pb_record()) {
                explain_info["type"] = "eq_ref";
                if (index_info.type == pb::I_UNIQ || index_info.type == pb::I_PRIMARY) {
                    explain_info["type"] = "const";
                }
            }
        }
        if (pos_index.has_sort_index()) {
            explain_info["sort_index"] = "1";
        }
        std::set<int32_t> field_map;
        for (auto& f : pri_info.fields) {
            field_map.insert(f.id);
        }
        if (index_info.type == pb::I_KEY || index_info.type == pb::I_UNIQ) {
            for (auto& f : index_info.fields) {
                field_map.insert(f.id);
            }
        }
        if (_tuple_desc != nullptr) {
            bool is_covering_index = true;
            for (auto slot : _tuple_desc->slots()) {
                if (field_map.count(slot.field_id()) == 0) {
                    is_covering_index = false;
                    break;
                }
            }
            if (is_covering_index) {
                explain_info["Extra"] = "Using index;";
            }
        }
    }
    output.push_back(explain_info);
}

void AccessPathMgr::show_cost(std::vector<std::map<std::string, std::string>>& path_infos) {
    for (auto& pair : _paths) {
        auto& path = pair.second;
        std::map<std::string, std::string> path_info;
        if (path->is_possible || path->index_type == pb::I_PRIMARY) {
            path->show_cost(&path_info, _filed_selectiy);
            path_infos.push_back(path_info);
        }
    }
}

int64_t AccessPathMgr::select_index_by_cost() {
    double min_cost = DBL_MAX;
    int64_t min_idx = 0;
    bool multi_0_0 = false;
    bool multi_1_0 = false;
    bool enable_use_cost = true;
    // 优化：只有一个possible时直接使用
    for (auto& pair : _paths) {
        auto& path = pair.second;
        if (!path->is_possible && path->index_type == pb::I_PRIMARY) {
            if (_possible_index_cnt > 0 && _cover_index_cnt > 0) {
                // 如果primary_key非possible，并且有其他索引可选时，跳过primary key
                continue;
            }
        } else if (!path->is_possible) {
            continue;
        }
        // _use_force_index时, 只有primary可能为非FORCE_INDEX，直接跳过
        if (_use_force_index && path->hint != AccessPath::FORCE_INDEX) {
            continue;
        }

        int64_t index_id = pair.first;
        path->calc_cost(nullptr, _filed_selectiy);
        if (path->cost < min_cost) {
            min_cost = path->cost;
            min_idx = index_id;
        }
        if (float_equal(path->selectivity, 0.0)) {
            if (multi_0_0) {
                enable_use_cost = false;
                break;
            } else {
                multi_0_0 = true;
            }
        } else if (float_equal(path->selectivity, 1.0)) {
            if (multi_1_0) {
                enable_use_cost = false;
                break;
            } else {
                multi_1_0 = true;
            }
        }
        DB_DEBUG("idx:%ld cost:%f", index_id, path->cost);
    }
    //兜底方案，如果出现多个0.0或者1.0可能代价计算有问题，使用基于规则的索引选择
    if (enable_use_cost) {
        return min_idx;
    } else {
        return select_index_common();
    }
}

// 判断是否全覆盖，只有被全覆盖的索引可以被干掉
bool full_coverage(const std::unordered_set<int32_t>& smaller, const std::unordered_set<int32_t>& bigger) {
    for (int32_t field_id : smaller) {
        if (bigger.count(field_id) == 0) {
            return false;
        }
    }

    return true;
}

// 两个索引比较择最优的，干掉另一个
// return  0 没有干掉任何一个
// return -1 outer被干掉
// return -2 inner被干掉
int AccessPathMgr::compare_two_path(SmartPath& outer_path, SmartPath& inner_path) {
    if (_use_force_index) {
        // 只有primary可能不是FORCE_INDEX, 应该防止primary干掉force index
        // @ref: https://dev.mysql.com/doc/refman/5.6/en/index-hints.html
        if (outer_path->hint != inner_path->hint) {
            return 0;
        }
    }
    // 主键ignore，靠select那边来按最低优先级处理
    if (outer_path->hint == AccessPath::IGNORE_INDEX || inner_path->hint == AccessPath::IGNORE_INDEX) {
        return 0;
    }
    if (outer_path->hit_index_field_ids.size() == inner_path->hit_index_field_ids.size()) {
        if (!full_coverage(outer_path->hit_index_field_ids, inner_path->hit_index_field_ids)) {
            return 0;
        }

        if (outer_path->is_cover_index() == inner_path->is_cover_index() &&
            outer_path->is_sort_index == inner_path->is_sort_index) {
            if (outer_path->index_other_condition.size() > 
                inner_path->index_other_condition.size()) {
                inner_path->is_possible = false;
                return -2;
            } else if (outer_path->index_other_condition.size() < 
                    inner_path->index_other_condition.size()) {
                outer_path->is_possible = false;
                return -1;
            } else {
                if (outer_path->index_type == pb::I_PRIMARY) {
                    outer_path->is_possible = false;
                    return -1;
                } else if (outer_path->index_info_ptr->is_global != inner_path->index_info_ptr->is_global) {
                    //优先选global，防止广播
                    if (inner_path->index_info_ptr->is_global) {
                        outer_path->is_possible = false;
                        return -1;
                    } else {
                        inner_path->is_possible = false;
                        return -2;
                    }
                } else if (outer_path->index_info_ptr->length > 0 
                        && inner_path->index_info_ptr->length > 0) {
                    if (outer_path->index_info_ptr->length > inner_path->index_info_ptr->length) {
                        outer_path->is_possible = false;
                        return -1;
                    } else {
                        inner_path->is_possible = false;
                        return -2;
                    }
                } else if (outer_path->cover_field_ids.size() > inner_path->cover_field_ids.size()) {
                    outer_path->is_possible = false;
                    return -1;
                } else {
                    inner_path->is_possible = false;
                    return -2;
                }
            }
        }

        if (!outer_path->is_cover_index() && !outer_path->is_sort_index) {
            outer_path->is_possible = false;
            return -1;
        }

        if (!inner_path->is_cover_index() && !inner_path->is_sort_index) {
            inner_path->is_possible = false;
            return -2;
        }
    } else if (outer_path->hit_index_field_ids.size() < inner_path->hit_index_field_ids.size()) {
        if (!full_coverage(outer_path->hit_index_field_ids, inner_path->hit_index_field_ids)) {
            return 0;
        }

        if (!outer_path->is_cover_index() && !outer_path->is_sort_index) {
            outer_path->is_possible = false;
            return -1;
        }

        if (outer_path->is_cover_index() == inner_path->is_cover_index() &&
            outer_path->is_sort_index == inner_path->is_sort_index) {
            outer_path->is_possible = false;
            return -1;
        }
    } else {
        if (!full_coverage(inner_path->hit_index_field_ids, outer_path->hit_index_field_ids)) {
            return 0;
        }

        if (!inner_path->is_cover_index() && !inner_path->is_sort_index) {
            inner_path->is_possible = false;
            return -2;
        }

        if (outer_path->is_cover_index() == inner_path->is_cover_index() &&
            outer_path->is_sort_index == inner_path->is_sort_index) {
            inner_path->is_possible = false;
            return -2;
        }
    }

    return 0;
}

void AccessPathMgr::inner_loop_and_compare(std::map<int64_t, SmartPath>::iterator outer_loop_iter) {
    auto inner_loop_iter = outer_loop_iter;
    while ((++inner_loop_iter) != _paths.end()) {
        if (!inner_loop_iter->second->is_possible) {
            continue;
        }

        if (inner_loop_iter->second->index_type != pb::I_PRIMARY && 
            inner_loop_iter->second->index_type != pb::I_UNIQ &&
            inner_loop_iter->second->index_type != pb::I_KEY) {
            continue;
        }

        int ret = compare_two_path(outer_loop_iter->second, inner_loop_iter->second);
        if (ret == -1) {
            // outer被干掉，跳出循环
            break;
        }
    }
}

// 两两比较，根据一些简单规则干掉次优索引
int64_t AccessPathMgr::pre_process_select_index() {
    if (_paths.empty()) {
        return 0;
    }

    if (_use_fulltext) {
        return 0;
    }

    int possible_cnt = 0;
    int cover_index_cnt = 0;
    int64_t possible_index = 0;
    for (auto outer_loop_iter = _paths.begin(); outer_loop_iter != _paths.end(); outer_loop_iter++) {
        if (!outer_loop_iter->second->is_possible) {
            continue;
        }

        if (outer_loop_iter->second->index_type != pb::I_PRIMARY && 
            outer_loop_iter->second->index_type != pb::I_UNIQ &&
            outer_loop_iter->second->index_type != pb::I_KEY) {
            continue;
        }

        if (outer_loop_iter->second->index_type == pb::I_PRIMARY || 
            outer_loop_iter->second->index_type == pb::I_UNIQ) {
            if (outer_loop_iter->second->index_field_ids.size() == outer_loop_iter->second->hit_index_field_ids.size()
                && outer_loop_iter->second->is_eq_or_in()) {
                // 主键或唯一键全命中，并且条件为eq或in，直接选择
                if (!_use_force_index || outer_loop_iter->second->hint == AccessPath::FORCE_INDEX) {
                    return outer_loop_iter->second->index_id;
                }
            }
        }

        inner_loop_and_compare(outer_loop_iter);
        if (outer_loop_iter->second->is_possible) {
            possible_cnt++;
            possible_index = outer_loop_iter->second->index_id;
        }
        if (outer_loop_iter->second->is_cover_index()) {
            cover_index_cnt++;
        }
    }

    _cover_index_cnt = cover_index_cnt;
    _possible_index_cnt = possible_cnt;

    // 仅有一个possible index直接选择这个索引
    if (possible_cnt == 1) {
        return possible_index;
    }

    return 0;

}

int64_t AccessPathMgr::select_index() {
    int64_t select_idx = pre_process_select_index();
    if (select_idx == 0) {
        if (SchemaFactory::get_instance()->get_statistics_ptr(_table_id) != nullptr 
            && SchemaFactory::get_instance()->is_switch_open(_table_id, TABLE_SWITCH_COST) && !_use_fulltext) {
            DB_DEBUG("table %ld has statistics", _table_id);
            select_idx = select_index_by_cost();
        } else {
            select_idx = select_index_common();
        }
    }

    return select_idx; 
}

int64_t ScanNode::select_index_in_baikaldb(const std::string& sample_sql) {
    // 预处理，使用倒排索引的sql不进行预处理，如果select_idx大于0则已经选出索引
    int64_t select_idx = _main_path.select_index();
    auto path = _main_path.path(select_idx);
    if (path->is_virtual) {
        // 虚拟索引需要重新选择
        _scan_indexs.clear();
        // 删除，确保下次不再选择该虚拟索引
        _main_path.delete_possible_index(select_idx);
        _main_path.reset();
        std::string& name = path->index_info_ptr->short_name;
        SchemaFactory::get_instance()->update_virtual_index_info(select_idx, name, sample_sql);
        DB_WARNING("hit virtual index, table_id: %ld, virtual_index_id: %ld, virtual_index_name: %s, sample_sql: %s", 
            _table_id, select_idx, name.c_str(), sample_sql.c_str());
        return select_index_in_baikaldb(sample_sql);
    }

    std::vector<ExprNode*> filter_condition;
    _select_idx = select_idx;
    calc_index_range();
    std::vector<int64_t> multi_reverse_index = _main_path.multi_reverse_index();
    if (multi_reverse_index.size() > 0) {
        _scan_indexs.clear();
        //有倒排索引，创建倒排索引树
        create_fulltext_index_tree();
        std::unordered_set<ExprNode*> other_condition;
        std::unordered_set<ExprNode*> need_cut_condition;
        select_idx = multi_reverse_index[0];
        for (auto index_id : multi_reverse_index) {
            auto path = _main_path.path(index_id);
            serialize_index_and_set_router_index(path->pos_index, &_main_path.path(_table_id)->pos_index,
                    path->is_covering_index);
            need_cut_condition.insert(path->need_cut_index_range_condition.begin(), 
                    path->need_cut_index_range_condition.end());
        }
        // 倒排多索引，直接上到其他过滤条件里
        for (auto index_id : multi_reverse_index) {
            auto path = _main_path.path(index_id);
            for (auto expr : path->index_other_condition) {
                if (need_cut_condition.count(expr) == 0) {
                    other_condition.insert(expr);
                }
            }
            for (auto expr : path->other_condition) {
                if (need_cut_condition.count(expr) == 0) {
                    other_condition.insert(expr);
                }
            }
        }
        filter_condition.insert(filter_condition.end(), other_condition.begin(), other_condition.end());
    } else {
        auto path = _main_path.path(select_idx);
        for (auto& expr : path->index_other_condition) {
            ExprNode::create_pb_expr(path->pos_index.add_index_conjuncts(), expr);
        }
        _scan_indexs.clear();
        // 索引还有清理逻辑，在plan_router里;primary->mutable_ranges()->Clear();
        if (path->index_info_ptr->is_global) {
            serialize_index_and_set_router_index(path->pos_index, &path->pos_index,
                    path->is_covering_index);
        } else {
            serialize_index_and_set_router_index(path->pos_index, &_main_path.path(_table_id)->pos_index,
                    path->is_covering_index);
        }
        filter_condition.insert(filter_condition.end(), path->other_condition.begin(),
                path->other_condition.end());
        _learner_use_diff_index = false;
        int64_t learner_idx = 0;
        if (path->need_select_learner_index() || _learner_path.has_disable_index()) {
            _learner_path.reset();
            learner_idx = _learner_path.select_index();
        }
        if (learner_idx != 0 && learner_idx != path->index_id) {
            auto learner_path = _learner_path.path(learner_idx);
            for (auto& expr : learner_path->index_other_condition) {
                ExprNode::create_pb_expr(learner_path->pos_index.add_index_conjuncts(), expr);
            }
            if (learner_path->index_info_ptr->is_global) {
                serialize_index_and_set_router_index(learner_path->pos_index, &learner_path->pos_index,
                    learner_path->is_covering_index, ScanIndexInfo::U_GLOBAL_LEARNER);
            } else {
                // 如果主集群和backup选择的索引都非全局二级索引则使用learner
                ScanIndexInfo::IndexUseFor backup_use_for = ScanIndexInfo::U_GLOBAL_LEARNER;
                if (!path->index_info_ptr->is_global) {
                    backup_use_for = ScanIndexInfo::U_LOCAL_LEARNER;
                } 
                serialize_index_and_set_router_index(learner_path->pos_index, &_main_path.path(_table_id)->pos_index,
                    learner_path->is_covering_index, backup_use_for);
            }
            _learner_use_diff_index = true;
            DB_WARNING("need_select_learner_index: %d, has_disable_index: %d, index_id: %ld, learner_idx: %ld, sample_sql: %s", 
                path->need_select_learner_index(), _learner_path.has_disable_index(), 
                path->index_id, learner_idx, sample_sql.c_str());
            std::vector<ExprNode*> learner_condition;
            learner_condition.insert(learner_condition.end(), learner_path->other_condition.begin(),
                    learner_path->other_condition.end());
            if (get_parent()->node_type() == pb::TABLE_FILTER_NODE ||
                get_parent()->node_type() == pb::WHERE_FILTER_NODE) {
                static_cast<FilterNode*>(get_parent())->modifiy_pruned_conjuncts_by_index_learner(learner_condition);
            }
        }
    }
    // modify filter conjuncts
    if (get_parent()->node_type() == pb::TABLE_FILTER_NODE ||
        get_parent()->node_type() == pb::WHERE_FILTER_NODE) {
        static_cast<FilterNode*>(get_parent())->modifiy_pruned_conjuncts_by_index(filter_condition);
    }
    return select_idx;
}

ScanNode* ScanNode::create_scan_node(const pb::PlanNode& node) {
    if (node.derive_node().scan_node().has_engine()) {
        pb::Engine engine = node.derive_node().scan_node().engine();
        switch (engine) {
            case pb::ROCKSDB:
            case pb::BINLOG:
            case pb::ROCKSDB_CSTORE:
                return new RocksdbScanNode;
            case pb::INFORMATION_SCHEMA:
                return new InformationSchemaScanNode;
            case pb::REDIS:
                return new RedisScanNode;
        }
    } else {
        return new RocksdbScanNode;
    }
    return nullptr;
}

int AccessPathMgr::choose_arrow_pb_reverse_index() {
    if (_multi_reverse_index.size() > 1) {
        int pb_type_num = 0;
        int arrow_type_num = 0;
        std::vector<int> pb_indexs;
        std::vector<int> arrow_indexs;
        pb_indexs.reserve(4);
        arrow_indexs.reserve(4);
        pb::StorageType filter_type = pb::ST_UNKNOWN;
        for (auto index_id : _multi_reverse_index) {
            DB_DEBUG("reverse_filter index [%ld]", index_id);
            pb::StorageType type = pb::ST_UNKNOWN;
            if (SchemaFactory::get_instance()->get_index_storage_type(index_id, type) == -1) {
                DB_FATAL("get index storage type error index [%ld]", index_id);
                return -1;
            }

            if (type == pb::ST_PROTOBUF_OR_FORMAT1) {
                pb_indexs.push_back(index_id);
                ++pb_type_num;
            } else if (type == pb::ST_ARROW) {
                arrow_indexs.push_back(index_id);
                ++arrow_type_num;
            }
        }
        filter_type = pb_type_num <= arrow_type_num ? pb::ST_PROTOBUF_OR_FORMAT1 : pb::ST_ARROW;
        DB_DEBUG("reverse_filter type[%s]", pb::StorageType_Name(filter_type).c_str());
        auto remove_indexs_func = [this](std::vector<int>& to_remove_indexs) {
            _multi_reverse_index.erase(std::remove_if(_multi_reverse_index.begin(), _multi_reverse_index.end(), [&to_remove_indexs](const int& index) {
                return std::find(to_remove_indexs.begin(), to_remove_indexs.end(), index) 
                    != to_remove_indexs.end() ? true : false;
            }), _multi_reverse_index.end());
        };

        if (filter_type == pb::ST_PROTOBUF_OR_FORMAT1) {
            remove_indexs_func(pb_indexs);
            _fulltext_use_arrow = true;
        } else if (filter_type == pb::ST_ARROW) {
            remove_indexs_func(arrow_indexs);
        }
    }
    return 0;   
}

int ScanNode::create_fulltext_index_tree(FulltextInfoNode* node, pb::FulltextIndex* root) {
    if (node == nullptr) {
        return 0;
    }
    switch(node->type) {
        case pb::FNT_TERM : {
            auto& inner_node_pair = boost::get<FulltextInfoNode::LeafNodeType>(node->info);
            auto& inner_node = inner_node_pair.second;
            root->set_fulltext_node_type(pb::FNT_TERM);
            auto possible_index = root->mutable_possible_index();
            possible_index->set_index_id(inner_node_pair.first);
            SmartRecord record_template = SchemaFactory::get_instance()->new_record(_table_id);

            if (inner_node.like_values.size() != 1) {
                DB_WARNING("like values size not equal one");
                return -1;
            }
            record_template->set_value(record_template->get_field_by_tag(
                inner_node.left_row_field_ids[0]), inner_node.like_values[0]);
            std::string str;
            record_template->encode(str);
            auto range = possible_index->add_ranges();
            auto range_type = inner_node.type;
            range->set_left_pb_record(str);
            range->set_right_pb_record(str);
            range->set_left_field_cnt(1);
            range->set_right_field_cnt(1);
            range->set_left_open(false);
            range->set_right_open(false);
            if (range_type == range::MATCH_LANGUAGE) {
                range->set_match_mode(pb::M_NARUTAL_LANGUAGE);
            } else if (range_type == range::MATCH_BOOLEAN) {
                range->set_match_mode(pb::M_BOOLEAN);
            }
            break;
        }
        case pb::FNT_AND : 
        case pb::FNT_OR : {
            auto& inner_node = boost::get<FulltextInfoNode::FulltextChildType>(node->info);
            if (inner_node.children.size() == 1) {
                if (create_fulltext_index_tree(inner_node.children[0].get(), root) != 0) {
                    return -1;
                }
            } else if (inner_node.children.size()  > 1) {
                root->set_fulltext_node_type(node->type);
                for (const auto& child : inner_node.children) {
                    auto child_iter = root->add_nested_fulltext_indexes();
                    if (create_fulltext_index_tree(child.get(), child_iter) == -1) {
                        return -1;
                    }
                }
            }
            break;
        }
        default : {
            DB_WARNING("unknown node type");
            break;
        }
    }
    return 0;
}

int ScanNode::create_fulltext_index_tree() {
    _fulltext_index_pb.reset(new pb::FulltextIndex);
    if (create_fulltext_index_tree(_fulltext_index_tree.root.get(), _fulltext_index_pb.get()) != 0) {
        DB_WARNING("create fulltext index tree error.");
        return -1;
    } else {
        if (_main_path.fulltext_use_arrow()) {
            pb::ScanNode* pb_scan_node = mutable_pb_node()->mutable_derive_node()->mutable_scan_node();
            auto fulltext_index_iter = pb_scan_node->mutable_fulltext_index();
            fulltext_index_iter->CopyFrom(*_fulltext_index_pb);
        }
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
