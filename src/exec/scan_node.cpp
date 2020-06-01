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
#include "redis_scan_node.h"

namespace baikaldb {
int64_t ScanNode::select_index() {
    std::multimap<uint32_t, int> prefix_ratio_id_mapping;
    std::unordered_set<int32_t> primary_fields;
    primary_fields = _paths[_table_id]->hit_index_field_ids;
    for (auto& pair : _paths) {
        int64_t index_id = pair.first;
        auto& path = pair.second;
        auto& pos_index = path->pos_index;
        auto info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            continue;
        }
        IndexInfo& info = *info_ptr;
        if (!path->is_possible && info.type != pb::I_PRIMARY) {
            continue;
        }
        auto index_state = info.state;
        if (index_state != pb::IS_PUBLIC) {
            DB_DEBUG("DDL_LOG index_selector skip index [%lld] state [%s] ", 
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
        // 普通索引如果都包含在主键里，则不选
        if (info.type == pb::I_UNIQ || info.type == pb::I_KEY) {
            bool contain_by_primary = true;
            for (int j = 0; j < field_count; j++) {
                if (primary_fields.count(info.fields[j].id) == 0) {
                    contain_by_primary = false;
                    break;
                }
            }
            if (contain_by_primary) {
                continue;
            }
        }
        // sort index 权重调整到全命中unique或primary索引之后
        if (pos_index.has_sort_index() && field_count > 0) {
            prefix_ratio_round = 100;
            index_priority = 190;
        }
        uint32_t prefix_ratio_index_score = (prefix_ratio_round << 16) | index_priority;
        // ignore index用到最低优先级，其实只有primary会走到这里
        if (path->hint == AccessPath::IGNORE_INDEX) {
            prefix_ratio_index_score = 0;
        }
        prefix_ratio_id_mapping.insert(std::make_pair(prefix_ratio_index_score, index_id));

        // 优先选倒排，没有就取第一个
        switch (info.type) {
            case pb::I_FULLTEXT:
                _multi_reverse_index.push_back(index_id);
                break;
            case pb::I_RECOMMEND:
                return index_id;
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
    if (node.derive_node().scan_node().has_engine()) {
        _engine = node.derive_node().scan_node().engine();
    }
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
    return 0;
}

void ScanNode::close(RuntimeState* state) {
    ExecNode::close(state);
    clear_possible_indexes();
    _multi_reverse_index.clear();
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
    if (!has_index()) {
        explain_info["type"] = "ALL";
    } else {
        explain_info["possible_keys"] = "";
        for (auto& pair : _paths) {
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
        
        auto& pos_index = _pb_node.derive_node().scan_node().indexes(0);
        int64_t index_id = pos_index.index_id();
        auto index_info = factory->get_index_info(index_id);
        auto pri_info = factory->get_index_info(_table_id);
        explain_info["key"] = index_info.short_name;
        explain_info["type"] = "range";
        if (pos_index.ranges_size() == 1) {
            int field_cnt = pos_index.ranges(0).left_field_cnt();
            if (field_cnt == index_info.fields.size() && 
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

int64_t ScanNode::select_index_by_cost() {
    double min_cost = DBL_MAX;
    int min_idx = 0;
    for (auto& pair : _paths) {
        auto& path = pair.second;
        int64_t index_id = pair.first;
        path->calc_cost();
        if (path->cost < min_cost) {
            min_cost = path->cost;
            min_idx = index_id;
        }
        DB_DEBUG("idx:%ld cost:%f", index_id, path->cost);
    }
    return min_idx;
}

int64_t ScanNode::select_index_in_baikaldb() {
    auto table_ptr = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (table_ptr == nullptr) {
        DB_FATAL("table info not exist : %ld", _table_id);
        return -1;
    }
    pb::ScanNode* pb_scan_node = mutable_pb_node()->mutable_derive_node()->mutable_scan_node();
    _router_index_id = _table_id; 
    _router_index = &_paths[_table_id]->pos_index;
    int64_t select_idx = 0;
    if (SchemaFactory::get_instance()->get_statistics_ptr(_table_id) != nullptr 
        && SchemaFactory::get_instance()->is_switch_open(_table_id, TABLE_SWITCH_COST)) {
        DB_DEBUG("table %ld has statistics", _table_id);
        select_idx = select_index_by_cost();
    } else {
        select_idx = select_index();
    }
    //DB_WARNING("idx:%ld table:%ld", select_idx, _table_id);
    std::unordered_set<ExprNode*> other_condition;
    if (_multi_reverse_index.size() > 0) {
        std::unordered_set<ExprNode*> need_cut_condition;
        select_idx = _multi_reverse_index[0];
        for (auto index_id : _multi_reverse_index) {
            auto pos_index = pb_scan_node->add_indexes();
            pos_index->CopyFrom(_paths[index_id]->pos_index);
            need_cut_condition.insert(_paths[index_id]->need_cut_index_range_condition.begin(), 
                    _paths[index_id]->need_cut_index_range_condition.end());
        }
        // 倒排多索引，直接上到其他过滤条件里
        for (auto index_id : _multi_reverse_index) {
            for (auto expr : _paths[index_id]->index_other_condition) {
                if (need_cut_condition.count(expr) == 0) {
                    other_condition.insert(expr);
                }
            }
            for (auto expr : _paths[index_id]->other_condition) {
                if (need_cut_condition.count(expr) == 0) {
                    other_condition.insert(expr);
                }
            }
        }
        _is_covering_index = _paths[select_idx]->is_covering_index;
    } else {
        auto pos_index = pb_scan_node->add_indexes();
        pos_index->CopyFrom(_paths[select_idx]->pos_index);
        int64_t index_id = _paths[select_idx]->index_id;
        _is_covering_index = _paths[select_idx]->is_covering_index;
        // 索引还有清理逻辑，在plan_router里;primary->mutable_ranges()->Clear();
        if (SchemaFactory::get_instance()->is_global_index(index_id) || 
                _paths[select_idx]->index_type == pb::I_PRIMARY) {
            _router_index_id = index_id;
            _router_index = pos_index;
        }
        other_condition = _paths[select_idx]->other_condition;
    }
    DB_DEBUG("select_idx:%d _is_covering_index:%d index:%ld table:%ld", 
            select_idx, _is_covering_index, select_idx, _table_id);
    // modify filter conjuncts
    if (get_parent()->node_type() == pb::TABLE_FILTER_NODE ||
        get_parent()->node_type() == pb::WHERE_FILTER_NODE) {
        static_cast<FilterNode*>(get_parent())->modifiy_pruned_conjuncts_by_index(other_condition);
    }
    return select_idx;
}

ScanNode* ScanNode::create_scan_node(const pb::PlanNode& node) {
    if (node.derive_node().scan_node().has_engine()) {
        pb::Engine engine = node.derive_node().scan_node().engine();
        switch (engine) {
            case pb::ROCKSDB:
                return new RocksdbScanNode;
            case pb::ROCKSDB_CSTORE:
                return new RocksdbScanNode(pb::ROCKSDB_CSTORE);
            case pb::REDIS:
                return new RedisScanNode;
                break;
        }
    } else {
        return new RocksdbScanNode;
    }
    return nullptr;
}

int ScanNode::choose_arrow_pb_reverse_index() {
    if (_multi_reverse_index.size() > 1) {
        int pb_type_num = 0;
        int arrow_type_num = 0;
        std::vector<int> pb_indexs;
        std::vector<int> arrow_indexs;
        pb_indexs.reserve(4);
        arrow_indexs.reserve(4);
        pb::StorageType filter_type = pb::ST_UNKNOWN;
        for (auto index_id : _multi_reverse_index) {
            DB_DEBUG("reverse_filter index [%lld]", index_id);
            pb::StorageType type = pb::ST_UNKNOWN;
            if (SchemaFactory::get_instance()->get_index_storage_type(index_id, type) == -1) {
                DB_FATAL("get index storage type error index [%lld]", index_id);
                return -1;
            }

            if (type == pb::ST_PROTOBUF) {
                pb_indexs.push_back(index_id);
                ++pb_type_num;
            } else if (type == pb::ST_ARROW) {
                arrow_indexs.push_back(index_id);
                ++arrow_type_num;
            }
        }
        filter_type = pb_type_num <= arrow_type_num ? pb::ST_PROTOBUF : pb::ST_ARROW;
        DB_DEBUG("reverse_filter type[%s]", pb::StorageType_Name(filter_type).c_str());
        auto remove_indexs_func = [this](std::vector<int>& to_remove_indexs) {
            _multi_reverse_index.erase(std::remove_if(_multi_reverse_index.begin(), _multi_reverse_index.end(), [&to_remove_indexs](const int& index) {
                return std::find(to_remove_indexs.begin(), to_remove_indexs.end(), index) 
                    != to_remove_indexs.end() ? true : false;
            }), _multi_reverse_index.end());
        };

        if (filter_type == pb::ST_PROTOBUF) {
            remove_indexs_func(pb_indexs);
        } else if (filter_type == pb::ST_ARROW) {
            remove_indexs_func(arrow_indexs);
        }
    }
    return 0;   
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
