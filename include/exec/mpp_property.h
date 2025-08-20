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
#include <unordered_map>
#include <vector>
#include <string>
#include "expr_node.h"

namespace baikaldb {

struct HashPartitionColumns {
    HashPartitionColumns() {}
    ~HashPartitionColumns() {
        for (auto& [_, expr] : hash_columns) {
            if (expr != nullptr) {
                ExprNode::destroy_tree(expr);
            }
        }
    }
    // // arrow_name(tupleid_slotid / tmp_1(agg产生的临时列) / hash_1) -> ColumnInfo
    // std::unordered_map<std::string, ColumnInfo> hash_columns;
    // 需要额外加projection产生hash列, hash_1 -> expr(a+b)
    std::unordered_map<std::string, ExprNode*> hash_columns;
    // sender计算hash值,必须按照执指定顺序, int32(1),int64(10)计算的hash值和 int64(10),int32(1)不一样
    std::vector<std::string> ordered_hash_columns;
    std::vector<std::string> need_add_projection_columns;
    bool has_temporary_column = false; 
    bool child_hash_column_missed = false;

    void add_slot_ref(ExprNode* expr) {
        std::string arrow_name = std::to_string(expr->tuple_id()) + "_" + std::to_string(expr->slot_id());
        hash_columns[arrow_name] = expr;
        ordered_hash_columns.emplace_back(arrow_name);
    }

    void add_column(const std::string& name) {
        hash_columns[name] = nullptr;
    }
    
    void add_need_projection_column(const std::string& name, ExprNode* expr) {
        hash_columns[name] = expr;
        need_add_projection_columns.emplace_back(name);
        ordered_hash_columns.emplace_back(name);
        has_temporary_column = true;
    }

    bool hash_partition_is_same(HashPartitionColumns* child) {
        if (this == child) {
            return true;
        }
        if (hash_columns.size() != child->hash_columns.size()) {
            return false;
        }
        return hash_partition_is_contain(child);
    }

    bool hash_partition_is_contain(HashPartitionColumns* child) {
        // parent: {a, b, c}, child: {a, b} : return true, no need exchange, 后续按照{a,b}探测
        // parent: {a, b}, child: {a, b, c} : return false, need exchange
        if (child->has_temporary_column || child->child_hash_column_missed) {
            return false;
        }
        if (hash_columns.size() < child->hash_columns.size()) {
            return false;
        }
        for (auto& c : child->hash_columns) {
            if (hash_columns.find(c.first) == hash_columns.end()) {
                return false;
            }
        }
        return true;
    }
};

struct NodePartitionProperty {
    pb::PartitionPropertyType type = pb::AnyType;

    // 除了join的partition有俩, 其他都只有一个
    std::vector<std::shared_ptr<HashPartitionColumns>> hash_partition_propertys;

    // 需要先cast string的hash列
    std::unordered_set<std::string> need_cast_string_columns;

    // 确定该节点子树没有任何rocksdb scan的数据输入, 这个子树不需要拆分fragment
    // 产生原因: 如1. expr is always false  2. index has null 3. limit 0导致的return_empty标记
    bool has_no_input_data = false;

    void set_single_partition() {
        type = pb::SinglePartitionType;
        hash_partition_propertys.clear();
    }
    void set_any_partition() {
        type = pb::AnyType;
        hash_partition_propertys.clear();
    }
    void add_need_cast_string_columns(const std::unordered_set<std::string>& names) {
        need_cast_string_columns.insert(names.begin(), names.end());
    }
    // for debug
    std::string print() {
        std::string ret = "type: " + pb::PartitionPropertyType_Name(type)
                             + ", hash_partition_propertys size: " + std::to_string(hash_partition_propertys.size());
        for (auto m : hash_partition_propertys) {
            ret += "\n hash_cols: [";
            for (auto c : m->hash_columns) {
                ret += c.first + ":" + pb::ExprNodeType_Name(c.second->node_type());
            }
            ret += "] \n";
        }  
        ret += "\n need_cast_string_columns: [";
        for (auto c : need_cast_string_columns) {
            ret += c + " ";
        }  
        ret += "] \n";    
        return ret;
    }
};
}