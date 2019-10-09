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
        for (auto& pos_index : _pb_node.derive_node().scan_node().indexes()) {
            int64_t index_id = pos_index.index_id();
            explain_info["possible_keys"] += factory->get_index_info(index_id).short_name;
            explain_info["possible_keys"] += ",";
        }
        explain_info["possible_keys"].pop_back();
        std::vector<int> tmp;
        int idx = select_index(tmp);
        auto& pos_index = _pb_node.derive_node().scan_node().indexes(idx);
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

ScanNode* ScanNode::create_scan_node(const pb::PlanNode& node) {
    if (node.derive_node().scan_node().has_engine()) {
        pb::Engine engine = node.derive_node().scan_node().engine();
        switch (engine) {
            case pb::ROCKSDB:
            case pb::ROCKSDB_CSTORE:
                return new RocksdbScanNode;
            case pb::REDIS:
                return new RedisScanNode;
                break;
        }
    } else {
        return new RocksdbScanNode;
    }
    return nullptr;
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
