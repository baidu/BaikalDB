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

#include "runtime_state.h"
#include "truncate_node.h"

namespace baikaldb {
int TruncateNode::init(const pb::PlanNode& node) { 
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _table_id =  node.derive_node().truncate_node().table_id();
    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);

    if (_table_info == nullptr) {
        DB_WARNING("table info not found _table_id:%ld", _table_id);
        return -1;
    }
    return 0;
}

int TruncateNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ExecNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    RocksWrapper* _db = RocksWrapper::get_instance();
    if (_db == nullptr) {
        DB_WARNING_STATE(state, "get rocksdb instance failed");
        return -1;
    }
    rocksdb::ColumnFamilyHandle* _data_cf = _db->get_data_handle();
    if (_data_cf == nullptr) {
        DB_WARNING_STATE(state, "get rocksdb data column family failed");
        return -1;
    }
    _region_id = state->region_id();

    MutTableKey region_start;
    MutTableKey region_end;
    region_start.append_i64(_region_id);
    region_end.append_i64(_region_id).append_u64(UINT64_MAX);

    rocksdb::WriteOptions write_options;
    //write_options.disableWAL = true;

    rocksdb::Slice begin(region_start.data());
    rocksdb::Slice end(region_end.data());
    TimeCost cost;
    auto res = _db->remove_range(write_options, _data_cf, begin, end, true);
    if (!res.ok()) {
        DB_WARNING_STATE(state, "truncate table failed: table:%ld, region:%ld, code=%d, msg=%s", 
            _table_id, _region_id, res.code(), res.ToString().c_str());
        return -1;
    }
    DB_WARNING_STATE(state, "truncate table:%ld, region:%ld, cost:%ld", 
            _table_id, _region_id, cost.get_time());
    /*
    res = _db->compact_range(rocksdb::CompactRangeOptions(), _data_cf, &begin, &end);
    if (!res.ok()) {
        DB_WARNING_STATE(state, "compact after truncated failed: table:%ld, region:%ld, code=%d, msg=%s", 
            _table_id, _region_id, res.code(), res.ToString().c_str());
        return -1;
    }
    */
    return 0;
}

void TruncateNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto truncate_node = pb_node->mutable_derive_node()->mutable_truncate_node();
    truncate_node->set_table_id(_table_id);
}

}
