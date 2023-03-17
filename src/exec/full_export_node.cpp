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
#include "full_export_node.h"
#include "network_socket.h"

namespace baikaldb {
//DEFINE_int32(region_per_batch, 4, "request region number in a batch");

int FullExportNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _op_type = pb::OP_SELECT;
    return 0;
}

int FullExportNode::open(RuntimeState* state) {
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    if (_return_empty) {
        return 0;
    }
    client_conn->seq_id++;
    state->seq_id = client_conn->seq_id;
    _scan_node = static_cast<RocksdbScanNode*>(get_node(pb::SCAN_NODE));
    for (auto& pair : _region_infos) {
        auto& info = pair.second;
        _start_key_sort[info.partition_id()][info.start_key()] = info.region_id();
    }
    for (auto& partition_pair : _start_key_sort) {
        for (auto& pair : partition_pair.second) {
            _send_region_ids.emplace_back(pair.second);
        }
    } 
    DB_WARNING("region_count:%ld", _send_region_ids.size());
    return 0;
}

bool FullExportNode::get_batch(RowBatch* batch) {
    auto iter = _fetcher_store.start_key_sort.begin();
    if (iter != _fetcher_store.start_key_sort.end()) {
        // _fetcher_store内部region_batch修改
        auto iter2 = _fetcher_store.region_batch.find(iter->second);
        _fetcher_store.start_key_sort.erase(iter);
        if (iter2 != _fetcher_store.region_batch.end()) {
            // region merge了，拿下一个
            if (iter2->second == nullptr) {
                return true;
            }
            batch->swap(*iter2->second);
            _fetcher_store.region_batch.erase(iter2);
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

int FullExportNode::get_next_region_infos() {
    SchemaFactory* schema_factory = SchemaFactory::get_instance(); 
    int64_t main_table_id = _scan_node->table_id();
    ScanIndexInfo& scan_index_info = *(_scan_node->main_scan_index());
    auto index_ptr = schema_factory->get_index_info_ptr(scan_index_info.router_index_id);
    if (index_ptr == nullptr) {
        DB_WARNING("index info not found index_id:%ld", scan_index_info.router_index_id);
        return -1;
    }
    if (scan_index_info.router_index->ranges_size() == 0) {
        scan_index_info.router_index->add_ranges();
    }
    scan_index_info.router_index->mutable_ranges(0)->set_left_key(_last_router_key);
    scan_index_info.router_index->mutable_ranges(0)->set_left_full(true);
    scan_index_info.router_index->mutable_ranges(0)->set_left_field_cnt(index_ptr->fields.size());
    scan_index_info.router_index->mutable_ranges(0)->set_left_open(false);
    int ret = schema_factory->get_region_by_key(main_table_id, 
            *index_ptr, scan_index_info.router_index,
            scan_index_info.region_infos,
            &scan_index_info.region_primary,
            _scan_node->get_partition(),
            true);
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    set_region_infos(scan_index_info.region_infos);
    _start_key_sort.clear();
    for (auto& pair : _region_infos) {
        auto& info = pair.second;
        _start_key_sort[info.partition_id()][info.start_key()] = info.region_id();
    }
    for (auto& partition_pair : _start_key_sort) {
        for (auto& pair : partition_pair.second) {
            _send_region_ids.emplace_back(pair.second);
        }
    } 
    DB_WARNING("region_count:%ld", _send_region_ids.size());
    return 0;
}

//SelectManagerNode::construct_primary_possible_index有类似代码，后续复用
int FullExportNode::calc_last_key(RuntimeState* state, MemRow* mem_row) {
    SchemaFactory* schema_factory = SchemaFactory::get_instance(); 
    int64_t main_table_id = _scan_node->table_id();
    SmartRecord record = schema_factory->new_record(main_table_id);
    int32_t tuple_id = _scan_node->tuple_id();
    auto pri_info = schema_factory->get_index_info_ptr(main_table_id);
    if (pri_info == nullptr) {
        DB_WARNING("pri index info not found table_id:%ld", main_table_id);
        return -1;
    }
    for (auto& pri_field : pri_info->fields) {
        int32_t field_id = pri_field.id;
        int32_t slot_id = state->get_slot_id(tuple_id, field_id);
        if (slot_id == -1) {
            DB_WARNING("field_id:%d tuple_id:%d, slot_id:%d", field_id, tuple_id, slot_id);
            return -1;
        }
        record->set_value(record->get_field_by_tag(field_id), mem_row->get_value(tuple_id, slot_id));
    }
    MutTableKey  key;
    if (record->encode_key(*pri_info.get(), key, pri_info->fields.size(), false, false) != 0) {
        DB_FATAL("Fail to encode_key left, table:%ld", pri_info->id);
        return -1;
    }

    ScanIndexInfo& scan_index_info = *(_scan_node->main_scan_index());
    scan_index_info.region_primary.clear();
    pb::PossibleIndex pos_index;
    pos_index.ParseFromString(scan_index_info.raw_index);
    pos_index.set_index_id(main_table_id);
    // fullexport 非eq
    pos_index.clear_is_eq();
    if (pos_index.ranges_size() == 0) {
        pos_index.add_ranges();
    }
    pos_index.mutable_ranges(0)->set_left_key(key.data());
    pos_index.mutable_ranges(0)->set_left_full(key.get_full());
    pos_index.mutable_ranges(0)->set_left_field_cnt(pri_info->fields.size());
    pos_index.mutable_ranges(0)->set_left_open(true);
    pos_index.SerializeToString(&scan_index_info.raw_index);
    return 0;
}

int FullExportNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (state->is_cancelled()) {
        DB_WARNING_STATE(state, "cancelled");
        state->set_eos();
        *eos = true;
        return 0;
    }
    if (_return_empty) {
        state->set_eos();
        *eos = true;
        return 0;
    }
    if (reached_limit()) {
        *eos = true;
        return 0;
    }
    
    if (get_batch(batch)) {
        _num_rows_returned += batch->size();
        if (reached_limit()) {
            *eos = true;
            _num_rows_returned = _limit;
            return 0;
        }
        return 0;
    }
    uint64_t log_id = state->log_id();

    if (_send_region_ids.empty()) {
        if (_last_router_key.empty()) {
            state->set_eos();
            DB_WARNING("process full select done log_id:%lu", log_id);
            *eos = true;
            return 0;
        }
        int ret = get_next_region_infos();
        if (ret < 0) {
            return -1;
        }
        if (_send_region_ids.empty()) {
            state->set_eos();
            DB_WARNING("process full select done log_id:%lu", log_id);
            *eos = true;
            return 0;
        }
    }
    
    _fetcher_store.scan_rows = 0;
    state->memory_limit_release_all();
    std::map<int64_t, pb::RegionInfo> infos;
    int64_t region_id = _send_region_ids.front();
    _send_region_ids.pop_front();
    auto iter = _region_infos.find(region_id);
    if (iter != _region_infos.end()) {
        infos[region_id] = iter->second;
    } else {
        return 0;
    }
    DB_WARNING("send region_id:%ld log_id:%lu", region_id, log_id);
    ++state->region_count;
    if (infos.empty()) {
        return 0;
    }
    int64_t last_region_id = region_id;
    int ret = _fetcher_store.run(state, infos, _children[0], state->seq_id, state->seq_id, _op_type);
    if (ret < 0) {
        DB_FATAL("fetcher_store run fail:%d", ret);
        return -1;
    }
    // 有split或merge，更新region信息
    if (_fetcher_store.start_key_sort.size() > 1) {
        for (auto& pair : _fetcher_store.start_key_sort) {
            last_region_id = pair.second;
            if (state->client_conn()->region_infos.count(last_region_id) != 0) {
                _region_infos[last_region_id] = state->client_conn()->region_infos[last_region_id];
            }
        }
    }
    // _last_router_key用于获取下一个region
    _last_router_key = _region_infos[last_region_id].end_key();
    if (_limit > 0) {
        bool match = false;
        std::vector<int64_t> match_region_ids;
        auto iter = _fetcher_store.start_key_sort.begin();
        while (iter != _fetcher_store.start_key_sort.end()) {
            last_region_id = iter->second;
            if (match) {
                iter = _fetcher_store.start_key_sort.erase(iter);
                _fetcher_store.region_batch.erase(last_region_id);
                match_region_ids.emplace_back(last_region_id);
                continue;
            }
            auto iter2 = _fetcher_store.region_batch.find(last_region_id);
            if (iter2 != _fetcher_store.region_batch.end()) {
                // region merge了
                if (iter2->second == nullptr) {
                    iter = _fetcher_store.start_key_sort.erase(iter);
                    _fetcher_store.region_batch.erase(last_region_id);
                    continue;
                } else if (iter2->second->size() >= _limit) {
                    //当前region还有数据，下轮循环从last_key开始
                    match = true;
                    match_region_ids.emplace_back(last_region_id);
                    int ret = calc_last_key(state, iter2->second->back().get());
                    if (ret < 0) { 
                        return -1;
                    }
                }
            }
            ++iter;
        }
        //待继续的region重新加入队列
        if (match_region_ids.size() > 0) {
            _send_region_ids.insert(_send_region_ids.begin(), match_region_ids.begin(), match_region_ids.end());
        }
    }
    return 0;
}
} 

/* vim: set ts=4 sw=4 sts=4 tw=100 */
