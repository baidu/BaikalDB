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

#include "index_ddl_manager_node.h"
#include "exec_node.h"
#include "schema_factory.h"
#include "runtime_state.h"
#include "select_manager_node.h"
#include "rocksdb_scan_node.h"
#include "lock_secondary_node.h"

namespace baikaldb {

IndexDDLManagerNode::IndexDDLManagerNode() {
}
IndexDDLManagerNode::~IndexDDLManagerNode() {
}

int IndexDDLManagerNode::open(RuntimeState* state) {
    int ret = 0;
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("task_%s connection is nullptr: %lu", _task_id.c_str(), state->txn_id);
        return -1;
    }
    client_conn->seq_id++;

    std::vector<ExecNode*> scan_nodes;
    get_node(pb::SCAN_NODE, scan_nodes);
    if (scan_nodes.size() != 1) {
        DB_WARNING("select manager has more than one scan node txn_id: %lu, log_id:%lu",
                    state->txn_id, state->log_id());
        return -1;
    }
    ON_SCOPE_EXIT(([this, state]() {
        state->memory_limit_release_all();
    }));
    RocksdbScanNode* scan_node = static_cast<RocksdbScanNode*>(scan_nodes[0]);
    auto limit = scan_node->get_limit();
    int64_t main_table_id = scan_node->table_id();
    ret = _fetcher_store.run(state, _region_infos, _children[0], client_conn->seq_id, client_conn->seq_id, pb::OP_SELECT_FOR_UPDATE);
    if (ret < 0) {
        DB_WARNING("task_%s select manager fetcher manager node open fail, txn_id: %lu, log_id:%lu", 
                _task_id.c_str(), state->txn_id, state->log_id());
        state->ddl_error_code = state->error_code;
        return ret;
    }
    client_conn->region_infos = _region_infos;
    std::vector<SmartRecord> insert_records;
    std::vector<SmartRecord> delete_records;
    insert_records.reserve(limit);
    std::map<int64_t, pb::RegionInfo> region_infos;
    
    SmartRecord record_template = SchemaFactory::get_instance()->new_record(main_table_id);
    auto pk_info = SchemaFactory::get_instance()->get_index_info(_table_id);
    if (pk_info.id == -1) {
        DB_FATAL("task_%s index not ready.", _task_id.c_str());
        return -1;
    }
    auto tuple_id = 0;
    int32_t ddl_scan_size = 0;
    std::string max_pk_str;
    std::string max_record;
    bool global_ddl_with_ttl = _fetcher_store.region_id_ttl_timestamp_batch.size() > 0 ? true : false;
    std::map<std::string, int64_t> record_ttl_map;
    for (auto& pair : _fetcher_store.start_key_sort) {
        auto iter = _fetcher_store.region_batch.find(pair.second);
        if (iter == _fetcher_store.region_batch.end()) {
            continue;
        }
        auto& batch = iter->second;
        if (batch == NULL || batch->size() == 0) {
            _fetcher_store.region_batch.erase(iter);
            continue;
        }
        if (!_is_global_index && !max_pk_str.empty()) {
            DB_WARNING("split only return first region, task_%s", _task_id.c_str());
            break;
        }
        auto& ttl_batch = _fetcher_store.region_id_ttl_timestamp_batch[pair.second];
        if (global_ddl_with_ttl && batch->size() != ttl_batch.size()) {
            DB_FATAL("region_id: %ld, batch size diff with ttl size %ld vs %ld", pair.second, batch->size(), ttl_batch.size());
            global_ddl_with_ttl = false;
        }
        int ttl_idx = 0;
        for (batch->reset(); !batch->is_traverse_over(); batch->next()) {
            ddl_scan_size++;
            std::unique_ptr<MemRow>& mem_row = batch->get_row();
            SmartRecord record = record_template->clone(false);
            auto construct_record = [state, &record, &mem_row, tuple_id, &insert_records](int64_t index_id) -> int {
                // ddl column时index_id=0
                if (index_id == 0) {
                    return 0;
                }
                auto index_info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
                if (index_info_ptr == nullptr) {
                    DB_FATAL("index info ptr is nullptr");
                    return -1;
                }

                for (auto& field : index_info_ptr->fields) {
                    int32_t field_id = field.id;
                    int32_t slot_id = state->get_slot_id(tuple_id, field_id);
                    record->set_value(record->get_field_by_tag(field_id), mem_row->get_value(tuple_id, slot_id));
                }
                return 0;
            };

            if (construct_record(_table_id) == -1 || construct_record(_index_id) == -1) {
                DB_WARNING("task_%s construct record error", _task_id.c_str());
                return -1;
            }
            insert_records.emplace_back(record);
            DB_DEBUG("record %s", record->debug_string().c_str());
            if (global_ddl_with_ttl) {
                std::string record_str;
                record->encode(record_str);
                record_ttl_map[record_str] = ttl_batch[ttl_idx++];
            }
            // 已排序，只 encode batch最后的record 或者 最后一个record。
            if (batch->index() + 1 == batch->size() || ddl_scan_size == limit) {
                MutTableKey max_pk_key;
                int ret = record->encode_key(pk_info, max_pk_key, -1, false, false);
                if (ret != 0) {
                    DB_WARNING("task_%s encode error.", _task_id.c_str());
                    return ret;
                }
                DB_DEBUG("get pk key %s", str_to_hex(max_pk_key.data()).c_str());
                max_record = max_pk_key.data();
                if (max_pk_key.data() > max_pk_str) {
                    DB_DEBUG("get max pk key %s", str_to_hex(max_pk_key.data()).c_str());
                    max_pk_str = max_pk_key.data();
                }
                state->ddl_pk_key_is_full = max_pk_key.get_full();
            }
        }
        _fetcher_store.region_batch.erase(iter);
        if (insert_records.size() >= limit) {
            DB_DEBUG("get limit %ld", limit);
            break;
        }
    }
    state->ddl_scan_size = ddl_scan_size;
    state->ddl_max_pk_key = max_record;
    state->ddl_max_router_key = max_pk_str;
    std::string first_record;
    std::string last_record;
    if (state->ddl_scan_size > 0) {
        first_record = insert_records[0]->to_string();
        last_record = insert_records.back()->to_string();
        if (state->first_record_ptr == nullptr) {
            state->first_record_ptr.reset(new std::string(first_record));
        }
        state->last_record_ptr.reset(new std::string(last_record));
    }
    DB_NOTICE("task_%s ddl scan size %d, first_record %s last_record %s max_pk_key %s log_id %lu", 
        _task_id.c_str(), ddl_scan_size, first_record.c_str(), last_record.c_str(), str_to_hex(max_pk_str).c_str(), state->log_id());

    if (_is_global_index && ddl_scan_size > 0) {
        std::map<int64_t, pb::RegionInfo> empty_region_infos;
        set_region_infos(empty_region_infos);
        set_op_type(pb::OP_INSERT);
        DMLNode* exec_node = static_cast<DMLNode*>(_children[1]);
        LockSecondaryNode* second_node = static_cast<LockSecondaryNode*>(exec_node);
        second_node->set_ttl_timestamp(record_ttl_map);
        ret = send_request_light(state, exec_node, _fetcher_store, client_conn->seq_id, insert_records, delete_records);
        if (ret == -1) {
            state->ddl_error_code = state->error_code;
            DB_FATAL("task_%s send request error [%d] log_id %lu.", _task_id.c_str(), state->error_code, state->log_id());
        } else {
            DB_NOTICE("task_%s scan record %d, insert record %d log_id %lu", _task_id.c_str(), ddl_scan_size, ret, state->log_id());
            if (ret != ddl_scan_size) {
                DB_FATAL("task_%s scan number and insert number not equal log_id %lu.", _task_id.c_str(), state->log_id());
                ret = -1;
            }
        }
    }
    return ret;
}
}
