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

#include "delete_manager_node.h"
#include "network_socket.h"
#include "query_context.h"
#include "binlog_context.h"

namespace baikaldb {
int DeleteManagerNode::open(RuntimeState* state) {
    ExecNode* child_node = _children[0];
    //如果没有全局二级索引，直接走逻辑
    if (child_node->node_type() != pb::SELECT_MANAGER_NODE) {
        int ret =  DmlManagerNode::open(state);
        if (ret >= 0) {
            if (process_binlog(state, true) < 0) {
                return -1;
            }
        }
        return ret;
    }
    return open_global_delete(state);
}

int DeleteManagerNode::init_delete_info(const pb::DeleteNode& delete_node) {
    _op_type = pb::OP_DELETE;
    _table_id = delete_node.table_id();
    for (auto& slot : delete_node.primary_slots()) {
        _primary_slots.push_back(slot);
    }
    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("get table info failed, table_id:%ld", _table_id);
        return -1;
    }
    return 0;
}

int DeleteManagerNode::init_delete_info(const pb::UpdateNode& update_node) {
    _op_type = pb::OP_DELETE;
    _table_id = update_node.table_id();
    for (auto& slot : update_node.primary_slots()) {
        _primary_slots.push_back(slot);
    }
    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("get table info failed, table_id:%ld", _table_id);
        return -1;
    }
    return 0;
}

int DeleteManagerNode::open_global_delete(RuntimeState* state) {
    ExecNode* select_manager_node = _children[0];
    auto ret = select_manager_node->open(state);
    if (ret < 0) {
        DB_WARNING("select manager node fail");
        return ret;
    }
    _tuple_id = state->tuple_descs()[0].tuple_id();
    SmartRecord record_template = SchemaFactory::get_instance()->new_record(*_table_info);
    bool eos = false;
    std::vector<SmartRecord>        scan_records;
    do {
        RowBatch batch;
        ret = select_manager_node->get_next(state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            SmartRecord record = record_template->clone(false);
            //将mem_table的值填到table_record中
            for (auto slot : _primary_slots) {
                record->set_value(record->get_field_by_tag(slot.field_id()),  row->get_value(_tuple_id, slot.slot_id()));
            }
            scan_records.push_back(record);
        }
    } while (!eos);

    if (scan_records.size() == 0) {
        // 删除的数据不存在
        return 0;
    }
    
    //对主表进行lock_delete操作, 上一步scan得到的数据没有加锁，在真正的加锁操作中间是有可能被修改的。
    //所以delete_primary删除的同时需要返回最新的数据到baikaldb
    DMLNode* pri_node = static_cast<DMLNode*>(_children[1]);
    int64_t affected_rows = send_request(state, pri_node, std::vector<SmartRecord>{}, scan_records);
    if (affected_rows < 0) {
        return affected_rows;
    }
    _children.erase(_children.begin() + 1);
    //从第三个孩子开始是索引数据的删除node
    if (_fetcher_store.index_records.size() <= 0) {
        return affected_rows;
    }
    _del_scan_records.swap(_fetcher_store.index_records[_table_info->id]);
    auto iter = _children.begin() + 1;
    while (iter != _children.end()) {
        DMLNode* sec_node = static_cast<DMLNode*>(*iter);
        ret = send_request(state, sec_node, std::vector<SmartRecord>{}, _del_scan_records);
        if (ret < 0) {
            return ret;
        }
        iter = _children.erase(iter);
    }
    process_binlog(state, false);
    return affected_rows;
}

int DeleteManagerNode::process_binlog(RuntimeState* state, bool is_local) {
    if (state->open_binlog() && _table_info->is_linked) {
        // delete没有数据删除跳过
        if (is_local && _fetcher_store.return_str_records.size() == 0) {
            return 0;
        }
        auto client = state->client_conn();
        auto binlog_ctx = client->get_binlog_ctx();
        auto ctx = client->get_query_ctx();
        pb::PrewriteValue* binlog_value = binlog_ctx->mutable_binlog_value();
        auto mutation = binlog_value->add_mutations();
        mutation->add_sequence(pb::MutationType::DELETE);
        mutation->set_table_id(_table_id);
        if (ctx != nullptr) {
            mutation->set_sql(ctx->sql);
            auto stat_info = &(ctx->stat_info);
            mutation->set_sign(stat_info->sign);
            binlog_ctx->add_sql_info(stat_info->family, stat_info->table, stat_info->sign);
        }
        binlog_ctx->set_table_info(_table_info);
        if (is_local) {
            SmartRecord record_template = _factory->new_record(_table_id);
            bool need_set_partition_record = true;
            for (auto& str_record : _fetcher_store.return_str_records) {
                mutation->add_deleted_rows(str_record);
                if (need_set_partition_record) {
                    SmartRecord record = record_template->clone(false);
                    auto ret = record->decode(str_record);
                    if (ret < 0) {
                        DB_FATAL("decode to record fail");
                        return -1;
                    }
                    binlog_ctx->set_partition_record(record);
                    need_set_partition_record = false;
                }
            }
        } else {
            binlog_ctx->set_partition_record(_del_scan_records[0]);
            for (auto& record : _del_scan_records) {
                std::string* row = mutation->add_deleted_rows();
                record->encode(*row);
            }
        }
    }
    return 0;
}

}
