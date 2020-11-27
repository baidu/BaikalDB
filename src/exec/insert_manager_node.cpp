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
#include "insert_manager_node.h"
#include "update_manager_node.h"
#include "insert_node.h"
#include "network_socket.h"
#include "type_utils.h"
#include "binlog_context.h"
#include <set>

namespace baikaldb {
int InsertManagerNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _node_type = pb::INSERT_MANAGER_NODE; 
    return 0;
}

int InsertManagerNode::init_insert_info(UpdateManagerNode* update_manager_node) {
    _op_type = pb::OP_INSERT;
    _table_id = update_manager_node->table_id();
    _table_info = _factory->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("no table found with table_id: %ld", _table_id);
        return -1;
    }
    for (const auto index_id : _table_info->indices) {
        auto info_ptr = _factory->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("no index info found with index_id: %ld", index_id);
            return -1;
        }
        if (info_ptr->type == pb::I_PRIMARY) {
            _pri_info = info_ptr;
        }
        if (info_ptr->type == pb::I_PRIMARY || info_ptr->type == pb::I_UNIQ) {
            _index_info_map[index_id] = info_ptr;
        }
    }
    _uniq_index_number = update_manager_node->uniq_index_number();
    return 0;
}

int InsertManagerNode::init_insert_info(InsertNode* insert_node) {
    _op_type = pb::OP_INSERT;
    _table_id = insert_node->table_id();
    _tuple_id = insert_node->tuple_id();
    _values_tuple_id = insert_node->values_tuple_id();
    _is_replace = insert_node->is_replace();
    _need_ignore = insert_node->need_ignore();
    _table_info = _factory->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("no table found with table_id: %ld", _table_id);
        return -1;
    }
    for (const auto index_id : _table_info->indices) {
        auto info_ptr = _factory->get_index_info_ptr(index_id);
        if (info_ptr == nullptr) {
            DB_WARNING("no index info found with index_id: %ld", index_id);
            return -1;
        }
        if (info_ptr->type == pb::I_PRIMARY) {
            _pri_info = info_ptr;
        }
        if (info_ptr->type == pb::I_PRIMARY || info_ptr->type == pb::I_UNIQ) {
            _index_info_map[index_id] = info_ptr;
        }
        if (info_ptr->is_global) {
            _affected_index_num++;
            if (info_ptr->type == pb::I_UNIQ) {
                _uniq_index_number++;
            }
        }
    }
    _update_slots.swap(insert_node->update_slots());
    _update_exprs.swap(insert_node->update_exprs());
    _insert_values.swap(insert_node->insert_values());
    _on_dup_key_update = _update_slots.size() > 0;
    return 0;
}

int InsertManagerNode::subquery_open(RuntimeState* state) {
    int ret = 0;
    for (auto expr : _select_projections) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("Expr::open fail:%d", ret);
            return ret;
        }
    }
    _table_info = _factory->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("no table found with table_id: %ld", _table_id);
        return -1;
    }
    for (size_t i = 0; i < _table_info->fields.size(); i++) {
        pb::PrimitiveType sub_type = _select_projections[i]->col_type();
        bool is_literal = _select_projections[i]->is_literal();
        pb::ExprNodeType node_type = _select_projections[i]->node_type();
        pb::PrimitiveType insert_type = _table_info->fields[i].type;
        if (!is_compatible_type(sub_type, insert_type, is_literal || (node_type == pb::FUNCTION_CALL))) {
            state->error_code = ER_TRUNCATED_WRONG_VALUE_FOR_FIELD;
            state->error_msg << "Incorrect cloumn type expect " << pb::PrimitiveType_Name(insert_type);
            state->error_msg << " but " << pb::PrimitiveType_Name(sub_type);
            return -1;
        }
    }
    // 最后一个clild就是_sub_query_node
    _children.pop_back();
    ret = _sub_query_node->open(_sub_query_runtime_state);
    if (ret < 0) {
        return ret;
    }
    SmartRecord record_template = _factory->new_record(_table_id);
    if (record_template == nullptr) {
        DB_WARNING("table not found table_id: %ld", _table_id);
        return -1;
    }
    bool eos = false;
    do {
        RowBatch batch;
        ret = _sub_query_node->get_next(_sub_query_runtime_state, &batch, &eos);
        if (ret < 0) {
            DB_WARNING("children:get_next fail:%d", ret);
            return ret;
        }
        for (batch.reset(); !batch.is_traverse_over(); batch.next()) {
            MemRow* row = batch.get_row().get();
            SmartRecord record = record_template->clone(false);
            for (size_t i = 0; i < _select_projections.size(); i++) {
                auto expr = _select_projections[i];
                ExprValue result = expr->get_value(row).cast_to(expr->col_type());
                record->set_value(record->get_field_by_idx(i), result);
            }
            _origin_records.push_back(record);
            //DB_WARNING("record:%s", record->debug_string().c_str());
        }
    } while (!eos);
    return  0;
}


void InsertManagerNode::process_binlog(RuntimeState* state, bool save_data) {
    if (state->open_binlog() && _table_info->is_linked) {
        auto client = state->client_conn();
        auto binlog_ctx = client->get_binlog_ctx();
        binlog_ctx->set_partition_record(_insert_scan_records[0]);
        binlog_ctx->set_table_info(_table_info);
        if (save_data) {
            pb::PrewriteValue* binlog_value = binlog_ctx->mutable_binlog_value();
            auto mutation = binlog_value->add_mutations();
            mutation->set_table_id(_table_id);
            if (_del_scan_records.size() == 0) {
                mutation->add_sequence(pb::MutationType::INSERT);
            } else {
                mutation->add_sequence(pb::MutationType::UPDATE);
            }
            for (auto& record : _insert_scan_records) {
                std::string* row = mutation->add_insert_rows();
                record->encode(*row);
            }
            for (auto& record : _del_scan_records) {
                std::string* row = mutation->add_deleted_rows();
                record->encode(*row);
            }
        }
    }
}

int InsertManagerNode::open(RuntimeState* state) {
    TimeCost cost;
    int ret = 0;
    if (_sub_query_node != nullptr) {
        ret = subquery_open(state);
        if (ret < 0) {
            return ret;
        }
        if (_origin_records.empty()) {
            return 0;
        }
    }
    // no global index for InsertNode
    if (_children[0]->node_type() == pb::INSERT_NODE) {
        if (_sub_query_node != nullptr) {
            DMLNode* dml_node = static_cast<DMLNode*>(_children[0]);
            ret = get_region_infos(state, dml_node, _origin_records,
                _del_scan_records, _region_infos);
            if (ret < 0) {
                return ret;
            }
        }
        return DmlManagerNode::open(state);
    }
    ret = process_records_before_send(state);
    if (ret < 0) {
        return -1;    
    }
    //int64_t pre_insert_cost = cost.get_time();
    //DB_WARNING("insert pre process time_cost: %ld, log_id: %lu", pre_insert_cost, state->log_id());
    for (auto expr : _update_exprs) {
        ret = expr->open();
        if (ret < 0) {
            DB_WARNING("expr open fail, log_id:%lu ret:%d", state->log_id(), ret);
            return ret;
        }
    }
    if (_need_ignore) {
        ret = insert_ignore(state);
    } else if (_is_replace) {
        ret =  insert_replace(state);
    } else if (_on_dup_key_update) {
        _dup_update_row = state->mem_row_desc()->fetch_mem_row();
        if (_tuple_id >= 0) {
            _tuple_desc = state->get_tuple_desc(_tuple_id);
        }
        if (_values_tuple_id >= 0) {
            _values_tuple_desc = state->get_tuple_desc(_values_tuple_id);
        }
        ret =  insert_on_dup_key_update(state);
    } else {
        ret =  basic_insert(state);
    }
    if (ret >=0) {
        process_binlog(state, true);
    } else {
        process_binlog(state, false);
    }
    return ret;
}

int InsertManagerNode::basic_insert(RuntimeState* state) {
    int ret = 0;
    _affected_rows = _insert_scan_records.size();
    // 主键和全局唯一二级索引串行执行
    size_t serial_num = _uniq_index_number;
    auto iter = _children.begin();
    size_t cur = 0;
    while (iter != _children.end() && cur < serial_num) {
        DMLNode* dml_node = static_cast<DMLNode*>(*iter);
        ret = send_request(state, dml_node, _insert_scan_records, _del_scan_records);
        if (ret < 0) {
            DB_WARNING("exec node failed, log_id:%lu ret:%d ", state->log_id(), ret);
            return -1;
        }
        iter = _children.erase(iter);
        cur++;
    }
    ret = send_request_concurrency(state, 0);
    if (ret < 0) {
        DB_WARNING("exec concurrency failed, log_id:%lu ret:%d ", state->log_id(), ret);
        return ret;
    }
    return _affected_rows;
}

int InsertManagerNode::insert_ignore(RuntimeState* state) {
    int ret = 0;
    // 获取主表数据和索引数据
    ret = get_record_from_store(state);
    if (ret < 0) {
        return -1;
    }
    // 处理冲突行
    for (auto pair : _index_info_map) {
        int64_t index_id = pair.first;
        auto key_ids_map = _index_keys_record_map[index_id];
        auto info = pair.second;
        auto return_records = _store_records[index_id];
        for (auto& record : return_records) {
            MutTableKey mt_key;
            ret = record->encode_key(*info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", info->id, state->log_id(), ret);
                return ret;
            } 
            std::string key = mt_key.data();
            auto ids_set = key_ids_map[key];
            // 移除冲突行
            for (auto id : ids_set) {
                _record_ids.erase(id);
            }
        }        
    }
    // 重新生成需要insert的行
    _insert_scan_records.clear();
    for (auto& id : _record_ids) {
        _insert_scan_records.push_back(_origin_records[id]);
    }
    if (_insert_scan_records.size() == 0) {
        return 0;
    }
    _affected_rows = _record_ids.size();
    // 写主表和全局二级索引并发
    ret = send_request_concurrency(state, 0);
    if (ret < 0) {
        DB_WARNING("exec concurrency failed log_id:%lu, ret:%d ", state->log_id(), ret);
        return ret;
    }
    return _affected_rows;
}

int InsertManagerNode::insert_replace(RuntimeState* state) {
    int ret = 0;
    // 获取主表数据和索引数据
    ret = get_record_from_store(state);
    if (ret < 0) {
        return -1;
    }
    // 反查主表
    ret = reverse_main_table(state);
    if (ret < 0) {
        DB_WARNING("reverse main_table failed log_id:%lu ret:%d", state->log_id(), ret);
        return -1;
    }
    // 判断冲突行
    std::set<int32_t> dup_record_ids;
    for (auto pair : _index_info_map) {
        int64_t index_id = pair.first;
        auto key_ids_map = _index_keys_record_map[index_id];
        auto info = pair.second;
        auto return_records = _store_records[index_id];
        for (auto& record : return_records) {
            MutTableKey mt_key;
            ret = record->encode_key(*info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", info->id, state->log_id(), ret);
                return ret;
            }
            std::string key = mt_key.data();
            auto ids_set = key_ids_map[key];
            // 移除冲突行,保留最后行
            if (ids_set.size() > 1) {
                auto last = std::prev(ids_set.end());
                for (auto it = ids_set.begin(); it != last; ++it) {
                    if (_record_ids.erase(*it)) {
                        _affected_rows++;
                    }
                }
            }
        }
        // 处理待插入的record有冲突的情况
        for (auto key_ids : key_ids_map) {
            auto ids_set = key_ids.second;
            if (ids_set.size() > 1) {
                auto last = std::prev(ids_set.end());
                for (auto it = ids_set.begin(); it != last; ++it) {
                    if (_record_ids.erase(*it)) {
                        dup_record_ids.insert(*it);
                    }
                }
            }
        }
    }
    if (dup_record_ids.size() > 0) {
        // 2=插入+删除
        _affected_rows += dup_record_ids.size() * 2;
    }
    _insert_scan_records.clear();
    for (auto& id : _record_ids) {
        _insert_scan_records.push_back(_origin_records[id]);
    }
    _del_scan_records = _store_records[_pri_info->id];
    _affected_rows += _insert_scan_records.size(); 
    _affected_rows += _del_scan_records.size(); 
    size_t start_child = 0;
    if (!_main_table_reversed) {
        ++start_child;
    }
    if (!_has_conflict_record) {
        // 完全没有冲突,写主表和全局二级索引全并发
        ret = send_request_concurrency(state, start_child);
        if (ret < 0) {
            DB_WARNING("exec concurrency failed, log_id:%lu ret:%d ", state->log_id(), ret);
            return ret;
        }
    } else {
        auto iter = _children.begin() + start_child;
        size_t serial_num = _uniq_index_number;
        size_t cur = 0;
        // 写主表和全局唯一二级索引串行,最后一个唯一索引和非唯一索引并发
        while (iter != _children.end() && cur < serial_num) {
            DMLNode* dml_node = static_cast<DMLNode*>(*iter);
            ret = send_request(state, dml_node, _insert_scan_records, _del_scan_records);
            if (ret < 0) {
                DB_WARNING("exec node failed, log_id:%lu ret:%d ", state->log_id(), ret);
                return -1;
            }
            iter = _children.erase(iter);
            cur++;
        }
        // 全局非唯一二级索引并行
        ret = send_request_concurrency(state, start_child);
        if (ret < 0) {
            DB_WARNING("exec concurrency failed, log_id:%lu ret:%d ", state->log_id(), ret);
            return ret;
        }
    }
    return _affected_rows;
}

int InsertManagerNode::insert_on_dup_key_update(RuntimeState* state) {
    int ret = 0;
    // 获取主表数据和索引数据
    ret = get_record_from_store(state);
    if (ret < 0) {
        return -1;
    }
    // 反查主表
    ret = reverse_main_table(state);
    if (ret < 0) {
        DB_WARNING("reverse main_table failed log_id:%lu ret:%d", state->log_id(), ret);
        return -1;
    }
    std::set<int32_t> dup_record_ids;
    for (auto pair : _index_info_map) {
        int64_t index_id = pair.first;
        auto key_ids_map = _index_keys_record_map[index_id];
        auto info = pair.second;
        auto return_records = _store_records[index_id];
        for (auto& record : return_records) {
            MutTableKey mt_key;
            ret = record->encode_key(*info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", info->id, state->log_id(), ret);
                return ret;
            }
            std::string key = mt_key.data();
            auto ids_set = key_ids_map[key];
            // 移除冲突行
            for (auto id : ids_set) {
                if (_record_ids.erase(id)) {
                    update_record(_on_dup_key_update_records[index_id][key]);
                }
            }
        }
        // 处理待插入的record有冲突的情况
        for (auto key_ids : key_ids_map) {
            auto ids_set = key_ids.second;
            if (ids_set.size() > 1) {
                auto min = ids_set.begin();
                for (auto it = std::next(min); it != ids_set.end(); ++it) {
                    if (_record_ids.erase(*it)) {
                        dup_record_ids.insert(*it);
                        update_record(_origin_records[*min]);
                    }
                }
                _record_ids.insert(*min);
            }
        }
    }
    _insert_scan_records.clear();
    for (auto& id : _record_ids) {
        _insert_scan_records.push_back(_origin_records[id]);
    }
    if (dup_record_ids.size() > 0) {
        // 2=插入+删除
        _affected_rows += dup_record_ids.size() * 2;
    }
    /* 不同的插入row与主表中的某row冲突
        insert into `t1`(id,name1,name2,class1,class2) values(1,'zhangsan1','zhangsan11',10,11);
        insert into `t1`(id,name1,name2,class1,class2,address1) values(2,'lisi2','lisi22',20,21);
        insert into `t1`(id,name1,name2,class1,class2) values(3,'wangwu3','wangwu33',30,3);
        insert into `t1`(id,name1,name2,class1,class2) values(4,'zhaoliu4','zhaoliu44',40,41);

        insert into `t1`(id,name1,name2,class1,class2) values(1,'zhangsan9','zhangsan99',90,91),
            (11,'zhangsan1','zhangsan1111',1100,1101) on duplicate key update class1 = class1+1;

        插入的第一行id=1和第二行name1='zhangsan1'与表中已有数据冲突，_on_dup_key_update_records中两个记录
        对应同一条主表返回的record
    */
    std::set<SmartRecord> updated_records;
    for (auto idx_pair : _on_dup_key_update_records) {
        for (auto key_pair : idx_pair.second) {
            updated_records.insert(key_pair.second);
        }
    }
    for (auto record :  updated_records) {
        _insert_scan_records.push_back(record);
    }
    _del_scan_records = _store_records[_pri_info->id];
    _affected_rows += _insert_scan_records.size();
    _affected_rows += _del_scan_records.size(); 
    size_t start_child = 0;
    if (!_main_table_reversed) {
        ++start_child;
    }
    if (!_has_conflict_record) {
        // 完全没有冲突,写主表和全局二级索引全并发
        ret = send_request_concurrency(state, start_child);
        if (ret < 0) {
            DB_WARNING("exec concurrency failed, log_id:%lu ret:%d ", state->log_id(), ret);
            return ret;
        }
    } else {
        auto iter = _children.begin() + start_child;
        size_t serial_num = _uniq_index_number;
        size_t cur = 0;
        // 写主表和全局唯一二级索引串行,最后一个唯一索引和非唯一索引并发
        while (iter != _children.end() && cur < serial_num) {
            DMLNode* dml_node = static_cast<DMLNode*>(*iter);
            ret = send_request(state, dml_node, _insert_scan_records, _del_scan_records);
            if (ret < 0) {
                DB_WARNING("exec node failed, log_id:%lu ret:%d", state->log_id(), ret);
                return -1;
            }
            iter = _children.erase(iter);
            cur++;
        }
        // 全局非唯一二级索引并行
        ret = send_request_concurrency(state, start_child);
        if (ret < 0) {
            DB_WARNING("exec concurrency failed, log_id:%lu ret:%d", state->log_id(), ret);
            return ret;
        }
    }
    return _affected_rows;
}

void InsertManagerNode::update_record(SmartRecord record) {
    // 处理values函数
    _dup_update_row->clear();
    if (_values_tuple_desc != nullptr) {
        for (auto slot : _values_tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
            _dup_update_row->set_value(slot.tuple_id(), slot.slot_id(),
                    record->get_value(field));
        }
    }
    // 更新数据
    auto row = _dup_update_row.get();
    if (_tuple_desc != nullptr) {
        for (auto slot : _tuple_desc->slots()) {
            auto field = record->get_field_by_tag(slot.field_id());
                row->set_value(slot.tuple_id(), slot.slot_id(),
                        record->get_value(field));
        }
    }
    for (size_t i = 0; i < _update_exprs.size(); i++) {
        auto& slot = _update_slots[i];
        auto expr = _update_exprs[i];
        record->set_value(record->get_field_by_tag(slot.field_id()),
            expr->get_value(row).cast_to(slot.slot_type()));
    }
}

int InsertManagerNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    //DB_WARNING("expr_optimize exec");
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("expr type_inferer fail:%d", ret);
        return ret;
    }
    for (auto expr : _update_exprs) {
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
    }
    for (auto expr : _insert_values) {
        ret = expr->expr_optimize();
        if (ret < 0) {
            DB_WARNING("expr type_inferer fail:%d", ret);
            return ret;
        }
        if (!expr->is_constant()) {
            DB_WARNING("insert expr must be constant");
            return -1;
        }
    }
    return 0;
}

int InsertManagerNode::reverse_main_table(RuntimeState* state) {
    int ret = 0;
    if (!_primary_record_key_record_map_construct) {
        for (auto record : _store_records[_pri_info->id]) {
            MutTableKey mt_key;
            ret = record->encode_key(*_pri_info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", _pri_info->id, state->log_id(), ret);
                return ret;
            }
            std::string key = mt_key.data();
            _primary_record_key_record_map[key] = record->clone();;
        }
        _primary_record_key_record_map_construct = true;
    }
    // 判断返回的二级索引数据是否主键已返回
    _insert_scan_records.clear();
    std::map<int64_t, std::set<std::string>> reversed_idx_keys_map;
    for (auto& pair : _store_records) {
        int64_t index_id = pair.first;
        if (index_id == _pri_info->id) {
            continue;
        }
        for (auto& record : pair.second) {
            MutTableKey mt_key;
            ret = record->encode_key(*_pri_info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, log_id:%lu record:%s", state->log_id(), record->debug_string().c_str());
                return ret;
            }
            std::string pk_key = mt_key.data();
            auto pk_record_iter = _primary_record_key_record_map.find(pk_key);
            if (pk_record_iter == _primary_record_key_record_map.end()) {
                // 二级索引的主键主表没有返回，需要反查主表
                _insert_scan_records.push_back(record);
                if (_on_dup_key_update) {
                    MutTableKey mt_key;
                    auto info = _index_info_map[index_id];
                    ret = record->encode_key(*info, mt_key, -1, false);
                    if (ret < 0) {
                        DB_WARNING("encode key failed, log_id:%lu record:%s", state->log_id(), record->debug_string().c_str());
                        return ret;
                    }
                    std::string key = mt_key.data();
                    reversed_idx_keys_map[index_id].insert(key);
                }
            } else {
                if (_on_dup_key_update) {
                    auto info = _index_info_map[index_id];
                    MutTableKey mt_key;
                    ret = record->encode_key(*info, mt_key, -1, false);
                    if (ret < 0) {
                        DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", index_id, state->log_id(), ret);
                        return ret;
                    }
                    std::string key = mt_key.data();
                    auto pk_record  = pk_record_iter->second;
                    _on_dup_key_update_records[index_id][key] = pk_record;
                    //DB_WARNING("index_id:%ld key:%s %d record:%s", index_id, str_to_hex(key).c_str(), key.size(), pk_record->debug_string().c_str());
                }
            }
        }
    }
    if (_primary_record_key_record_map.size() == 0 && _insert_scan_records.size() == 0) {
        // 完全没有冲突
        _has_conflict_record = false;
        return 0;
    }
    // 反查主表
    if (_insert_scan_records.size() > 0) {
        DMLNode* pri_node = static_cast<DMLNode*>(_children[0]);
        ret = send_request(state, pri_node, _insert_scan_records, _del_scan_records);
        if (ret < 0) {
            DB_WARNING("exec node failed, log_id:%lu ret:%d ", state->log_id(), ret);
            return -1;
        }
        _main_table_reversed = true;
        _children.erase(_children.begin());
        add_store_records();
        if (_on_dup_key_update) {
            for (auto record : _store_records[_pri_info->id]) {
                for (auto pair : reversed_idx_keys_map) {
                    int64_t index_id = pair.first;
                    auto info = _index_info_map[index_id];
                    MutTableKey mt_key;
                    ret = record->encode_key(*info, mt_key, -1, false);
                    if (ret < 0) {
                        DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", index_id, state->log_id(), ret);
                        return ret;
                    }
                    std::string key = mt_key.data();
                    if (pair.second.count(key) > 0) {
                        _on_dup_key_update_records[index_id][key] = record->clone();
                        //DB_WARNING("index_id:%ld key:%s %d record:%d", index_id, str_to_hex(key).c_str(), key.size(), record->debug_string().c_str());
                    }
                }
            }
        }
    }
    return 0;
}

int InsertManagerNode::get_record_from_store(RuntimeState* state) {
    int ret = 0;
    // 获取主表数据
    DMLNode* pri_node = static_cast<DMLNode*>(_children[0]);
    ret = send_request(state, pri_node, _insert_scan_records, _del_scan_records);
    if (ret < 0) {
        DB_WARNING("exec node failed, log_id:%lu ret:%d ", state->log_id(), ret);
        return -1;
    }
    // send_request成功后会把node缓存到NetworkSocket的cache_plan,这里需要将node移除
    // 防止二次释放
    _children.erase(_children.begin());
    add_store_records();
    if (_on_dup_key_update) {
        int64_t index_id = _pri_info->id;
        for (auto record : _store_records[index_id]) {
            MutTableKey mt_key;
            ret = record->encode_key(*_pri_info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", index_id, state->log_id(), ret);
                return ret;
            }
            std::string key = mt_key.data();
            //DB_WARNING("index_id:%ld key:%s %d record:%s", index_id, str_to_hex(key).c_str(), key.size(), record->debug_string().c_str());
            SmartRecord pk_record = record->clone();
            _on_dup_key_update_records[index_id][key] = pk_record;
            _primary_record_key_record_map[key] = pk_record;
        }
        _primary_record_key_record_map_construct = true;
    }
    // 获取二级索引数据，返回索引数据+pk数据
    auto iter = _children.begin();
    size_t cnt = 0;
    auto node_type = (*iter)->node_type();
    while (node_type == pb::LOCK_SECONDARY_NODE) {
        DMLNode* sec_node = static_cast<DMLNode*>(*iter);
        ret = send_request(state, sec_node, _insert_scan_records, _del_scan_records);
        if (ret < 0) {
            DB_WARNING("exec node failed, log_id:%lu ret:%d ", state->log_id(), ret);
            return -1;
        }
        cnt++;
        iter = _children.erase(iter);
        node_type = (*iter)->node_type();
        add_store_records();
    }
    return 0;
}

void InsertManagerNode::set_err_message(IndexInfo& index_info,
                                        SmartRecord& record,
                                        RuntimeState* state) {
    if (index_info.type == pb::I_PRIMARY) {
        DB_WARNING("has dup key index:%ld log_id:%lu ", index_info.id, state->log_id());
        state->error_code = ER_DUP_ENTRY;
        state->error_msg << "Duplicate entry: '" <<
          record->get_index_value(index_info) << "' for key 'PRIMARY'";
    } else {
        DB_WARNING("has dup key index:%ld log_id:%lu", index_info.id, state->log_id());
        state->error_code = ER_DUP_ENTRY;
        state->error_msg << "Duplicate entry: '" << record->get_index_value(index_info) <<
             "' for key '" << index_info.short_name << "'";
    }
}

int InsertManagerNode::process_records_before_send(RuntimeState* state) {
    int32_t ret = 0;
    int32_t id = 0; 
    std::set<int> need_remove_ids;
    for (auto record : _origin_records) {
        for (const auto pair : _index_info_map) {
            auto info = *pair.second;
            MutTableKey mt_key;
            int64_t index_id = info.id;
            ret = record->encode_key(info, mt_key, -1, false);
            if (ret < 0) {
                DB_WARNING("encode key failed, index:%ld log_id:%lu ret:%d", index_id, state->log_id(), ret);
                return ret;
            }
            std::string key = mt_key.data();
            if (_is_replace || _on_dup_key_update) {
                _record_ids.insert(id); 
                _index_keys_record_map[index_id][key].insert(id);
                continue;
            }

            //DB_WARNING("index_id:%ld key:%s id:%d", index_id, key.c_str(), id);
            auto iter = _index_keys_record_map[index_id].find(key);
            if (iter == _index_keys_record_map[index_id].end()) {
                _record_ids.insert(id); 
                _index_keys_record_map[index_id][key].insert(id);
            } else {
                // has duplicate key
                if (_need_ignore) {
                    // igonre
                    need_remove_ids.insert(id);
                } else {
                    // basic insert: set error message and retrun 
                    set_err_message(info, record, state);
                    return -1;
                }
            }
        }
        id++;
    }
    for (auto id : need_remove_ids) {
        for (auto index_key_pair : _index_keys_record_map) {
            for (auto key_ids_pair : index_key_pair.second) {
                key_ids_pair.second.erase(id);
                _record_ids.erase(id);
            }
        }
    }

    for (auto& id : _record_ids) {
        _insert_scan_records.push_back(_origin_records[id]);
    }
    return 0;
}

} 
/* vim: set ts=4 sw=4 sts=4 tw=100 */
