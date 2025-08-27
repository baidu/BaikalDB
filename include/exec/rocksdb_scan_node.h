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

#include "scan_node.h"
#include "table_record.h"
#include "table_iterator.h"
#include "transaction.h"
#include "reverse_index.h"
#include "reverse_interface.h"
#include "select_manager_node.h"
#include "arrow_io_excutor.h"

namespace baikaldb {
class ReverseIndexBase;
class RocksdbVectorizedReader;
class BatchTableKey {
public:
    BatchTableKey() {
        _row_key_pairs.reserve(ROW_BATCH_CAPACITY);
        _batch_vector.reserve(3);
    }
    void add_key(const std::string& left_key, bool left_full, const std::string& right_key, bool right_full) {
        _row_key_pairs.emplace_back(TableKeyPair(left_key, left_full, right_key, right_full));
    }
    void set_start_capacity(size_t row_batch_capacity) {
        _row_batch_capacity = row_batch_capacity;
        _batch_vector.reserve(std::min(_row_batch_capacity * 2, ROW_BATCH_CAPACITY));
    }

    size_t size() {
        return _row_key_pairs.size();
    }
    const TableKeyPair* get_next() {
        return &_row_key_pairs[_idx++];
    }
    const std::vector<TableKeyPair*>& get_next_batch() {
        size_t batch_size = ROW_BATCH_CAPACITY;
        if (_row_batch_capacity * _multiple < ROW_BATCH_CAPACITY) {
            batch_size = _row_batch_capacity * _multiple;
            //两倍扩散
            _multiple *= 2;
        }
        _batch_vector.clear();
        for (size_t i = 0; i < batch_size && _idx < size(); ++i) {
            _batch_vector.emplace_back(&_row_key_pairs[_idx++]);
        }
        return _batch_vector;
    }
    bool is_traverse_over() {
        return _idx >= size();
    }
private:
    std::vector<TableKeyPair> _row_key_pairs;
    std::vector<TableKeyPair*> _batch_vector;
    size_t _idx = 0;
    int _multiple = 1;
    size_t _row_batch_capacity = ROW_BATCH_CAPACITY;
};

class RocksdbScanNode : public ScanNode {
public:
    RocksdbScanNode() {
        _is_rocksdb_scan_node = true;
    }
    RocksdbScanNode(pb::Engine engine): ScanNode(engine) {
        _is_rocksdb_scan_node = true;
    }
    void inc_num_rows_returned(int64_t num) {
        _num_rows_returned += num;
        _num_rows_returned_by_range += num;
    }
    bool vectorized_filtered() {
        return _vectorized_filtered;
    }
    virtual ~RocksdbScanNode() {
        for (auto expr : _scan_conjuncts) {
            ExprNode::destroy_tree(expr);
        }
        for (auto expr : _vector_filter_conjuncts) {
            ExprNode::destroy_tree(expr);
        }
        for (auto expr : _pre_filter_conjuncts) {
            ExprNode::destroy_tree(expr);
        }
        for (auto expr : _update_exprs) {
            ExprNode::destroy_tree(expr);
        }
        delete _index_iter;
        delete _table_iter;
        if (_reverse_index != nullptr) {
            _reverse_index->clear();
        }
    }
    virtual int init(const pb::PlanNode& node);
    virtual int predicate_pushdown(std::vector<ExprNode*>& input_exprs);
    // TODO 索引条件下推后续也不需要做
    bool need_pushdown(ExprNode* expr);
    int index_condition_pushdown();
    int index_condition_pullup(); // 将向量索引无法过滤的条件添加回filter node中；
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos);
    virtual void close(RuntimeState* state);
    virtual bool can_use_arrow_vector(RuntimeState* state) {
        return true;
    }
    virtual int build_arrow_declaration(RuntimeState* state);
    bool contain_condition(ExprNode* expr) {
        std::unordered_set<int32_t> related_tuple_ids;
        expr->get_all_tuple_ids(related_tuple_ids);
        if (related_tuple_ids.size() == 1 && *(related_tuple_ids.begin()) == _tuple_id) {
            return true;
        } 
        return false;
    }
    virtual void transfer_pb(int64_t region_id, pb::PlanNode* pb_node);
    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
        ScanNode::find_place_holder(placeholders);
        for (auto& expr : _scan_conjuncts) {
            expr->find_place_holder(placeholders);
        }
    }
    // todo: 编码到PossibleIndex的条件未处理
    bool check_satisfy_condition(MemRow* row) override {
        if (!need_copy(row, _scan_conjuncts)) {
            return false;
        }
        return true;
    }

    int64_t copy_multiget_rows(RowBatch* output_batch, std::vector<ExprNode*>* conjuncts);

    int32_t get_partition_field() {
        return _table_info->partition_info.partition_field();
    }

    int64_t get_partition_num() {
        return _table_info->partition_num;
    }

    int decode_key_points(RuntimeState* state, RowBatch* batch, const rocksdb::Slice& key);

    int get_key_points(RuntimeState* state, RowBatch* batch, bool* eos);

private:
    int get_next_by_table_get(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_by_table_seek(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_by_index_get(RuntimeState* state, RowBatch* batch, bool* eos);
    int get_next_by_index_seek(RuntimeState* state, RowBatch* batch, bool* eos);
    int lock_primary(RuntimeState* state, MemRow* row);
    int index_ddl_work(RuntimeState* state, MemRow* row);
    int column_ddl_work(RuntimeState* state, MemRow* row);
    int process_ddl_work(RuntimeState* state, MemRow* row);
    int choose_index(RuntimeState* state);
    int vectorize_filter(RuntimeState* state, std::shared_ptr<Chunk> chunk);

    int multi_get_next(pb::StorageType st, SmartRecord record) {
        if (st == pb::ST_PROTOBUF_OR_FORMAT1 || st == pb::ST_ARROW) {
            return _m_index.get_next(record);
        }
        return -1;
    }
    bool multi_valid(pb::StorageType st) {
        if (st == pb::ST_PROTOBUF_OR_FORMAT1 || st == pb::ST_ARROW) {
            return _m_index.valid();
        }
        return false;
    }

    bool range_reach_limit() {
        if (_sort_use_index_by_range) {
            return _sort_limit_by_range != -1 &&
                    _num_rows_returned_by_range >= _sort_limit_by_range;
        }
        return false;
    }

private:
    std::map<int32_t, FieldInfo*> _field_ids;
    std::map<int32_t, FieldInfo*> _ddl_field_ids;
    std::vector<int32_t> _filt_field_ids;
    std::vector<int32_t> _trivial_field_ids;
    std::vector<int32_t> _field_slot;
    MemRowDescriptor* _mem_row_desc;
    SchemaFactory* _factory = nullptr;
    int64_t _index_id = -1;
    int64_t _region_id;
    bool _use_get = false;
    bool _is_ddl_work = false;
    bool _is_global_index = false;
    bool _has__weight = false;
    pb::DDLType _ddl_work_type = pb::DDL_NONE;
    int64_t _ddl_index_id = -1;

    // 如果用了排序列做索引，就不需要排序了
    bool _sort_use_index = false;
    bool _scan_forward = true; //scan的方向
    bool _sort_use_index_by_range = false;
    bool _use_encoded_key = false;
    bool _range_key_sorted = false;
    bool _bool_and = false;
    int64_t _sort_limit_by_range = 0;
    int64_t _num_rows_returned_by_range = 0;
    
    //被选择的索引
    std::vector<SmartRecord> _left_records; // vector index复用
    std::vector<SmartRecord> _right_records;
    BatchTableKey _scan_range_keys;
    BatchRecord   _multiget_records;
    RowBatch      _multiget_row_batch;
    int _left_field_cnt = 0;
    int _right_field_cnt = 0;
    bool _left_open = false;
    bool _right_open = false;
    bool _like_prefix = false;
    bool _is_eq = false;
    std::vector<pb::SlotDescriptor> _update_slots;
    std::vector<ExprNode*> _update_exprs;
    // trace使用
    int _scan_rows = 0;
    int64_t _read_disk_size = 0;
    size_t _idx = 0;
    //后续做下推用
    std::vector<ExprNode*> _scan_conjuncts;
    std::vector<ExprNode*> _vector_filter_conjuncts;
    std::vector<ExprNode*> _pre_filter_conjuncts; // 在向量索引中前过滤的条件
    IndexIterator* _index_iter = nullptr;
    TableIterator* _table_iter = nullptr;
    ReverseIndexBase* _reverse_index = nullptr;
    VectorIndex* _vector_index = nullptr;
    std::string _vector_word;
    int _topk = 10;
    int _efsearch = 16;
    uint64_t _separate_value = 0;

    SmartTable       _table_info;
    SmartIndex       _pri_info;
    SmartIndex       _index_info;
    SmartIndex       _ddl_index_info;
    pb::RegionInfo*  _region_info;
    std::vector<IndexInfo> _reverse_infos;
    std::vector<std::string> _query_words;
    std::vector<pb::MatchMode> _match_modes;
    std::vector<ReverseIndexBase*> _reverse_indexes;
    MutilReverseIndex _m_index;

    std::map<int32_t, int32_t> _index_slot_field_map;
    pb::StorageType _storage_type = pb::ST_UNKNOWN;
    bool _new_fulltext_tree = false;

    // vectorized
    // std::shared_ptr<BthreadArrowExecutor> _arrow_io_executor;
    std::shared_ptr<Chunk> _filter_chunk;
    std::shared_ptr<RocksdbVectorizedReader> _arrow_vectorized_reader;
    arrow::Expression _arrow_scan_conjuncts;
    arrow::Expression _arrow_filter_conjuncts;
    BatchRecord   _multiget_filter_records;
    int _arrow_filter_batch_size = 1024;
    bool _vectorized_filtered = false; 
    std::shared_ptr<BthreadArrowExecutor> _arrow_io_executor;
};

class RocksdbVectorizedReader : public arrow::RecordBatchReader {
public:
    RocksdbVectorizedReader() {}
    virtual ~RocksdbVectorizedReader() {}

    void set_arrow_scan_conjuncts(arrow::Expression* expr) {
        _arrow_scan_conjuncts = expr;
    }

    void set_arrow_filter_conjuncts(arrow::Expression* expr, int64_t limit);

    // for streaming output use
    std::shared_ptr<arrow::Schema> schema() const override { return _schema; }

    int init(RocksdbScanNode* scan_node, RuntimeState* state);
    
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

    int add_first_row_batch(RowBatch* batch);

private:
    RocksdbScanNode* _scan_node;
    RuntimeState* _state;
    bool _eos = false;
    bool _first_batch_need_handle = false;
    std::shared_ptr<arrow::Schema> _schema; // acero输入数据schema
    RowBatch _batch;
    arrow::Expression* _arrow_scan_conjuncts = nullptr; // scannode的index conjuncts
    arrow::Expression* _arrow_filter_conjuncts = nullptr;// filternode的conjuncts
    int64_t _filter_node_limit = -1;
    int64_t _pass_filter_count = 0;
};
} // end of namespace

/* vim: set ts=4 sw=4 sts=4 tw=100 */
