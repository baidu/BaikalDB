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
#include "sort_merge.h"
#include "row2column.h"
namespace baikaldb {

// 需要init时在内存中排序完成
int UnOrderSingleRowReader::init() {
    if (_init) {
        return 0;
    }
    TimeCost cost;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> record_batch;
        auto s = _reader->ReadNext(&record_batch);
        if (!s.ok()) {
            DB_COLUMN_FATAL("UnOrderSingleRowReader read next failed");
            return -1;
        }
        if (record_batch == nullptr) {
            break;
        }
        auto rb2 = std::make_shared<RecordBatch2>(record_batch, _schema_info);
        _record_batches.push_back(rb2);
        for (int i = 0; i < record_batch->num_rows(); ++i) {
            UnorderRow row;
            row.pos = i;
            row.batch = rb2.get();
            _sort.emplace_back(row);
        }
    }

    std::sort(_sort.begin(), _sort.end());

    _init = true;
    DB_NOTICE("init unorder reader success, row_num: %ld, cost: %ld", _sort.size(), cost.get_time());
    return 0;
}

int UnOrderSingleRowReader::get_next(SingleRow& row) {
    if (init() != 0) {
        return -1;
    }

    if (_sort_pos >= _sort.size()) {
        row.set_invalid();
        return 0;
    } else {
        const auto& unorderrow = _sort[_sort_pos];
        unorderrow.batch->get_full_row(row.get_values(), unorderrow.pos);
        _sort_pos++;
    }
    
    return 0;
}

int OrderSingleRowReader::get_next(SingleRow& row) {
    if (init() != 0) {
        return -1;
    }
    if (_record_batch2 == nullptr) {
        row.set_invalid();
        return 0;
    }
    
    if (_pos >= _record_batch2->num_rows()) {
        std::shared_ptr<arrow::RecordBatch> rb;
        auto s = _reader->ReadNext(&rb);
        if (!s.ok()) {
            DB_COLUMN_FATAL("read next failed");
            return -1;
        }
        _record_batch2 = nullptr;
        _pos = 0;
        if (rb == nullptr) {
            row.set_invalid();
            return 0;
        } else {
            _record_batch2 = std::make_shared<RecordBatch2>(rb, _schema_info);
            _record_batch2->get_full_row(row.get_values(), _pos);
            _pos++;
        }
    } else {
        _record_batch2->get_full_row(row.get_values(), _pos);
        _pos++;
    }
    return 0;
}

int SortMerge::get_next(std::shared_ptr<arrow::RecordBatch>* out) {
    TimeCost cost;
    while (true) {
        SingleRow merged_row;
        int ret = get_merged_row(merged_row);
        if (ret < 0) {
            DB_COLUMN_FATAL("get_merged_rows failed");
            return -1;
        }

        if (!merged_row.is_valid()) {
            break;
        }

        auto keytype = merged_row.get_keytype();
        if (keytype == COLUMN_KEY_DELETE && _options.is_base_compact) {
            continue;
        }

        if (keytype == COLUMN_KEY_MERGE) {
            _merge_count++;
        } else if (keytype == COLUMN_KEY_DELETE) {
            _delete_count++;
        } else {
            _put_count++;
        }

        ret = _column_record->append_row(merged_row.get_values());
        if (ret != 0) {
            DB_COLUMN_FATAL("append rows failed");
            return -1;
        }

        if (_column_record->is_full()) {
            break;
        }
    }

    if (_column_record->size() > 0) {
        int ret = _column_record->finish_and_make_record_batch(out);
        if (ret != 0) {
            DB_COLUMN_FATAL("finish and make record batch failed");
            return -1;
        }
        // DB_NOTICE("region_id: %ld, get next record batch: %ld, cost: %ld", _options.region_id, (*out)->num_rows(), cost.get_time());
    } else {
        out->reset();
    }
    return 0;
}

int SortMerge::multi_reader_deal_first_row() {
    if (!_heap.empty()) {
        return 0;
    }

    _heap.reserve(_readers.size());
    for (size_t i = 0; i < _readers.size(); ++i) {
        SingleRow first_row;
        first_row.alloc_row(&_cache_row_pool);
        int ret = _readers[i]->get_next(first_row);
        if (ret != 0) {
            DB_COLUMN_FATAL("get first row failed");
            return ret;
        }

        first_row.slot = i;
        _heap.push(std::move(first_row));
    }

    _heap.make_heap();

    return 0;
}

int SortMerge::multi_reader_get_row(SingleRow& row) {
    int ret = multi_reader_deal_first_row();
    if (ret != 0) {
        DB_COLUMN_FATAL("deal first row failed");
        return -1;
    }

    _heap.swap_top(row);
    if (!row.is_valid()) {
        return 0;
    }
    SingleRow slot_row;
    slot_row.alloc_row(&_cache_row_pool);
    ret = _readers[row.slot]->get_next(slot_row);
    if (ret != 0) {
        return ret;
    }
    slot_row.slot = row.slot;
    _heap.replace_top(std::move(slot_row));

    return 0;
}

int SortMerge::get_merged_row(SingleRow& row) {
    while (true) {
        SingleRow sorted_row;
        int ret = get_sorted_row(sorted_row);
        if (ret != 0) {
            DB_COLUMN_FATAL("get sorted row failed");
            return -1;
        }
        if (!sorted_row.is_valid()) {
            break;
        }
        if (_merge_queue.empty()) {
            _merge_queue.push(std::move(sorted_row));
        } else {
            auto& last_row = _merge_queue.back();
            if (last_row.compare_by_primary_key(sorted_row) == 0) {
                ret  = last_row.merge(sorted_row);
                if (ret != 0) {
                    DB_COLUMN_FATAL("merge rows failed");
                    return -1;
                }
            } else {
                _merge_queue.push(std::move(sorted_row));
            }
        }
        if (_merge_queue.size() >= 2) {
            break;
        }
    }

    if (_merge_queue.empty()) {
        row.set_invalid();
    } else {
        row = std::move(_merge_queue.front());
        _merge_queue.pop();
    }

    return 0;
}

int AceroMerge::init() {
    if (_init) {
        return 0;
    }
    auto reader = std::make_shared<AceroMergeReader>(_readers);
    std::function<arrow::Iterator<std::shared_ptr<::arrow::RecordBatch>>()> iter_maker = [reader] () {
        arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = ::arrow::MakeIteratorFromReader(reader);
        return batch_it;
    };

    auto schema_info = _options.schema_info;
    std::vector<arrow::FieldRef> group_by_fields;
    std::vector<arrow::compute::SortKey> sort_keys;
    std::vector<arrow::compute::Aggregate> aggregates;
    group_by_fields.reserve(schema_info->key_fields.size());
    sort_keys.reserve(schema_info->key_fields.size());
    aggregates.reserve(schema_info->value_fields.size() + 1);
    for (size_t i = 0; i < schema_info->schema->num_fields(); ++i) {
        std::string name = schema_info->schema->field(i)->name();
        if (i < schema_info->key_fields.size()) {
            group_by_fields.emplace_back(arrow::FieldRef(name));
            sort_keys.emplace_back(arrow::FieldRef(name), arrow::compute::SortOrder::Ascending);
        } else {
            if (schema_info->need_sum_idx.count(i) > 0) {
                aggregates.emplace_back("hash_sum", 
                                        /*options*/nullptr, 
                                        arrow::FieldRef(name), 
                                        /*new field name*/name);
            } else {
                aggregates.emplace_back("hash_one", 
                                        /*options*/nullptr, 
                                        arrow::FieldRef(name), 
                                        /*new field name*/name);
            }
        }
    }

    arrow::compute::Ordering ordering{sort_keys, arrow::compute::NullPlacement::AtStart};

    auto declaration = arrow::acero::Declaration::Sequence({
            {"record_batch_source", arrow::acero::RecordBatchSourceNodeOptions{reader->schema(), std::move(iter_maker)}},
            {"aggregate", arrow::acero::AggregateNodeOptions{aggregates, group_by_fields}},
            {"order_by", arrow::acero::OrderByNodeOptions{ordering}}
        });
    arrow::Result<std::shared_ptr<arrow::Table>> final_table = arrow::acero::DeclarationToTable(std::move(declaration), false);
    if (!final_table.ok()) {
        DB_WARNING("merge failed");
        return -1;
    }

    std::shared_ptr<arrow::Table> table = *final_table;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> record_batch_result = table->CombineChunksToBatch();
    if (!record_batch_result.ok()) {
        DB_FATAL("arrow CombineChunksToBatch fail");
        return -1;
    }
    _record_batch = *record_batch_result;
    _init = true;
    return 0;
}

arrow::Status AceroMerge::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    int ret = init();
    if (ret < 0) {
        return arrow::Status::IOError("AceroMerge init failed");
    }

    if (_pos >= _record_batch->num_rows()) {
        out->reset();
        return arrow::Status::OK();
    }

    if (_pos + _options.batch_size <= _record_batch->num_rows()) {
        *out = _record_batch->Slice(_pos, _options.batch_size);
        _pos += _options.batch_size;
        return arrow::Status::OK();
    } else {
        *out = _record_batch->Slice(_pos, _record_batch->num_rows() - _pos);
        _pos = _record_batch->num_rows();
        return arrow::Status::OK();
    }
}

} // namespance baikaldb