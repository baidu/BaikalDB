#include "parquet_scan_node.h"
#include "store.h"
#include "vectorize_helpper.h"

namespace baikaldb {
int ParquetScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ScanNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret: %d", ret);
        return ret;
    }
    _file_manager = ParquetFileManager::get_instance();
    if (_file_manager == nullptr) {
        DB_WARNING("_file_manager is nullptr");
        return -1;
    }
    _factory = SchemaFactory::get_instance();
    if (_factory == nullptr) {
        DB_WARNING("_factory is nullptr");
        return -1;
    }
    _table_info = _factory->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("table_info is nullptr");
        return -1;
    }
    return 0;
}

int ParquetScanNode::open(RuntimeState* state) {
    int ret = 0;
    ret = ScanNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail: %d", ret);
        return ret;
    }
    if (_is_explain) {
        return 0;
    }
    ret = process_index(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "process_index fail: %d", ret);
        return ret;
    }
    _region_id = state->region_id();

    // 建立field id到field info的映射
    for (const auto& slot : _tuple_desc->slots()) {
        FieldInfo* field_info = _table_info->get_field_ptr(slot.field_id());
        if (field_info == nullptr) {
            DB_WARNING("field not found region_id: %ld, field_id: %d", _region_id, slot.field_id());
            // 兼容加列场景，db/store心跳不同步问题
            continue;
        }
        _field_id2info_map[field_info->id] = field_info;
    }
    if (get_qualified_record_batch_readers(_parquet_file2reader_map, _parquet_file_not_exist_column_map) != 0) {
        DB_WARNING("get qualified record batch readers fail");
        return -1;
    }
    return 0;
}

int ParquetScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    // 列存模式走列式执行引擎，get_next直接返回
    set_node_exec_type(pb::EXEC_ARROW_ACERO);
    *eos = true;
    return 0;
}

void ParquetScanNode::close(RuntimeState* state) {
    ScanNode::close(state);
}

int ParquetScanNode::build_arrow_declaration(RuntimeState* state) {
    int ret = 0;
    // add SourceNode
    std::shared_ptr<ParquetVectorizedReader> vectorized_reader = std::make_shared<ParquetVectorizedReader>();
    ret = vectorized_reader->init(state, this);
    if (ret != 0) {
        DB_WARNING("Fail to init vectorized_reader");
        return -1;
    }
    std::function<arrow::Iterator<std::shared_ptr<::arrow::RecordBatch>>()> iter_maker = [vectorized_reader] () {
        arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = ::arrow::MakeIteratorFromReader(vectorized_reader);
        return batch_it;
    };
    arrow::acero::Declaration dec{"record_batch_source",
            arrow::acero::RecordBatchSourceNodeOptions{vectorized_reader->schema(), std::move(iter_maker)}};
    state->append_acero_declaration(dec);
    return 0;
}

int ParquetScanNode::process_index(RuntimeState* state) {
    const auto& scan_pb = _pb_node.derive_node().scan_node();
    if (scan_pb.indexes_size() == 0) {
        DB_FATAL_STATE(state, "no index");
        return -1;
    }
    pb::PossibleIndex pos_index;
    pos_index.ParseFromString(scan_pb.indexes(0));
    if (pos_index.index_id() != _table_id) {
        DB_WARNING_STATE(state, "ParquetScanNode only support primary key index");
        return -1;
    }
    if (pos_index.ranges_size() == 0) {
        DB_WARNING_STATE(state, "PossibleIndex has no range");
        return 0;
    }
    DB_WARNING("PossibleIndex: %s", pos_index.ShortDebugString().c_str());
    _key_ranges = std::vector<pb::PossibleIndex::Range>(pos_index.ranges().begin(), pos_index.ranges().end());
    return 0;
}

int ParquetScanNode::get_qualified_record_batch_readers(
        std::unordered_map<std::string, ReaderInfo>& parquet_file2reader_map,
        std::unordered_map<std::string, std::vector<FieldInfo*>>& parquet_file_not_exist_column_map) {
    // 获取该region最新版本的所有parquet_file集合 
    SmartRegion region = Store::get_instance()->get_region(_region_id);
    if (region == nullptr) {
        DB_WARNING("region is nullptr");
        return -1;
    }
    
    if (region->get_column_files(_key_ranges, _parquet_files) != 0) {
        DB_WARNING("Fail to get_column_files");
        return -1;
    }

    for (const auto& parquet_file : _parquet_files) {
        const std::string file_path = parquet_file->get_file_path();
        // 获取column_indices，以及在parquet文件里不存在的列
        // 在parquet文件中不存在的列需要补充默认值或NULL
        const std::unordered_map<std::string, int>& column_name2index_map = parquet_file->get_column_name2index_map();
        std::vector<int> exist_column_indices;
        std::vector<FieldInfo*> not_exist_columns;
        for (const auto& [_, field_info] : _field_id2info_map) {
            const std::string& field_name = field_info->lower_short_name;
            if (column_name2index_map.find(field_name) != column_name2index_map.end()) {
                exist_column_indices.emplace_back(column_name2index_map.at(field_name));
                // DB_WARNING("field_name: %s, index: %d", field_name.c_str(), column_name2index_map.at(field_name));
            } else {
                DB_WARNING("field_name: %s, not exist", field_name.c_str());
                not_exist_columns.emplace_back(field_info);
            }
        }
        if (!not_exist_columns.empty()) {
            parquet_file_not_exist_column_map[file_path].swap(not_exist_columns);
        }
        std::unique_ptr<::arrow::RecordBatchReader> reader;
        std::vector<int> row_group_indices;
        bool fill_cache = false;
        if (_key_ranges.empty()) {
            // 获取所有rowgroup的数据
            std::shared_ptr<::parquet::FileMetaData> file_metadata = parquet_file->get_file_metadata();
            if (file_metadata == nullptr) {
                DB_WARNING("file_metadata is nullptr");
                return -1;
            }
            if (file_metadata->num_row_groups() == 0) {
                DB_WARNING("parquet file has no row group");
                continue;
            }
            
            row_group_indices.reserve(file_metadata->num_row_groups());
            for (int i = 0; i < file_metadata->num_row_groups(); ++i) {
                row_group_indices.emplace_back(i);
            }
            auto status = parquet_file->GetRecordBatchReader(row_group_indices, exist_column_indices, &reader);
            if (!status.ok()) {
                DB_WARNING("Fail to get_record_batch_reader");
                return -1;
            }
        } else {
            // 获取符合条件的row_group_indices和row_ranges
            std::vector<std::vector<std::pair<int64_t, int64_t>>> row_ranges;
            if (parquet_file->get_qualified_rowgroup_and_rowranges(_key_ranges, row_group_indices, row_ranges) != 0) {
                DB_WARNING("Fail to get_qualified_rowgroup_and_rowranges");
                return -1;
            }
            if (row_group_indices.empty()) {
                DB_WARNING("parquet file has no qualified data");
                continue;
            }
            // 获取record_batch_reader
            auto status = parquet_file->GetRecordBatchReader(row_group_indices, exist_column_indices, row_ranges, &reader);
            if (!status.ok()) {
                DB_WARNING("Fail to get_record_batch_reader");
                return -1;
            }
            fill_cache = true;
        }
        std::vector<ReadRange> read_ranges;
        read_ranges.reserve(row_group_indices.size() * exist_column_indices.size());
        parquet_file->parser_position(row_group_indices, exist_column_indices, read_ranges);
        auto file_info = parquet_file->get_file_info();
        ReaderInfo reader_info;
        reader_info.read_contents = std::make_shared<ReadContents>();
        reader_info.read_contents->fill_cache = fill_cache;
        reader_info.read_contents->file_short_name = parquet_file->get_file_short_name();
        reader_info.read_contents->ranges.swap(read_ranges);
        reader_info.read_contents->region_id = file_info->region_id;
        reader_info.read_contents->start_version = file_info->start_version;
        reader_info.read_contents->end_version = file_info->end_version;
        reader_info.read_contents->file_idx = ColumnFileInfo::get_file_idx(parquet_file->get_file_short_name());
        if (reader_info.read_contents->file_idx < 0) {
            DB_COLUMN_FATAL("Fail to get file idx : %s", parquet_file->get_file_short_name().c_str());
            return -1;
        }
        reader_info.reader = std::move(reader);
        parquet_file2reader_map[file_path] = reader_info;
    }
    return 0;
}

// ParquetVectorizedReader
int ParquetVectorizedReader::init(RuntimeState* state, ParquetScanNode* parquet_scan_node) {
    if (state == nullptr) {
        DB_WARNING("_state is nullptr");
        return -1;
    }
    if (parquet_scan_node == nullptr) {
        DB_WARNING("_parquet_scan_node is nullptr");
        return -1;
    }
    _state = state;
    _parquet_scan_node = parquet_scan_node;
    _field_id2info_map = _parquet_scan_node->get_field_id2info_map();
    _parquet_file2reader_map = _parquet_scan_node->get_parquet_file2reader_map();
    _parquet_file_not_exist_column_map = _parquet_scan_node->get_parquet_file_not_exist_column_map();

    // 构造schema
    pb::TupleDescriptor* tuple_desc = _parquet_scan_node->get_tuple();
    if (tuple_desc == nullptr) {
        DB_WARNING("Fail to get tuple desc");
        return -1;
    }
    // 需要按照tuple_desc的slot顺序构造arrow schema
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    arrow_fields.reserve(tuple_desc->slots().size());
    for (const auto& slot : tuple_desc->slots()) {
        const std::string& field_name = std::to_string(slot.tuple_id()) + "_" + std::to_string(slot.slot_id());
        std::shared_ptr<arrow::Field> arrow_field = ColumnRecord::make_schema(field_name, 
                                                        arrow::Type::type(primitive_to_arrow_type(slot.slot_type())));
        if (arrow_field == nullptr) {
            DB_WARNING("Fail to make arrow field");
            return -1;
        }
        arrow_fields.emplace_back(arrow_field);

        // 建立tupleid_slotid到列名的映射，用于后面parquet_schema转化成baikaldb schema
        if (_field_id2info_map->find(slot.field_id()) == _field_id2info_map->end()) {
            DB_WARNING("Fail to find field info, slot: %d", slot.field_id());
            // 兼容加列场景，db/store心跳不同步问题
            continue;
        }
        const FieldInfo* field_info = _field_id2info_map->at(slot.field_id());
        if (field_info == nullptr) {
            DB_WARNING("Fail to get field info, slot: %d", slot.field_id());
            return -1;
        }
        _column_name_map[field_name] = field_info->lower_short_name;
    }
    _arrow_schema = arrow::schema(arrow_fields);

    // 设置parquet文件读迭代器
    _reader_iter = _parquet_file2reader_map->begin();
    return 0;
}

// 支持limit
// 每次最多读取FLAGS_chunk_size行
arrow::Status ParquetVectorizedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {    
    out->reset();
    ON_SCOPE_EXIT([]() {
        ParquetCache::get_instance()->set_bthread_local(nullptr);
    });
    const int64_t limit = _parquet_scan_node->get_limit();
    if (limit > 0 && _processed_row_cnt >= limit) {
        return arrow::Status::OK();
    }

    TimeCost cost;
    if (_record_batch == nullptr) {
        std::shared_ptr<arrow::RecordBatch> record_batch = nullptr;
        while (_reader_iter != _parquet_file2reader_map->end()) {
            ParquetCache::get_instance()->set_bthread_local(_reader_iter->second.read_contents.get());
            auto status = _reader_iter->second.reader->ReadNext(&record_batch);
            if (!status.ok()) {
                DB_WARNING("Fail to read next record batch, %s", status.message().c_str());
                return status;
            }
            if (record_batch == nullptr) {
                DB_WARNING("file: %s, ReadNext cost: %ld", _reader_iter->first.c_str(), cost.get_time());
                ++_reader_iter;
                continue;
            } 
            DB_DEBUG("file: %s, ReadNext cost: %ld, num rows: %ld, num columns: %d", _reader_iter->first.c_str(), cost.get_time(), 
                record_batch->num_rows(), record_batch->num_columns());
            // 添加查询需要，但是在parquet文件中不存在的列
            const std::string& file_name = _reader_iter->first;
            if (_parquet_file_not_exist_column_map->find(file_name) != _parquet_file_not_exist_column_map->end()) {
                const auto& not_exist_columns = _parquet_file_not_exist_column_map->at(file_name);
                for (const auto& field_info : not_exist_columns) {
                    TimeCost add_column_cost;
                    std::shared_ptr<arrow::Field> arrow_field = ColumnRecord::make_schema(field_info->lower_short_name, 
                                                                arrow::Type::type(primitive_to_arrow_type(field_info->type)));
                    if (arrow_field == nullptr) {
                        DB_WARNING("Fail to get arrow type, field_id: %d, field_type: %d", field_info->id, field_info->type);
                        return arrow::Status::IOError("Fail to get arrow type");
                    }
                    std::shared_ptr<arrow::Array> arrow_array = ColumnRecord::make_array_from_exprvalue(
                                        field_info->type, field_info->default_expr_value, record_batch->num_rows());
                    if (arrow_array == nullptr) {
                        return arrow::Status::IOError("Fail to make array from expr value");
                    }
                    auto new_record_batch_ret = 
                        record_batch->AddColumn(record_batch->num_columns(), arrow_field, arrow_array);
                    if (!new_record_batch_ret.ok()) {
                        return arrow::Status::IOError("Fail to add column");
                    } else {
                        record_batch = *new_record_batch_ret;
                    }
                }
            }
            break;
        }
        // eof
        if (record_batch == nullptr) {
            out->reset();
            return arrow::Status::OK();
        }
        // 转换schema，列名转化为tupleid_slotid形式，类型也可能发生转化
        int ret = VectorizeHelpper::change_arrow_record_batch_schema(
                    _column_name_map, _arrow_schema, record_batch, &_record_batch);
        if (ret != 0) {
            return arrow::Status::IOError("Fail to change arrow record batch schema");
        }
        // ColumnRecord::TEST_print_record_batch(_record_batch);
        DB_DEBUG("change arrow record batch schema in: %ld, %d out: %ld, %d", 
            record_batch->num_rows(), record_batch->num_columns(), _record_batch->num_rows(), _record_batch->num_columns());
    }
    if (_record_batch == nullptr) {
        return arrow::Status::IOError("Fail to get record batch");
    }
    // limit功能
    // 每次最多读取FLAGS_chunk_size行
    int64_t slice_cnt = FLAGS_chunk_size;
    if (limit > 0 && _processed_row_cnt + slice_cnt >= limit) {
        slice_cnt = limit - _processed_row_cnt;
    }
    *out = _record_batch->Slice(_row_idx_in_record_batch, slice_cnt);
    _processed_row_cnt += (*out)->num_rows();
    _row_idx_in_record_batch += (*out)->num_rows();
    if (_record_batch->num_rows() <= _row_idx_in_record_batch) {
        _record_batch.reset();
        _row_idx_in_record_batch = 0;
    }
    // 记录扫描行数及扫描时间
    _state->inc_num_scan_rows((*out)->num_rows());
    return arrow::Status::OK();
}

} // namespace baikaldb