#include "parquet_scan_node.h"
#include "store.h"
#include "vectorize_helpper.h"
#include "sort_merge.h"

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
    _pri_info = _factory->get_index_info_ptr(_table_id);
    if (_pri_info == nullptr) {
        DB_WARNING("primary index info is nullptr");
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
    if (get_qualified_record_batch_readers(_parquet_file_readers) != 0) {
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
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
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
    LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, vectorized_reader->schema(), nullptr);
    return 0;
}

int ParquetScanNode::process_index(RuntimeState* state) {
    const auto& scan_pb = _pb_node.derive_node().scan_node();
    if (scan_pb.indexes_size() == 0) {
        DB_FATAL_STATE(state, "no index");
        return -1;
    }
    _possible_index.ParseFromString(scan_pb.indexes(0));
    if (_possible_index.index_id() != _table_id) {
        DB_WARNING_STATE(state, "ParquetScanNode only support primary key index");
        return -1;
    }
    if (_possible_index.ranges_size() == 0) {
        DB_WARNING_STATE(state, "PossibleIndex has no range");
        return 0;
    }
    DB_WARNING("PossibleIndex: %s", _possible_index.ShortDebugString().c_str());
    return 0;
}

void ParquetScanNode::get_qualified_parquet_file_readers(std::vector<std::shared_ptr<ParquetFileReader>>& parquet_file_readers, 
    const std::vector<std::shared_ptr<ParquetFile>>& parquet_files, std::shared_ptr<arrow::Schema> schema) {
    for (const auto& parquet_file : parquet_files) {
        ParquetFileReaderOptions options;
        options.pos_index = &_possible_index;
        for (const auto& [_, field_info] : _field_id2info_map) {
            options.lower_short_name_fields[field_info->lower_short_name] = *field_info;
        }
        options.schema = schema;
        auto parquet_reader = std::make_shared<ParquetFileReader>(options, parquet_file);
        parquet_file_readers.emplace_back(parquet_reader);
    }
}

int ParquetScanNode::get_qualified_record_batch_readers(std::vector<std::shared_ptr<::arrow::RecordBatchReader>>& record_batch_readers) {
    // 获取该region最新版本的所有parquet_file集合 
    SmartRegion region = Store::get_instance()->get_region(_region_id);
    if (region == nullptr) {
        DB_WARNING("region is nullptr");
        return -1;
    }
    std::vector<std::shared_ptr<ParquetFile>> parquet_files_tmp;
    if (region->get_column_files(_possible_index, parquet_files_tmp) != 0) {
        DB_WARNING("Fail to get_column_files");
        return -1;
    }

    std::vector<std::shared_ptr<ParquetFile>> parquet_files;
    parquet_files.reserve(parquet_files_tmp.size());
    int cumulatives_file_count = 0;
    bool column_only_read_base = _table_info->schema_conf.column_only_read_base();
    // 只获取base层的parquet_file，可以不用merge_on_read
    for (auto f : parquet_files_tmp) {
        // base层文件start_version为0
        if (f->get_file_info()->start_version == 0) {
            parquet_files.emplace_back(f);
        } else if (!column_only_read_base) {
            cumulatives_file_count++;
            parquet_files.emplace_back(f);
        }
    }

    bool has_put_or_delete = false;
    for (const auto& f : parquet_files) {
        auto info = f->get_file_info();
        if (info->start_version != 0 && (info->put_count > 0 || info->delete_count > 0)) {
            has_put_or_delete = true;
            break;
        }
    }

    std::unordered_map<int32_t, FieldInfo*> field_id2info_map = _field_id2info_map;
    if (cumulatives_file_count > 0 && has_put_or_delete) {
        // merge on read时，需要补充主键
        for (const auto& f : _pri_info->fields) {
            FieldInfo* field_info = _table_info->get_field_ptr(f.id);
            if (field_info == nullptr) {
                DB_WARNING("field not found region_id: %ld, field_id: %d", _region_id, f.id);
                continue;
            }
            field_id2info_map[f.id] = field_info;
        }
        std::shared_ptr<ColumnSchemaInfo> schema_info = ColumnRecord::make_column_schema(_table_info->id, _table_info, _pri_info, field_id2info_map);
        if (schema_info == nullptr) {
            DB_FATAL("get schema info failed");
            return -1;
        }
        std::vector<std::shared_ptr<ParquetFileReader>> parquet_file_readers;
        parquet_file_readers.reserve(parquet_files.size());
        get_qualified_parquet_file_readers(parquet_file_readers, parquet_files, schema_info->schema_with_order_info);
        std::vector<std::shared_ptr<SingleRowReader>> single_row_readers;
        single_row_readers.reserve(parquet_file_readers.size());
        SortMergeOptions merge_options;
        merge_options.batch_size = 1024;
        merge_options.is_base_compact = true;
        merge_options.schema_info = schema_info;
        for (auto& r : parquet_file_readers) {
            single_row_readers.emplace_back(std::make_shared<OrderSingleRowReader>(r, schema_info.get()));
        }
        record_batch_readers.emplace_back(std::make_shared<SortMerge>(merge_options, single_row_readers));
    } else {
        std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
        arrow_fields.reserve(_field_id2info_map.size());
        for (const auto& [_, field_info] : _field_id2info_map) {
            std::shared_ptr<arrow::Field> arrow_field = VectorizeHelpper::make_field(field_info->lower_short_name, 
                                                            arrow::Type::type(primitive_to_arrow_type(field_info->type)));
            if (arrow_field == nullptr) {
                DB_WARNING("Fail to make arrow field");
                return -1;
            }
            arrow_fields.emplace_back(arrow_field);
        }
        std::shared_ptr<arrow::Schema> arrow_schema = arrow::schema(arrow_fields);

        std::vector<std::shared_ptr<ParquetFileReader>> parquet_file_readers;
        parquet_file_readers.reserve(parquet_files.size());
        get_qualified_parquet_file_readers(parquet_file_readers, parquet_files, arrow_schema);
        for (auto& r : parquet_file_readers) {
            record_batch_readers.emplace_back(r);
        }
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
        DB_WARNING("parquet_scan_node is nullptr");
        return -1;
    }
    _state = state;
    _parquet_scan_node = parquet_scan_node;
    _field_id2info_map = _parquet_scan_node->get_field_id2info_map();
    _parquet_file_readers = _parquet_scan_node->get_parquet_file_readers();

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
        std::shared_ptr<arrow::Field> arrow_field = VectorizeHelpper::make_field(field_name, 
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
        _column_type_map[field_name] = field_info->type;
    }
    _arrow_schema = arrow::schema(arrow_fields);

    // 设置parquet文件读迭代器
    _reader_iter = _parquet_file_readers->begin();
    return 0;
}

// 支持limit
// 每次最多读取FLAGS_chunk_size行
arrow::Status ParquetVectorizedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {    
    out->reset();
    const int64_t limit = _parquet_scan_node->get_limit();
    if (limit > 0 && _processed_row_cnt >= limit) {
        return arrow::Status::OK();
    }

    TimeCost cost;
    if (_record_batch == nullptr) {
        std::shared_ptr<arrow::RecordBatch> record_batch = nullptr;
        while (_reader_iter != _parquet_file_readers->end()) {
            auto status = (*_reader_iter)->ReadNext(&record_batch);
            if (!status.ok()) {
                DB_WARNING("Fail to read next record batch, %s", status.message().c_str());
                return status;
            }
            if (record_batch == nullptr) {
                DB_WARNING("file:, ReadNext cost: %ld", /*_reader_iter->first.c_str(),*/ cost.get_time());
                ++_reader_iter;
                continue;
            } 
            DB_DEBUG("file ReadNext cost: %ld, num rows: %ld, num columns: %d", /*_reader_iter->first.c_str(), */cost.get_time(), 
                record_batch->num_rows(), record_batch->num_columns());
       
            break;
        }
        // eof
        if (record_batch == nullptr) {
            out->reset();
            return arrow::Status::OK();
        }
        // 转换schema，列名转化为tupleid_slotid形式，类型也可能发生转化
        int ret = VectorizeHelpper::change_arrow_record_batch_schema(
                    _column_name_map, _column_type_map, _arrow_schema, record_batch, &_record_batch);
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