#include "file_scan_node.h"
#include "select_manager_node.h"
#include "vectorize_helpper.h"

namespace baikaldb {

DEFINE_int32(inner_file_scan_concurrency, 4, "inner_file_scan_concurrency");
DECLARE_int32(file_block_size);
DECLARE_int32(chunk_size);

/// FileScanNode
int FileScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ScanNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret: %d", ret);
        return ret;
    }
    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("table info is null, table id: %ld", _table_id);
        return -1;
    }
    _pb_file_info = _table_info->dblink_info.file_info();
    const auto& pb_files = node.derive_node().scan_node().files();
    _files.assign(pb_files.begin(), pb_files.end());
    return 0;
}

int FileScanNode::open(RuntimeState* state) {
    if (state == nullptr) {
        DB_WARNING("state is nullptr");
        return -1;
    }
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    int ret = 0;
    ret = ScanNode::open(state);
    if (ret < 0) {
        DB_WARNING_STATE(state, "ExecNode::open fail:%d", ret);
        return ret;
    }
    if (_table_info == nullptr) {
        DB_WARNING("_table_info is nullptr");
        return -1;
    }
    // 离线文件查询走列式
    set_node_exec_type(pb::EXEC_ARROW_ACERO);
    // 建立field_id到slot_id的映射，建立field_id到field_info的映射
    _field_id2slot = std::vector<int32_t>(_table_info->fields.back().id + 1, 0);
    _field_id2info = std::vector<FieldInfo*>(_table_info->fields.back().id + 1, nullptr);
    for (const auto& slot : _tuple_desc->slots()) {
        if (slot.field_id() >= _field_id2slot.size()) {
            DB_WARNING("vector out of range, field_id: %d", slot.field_id());
            continue;
        }
        _field_id2slot[slot.field_id()] = slot.slot_id();
        FieldInfo* field_info = _table_info->get_field_ptr(slot.field_id());
        if (field_info == nullptr) {
            DB_WARNING("field not found, field_id: %d", slot.field_id());
            continue;
        }
        _field_id2info[slot.field_id()] = field_info;
    }
    // 获取分区字段集合和数据字段集合
    std::unordered_set<std::string> partition_field_set;
    for (const auto& partition_field : _pb_file_info.partition_fields()) {
        partition_field_set.insert(partition_field);
        FieldInfo* field_info = _table_info->get_field_ptr(partition_field);
        if (field_info == nullptr) {
            DB_WARNING("field not found, field_name: %s", partition_field.c_str());
            return -1;
        }
        _partition_fields.emplace_back(field_info);
    }
    for (auto& field : _table_info->fields) {
        if (partition_field_set.find(field.short_name) == partition_field_set.end()) {
            _data_fields.emplace_back(&field);
        }
    }
    _vectorized_reader = std::make_shared<FileVectorizedReader>();
    if (_vectorized_reader == nullptr) {
        DB_WARNING("_vectorized_reader is nullptr");
        return -1;
    }
    ret = _vectorized_reader->init(this, state);
    if (ret != 0) {
        DB_WARNING("Fail to init vectorized_reader");
        return -1;
    }
    return 0;
}

int FileScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    // 离线文件查询只走列式
    *eos = true;
    return 0;
}

void FileScanNode::close(RuntimeState* state) {
    ScanNode::close(state);
    if (_vectorized_reader != nullptr) {
        _vectorized_reader->close();
    }
    _field_id2slot.clear();
    _field_id2info.clear();
    _partition_fields.clear();
    _data_fields.clear();
    _vectorized_reader = nullptr;
    _arrow_io_executor = nullptr;
}

void FileScanNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    auto* scan_pb = pb_node->mutable_derive_node()->mutable_scan_node();
    for (const auto& file : _files) {
        scan_pb->add_files()->CopyFrom(file);
    }
}

int FileScanNode::build_arrow_declaration(RuntimeState* state) {
    if (state == nullptr) {
        DB_WARNING("state is nullptr");
        return -1;
    }
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [this] () {
        arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(_vectorized_reader);
        return batch_it;
    };
    bool is_delay_fetch = false;
    if (_related_manager_node != nullptr) {
        is_delay_fetch = _related_manager_node->is_delay_fetcher_store();
    }
    if (state->vectorlized_parallel_execution == false) {
        arrow::acero::Declaration dec{"record_batch_source",
            arrow::acero::RecordBatchSourceNodeOptions{_vectorized_reader->schema(), std::move(iter_maker)}}; 
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, _vectorized_reader->schema(), &is_delay_fetch);
        state->append_acero_declaration(dec);
    } else {
        auto executor = BthreadArrowExecutor::Make(1);
        _arrow_io_executor = *executor;
        arrow::acero::Declaration dec{"record_batch_source",
            arrow::acero::RecordBatchSourceNodeOptions{_vectorized_reader->schema(), std::move(iter_maker), _arrow_io_executor.get()}}; 
        LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(dec, _vectorized_reader->schema(), &is_delay_fetch);
        state->append_acero_declaration(dec);
    }
    return 0;
}

/// FileVectorizedReader
int FileVectorizedReader::init(FileScanNode* scan_node, RuntimeState* state) {
    if (scan_node == nullptr) {
        DB_WARNING("scan_node is nullptr");
        return -1;
    }
    _scan_node = scan_node;
    if (state == nullptr) {
        DB_WARNING("_state is nullptr");
        return -1;
    }
    _state = state;
    // 生成schema
    _schema = VectorizeHelpper::make_schema(_scan_node->get_tuple());
    if (_schema == nullptr) {
        DB_WARNING("Fail to make_schema");
        return -1;
    }
    SelectManagerNode* related_manager_node = 
        static_cast<SelectManagerNode*>(_scan_node->get_related_manager_node());
    if (related_manager_node != nullptr) {
        _is_delay_fetch = related_manager_node->is_delay_fetcher_store();
        _index_cond = related_manager_node->get_index_collector_cond();
    }
    if (_is_delay_fetch) {
        return 0;
    }
    run_file_scan_thread();
    return 0;
}

arrow::Status FileVectorizedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    int ret = 0;
    if (_is_delay_fetch) {
        if (_index_cond != nullptr) {
            _index_cond->cond.wait();
            if (_index_cond->index_cnt == 0) {
                // join驱动表没数据
                out->reset();
                return arrow::Status::OK();
            }
        }
        run_file_scan_thread();
        _is_delay_fetch = false;
    }
    if (_state->is_cancelled()) {
        DB_WARNING_STATE(_state, "cancelled");
        _eos = true;
        return arrow::Status::OK();
    }
    const int64_t limit = _scan_node->get_limit();
    if (limit > 0 && _processed_row_cnt >= limit) {
        _eos = true;
        return arrow::Status::OK();
    }
    if (_eos) {
        out->reset();
        return arrow::Status::OK();
    }
    if (_record_batch == nullptr) {
        bool ret = _record_batches.blocking_get(&_record_batch);
        if (!_is_succ) {
            return arrow::Status::IOError("Fail to file scan");
        }
        if (!ret) {
            _eos = true;
            return arrow::Status::OK();
        }
    }
    if (_record_batch != nullptr) {
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
    }
    return arrow::Status::OK();
}

void FileVectorizedReader::run_file_scan_thread() {
    auto file_scan_thread = [this] () {
        const pb::FileInfo& file_info = _scan_node->get_pb_file_info();
        TimeCost tm;
        int64_t create_filesystem_tm = -1;
        int64_t total_file_scan_thread_tm = -1;
        std::shared_ptr<FileSystem> fs = create_filesystem(file_info.cluster(), 
                                                           file_info.username(), 
                                                           file_info.password(), 
                                                           AFS_CLIENT_CONF_PATH);
        ScopeGuard guard([this, &fs, &create_filesystem_tm, &total_file_scan_thread_tm] () {
            destroy_filesystem(fs);
        });
        if (fs == nullptr) {
            DB_WARNING("Fail to create_filesystem");
            _is_succ = false;
            return;
        }
        create_filesystem_tm = tm.get_time();
        // 生成并执行file scanner
        const auto& files = _scan_node->get_files();
        std::vector<std::shared_ptr<FileScanner>> file_scanners;
        for (const auto& file : files) {
            std::shared_ptr<FileScanner> file_scanner;
            switch (file_info.format()) {
            case pb::CSV: {
                file_scanner = std::make_shared<CSVScanner>(fs.get(), file);
                break;
            }
            case pb::PARQUET: {
                file_scanner = std::make_shared<ParquetScanner>(fs.get(), file);
                break;
            }
            default: {
                DB_WARNING("Invalid file_format: %d", file_info.format());
                _is_succ = false;
                break;
            }
            }
            if (!_is_succ) {
                break;
            }
            if (file_scanner == nullptr) {
                DB_WARNING("file_scanner is nullptr");
                _is_succ = false;
                break;
            }
            int ret = file_scanner->init(this, _scan_node);
            if (ret == -2) { // -2表示Parquet目录中非parquet文件，如_SUCCESS文件
                continue;
            }
            if (ret != 0) { 
                DB_WARNING("Fail to init file_scanner");
                _is_succ = false;
                break;
            }
            ret = file_scanner->run();
            if (ret < 0) {
                DB_WARNING("Fail to run file_scanner");
                _is_succ = false;
                break;
            }
            file_scanners.emplace_back(file_scanner);
        }
        _file_concurrency_cond.wait(-_file_concurrency);
        _record_batches.shutdown();
        total_file_scan_thread_tm = tm.get_time();
        DB_WARNING("create_filesystem_tm: %ld, file_scan_thread total tm: %ld", 
                    create_filesystem_tm, total_file_scan_thread_tm);
    };
    _file_scan_bth.run(file_scan_thread);
}

/// FileScanner
int FileScanner::init(FileVectorizedReader* vectorized_reader, FileScanNode* scan_node) {
    _vectorized_reader = vectorized_reader;
    _scan_node = scan_node;
    if (_vectorized_reader == nullptr) {
        DB_WARNING("_vectorized_reader is nullptr");
        return -1;
    }
    if (_scan_node == nullptr) {
        DB_WARNING("_scan_node is nullptr");
        return -1;
    }
    if (_fs == nullptr) {
        DB_WARNING("_fs is nulpptr");
        return -1;
    }
    const auto& partition_fields = scan_node->get_partition_fields();
    if (partition_fields.size() != _partition_vals.size()) {
        DB_WARNING("partition_fields.size[%lu] not equal to partition_vals.size[%lu]", 
                    partition_fields.size(), _partition_vals.size());
        return -1;
    }
    return 0;
}

/// CSVScanner::BlockImpl
class CSVScanner::BlockImpl {
public:
    BlockImpl(CSVScanner* scanner,
              FileSystem* fs,
              const std::string& file_path,
              size_t file_size,
              size_t start_pos,
              size_t end_pos,
              std::atomic<bool>& eos,
              std::atomic<bool>& is_succ)
              : _scanner(scanner)
              , _fs(fs)
              , _file_path(file_path)
              , _file_size(file_size)
              , _start_pos(start_pos)
              , _end_pos(end_pos)
              , _cur_pos(start_pos)
              , _eos(eos)
              , _is_succ(is_succ) {
        if (_start_pos == 0) {
            _escape_first_line = false;
        }
    }
    virtual ~BlockImpl() {
        if (_read_buffer != nullptr) {
            free(_read_buffer);
            _read_buffer = nullptr;
        }
        if (_file != nullptr) {
            if (_fs != nullptr) {
                _fs->close_reader(_file);
            }
            _file = nullptr;
        }
    }

    int init();
    // @brief 读取一个块的数据，对每一行进行处理
    // @return 1: 已读完 / 0: 未读完 / others: 读失败
    int read_and_exec();
    // @brief 读取完一个块，处理未提交的_batch
    int read_done();

private:
    bool block_finish();
    int read_buffer_resize(int64_t size);
    int process_line(const std::string& line);

private:
    CSVScanner* _scanner = nullptr;
    FileSystem* _fs = nullptr;
    std::shared_ptr<FileReader> _file;
    std::string _file_path;
    int64_t _file_size = -1;
    int64_t _start_pos = -1;
    int64_t _end_pos = -1;
    int64_t _cur_pos = -1;
    char* _read_buffer = nullptr;
    int64_t _read_buffer_size = 0;
    bool _escape_first_line = true;
    RowBatch _batch;
    std::atomic<bool>& _eos;
    std::atomic<bool>& _is_succ;
};

int CSVScanner::BlockImpl::init() {
    if (_fs == nullptr) {
        DB_WARNING("_fs is nullptr");
        return -1;
    }
    _file = _fs->open_reader(_file_path);
    if (_file == nullptr) {
        DB_WARNING("Fail to open_reader, _file_path: %s", _file_path.c_str());
        return -1;
    }
    _read_buffer_size = FLAGS_file_buffer_size * 1024 * 1024ULL;
    _read_buffer = static_cast<char*>(malloc(_read_buffer_size));
    if (_read_buffer == nullptr) {
        DB_WARNING("Fail to malloc");
        return -1;
    }
    if (_scanner == nullptr) {
        DB_WARNING("_scanner is nullptr");
        return -1;
    }
    if (_scanner->get_vectorized_reader() == nullptr) {
        DB_WARNING("_scanner->get_vectorized_reader() is nullptr");
        return -1;
    }
    if (_scanner->get_scan_node() == nullptr) {
        DB_WARNING("_scanner->get_scan_node() is nullptr");
        return -1;
    }
    if (_scanner->get_scan_node()->get_tuple() == nullptr) {
        DB_WARNING("tuple_desc is nullptr");
        return -1;
    }
    _batch.init_chunk({_scanner->get_scan_node()->get_tuple()}, nullptr);
    return 0;
}

int CSVScanner::BlockImpl::read_buffer_resize(int64_t size) {
    if (_read_buffer != nullptr) {
        free(_read_buffer);
        _read_buffer = nullptr;
    }
    _read_buffer_size = size;
    _read_buffer = static_cast<char*>(malloc(_read_buffer_size));
    if (_read_buffer == nullptr) {
        DB_WARNING("Fail to malloc");
        return -1;
    }
    return 0;
}

bool CSVScanner::BlockImpl::block_finish() {
    // 只有_cur_pos > _end_pos才能判断完成，相等说明_end_pos正好在行首，需要多读一行，因为下一个block跳过了首行
    if (_cur_pos > _end_pos) {
        return true;
    }
    if (_cur_pos == _end_pos && _end_pos == _file_size) {
        return true;
    }
    if (_start_pos == _end_pos) {
        return true;
    }
    if (_eos) {
        return true;
    }
    if (!_is_succ) {
        return true;
    }
    return false;
}

int CSVScanner::BlockImpl::read_and_exec() {
    if (block_finish()) {
        return 1;
    }
    const int64_t file_pos = _cur_pos;
    int64_t buf_pos = 0;
    int64_t buf_size = _file->read(file_pos, _read_buffer, _read_buffer_size);
    if (buf_size < 0) {
        DB_WARNING("Fail to read, file: %s, pos: %ld", _file_path.c_str(), file_pos);
        return -1;
    }
    if (buf_size == 0) {
        // 已读完
        return 1;
    }
    MemBuf mem_buf(_read_buffer, _read_buffer + buf_size);
    std::istream f(&mem_buf);
    // 跳过首行
    if (_escape_first_line) {
        std::string line;
        std::getline(f, line);
        if (f.eof()) {
            // 增大buffer重试
            if (read_buffer_resize(_read_buffer_size * 2) < 0) {
                return -1;
            }
            return 0;
        }
        buf_pos += line.size() + 1;
        _escape_first_line = false;
    }
    _cur_pos = file_pos + buf_pos;
    if (block_finish()) {
        return 1;
    }
    bool has_get_line = false;
    while (!f.eof()) {
        std::string line;
        std::getline(f, line);
        // eof直接退出不更新 _cur_pos, 下次从_cur_pos继续读
        if (f.eof()) {
            buf_pos += line.size();
            // 最后一块特殊处理，不需要跳过
            if (file_pos + buf_pos == _end_pos && _end_pos == _file_size) {
                if (!line.empty() && process_line(line) < 0) {
                    return -1;
                }
                return 1;
            }
            if (_cur_pos <= _end_pos && !has_get_line) {
                // 增大buffer重试
                if (read_buffer_resize(_read_buffer_size * 2) < 0) {
                    return -1;
                }
                return 0;
            }
            return 0;
        }
        has_get_line = true;
        buf_pos += line.size() + 1;
        _cur_pos = file_pos + buf_pos;
        if (!line.empty() && process_line(line) < 0) {
            return -1;
        }
        if (block_finish()) {
            return 1;
        }
    }
    return 0;
}

int CSVScanner::BlockImpl::read_done() {
    if (!_batch.empty()) {
        std::shared_ptr<arrow::RecordBatch> record_batch;
        int ret = _batch.finish_and_make_record_batch(&record_batch);
        if (ret < 0) {
            DB_WARNING("arrow chunk finish and make record batch fail");
            return -1;
        }
        auto& record_batches = _scanner->get_vectorized_reader()->get_record_batches();
        record_batches.blocking_put(record_batch);
    }
    return 0;
}

// 处理文件行
//  - 如果文件行字段数比表字段数多，则忽略多于字段；
//  - 如果文件行字段数比表字段数少，则补充默认值；
int CSVScanner::BlockImpl::process_line(const std::string& line) {
    FileScanNode* scan_node = _scanner->get_scan_node();
    const auto& tuple_id = scan_node->tuple_id();
    const auto& delimiter = scan_node->get_pb_file_info().delimiter();
    const auto& field_id2slot = scan_node->get_field_id2slot();
    const auto& partition_fields = scan_node->get_partition_fields();
    const auto& data_fields = scan_node->get_data_fields();
    std::vector<std::string> line_field_vec;
    boost::split(line_field_vec, line, boost::is_any_of(delimiter)); // 当前按单字符分割
    for (int i = 0; i < line_field_vec.size(); ++i) {
        if (i >= data_fields.size()) {
            break;
        }
        FieldInfo* field = data_fields[i];
        if (field == nullptr) {
            DB_WARNING("field is nullptr");
            return -1;
        }
        if (field->id >= field_id2slot.size() || field_id2slot[field->id] <= 0) {
            continue;
        }
        ExprValue value(field->type, line_field_vec[i]);
        _batch.set_chunk_tmp_row_value(tuple_id, field_id2slot[field->id], value);
    }
    for (int i = line_field_vec.size(); i < data_fields.size(); ++i) {
        FieldInfo* field = data_fields[i];
        if (field == nullptr) {
            DB_WARNING("field is nullptr");
            return -1;
        }
        if (field->id >= field_id2slot.size() || field_id2slot[field->id] <= 0) {
            continue;
        }
        _batch.set_chunk_tmp_row_value(tuple_id, field_id2slot[field->id], field->default_expr_value);
    }
    // 填充分区字段
    const auto& partition_vals = _scanner->get_partition_vals();
    for (int i = 0; i < partition_fields.size(); ++i) {
        FieldInfo* field = partition_fields[i];
        if (field == nullptr) {
            DB_WARNING("field is nullptr");
            return -1;
        }
        if (field->id >= field_id2slot.size() || field_id2slot[field->id] <= 0) {
            continue;
        }
        ExprValue value(field->type, partition_vals[i]);
        _batch.set_chunk_tmp_row_value(tuple_id, field_id2slot[field->id], value);
    }
    if (_batch.add_chunk_row() != 0) {
        DB_FATAL("add chunk row fail");
        return -1;
    }
    if (_batch.is_full()) {
        std::shared_ptr<arrow::RecordBatch> record_batch;
        int ret = _batch.finish_and_make_record_batch(&record_batch);
        if (ret < 0) {
            DB_WARNING("arrow chunk finish and make record batch fail");
            return -1;
        }
        auto& record_batches = _scanner->get_vectorized_reader()->get_record_batches();
        record_batches.blocking_put(record_batch);
    }
    return 0;
}

/// CSVScanner
int CSVScanner::init(FileVectorizedReader* vectorized_reader, FileScanNode* scan_node) {
    int ret = FileScanner::init(vectorized_reader, scan_node);
    if (ret != 0) {
        return -1;
    }
    ret = _fs->get_file_info(_file_path, _file_info, nullptr);
    if (ret < 0) {
        DB_WARNING("Fil to get_file_info, ret: %d, file_path: %s", ret, _file_path.c_str());
        return -1;
    }
    if (_file_info.mode != FileMode::I_FILE) {
        DB_WARNING("Invalid file_info mode: %d, file_path: %s", (int)_file_info.mode, _file_path.c_str());
        return -1;
    }
    return 0;
}

int CSVScanner::run() {
    BthreadCond& file_concurrency_cond = _vectorized_reader->get_file_concurrency_cond();
    std::atomic<bool>& is_succ = _vectorized_reader->get_is_succ();
    std::atomic<bool>& eos = _vectorized_reader->get_eos();
    const int64_t file_block_size = FLAGS_file_block_size * 1024 * 1024LL;
    if (file_block_size <= 0) {
        DB_WARNING("Invalid file_block_size: %ld", file_block_size);
        return -1;
    }
    int64_t block_num = _file_info.size / file_block_size + 1;
    for (int i = 0; i < block_num; ++i) {
        if (!is_succ) {
            break;
        }
        if (eos) {
            break;
        }
        int64_t start_pos = i * file_block_size;
        int64_t end_pos = (i + 1) * file_block_size;
        end_pos = end_pos > _file_info.size ? _file_info.size : end_pos;
        auto process_block = 
                [this, start_pos, end_pos, &file_concurrency_cond, &is_succ, &eos] () {
            TimeCost tm;
            bool is_cur_succ = false;
            ON_SCOPE_EXIT(([&file_concurrency_cond, &is_succ, &is_cur_succ]() {
                file_concurrency_cond.decrease_signal();
                if (!is_cur_succ) {
                    is_succ = false;
                }
            }));
            BlockImpl block(this, _fs, _file_path, _file_info.size, start_pos, end_pos, eos, is_succ);
            int ret = block.init();
            if (ret < 0) {
                DB_WARNING("Fail to init block");
                return;
            }
            while (true) {
                ret = block.read_and_exec();
                if (ret < 0) {
                    DB_WARNING("Fail to read_and_exec");
                    return;
                }
                if (ret == 1) {
                    ret = block.read_done();
                    if (ret < 0) {
                        return;
                    }
                    break;
                }
            }
            is_cur_succ = true;
            DB_WARNING("process_block tm: %ld", tm.get_time());
        };
        file_concurrency_cond.increase_wait();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(process_block);
    }
    return 0;
}

/// ParquetScanner
int ParquetScanner::init(FileVectorizedReader* vectorized_reader, FileScanNode* scan_node) {
    int ret = FileScanner::init(vectorized_reader, scan_node);
    if (ret != 0) {
        return -1;
    }
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    ret = get_file_reader(reader);
    if (ret != 0) {
        DB_WARNING("Fail to get_file_reader, file: %s", _file_path.c_str());
        return ret;
    }
    _file_metadata = reader->parquet_reader()->metadata();
    if (_file_metadata == nullptr) {
        DB_WARNING("FileMetaData is nullptr, file: %s", _file_path.c_str());
        return -1;
    }
    const ::parquet::SchemaDescriptor* schema = _file_metadata->schema();
    if (schema == nullptr) {
        DB_WARNING("schema is nullptr, file: %s", _file_path.c_str());
        return -1;
    }
    // Schema处理
    std::unordered_map<std::string, int> column_name2index_map;
    for (int i = 0; i < schema->num_columns(); ++i) {
        const ::parquet::ColumnDescriptor* column = schema->Column(i);
        if (column == nullptr) {
            DB_WARNING("column is nullptr, file: %s", _file_path.c_str());
            return -1;
        }
        column_name2index_map[column->name()] = i;
    }
    const auto& field_id2info = _scan_node->get_field_id2info();
    for (const auto& field_info : field_id2info) {
        if (field_info == nullptr) {
            continue;
        }
        const std::string& field_name = field_info->short_name;
        if (column_name2index_map.find(field_name) != column_name2index_map.end()) {
            _exist_column_indices.emplace_back(column_name2index_map.at(field_name));
        } else {
            _not_exist_columns.emplace_back(field_info);
        }
    }
    pb::TupleDescriptor* tuple_desc = _scan_node->get_tuple();
    for (const auto& slot : tuple_desc->slots()) {
        if (slot.field_id() >= field_id2info.size() || field_id2info[slot.field_id()] == nullptr) {
            continue;
        }
        const std::string& field_name = std::to_string(slot.tuple_id()) + "_" + std::to_string(slot.slot_id());
        _column_name_map[field_name] = field_id2info[slot.field_id()]->short_name;
        _column_type_map[field_name] = field_id2info[slot.field_id()]->type;
    }
    const auto& partition_fields = scan_node->get_partition_fields();
    for (int i = 0; i < partition_fields.size(); ++i) {
        _partition_id2val_map[partition_fields[i]->id] = _partition_vals[i];
    }
    return 0;
}

int ParquetScanner::run() {
    BthreadCond& file_concurrency_cond = _vectorized_reader->get_file_concurrency_cond();
    std::atomic<bool>& is_succ = _vectorized_reader->get_is_succ();
    std::atomic<bool>& eos = _vectorized_reader->get_eos();
    auto& record_batches = _vectorized_reader->get_record_batches();
    for (size_t row_group_idx = 0; row_group_idx < _file_metadata->num_row_groups(); ++row_group_idx) {
        if (!is_succ) {
            break;
        }
        if (eos) {
            break;
        }
        auto process_row_group = 
                [this, &file_concurrency_cond, &is_succ, &eos, &record_batches, row_group_idx] () {
            TimeCost tm;
            bool is_cur_succ = false;
            ON_SCOPE_EXIT(([&file_concurrency_cond, &is_succ, &is_cur_succ]() {
                file_concurrency_cond.decrease_signal();
                if (!is_cur_succ) {
                    is_succ = false;
                }
            }));
            std::unique_ptr<::parquet::arrow::FileReader> reader;
            if (get_file_reader(reader) != 0) {
                DB_WARNING("Fail to get_file_reader, file_path: %s", _file_path.c_str());
                return;
            }
            std::unique_ptr<::arrow::RecordBatchReader> record_batch_reader;
            auto status = reader->GetRecordBatchReader({ row_group_idx }, _exist_column_indices, &record_batch_reader);
            if (!status.ok()) {
                DB_WARNING("Fail to GetRecordBatchReader, row_group_idx: %ld, path: %s, reason: %s", 
                            row_group_idx, _file_path.c_str(), status.message().c_str());
                return;
            }
            if (record_batch_reader == nullptr) {
                DB_WARNING("RecordBatchReader is nullptr, row_group_idx: %ld, path: %s",
                            row_group_idx, _file_path.c_str());
                return;
            }
            while (true) {
                if (!is_succ) {
                    break;
                }
                if (eos) {
                    break;
                }
                std::shared_ptr<::arrow::RecordBatch> batch;
                auto status = record_batch_reader->ReadNext(&batch);
                if (!status.ok()) {
                    DB_WARNING("RecordBatchReader fail to ReadNext, row_group_idx: %ld, path: %s, reason: %s", 
                                row_group_idx, _file_path.c_str(), status.message().c_str());
                    return;
                }
                if (batch == nullptr) {
                    break;
                }
                int ret = process_record_batch(batch);
                if (ret != 0) {
                    DB_WARNING("Fail to process_record_batch");
                    return;
                }
                record_batches.blocking_put(batch);
            }
            is_cur_succ = true;
            DB_WARNING("process_row_group tm: %ld", tm.get_time());
        };
        file_concurrency_cond.increase_wait();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(process_row_group);
    }
    return 0;
}

// 填充文件中不存在的列，并转换schema
int ParquetScanner::process_record_batch(std::shared_ptr<::arrow::RecordBatch>& record_batch) {
    const auto& schema = _vectorized_reader->schema();
    if (record_batch == nullptr) {
        DB_WARNING("record_batch is nullptr");
        return -1;    
    }
    // 填充文件中不存在的列
    for (auto* field_info : _not_exist_columns) {
        std::shared_ptr<arrow::Field> arrow_field = VectorizeHelpper::make_field(
            field_info->short_name, arrow::Type::type(primitive_to_arrow_type(field_info->type)));
        if (arrow_field == nullptr) {
            DB_WARNING("Fail to get arrow type, field_id: %d, field_type: %d", field_info->id, field_info->type);
            return -1;
        }
        ExprValue value = field_info->default_expr_value;
        if (_partition_id2val_map.find(field_info->id) != _partition_id2val_map.end()) {
            // 分区字段值
            value = ExprValue(field_info->type, _partition_id2val_map[field_info->id]);
        }
        std::shared_ptr<arrow::Array> arrow_array = 
            VectorizeHelpper::make_array_from_exprvalue(field_info->type, value, record_batch->num_rows());
        if (arrow_array == nullptr) {
            DB_WARNING("Fail to make array from expr value");
            return -1;
        }
        auto new_record_batch_ret = record_batch->AddColumn(record_batch->num_columns(), arrow_field, arrow_array);
        if (!new_record_batch_ret.ok()) {
            DB_WARNING("Fail to AddColumn");
            return -1;
        }
        record_batch = *new_record_batch_ret;
    }
    // 转换schema，列名转化为tupleid_slotid形式，类型也可能发生转化
    int ret = VectorizeHelpper::change_arrow_record_batch_schema(
                    _column_name_map, _column_type_map, schema, record_batch, &record_batch);
    if (ret != 0) {
        DB_WARNING("Fail to change arrow record batch schema");
        return -1;
    }
    return 0;
}

int ParquetScanner::get_file_reader(std::unique_ptr<::parquet::arrow::FileReader>& reader) {
    // Open parquet file
    auto res = _fs->open_arrow_file(_file_path);
    if (!res.ok()) {
        DB_WARNING("Fail to open ParquetReader, reason: %s", res.status().message().c_str());
        return -1;
    }
    auto infile = std::move(res).ValueOrDie();
    // Avoid reading whole file data into memory at once
    ::parquet::ReaderProperties read_properties = ::parquet::default_reader_properties();
    read_properties.enable_buffered_stream();
    read_properties.set_buffer_size(FLAGS_file_buffer_size * 1024 * 1024ULL);
    // Set GetRecordBatchReader batch size, default: 65536
    ::parquet::ArrowReaderProperties arrow_properties = ::parquet::default_arrow_reader_properties();
    arrow_properties.set_batch_size(FLAGS_chunk_size);
    arrow_properties.set_pre_buffer(false);
    ::parquet::arrow::FileReaderBuilder builder;
    builder.properties(arrow_properties);
    auto status = builder.Open(infile, read_properties);
    if (!status.ok()) {
        // 非Parquet文件Open会失败
        DB_WARNING("FileBuilder fail to open file, file: %s, reason: %s", 
                    _file_path.c_str(), status.message().c_str());
        return -2; 
    }
    status = builder.Build(&reader);
    if (!status.ok()) {
        DB_WARNING("FileBuilder fail to build reader, file: %s reason: %s",
                    _file_path.c_str(), status.message().c_str());
        return -1;
    }
    if (reader == nullptr) {
        DB_WARNING("FileReader is nullptr, file: %s", _file_path.c_str());
        return -1;
    }
    if (reader->parquet_reader() == nullptr) {
        DB_WARNING("ParquetReader is nullptr, file: %s", _file_path.c_str());
        return -1;
    }
    return 0;
}

} // namespace baikaldb