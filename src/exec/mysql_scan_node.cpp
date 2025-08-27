#include "mysql_scan_node.h"

namespace baikaldb {

int MysqlScanNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ScanNode::init(node);
    if (ret != 0) {
        DB_WARNING("ExecNode::init fail, ret: %d", ret);
        return ret;
    }
    return 0;
}

int MysqlScanNode::open(RuntimeState* state) {
    if (state == nullptr) {
        DB_WARNING("state is nullptr");
        return -1;
    }
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), OPEN_TRACE, nullptr);
    _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (_table_info == nullptr) {
        DB_WARNING("table info is null, table id: %ld", _table_id);
        return -1;
    }
    _mysql_interact.reset(new MysqlInteract(_table_info->dblink_info.mysql_info()));
    _mysql_result.reset(new baikal::client::ResultSet());
    _tuple_desc = state->get_tuple_desc(_tuple_id);
    if (_tuple_desc == nullptr) {
        DB_WARNING("tuple desc is nullptr");
        return -1;
    }
    if (_related_manager_node == nullptr) {
        DB_WARNING("related_manager_node is nullptr");
        return -1;
    }
    if (_related_manager_node->is_delay_fetcher_store()) {
        set_node_exec_type(pb::EXEC_ARROW_ACERO);
        return 0;
    }
    int ret = query_sql(state);
    if (ret < 0) {
        DB_WARNING("Fail to query mysql");
        return -1;
    }
    return 0;
}

int MysqlScanNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (!_has_query_sql) {
        // delay fetch场景第一次get_next时，未执行过query_sql，直接返回；
        return 0;
    }
    if (state == nullptr) {
        DB_WARNING("state is nullptr");
        return -1;
    }
    START_LOCAL_TRACE(get_trace(), state->get_trace_cost(), GET_NEXT_TRACE, ([this](TraceLocalNode& local_node) {
        local_node.set_scan_rows(_scan_rows);
    }));
    ON_SCOPE_EXIT(([this, state]() {
        state->set_num_scan_rows(_scan_rows);
    }));
    if (_tuple_desc == nullptr) {
        DB_WARNING("tuple desc is nullptr");
        return -1;
    }
    MemRowDescriptor* mem_row_desc = state->mem_row_desc();
    if (mem_row_desc == nullptr) {
        DB_WARNING("mem_row_desc is nullptr");
        return -1;
    }
    int64_t used_size = 0;
    while (true) {
        if (state->is_cancelled()) {
            DB_WARNING_STATE(state, "cancelled");
            *eos = true;
            break;
        }
        if (reached_limit()) {
            *eos = true;
            break;
        }
        if (batch->is_full()) {
            break;
        }
        if (!_mysql_result->next()) {
            // 流式读需要判断mysql_errno是否为0
            int error_code = _mysql_interact->get_error_code(); 
            if (error_code != 0) {
                DB_WARNING("Fail to fetch mysql result, error_code: %d", error_code);
                return -1;
            }
            *eos = true;
            break;
        }
        std::unique_ptr<MemRow> mem_row = mem_row_desc->fetch_mem_row();
        if (mem_row == nullptr) {
            DB_WARNING("mem_row is nullptr");
            return -1;
        }
        for (const auto& [outer_slot_id, inner_column_id] : _slot_column_mapping) {
            std::string mysql_field_val;
            int ret = _mysql_result->get_string(inner_column_id, &mysql_field_val);
            if (ret != baikal::client::SUCCESS && ret != baikal::client::VALUE_IS_NULL) {
                DB_WARNING("Fail to get string ret: %d, outer_slot_id: %d, inner_column_id: %d",
                            ret, outer_slot_id, inner_column_id);
                return -1;
            }
            int slot_idx = get_slot_idx(*_tuple_desc, outer_slot_id);
            if (slot_idx < 0 || slot_idx >= _tuple_desc->slots_size()) {
                DB_WARNING("slot idx is invalid, slot_idx: %d, slots_size: %d", 
                            slot_idx, _tuple_desc->slots_size());
                return -1;
            }
            const pb::SlotDescriptor& slot_desc = _tuple_desc->slots(slot_idx);
            ExprValue expr_value;
            if (ret != baikal::client::VALUE_IS_NULL) {
                expr_value = ExprValue(slot_desc.slot_type(), mysql_field_val);
            }
            mem_row->set_value(slot_desc.tuple_id(), slot_desc.slot_id(), expr_value);
        }
        _scan_rows++;
        used_size += mem_row->used_size();
        if (used_size > 1024 * 1024) {
            if (state->memory_limit_exceeded(std::numeric_limits<int>::max(), used_size) != 0) {
                state->error_code = ER_TOO_BIG_SELECT;
                state->error_msg.str("select reach memory limit");
                return -1;
            }
            used_size = 0;
        }
        if (batch->use_memrow()) {
            // 行存
            batch->move_row(std::move(mem_row));
        } else {
            // 列存
            int ret = batch->get_chunk()->add_row({_tuple_desc}, mem_row.get());
            if (ret != 0) {
                DB_WARNING("Fail to add row to chunk");
                return -1;
            }
        }
    }
    // 与Rocksdb保持一致，如果只有一个batch直接走行存，否则走列存
    if (state->execute_type == pb::EXEC_ARROW_ACERO) {
        if (*eos == false && batch->use_memrow()) {
            // 行数据转列recordbatch
            _vectorized_reader = std::make_shared<MysqlVectorizedReader>();
            if (_vectorized_reader == nullptr) {
                DB_WARNING("_vectorized_reader is nullptr");
                return -1;
            }
            int ret = _vectorized_reader->init(this, state);
            if (ret != 0) {
                DB_WARNING("Fail to init vectorized_reader");
                return -1;
            }
            ret = _vectorized_reader->add_first_row_batch(batch);
            if (ret != 0) {
                DB_WARNING("Fail to add_first_row_batch");
                return 0;
            }
            set_node_exec_type(pb::EXEC_ARROW_ACERO);
        }
    }
    return 0;
}

void MysqlScanNode::close(RuntimeState* state)  {
    ScanNode::close(state);
    for (auto expr : _scan_filter_exprs) {
        if (expr != nullptr) {
            expr->close();
        }
    }
    _vectorized_reader = nullptr;
    _has_query_sql = false;
    _scan_rows = 0;
}

int MysqlScanNode::build_arrow_declaration(RuntimeState* state) {
    if (state == nullptr) {
        DB_WARNING("state is nullptr");
        return -1;
    }
    START_LOCAL_TRACE_WITH_PARTITION_PROPERTY(get_trace(), state->get_trace_cost(), &_partition_property, OPEN_TRACE, nullptr);
    if (_vectorized_reader == nullptr) {
        // parallel场景
        _vectorized_reader = std::make_shared<MysqlVectorizedReader>();
        if (_vectorized_reader == nullptr) {
            DB_WARNING("_vectorized_reader is nullptr");
            return -1;
        }
        int ret = _vectorized_reader->init(this, state);
        if (ret != 0) {
            DB_WARNING("Fail to init vectorized_reader");
            return -1;
        }
    }
    std::function<arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>()> iter_maker = [this] () {
        arrow::Iterator<std::shared_ptr<arrow::RecordBatch>> batch_it = arrow::MakeIteratorFromReader(_vectorized_reader);
        return batch_it;
    };
    if (_related_manager_node == nullptr) {
        DB_WARNING("related_manager_node is nullptr");
        return -1;
    }
    bool is_delay_fetch = _related_manager_node->is_delay_fetcher_store();
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

int MysqlScanNode::query_sql(RuntimeState* state) {
    std::string sql;
    int ret = construct_sql(state, sql);
    if (ret < 0) {
        DB_WARNING("Fail to construct sql, ret: %d", ret);
        return -1;
    }
    if (_mysql_interact == nullptr) {
        DB_WARNING("_mysql_interact is nullptr");
        return -1;
    }
    // 流式读Mysql数据
    ret = _mysql_interact->query(sql, _mysql_result.get(), false);
    if (ret < 0) {
        DB_WARNING("Fail to query mysql, ret: %d, sql: %s", ret, sql.c_str());
        return -1;
    }
    _has_query_sql = true;
    return 0;
}

int MysqlScanNode::construct_sql(RuntimeState* state, std::string& sql) {
    if (state == nullptr) {
        DB_WARNING("state is nullptr");
        return -1;
    }
    if (_tuple_desc == nullptr) {
        DB_WARNING("_tuple_desc is nullptr");
        return -1;
    }
    if (_table_info == nullptr) {
        DB_WARNING("_table_info is nullptr");
        return -1;
    }
    if (_mysql_interact == nullptr) {
        DB_WARNING("_mysql_interact is nullptr");
        return -1;
    }
    const std::string& mysql_db_name = _table_info->dblink_info.mysql_info().database_name();
    const std::string& mysql_table_name = _table_info->dblink_info.mysql_info().table_name();
    // 获取Select_fields，构造slotid_fieldname_map和_slot_column_mapping
    int column_id = 0;
    std::unordered_map<int32_t, std::string> slotid_fieldname_map;
    std::vector<std::string> select_fields;
    for (const auto& slot_desc : _tuple_desc->slots()) {
        FieldInfo* field = _table_info->get_field_ptr(slot_desc.field_id());
        if (field == nullptr) {
            DB_WARNING("field is nullptr");
            return -1;
        }
        std::string field_name = "`" + mysql_db_name + "`.`" + mysql_table_name + "`.`" + field->short_name + "`";
        slotid_fieldname_map[slot_desc.slot_id()] = field_name;
        _slot_column_mapping[slot_desc.slot_id()] = column_id++;
        select_fields.emplace_back(field_name);
    }
    // 获取过滤条件，从父节点的FILTER_NODE获取过滤条件，下推到Mysql
    if (_parent->node_type() == pb::WHERE_FILTER_NODE || _parent->node_type() == pb::TABLE_FILTER_NODE) {
        std::vector<ExprNode*>* parent_filter_exprs = _parent->mutable_conjuncts();
        if (parent_filter_exprs == nullptr) {
            DB_WARNING("parent_filter_exprs is nullptr");
            return -1;
        }
        if (parent_filter_exprs->size() > 0) {
            std::swap(*parent_filter_exprs, _scan_filter_exprs);
        }
    }
    std::vector<std::string> filters;
    for (auto* expr : _scan_filter_exprs) {
        if (expr == nullptr) {
            DB_WARNING("expr is nullptr");
            return -1;
        }
        std::string filter_str = expr->to_sql(slotid_fieldname_map, _mysql_interact->get_connection());
        if (filter_str.empty()) {
            DB_WARNING("filter str is empty");
            return -1;
        }
        filters.emplace_back(filter_str);
    }
    // 获取Limit
    std::string limit_str;
    if (_limit >= 0) {
        limit_str = std::to_string(_limit);
    }
    // 构造SQL
    sql.clear();
    sql.reserve(64);
    sql = "SELECT ";
    for (int i = 0; i < select_fields.size(); ++i) {
        sql += select_fields[i];
        if (i != select_fields.size() - 1) {
            sql += ",";
        }
    }
    sql += " FROM ";
    sql += "`" + mysql_db_name + "`.`" + mysql_table_name + "`";
    if (!filters.empty()) {
        sql += " WHERE ";
        for (int i = 0; i < filters.size(); ++i) {
            sql += filters[i];
            if (i != filters.size() - 1) {
                sql += " AND ";
            }
        }
    }
    if (!limit_str.empty()) {
        sql += " LIMIT ";
        sql += limit_str;
    }
    DB_WARNING("request sql: %s", sql.c_str());
    return 0;
}

int MysqlVectorizedReader::init(MysqlScanNode* scan_node, RuntimeState* state) {
    _scan_node = scan_node;
    _state = state;
    if (_scan_node == nullptr) {
        DB_WARNING("_scan_node is nullptr");
        return -1;
    }
    _batch.init_chunk({_scan_node->get_tuple()}, &_schema);
    if (_schema == nullptr) {
        DB_WARNING("_schema is nullptr");
        return -1;
    }
    SelectManagerNode* related_manager_node = 
        static_cast<SelectManagerNode*>(_scan_node->get_related_manager_node());
    if (related_manager_node == nullptr) {
        DB_WARNING("_related_manager_node is nullptr");
        return -1;
    }
    _is_delay_fetch = related_manager_node->is_delay_fetcher_store();
    _index_cond = related_manager_node->get_index_collector_cond();
    return 0;
}

arrow::Status MysqlVectorizedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    int ret = 0;
    // delay fetch场景，需要再第一个ReadNext时访问mysql获取数据
    if (_is_delay_fetch) {
        if (_index_cond != nullptr) {
            _index_cond->cond.wait();
            if (_index_cond->index_cnt == 0) {
                // join驱动表没数据
                out->reset();
                return arrow::Status::OK();
            }
        }
        ret = _scan_node->query_sql(_state);
        if (ret != 0) {
            return arrow::Status::IOError("query sql fail");
        }
        _is_delay_fetch = false;
    }
    if (_state->is_cancelled()) {
        DB_WARNING_STATE(_state, "cancelled");
        _eos = true;
        return arrow::Status::OK();
    }
    if (_eos) {
        out->reset();
        return arrow::Status::OK();
    }
    // LIMIT已经下推到Mysql中，此处不需要处理LIMIT
    if (!_first_batch_need_handle) {
        _batch.set_capacity(std::min(FLAGS_chunk_size, 2 * (int32_t)_batch.capacity()));
        ret = _scan_node->get_next(_state, &_batch, &_eos); 
        if (ret < 0) {
            DB_FATAL("rocksdb scan node get_next fail in vectorize mode");
            return arrow::Status::IOError("RocksdbVectorizedReader read fail");
        }
    } else {
        _first_batch_need_handle = false;
    }
    std::shared_ptr<arrow::RecordBatch> record_batch;
    ret = _batch.finish_and_make_record_batch(&record_batch);
    if (ret < 0) {
        DB_FATAL("arrow chunk finish and make record batch fail");
        return arrow::Status::IOError("chunk finish and make record batch fail");
    }
    *out = record_batch;
    return arrow::Status::OK();
}

int MysqlVectorizedReader::add_first_row_batch(RowBatch* batch) {
    if (batch == nullptr) {
        DB_WARNING("batch is nullptr");
        return -1;
    }
    if (batch->add_row_batch_to_chunk({_scan_node->get_tuple()}, _batch.get_chunk()) != 0) {
        DB_WARNING("Fail to add_row_batch_to_chunk");
        return -1;
    }
    if (batch->size() > 0) {
        _batch.set_capacity(_batch.size());
    }
    _first_batch_need_handle = true;
    return 0;
}

} // namespace baikaldb