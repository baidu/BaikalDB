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

#include "mysql_interact.h"
#include "select_manager_node.h"

namespace baikaldb {

class MysqlScanNode;
class MysqlVectorizedReader : public arrow::RecordBatchReader {
public:
    MysqlVectorizedReader() {}
    virtual ~MysqlVectorizedReader() {}
    int init(MysqlScanNode* scan_node, RuntimeState* state);
    int add_first_row_batch(RowBatch* batch);
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;
    std::shared_ptr<arrow::Schema> schema() const override { 
        return _schema; 
    }

private:
    std::shared_ptr<arrow::Schema> _schema;
    RowBatch _batch;
    RuntimeState* _state = nullptr;
    MysqlScanNode* _scan_node = nullptr;
    bool _eos = false;
    bool _first_batch_need_handle = false;
    // paralle模式使用
    bool _is_delay_fetch = false;
    std::shared_ptr<IndexCollectorCond> _index_cond;
};

class MysqlScanNode : public ScanNode {
public:
    MysqlScanNode() {
        _is_mysql_scan_node = true;
    }
    virtual ~MysqlScanNode() {
        for (auto expr : _scan_filter_exprs) {
            ExprNode::destroy_tree(expr);
        }
    }
    virtual int init(const pb::PlanNode& node) override;
    virtual int open(RuntimeState* state) override;
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) override;
    virtual void close(RuntimeState* state) override;
    virtual int build_arrow_declaration(RuntimeState* state) override;
    virtual bool can_use_arrow_vector(RuntimeState* state) override {
        return true;
    }
    virtual void find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) override {
        ExecNode::find_place_holder(placeholders);
        for (auto& expr : _scan_filter_exprs) {
            if (expr != nullptr) {
                expr->find_place_holder(placeholders);
            }
        }
    }
    pb::TupleDescriptor* get_tuple() {
        return _tuple_desc;
    }
    int query_sql(RuntimeState* state);

private:
    int construct_sql(RuntimeState* state, std::string& sql);

private:
    bool _has_query_sql = false;
    std::unique_ptr<MysqlInteract> _mysql_interact;
    std::unique_ptr<baikal::client::ResultSet> _mysql_result;
    std::map<int32_t, int32_t> _slot_column_mapping;
    std::vector<ExprNode*> _scan_filter_exprs;
    SmartTable _table_info;
    pb::TupleDescriptor* _tuple_desc = nullptr;
    int _scan_rows = 0;
    // 列式执行使用
    std::shared_ptr<MysqlVectorizedReader> _vectorized_reader;
    std::shared_ptr<BthreadArrowExecutor> _arrow_io_executor;
};

} // namespace baikaldb