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

#include <vector>
#include "mysql_wrapper.h"
#include "exec_node.h"
#include "data_buffer.h"
#include "sorter.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
class PacketNode : public ExecNode {
public:
    PacketNode() {
        _wrapper = MysqlWrapper::get_instance();
    }
    virtual ~PacketNode() {
        for (auto expr : _projections) {
            ExprNode::destroy_tree(expr);
        }
    }
    virtual int init(const pb::PlanNode& node);
    virtual int expr_optimize(QueryContext* ctx);
    virtual int open(RuntimeState* state);
    virtual int get_next(RuntimeState* state);
    virtual void close(RuntimeState* state);

    pb::OpType op_type() {
        return _op_type;
    }

    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders);

    size_t field_count() {
        return _fields.size();
    }
    int pack_fields(DataBuffer* buffer, int& packet_id);
    
    // COM_STMT_EXECUTE use ProtocolBinary for result set
    void set_binary_protocol(bool binary) {
        _binary_protocol = binary;
    }
    std::vector<ExprNode*>& mutable_projections() {
        return _projections;
    }
    std::vector<ResultField>& mutable_fields() {
        return _fields;
    }

private:
    int open_histogram(RuntimeState* state);
    int open_cmsketch(RuntimeState* state);
    int open_analyze(RuntimeState* state);
    int open_trace(RuntimeState* state);
    int handle_trace(RuntimeState* state);
    int handle_trace2(RuntimeState* state);
    int handle_show_cost(RuntimeState* state);
    void pack_trace2(std::vector<std::map<std::string, std::string>>& info, const pb::TraceNode& trace_node,
        int64_t& total_scan_rows, int64_t& total_index_filter, int64_t& total_get_primary, int64_t& total_where_filter);
    int handle_explain(RuntimeState* state);
    int pack_ok(int num_affected_rows, NetworkSocket* client);
    // 先不用，err在外部填
    int pack_err();
    int pack_head();
    int pack_fields();
    int pack_vector_row(const std::vector<std::string>& row);
    int pack_text_row(MemRow* row);
    int pack_binary_row(MemRow* row);
    int pack_eof();
    int fatch_expr_subquery_results(RuntimeState* state);

private:
    bool _binary_protocol = false;
    pb::OpType _op_type;
    std::vector<ExprNode*> _projections;
    std::vector<ResultField> _fields;
    NetworkSocket* _client = nullptr;
    MysqlWrapper* _wrapper = nullptr;
    DataBuffer* _send_buf = nullptr;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
