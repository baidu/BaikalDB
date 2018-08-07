// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

namespace baikaldb {
class PacketNode : public ExecNode {
public:
    PacketNode() {
    }
    virtual ~PacketNode() {
        for (auto expr : _projections) {
            ExprNode::destory_tree(expr);
        }
    }
    virtual int init(const pb::PlanNode& node);
    virtual int expr_optimize(std::vector<pb::TupleDescriptor>* tuple_descs);
    virtual int open(RuntimeState* state);
    virtual void close(RuntimeState* state);

    pb::OpType op_type() {
        return _op_type;
    }

private:
    int pack_ok(int num_affected_rows, int64_t last_insert_id = 0);
    // 先不用，err在外部填
    int pack_err();
    int pack_head();
    int pack_fields();
    int pack_row(MemRow* row);
    int pack_eof();

private:
    pb::OpType _op_type;
    std::vector<ExprNode*> _projections;
    std::vector<ResultField> _fields;
    int _packet_id = 1;
    MysqlWrapper* _wrapper = nullptr;
    DataBuffer* _send_buf = nullptr;
};
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
