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

#include "limit_node.h"
#include "runtime_state.h"
#include "query_context.h"
 
namespace baikaldb {
int LimitNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    const pb::LimitNode& limit_node = node.derive_node().limit_node();
    _offset = limit_node.offset();

    if (limit_node.has_offset_expr()) {
        ret = ExprNode::create_tree(limit_node.offset_expr(), &_offset_expr);
        if (ret < 0) {
            return ret;
        }
    }
    if (limit_node.has_count_expr()) {
        ret = ExprNode::create_tree(limit_node.count_expr(), &_count_expr);
        if (ret < 0) {
            return ret;
        }
    }
    return 0;
}

int LimitNode::expr_optimize(QueryContext* ctx) {
    int ret = 0;
    ret = ExecNode::expr_optimize(ctx);
    if (ret < 0) {
        DB_WARNING("ExecNode::optimize fail, ret:%d", ret);
        return ret;
    }
    if (_offset_expr != nullptr) {
        // cal offset_expr value
        ret = _offset_expr->type_inferer();
        if (ret < 0) {
            DB_WARNING("offset_expr type_inferer fail:%d", ret);
            return ret;
        }
        if (_offset_expr->is_constant()) {
            ret = _offset_expr->open();
            if (ret < 0) {
                DB_WARNING("expr open fail:%d", ret);
                return ret;
            }
            ExprValue value = _offset_expr->get_value(nullptr);
            _offset_expr->close();
            if (value.is_int()) {
                _offset = value.get_numberic<int64_t>();
                // place holder 没替换前不做expr_optimize
            } else {
                DB_WARNING("invalid offset_expr type: %d", _offset_expr->node_type());
                return -1;
            }
        } else {
            DB_WARNING("invalid offset_expr type: %d", _offset_expr->node_type());
            return -1;
        }
    }
    if (_count_expr != nullptr) {
        // cal offset_expr value
        ret = _count_expr->type_inferer();
        if (ret < 0) {
            DB_WARNING("count_expr type_inferer fail:%d", ret);
            return ret;
        }
        //常量表达式计算
        _count_expr->const_pre_calc();
        if (_count_expr->is_constant()) {
            ret = _count_expr->open();
            if (ret < 0) {
                DB_WARNING("expr open fail:%d", ret);
                return ret;
            }
            ExprValue value = _count_expr->get_value(nullptr);
            _count_expr->close();
            if (value.is_int()) {
                _limit = value.get_numberic<int64_t>();
            } else {
                DB_WARNING("invalid count_expr type: %d", _count_expr->node_type());
                return -1;
            }
        } else {
            DB_WARNING("invalid count_expr type: %d", _count_expr->node_type());
            return -1;
        }
    }
    if (_limit == 0) {
        DB_WARNING("limit is 0");
        ctx->return_empty = true;
    }
    // DB_WARNING("offset and count: %ld, %ld", _offset, _limit);
    return 0;
}

void LimitNode::transfer_pb(int64_t region_id, pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(region_id, pb_node);
    pb::DerivePlanNode* derive = pb_node->mutable_derive_node();
    pb::LimitNode* limit = derive->mutable_limit_node();
    limit->set_offset(_offset);
    return;
}

void LimitNode::find_place_holder(std::unordered_multimap<int, ExprNode*>& placeholders) {
    ExecNode::find_place_holder(placeholders);
    if (_offset_expr) {
        _offset_expr->find_place_holder(placeholders);
    }
    if (_count_expr) {
        _count_expr->find_place_holder(placeholders);
    }
}

int LimitNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (reached_limit()) {
        state->set_eos();
        *eos = true;
        return 0;
    }
    int ret = 0;
    ret = _children[0]->get_next(state, batch, eos);
    if (ret < 0) {
        DB_WARNING("_children get_next fail");
        return ret;
    }
    while (_num_rows_skipped < _offset) {
        if (_num_rows_skipped + (int)batch->size() <= _offset) {
            _num_rows_skipped += batch->size();
            batch->clear();
            if (*eos) {
                return 0;
            }
            ret = _children[0]->get_next(state, batch, eos);
            if (ret < 0) {
                DB_WARNING("_children get_next fail");
                return ret;
            }
        } else {
            int num_skip_rows = _offset - _num_rows_skipped;
            _num_rows_skipped = _offset;
            batch->skip_rows(num_skip_rows);
            break;
        }
    }
    _num_rows_returned += batch->size();
    if (reached_limit()) {
        state->set_eos();
        *eos = true;
        int keep_nums = batch->size() - (_num_rows_returned - _limit);
        batch->keep_first_rows(keep_nums);
        _num_rows_returned = _limit;
        return 0;
    }
    return 0;
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
