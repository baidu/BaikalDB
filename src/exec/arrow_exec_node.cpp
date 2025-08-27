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
#include <string>
#include <map>
#include <unordered_map>
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow_exec_node.h"
#include "arrow/scalar.h"
#include "vectorize_helpper.h"
#include <fstream>
#include <iostream>

namespace baikaldb {
DEFINE_int32(change_no_index_join_lines, -1, "change no_index_join threshold");
/*  IndexCollecterNode(ICN)
 *                           Join(c3)
 *                     /                  \   
 *                   ICN(c3)           Join(c4)
 *                  /                    /   \
 *                 Join(c1)         ICN(c4)   t_e(c4)
 *                /      \            /
 *            ICN(c1)   Join(c2)   t_d(c3)
 *              /        /  \
 *            t_a    ICN(c2) t_c(c2)  
 *                     /
 *                 t_b(c1)
 */
arrow::Result<arrow::acero::ExecNode*> IndexCollecterNode::Make(arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs,
                            const arrow::acero::ExecNodeOptions& options) {
    auto s = arrow::acero::ValidateExecNodeInputs(plan, inputs, 1, "IndexCollecterNode");
    if (!s.ok()) {
        return arrow::Status::TypeError("IndexCollecterNode valid fail");
    }
    const auto& opt = arrow::internal::checked_cast<const IndexCollectorNodeOptions&>(options);
    if (opt.state == nullptr || opt.join_node == nullptr) {
        return arrow::Status::TypeError("IndexCollecterNode state or joinnode is null");
    }
    auto schema = inputs[0]->output_schema();
    return plan->EmplaceNode<IndexCollecterNode>(plan, std::move(inputs), std::move(schema), 
                opt.state, opt.join_node, opt.limit_cnt);
}

arrow::Result<arrow::compute::ExecBatch> IndexCollecterNode::ProcessBatch(arrow::compute::ExecBatch batch) {
    // 构建in filter
    std::lock_guard<bthread::Mutex> lock(_mutex);
    if (_limit_cnt > 0 && _passed_row_cnt >= _limit_cnt) {
        return batch;
    }
    if (_transfer_to_no_index_join) {
        return batch;
    }
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> res = batch.ToRecordBatch(output_schema_);
    if (!res.ok()) {
        return arrow::Status::ExecutionError("IndexCollecterNode Process ExecBatch to RecordBatch fail");
    }
    std::shared_ptr<arrow::RecordBatch> record_batch = *res;
    if (_limit_cnt > 0) {
        record_batch = record_batch->Slice(0, _limit_cnt - _passed_row_cnt);
    }
    int in_condition_size = _join_node->vectorize_index_collector(_state, record_batch);
    if (in_condition_size < 0) {
        return arrow::Status::ExecutionError("IndexCollecterNode build in filter fail");
    }
    _passed_row_cnt += record_batch->num_rows();
    if (FLAGS_change_no_index_join_lines > 0
         && in_condition_size >= FLAGS_change_no_index_join_lines) {
        _transfer_to_no_index_join = true;
        _join_node->clear_in_conditions();
        if (0 != _join_node->runtime_filter(_state)) {
            DB_FATAL("arrow IndexCollecterNode runtime_filter fail");
            // 失败也需要唤醒等待的select manager node
            notify_all();
            return arrow::Status::ExecutionError("arrow IndexCollecterNode runtime_filter fail");
        }
        notify_all();
    }
    return batch;
}

int IndexCollecterNode::Finish(int& total_output_batch_cnt) {
    std::lock_guard<bthread::Mutex> lock(_mutex);
    if (_passed_row_cnt > 0 && !_transfer_to_no_index_join) {
        if (0 != _join_node->runtime_filter(_state)) {
            DB_FATAL("arrow IndexCollecterNode runtime_filter fail");
            // 失败也需要唤醒等待的select manager node
            notify_all();
            return -1;
        }
    }
    return notify_all();
}

int IndexCollecterNode::notify_all() {
    std::shared_ptr<IndexCollectorCond> index_cond = _join_node->get_index_collecter_cond();
    if (index_cond == nullptr) {
        DB_FATAL("arrow IndexCollecterNode get cond fail");
        return -1;
    }
    index_cond->index_cnt = _passed_row_cnt;
    // 必须broadcast, 可能有多个sourcenode在等待
    index_cond->cond.decrease_broadcast();
    return 0;
}

arrow::Status IndexCollecterNode::StopProducingImpl() {
    std::shared_ptr<IndexCollectorCond> index_cond = _join_node->get_index_collecter_cond();
    if (index_cond == nullptr) {
        DB_FATAL("IndexCollecterNode get cond fail");
        return arrow::Status::OK();
    }
    // 必须broadcast, 可能有多个sourcenode在等待
    index_cond->cond.decrease_broadcast();
    return arrow::Status::OK();
}

/*
 * UnorderedFetchNode (LIKE acero FetchNode but support un-ordered data)
 */
arrow::Result<arrow::acero::ExecNode*> UnorderedFetchNode::Make(arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs,
                            const arrow::acero::ExecNodeOptions& options) {
    auto s = arrow::acero::ValidateExecNodeInputs(plan, inputs, 1, "UnorderedFetchNode");
    if (!s.ok()) {
        return arrow::Status::TypeError("UnorderedFetchNode valid fail");
    }
    const auto& opt = arrow::internal::checked_cast<const UnorderedFetchNodeOptions&>(options);
    if (opt.offset_cnt < 0) {
        return arrow::Status::Invalid("`offset` must be non-negative");
    }
    if (opt.limit_cnt < 0) {
        return arrow::Status::Invalid("`count` must be non-negative");
    }
    auto schema = inputs[0]->output_schema();
    return plan->EmplaceNode<UnorderedFetchNode>(plan, std::move(inputs), std::move(schema), 
                opt.offset_cnt, opt.limit_cnt);
}

arrow::Result<arrow::compute::ExecBatch> UnorderedFetchNode::ProcessBatch(arrow::compute::ExecBatch batch) {
    // TODO early return
    std::lock_guard<bthread::Mutex> lock(_mutex);
    int64_t batch_offset = 0;
    if (_passed_row_cnt >= _offset_cnt + _limit_cnt) {
        // exceed limit
        return batch.Slice(0, 0);
    }
    if (_passed_row_cnt < _offset_cnt) {
        batch_offset = _offset_cnt - _passed_row_cnt;
    }
    _passed_row_cnt += batch.length;
    if (batch_offset >= batch.length) {
        // core when offset > batch.length, careful
        return batch.Slice(0, 0);
    }
    if (_returned_row_cnt >= _limit_cnt) {
        // exceed limit
        return batch.Slice(0, 0);
    }
    batch = batch.Slice(batch_offset, _limit_cnt - _returned_row_cnt);
    _returned_row_cnt += batch.length;
    return batch;
}

/*
 * AceroExchangeSenderNode
 */
arrow::Result<arrow::acero::ExecNode*> AceroExchangeSenderNode::Make(
                            arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs,
                            const arrow::acero::ExecNodeOptions& options) {
    auto s = arrow::acero::ValidateExecNodeInputs(plan, inputs, 1, "AceroExchangeSenderNode");
    if (!s.ok()) {
        return arrow::Status::TypeError("AceroExchangeSenderNode valid fail");
    }
    const auto& opt = arrow::internal::checked_cast<const AceroExchangeSenderNodeOptions&>(options);
    if (opt.state == nullptr || opt.exchange_sender_node == nullptr) {
        return arrow::Status::TypeError("AceroExchangeSenderNode state or exchange_sender_node is null");
    }
    auto schema = inputs[0]->output_schema();
    if (opt.exchange_sender_node->init_schema(schema) != 0) {
        return arrow::Status::TypeError("exchange_sender_node init_schema fail");
    }
    return plan->EmplaceNode<AceroExchangeSenderNode>(
                    plan, std::move(inputs), std::move(schema), 
                    opt.state, opt.exchange_sender_node, opt.limit_cnt);
}

arrow::Result<arrow::compute::ExecBatch> AceroExchangeSenderNode::ProcessBatch(arrow::compute::ExecBatch batch) {
    // limit处理
    if (_limit_cnt > 0) {
        int slice_cnt = 0;
        {
            std::lock_guard<std::mutex> lock(_mtx);
            int64_t limit = _limit_cnt - _processed_row_cnt;
            if (limit < 0) {
                return arrow::Status::Invalid("Invalid limit");
            }
            slice_cnt = std::min(batch.length, limit);
            _processed_row_cnt += slice_cnt;

        }
        batch = batch.Slice(0, slice_cnt);
    }
    if (batch.length == 0) {
        return batch;
    }
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> record_batch_ret = batch.ToRecordBatch(output_schema_);
    if (!record_batch_ret.ok()) {
        return arrow::Status::ExecutionError("AceroExchangeSenderNode Process ExecBatch to RecordBatch fail");
    }
    std::shared_ptr<arrow::RecordBatch> record_batch = *record_batch_ret;
    int ret = _exchange_sender_node->send_curr_record_batch(_state, record_batch);
    if (ret != 0) {
        return arrow::Status::ExecutionError("AceroExchangeSenderNode send_record_batch fail");
    }
    batch = batch.Slice(0, 0);
    return batch;
}

int AceroExchangeSenderNode::Finish(int& total_output_batch_cnt) {
    auto record_batch_ret = arrow::RecordBatch::MakeEmpty(output_schema_);
    if (!record_batch_ret.ok()) {
        DB_FATAL("RecordBatch::MakeEmpty fail, %s", record_batch_ret.status().ToString().c_str());
        return -1;
    }
    auto record_batch = *record_batch_ret;
    int ret = _exchange_sender_node->send_eof_record_batch(_state, record_batch);
    if (ret != 0) {
        return ret;
    }
    return 0;
}

/*
 * MakeDefaultAggRowWhenNoInputNode 
 * 兼容mysql, 如select min(id), count(id), sum(id) from t1, agg如果没输入数据返回 [null, 0, null])
 */
arrow::Result<arrow::acero::ExecNode*> MakeDefaultAggRowWhenNoInputNode::Make(arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs,
                            const arrow::acero::ExecNodeOptions& options) {
    auto s = arrow::acero::ValidateExecNodeInputs(plan, inputs, 1, "MakeDefaultAggRowWhenNoInputNode");
    if (!s.ok()) {
        return arrow::Status::TypeError("MakeDefaultAggRowWhenNoInputNode valid fail");
    }
    const auto& opt = arrow::internal::checked_cast<const MakeDefaultAggRowWhenNoInputOptions&>(options);
    auto schema = inputs[0]->output_schema();
    return plan->EmplaceNode<MakeDefaultAggRowWhenNoInputNode>(plan, std::move(inputs), std::move(schema), opt.count_names);
}

arrow::Result<arrow::compute::ExecBatch> MakeDefaultAggRowWhenNoInputNode::ProcessBatch(arrow::compute::ExecBatch batch) {
    std::lock_guard<bthread::Mutex> lock(_mutex);
    _passed_row_cnt += batch.length;
    _passed_batch_cnt++;
    return batch;
}

int MakeDefaultAggRowWhenNoInputNode::Finish(int& total_output_batch_cnt) {
    if (_passed_row_cnt > 0) {
        return 0;
    }
    arrow::ExecBatch batch{{}, 1};
    batch.values.resize(output_schema_->num_fields());
    for (int i = 0; i < output_schema_->num_fields(); ++i) {
        auto& f = output_schema_->field(i);
        if (_count_columns.count(f->name()) > 0) {
            auto zero_value = arrow::MakeScalar(f->type(), 0);
            if (!zero_value.ok()) {
                DB_FATAL("MakeDefaultAggRowWhenNoInputNode MakeNullScalar fail, %s", zero_value.status().ToString().c_str());
                return -1;
            }
            batch.values[i] = *zero_value;
        } else {
            batch.values[i] = arrow::MakeNullScalar(f->type());
        }
    }
    auto s = output_->InputReceived(this, batch);
    if (!s.ok()) {
        DB_FATAL("MakeDefaultAggRowWhenNoInputNode output_->InputReceived fail, %s", s.ToString().c_str());
        return -1;
    }
    // 向上游输出的batch数, 与基类里自己input_received()收到的batch数不一样
    total_output_batch_cnt += 1;
    return 0;
}

/*
 * TopKNode (LIKE acero SelectKSinkNodeOptions but support un-sinked)
 */
arrow::Result<arrow::acero::ExecNode*> TopKNode::Make(arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs,
                            const arrow::acero::ExecNodeOptions& options) {
    auto s = arrow::acero::ValidateExecNodeInputs(plan, inputs, 1, "TopKNode");
    if (!s.ok()) {
        return arrow::Status::TypeError("TopKNode valid fail");
    }
    const auto& opt = arrow::internal::checked_cast<const TopKNodeOptions&>(options);
    auto schema = inputs[0]->output_schema();
    return plan->EmplaceNode<TopKNode>(plan, std::move(inputs), std::move(schema), opt);
}


arrow::Status TopKNode::InputReceived(arrow::acero::ExecNode* input, arrow::compute::ExecBatch batch) {
    auto scope = TraceInputReceived(batch);
    if (inputs_.size() > 0) {
        DCHECK_EQ(input, inputs_[0]);
    }
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> record_batch,
                          batch.ToRecordBatch(output_schema_));
    {
      std::lock_guard lk(mutex_);
      accumulation_queue_.push_back(std::move(record_batch));
    }
    if (counter_.Increment()) {
      return DoSort();
    }
    return arrow::Status::OK();
}

arrow::Status TopKNode::DoSort() {
    ARROW_ASSIGN_OR_RAISE(
        auto table,
        arrow::Table::FromRecordBatches(output_schema_, std::move(accumulation_queue_)));
    arrow::compute::ExecContext* ctx = plan_->query_context()->exec_context();
    arrow::compute::SortOptions sort_options(ordering_.sort_keys(), ordering_.null_placement());
    // [TODO]SelectKUnstable目前有bug可能会丢掉null行, 这里先用全排, 但是可以下推limit避免大量数据拷贝
    // arrow::compute::SelectKOptions options(k_, ordering_.sort_keys());
    // ARROW_ASSIGN_OR_RAISE(auto indices, arrow::compute::SelectKUnstable(table, options, ctx));
    ARROW_ASSIGN_OR_RAISE(auto indices, arrow::compute::SortIndices(table, sort_options, ctx));
    if (indices->length() > k_) {
        indices = indices->Slice(0, k_);
    }
    // [TODO]每一列会先merge成一块连续内存, 再take成新的recordbatch
    ARROW_ASSIGN_OR_RAISE(arrow::Datum sorted,
                          arrow::compute::Take(table, indices, arrow::compute::TakeOptions::NoBoundsCheck(), ctx));
    const std::shared_ptr<arrow::Table>& sorted_table = sorted.table();
    arrow::TableBatchReader reader(*sorted_table);
    reader.set_chunksize(arrow::acero::ExecPlan::kMaxBatchSize); // 1 << 15;
    int batch_index = 0;
    while (true) {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> next, reader.Next());
        if (next == nullptr) {
            return output_->InputFinished(this, batch_index);
        }
        int index = batch_index++;
        plan_->query_context()->ScheduleTask(
            [this, batch = std::move(next), index]() mutable {
                arrow::compute::ExecBatch exec_batch(*batch);
                exec_batch.index = index;
                return output_->InputReceived(this, std::move(exec_batch));
            },
            "TopKNode::ProcessBatch");
    }
    return arrow::Status::OK();
}

arrow::Status TopKNode::InputFinished(arrow::acero::ExecNode* input, int total_batches) {
    if (inputs_.size() > 0) {
        DCHECK_EQ(input, inputs_[0]);
    }
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    if (counter_.SetTotal(total_batches)) {
        return DoSort();
    }
    return arrow::Status::OK();
}

/*
 * DebugPrintNode 
 * 用于debug辅助排查问题, 将收到每一个的ExecBatch数据明文打印到单独的文本文件"debug_print_<this>_行数"中
 * 能插入到任何arrow node前后打印出所有数据, 使用方法:
 * arrow::acero::Declaration dec2{"debug_print", arrow::acero::ExecNodeOptions{}};
 * LOCAL_TRACE_ARROW_PLAN(dec2);
 * state->append_acero_declaration(dec2);
 */
arrow::Result<arrow::acero::ExecNode*> DebugPrintNode::Make(arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs,
                            const arrow::acero::ExecNodeOptions& options) {
    auto s = arrow::acero::ValidateExecNodeInputs(plan, inputs, 1, "DebugPrintNode");
    if (!s.ok()) {
        return arrow::Status::TypeError("DebugPrintNode valid fail");
    }
    auto schema = inputs[0]->output_schema();
    return plan->EmplaceNode<DebugPrintNode>(plan, std::move(inputs), std::move(schema));
}

arrow::Result<arrow::compute::ExecBatch> DebugPrintNode::ProcessBatch(arrow::compute::ExecBatch batch) {
    std::lock_guard<bthread::Mutex> lock(_mutex);
    _passed_row_cnt += batch.length;
    DB_WARNING("DebugPrintNode: %p got batch, batch.length: %ld, _passed_row_cnt: %ld, ", this, batch.length, _passed_row_cnt);
    DB_WARNING("DebugPrintNode: %p got batch, schema: %s, data: %s", this, output_schema_->ToString().c_str(), batch.ToString().c_str());
    auto recordbatch = batch.ToRecordBatch(output_schema_);
    auto res = arrow::Table::FromRecordBatches({*recordbatch});
    if (!res.ok()) {
        DB_WARNING("DebugPrintNode table fail, %s", res.status().ToString().c_str());
        return arrow::Status::TypeError("DebugPrintNode FromRecordBatches fail");
    }
    auto table = *res;

    std::ostringstream os;
    os <<  "./debug_print_"<< this << "." << table->num_rows();
    std::ofstream outfile(os.str());

    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns = table->columns();
    for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
        std::string row_str = "";
        for (auto field_idx = 0; field_idx < columns.size(); ++field_idx) {
            ExprValue expr_value = VectorizeHelpper::get_vectorized_value(columns[field_idx].get(), row_idx);
            if (expr_value.is_null()) {
                row_str += "NULL\t";
            } else {
                row_str += expr_value.get_string() + "\t";
            }
        }
        outfile << row_str << std::endl;
    }
    outfile.close(); // 关闭文件
    return batch;
}

int DebugPrintNode::Finish(int& total_output_batch_cnt) {
    std::lock_guard<bthread::Mutex> lock(_mutex);
    DB_WARNING("DebugPrintNode Finish, _passed_row_cnt: %ld, %p, schema: %s", 
            _passed_row_cnt, this, output_schema_->ToString().c_str());
    return 0;
}

/*
/* RegisterAllArrowExecNode
 */
int ArrowExecNodeManager::RegisterAllArrowExecNode() {
    // index collector node for index join
    auto exec_registry = arrow::acero::default_exec_factory_registry();
    auto s = exec_registry->AddFactory("index_collector", IndexCollecterNode::Make);
    if (!s.ok()) {
        return -1;
    }
    // Limit Node for unordered data
    s = exec_registry->AddFactory("limit", UnorderedFetchNode::Make);
    if (!s.ok()) {
        return -1;
    }
    // ExchangeSenderNode
    s = exec_registry->AddFactory("exchange_sender", AceroExchangeSenderNode::Make);
    if (!s.ok()) {
        return -1;
    }
    // MakeDefaultAggRowWhenNoInputNode
    s = exec_registry->AddFactory("make_default_agg_row_when_no_input", MakeDefaultAggRowWhenNoInputNode::Make);
    if (!s.ok()) {
        return -1;
    }
    // DebugPrintNode
    s = exec_registry->AddFactory("debug_print", DebugPrintNode::Make);
    if (!s.ok()) {
        return -1;
    }
    // TopKNode
    s = exec_registry->AddFactory("topk", TopKNode::Make);
    if (!s.ok()) {
        return -1;
    }
    return 0;
}
}  // namespace baikal
/* vim: set ts=4 sw=4 sts=4 tw=100 */
