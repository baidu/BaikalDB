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
#include "acero_base_node.h"
#include "arrow/compute/exec.h"
#include "arrow/acero/query_context.h"

namespace baikaldb {
AceroBaseNode::AceroBaseNode(arrow::acero::ExecPlan* plan, std::vector<arrow::acero::ExecNode*> inputs,
                 std::shared_ptr<arrow::Schema> output_schema)
    : arrow::acero::ExecNode(plan, std::move(inputs), /*input_labels=*/{"target"}, std::move(output_schema)),
      arrow::acero::TracedNode(this), 
      sequencing_queue_(arrow::acero::util::SequencingQueue::Make(this)) {}

arrow::Status AceroBaseNode::InputFinished(arrow::acero::ExecNode* input, int total_batches) {
    DCHECK_EQ(input, inputs_[0]);
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    if (input_counter_.SetTotal(total_batches)) {
        if (!finished_) {
            finished_ = true;
            int output_received_batch_count = 0;
            if (ordering().is_unordered()) {
                output_received_batch_count = total_batches;
            } else {
                output_received_batch_count = out_batch_count_;
            }
            if (0 != this->Finish(output_received_batch_count)) {
                return arrow::Status::ExecutionError("arrow execnode Finish failed");
            }
            ARROW_RETURN_NOT_OK(inputs_[0]->StopProducing());
            ARROW_RETURN_NOT_OK(output_->InputFinished(this, output_received_batch_count));
        }
    }
    return arrow::Status::OK();
}

// Right now this assumes the map operation will always maintain ordering.  This
// may change in the future but is true for the current map nodes (filter/project)
const arrow::compute::Ordering& AceroBaseNode::ordering() const { return inputs_[0]->ordering(); }

arrow::Status AceroBaseNode::StartProducing() {
    NoteStartProducing(ToStringExtra());
    return arrow::Status::OK();
}

void AceroBaseNode::PauseProducing(arrow::acero::ExecNode* output, int32_t counter) {
    if (inputs_.size() > 0) {
        inputs_[0]->PauseProducing(this, counter);
    }
}

void AceroBaseNode::ResumeProducing(arrow::acero::ExecNode* output, int32_t counter) {
    if (inputs_.size() > 0) {
        inputs_[0]->ResumeProducing(this, counter);
    }
}

arrow::Status AceroBaseNode::StopProducingImpl() { return arrow::Status::OK(); }

arrow::Status AceroBaseNode::InputReceived(arrow::acero::ExecNode* input, arrow::compute::ExecBatch batch) {
    auto scope = TraceInputReceived(batch);
    DCHECK_EQ(input, inputs_[0]);
    if (!ordering().is_unordered()) {
        // 处理有序数据
        return sequencing_queue_->InsertBatch(std::move(batch));
    }

    // 处理无序数据
    arrow::compute::Expression guarantee = batch.guarantee;
    int64_t index = batch.index;
    ARROW_ASSIGN_OR_RAISE(auto output_batch, ProcessBatch(std::move(batch)));
    output_batch.guarantee = guarantee;
    output_batch.index = index;
    ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(output_batch)));
    if (input_counter_.Increment()) {
        // Increment() return true if the counter equel to total batches
        finished_ = true;
        std::optional<int> total_cnt = input_counter_.total();
        if (!total_cnt.has_value()) {
            return arrow::Status::ExecutionError("invalid input counter total count");
        }
        int output_received_batch_count = total_cnt.value();
        if (0 != this->Finish(output_received_batch_count)) {
            return arrow::Status::ExecutionError("arrow execnode Finish failed");
        }
        ARROW_RETURN_NOT_OK(output_->InputFinished(this, output_received_batch_count));
    }
    return arrow::Status::OK();
}

int AceroBaseNode::Finish(int& total_output_batch_count) {
    return 0;
}

arrow::Result<std::optional<arrow::acero::util::SequencingQueue::Task>> AceroBaseNode::Process(arrow::ExecBatch batch) {
    if (finished_) {
        return std::nullopt;
    }
    std::optional<arrow::acero::util::SequencingQueue::Task> task_or_none;
    arrow::compute::Expression guarantee = batch.guarantee;
    ARROW_ASSIGN_OR_RAISE(auto output_batch, ProcessBatch(std::move(batch)));
    int new_index = out_batch_count_++;
    task_or_none = [this, new_index, batch = std::move(output_batch)]() mutable {
        batch.index = new_index;
        return output_->InputReceived(this, std::move(batch));
    };
    if (input_counter_.Increment()) {
        // Increment() return true if the counter equel to total batches
        finished_ = true;
        int output_received_batch_count = out_batch_count_;
        if (0 != this->Finish(output_received_batch_count)) {
            return arrow::Status::ExecutionError("arrow execnode Finish failed");
        }
        ARROW_RETURN_NOT_OK(inputs_[0]->StopProducing());
        ARROW_RETURN_NOT_OK(output_->InputFinished(this, output_received_batch_count));
    }
    return task_or_none;
}

void AceroBaseNode::Schedule(arrow::acero::util::SequencingQueue::Task task) {
    plan_->query_context()->ScheduleTask(std::move(task), "AceroBaseNode::ProcessBatch");
}
}  // namespace baikal
/* vim: set ts=4 sw=4 sts=4 tw=100 */
