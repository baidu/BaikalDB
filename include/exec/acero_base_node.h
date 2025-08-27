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

#include <string>
#include <map>
#include <unordered_map>
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/util.h"
#include "arrow/acero/accumulation_queue.h"

namespace baikaldb {
// 类似map_node, 处理Finish报错
// 集成acero自定义节点可以直接集成这个类, input schemah和output schema一致
// 只需要关注ProcessBatch\Finish三个函数自定义实现即可
class AceroBaseNode : public arrow::acero::ExecNode, 
                      public arrow::acero::TracedNode, 
                      public arrow::acero::util::SequencingQueue::Processor {   
public:
    AceroBaseNode(arrow::acero::ExecPlan* plan, std::vector<arrow::acero::ExecNode*> inputs, std::shared_ptr<arrow::Schema> output_schema);

    const arrow::compute::Ordering& ordering() const override;

    arrow::Status StartProducing() override;

    void PauseProducing(arrow::acero::ExecNode* output, int32_t counter) override;

    void ResumeProducing(arrow::acero::ExecNode* output, int32_t counter) override;

    arrow::Status InputReceived(arrow::acero::ExecNode* input, arrow::compute::ExecBatch batch) override;

    arrow::Status InputFinished(arrow::acero::ExecNode* input, int total_batches) override;

    // for ordered input
    arrow::Result<std::optional<arrow::acero::util::SequencingQueue::Task>> Process(arrow::ExecBatch batch) override;

    void Schedule(arrow::acero::util::SequencingQueue::Task task) override;
protected:
    arrow::Status StopProducingImpl() override;

    /// Transform a batch
    ///
    /// The output batch will have the same guarantee as the input batch
    /// If this was the last batch this call may trigger Finish()
    // 可能会并发
    virtual arrow::Result<arrow::compute::ExecBatch> ProcessBatch(arrow::compute::ExecBatch batch) = 0;

    /// Function called after all data has been received
    ///
    /// By default this does nothing.  Override this to provide a custom implementation.
    virtual int Finish(int& total_output_batch_count);

protected:
    // Counter for the number of batches received
    arrow::acero::AtomicCounter input_counter_;

    // only for ordered input
    bool finished_ = false;
    int32_t out_batch_count_ = 0;
    std::unique_ptr<arrow::acero::util::SequencingQueue> sequencing_queue_;
};
}  // namespace baikal
/* vim: set ts=4 sw=4 sts=4 tw=100 */
