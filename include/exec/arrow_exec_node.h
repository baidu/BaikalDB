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
#include "acero_base_node.h"
#include "arrow/acero/exec_plan.h"
#include "arrow_exec_node_manager.h"
#include "join_node.h"
#include "common.h"

namespace baikaldb {
class IndexCollecterNode : public AceroBaseNode {
public:
    IndexCollecterNode(arrow::acero::ExecPlan* plan, std::vector<arrow::acero::ExecNode*> inputs, std::shared_ptr<arrow::Schema> output_schema, 
            RuntimeState* s, JoinNode* join_node, int64_t limit = -1)
      : AceroBaseNode(plan, std::move(inputs), output_schema),
        _state(s), _join_node(join_node), _limit_cnt(limit) {}

    ~IndexCollecterNode() {}

    static arrow::Result<arrow::acero::ExecNode*> Make(arrow::acero::ExecPlan* plan, std::vector<arrow::acero::ExecNode*> inputs,
                                const arrow::acero::ExecNodeOptions& options);

    const char* kind_name() const override { return "IndexCollecterNode"; }

    arrow::Result<arrow::compute::ExecBatch> ProcessBatch(arrow::compute::ExecBatch batch) override;

    int Finish(int& total_output_batch_count) override;

    std::string ToStringExtra(int indent = 0) const override {
        return "IndexCollectorNode, limit=" + std::to_string(_limit_cnt);
    }
protected:
    arrow::Status StopProducingImpl() override;
    int notify_all();

private:
    RuntimeState* _state = nullptr;
    JoinNode* _join_node = nullptr;
    int64_t _limit_cnt = -1;
    int64_t _passed_row_cnt = 0;
    bthread::Mutex _mutex;
    bool _transfer_to_no_index_join = false;
};

// https://github.com/apache/arrow/issues/34941 
class UnorderedFetchNode : public AceroBaseNode {
public:
    UnorderedFetchNode(arrow::acero::ExecPlan* plan, std::vector<arrow::acero::ExecNode*> inputs, std::shared_ptr<arrow::Schema> output_schema, 
            int64_t offset, int64_t limit = -1)
      : AceroBaseNode(plan, std::move(inputs), output_schema), _offset_cnt(offset), _limit_cnt(limit) {}

    ~UnorderedFetchNode() {}

    static arrow::Result<arrow::acero::ExecNode*> Make(arrow::acero::ExecPlan* plan, std::vector<arrow::acero::ExecNode*> inputs,
                                const arrow::acero::ExecNodeOptions& options);

    const char* kind_name() const override { return "UnorderedFetchNode"; }

    arrow::Result<arrow::compute::ExecBatch> ProcessBatch(arrow::compute::ExecBatch batch) override;

    int Finish(int& total_output_batch_count) override {
        return 0;
    }

    std::string ToStringExtra(int indent = 0) const override {
        return "UnorderedFetchNode, offset=" + std::to_string(_offset_cnt) + "  limit=" + std::to_string(_limit_cnt);
    }
private:
    int64_t _offset_cnt = -1;
    int64_t _limit_cnt = -1;
    int64_t _passed_row_cnt = 0;
    int64_t _returned_row_cnt = 0;
    bthread::Mutex _mutex;
};

class AceroExchangeSenderNode : public AceroBaseNode {
public:
    AceroExchangeSenderNode(arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs, 
                            std::shared_ptr<arrow::Schema> output_schema,
                            RuntimeState* state,
                            ExchangeSenderNode* exchange_sender_node,
                            int64_t limit)
      : AceroBaseNode(plan, std::move(inputs), output_schema)
      , _state(state)
      , _exchange_sender_node(exchange_sender_node)
      , _limit_cnt(limit) {}
    ~AceroExchangeSenderNode() {}

    static arrow::Result<arrow::acero::ExecNode*> Make(
                            arrow::acero::ExecPlan* plan, 
                            std::vector<arrow::acero::ExecNode*> inputs,
                            const arrow::acero::ExecNodeOptions& options);
    arrow::Result<arrow::compute::ExecBatch> ProcessBatch(arrow::compute::ExecBatch batch) override;
    int Finish(int& total_output_batch_count) override;
    const char* kind_name() const override { 
        return "AceroExchangeSenderNode"; 
    }
    std::string ToStringExtra(int indent = 0) const override {
        return "AceroExchangeSenderNode";
    }

private:
    RuntimeState* _state = nullptr;
    ExchangeSenderNode* _exchange_sender_node = nullptr;
    int64_t _limit_cnt = -1;
    std::mutex _mtx;
    int64_t _processed_row_cnt = 0;
};

class MakeDefaultAggRowWhenNoInputNode : public AceroBaseNode {
public:
    MakeDefaultAggRowWhenNoInputNode(arrow::acero::ExecPlan* plan, 
                                    std::vector<arrow::acero::ExecNode*> inputs, 
                                    std::shared_ptr<arrow::Schema> output_schema, 
                                    const std::unordered_set<std::string>& count_columns)
      : AceroBaseNode(plan, std::move(inputs), output_schema), _count_columns(count_columns) {}

    ~MakeDefaultAggRowWhenNoInputNode() {}

    static arrow::Result<arrow::acero::ExecNode*> Make(arrow::acero::ExecPlan* plan, 
                                    std::vector<arrow::acero::ExecNode*> inputs,
                                    const arrow::acero::ExecNodeOptions& options);

    const char* kind_name() const override { return "MakeDefaultAggRowWhenNoInputNode"; }

    arrow::Result<arrow::compute::ExecBatch> ProcessBatch(arrow::compute::ExecBatch batch) override;

    int Finish(int& total_output_batch_count) override;

    std::string ToStringExtra(int indent = 0) const override {
        return "MakeDefaultAggRowWhenNoInputNode";
    }
private:
    int64_t _passed_batch_cnt = 0;
    int64_t _passed_row_cnt = 0;
    std::unordered_set<std::string> _count_columns;
    bthread::Mutex _mutex;
};

// acero里的selectK是sinknode，在这里实现一个非sinknode的SelectKNode
class TopKNode : public arrow::acero::ExecNode, public arrow::acero::TracedNode {
public:
    TopKNode(arrow::acero::ExecPlan* plan, 
            std::vector<arrow::acero::ExecNode*> inputs, 
            std::shared_ptr<arrow::Schema> output_schema,
            const TopKNodeOptions& options)
      : arrow::acero::ExecNode(plan, std::move(inputs), /*input_labels=*/{"input"}, std::move(output_schema)),
        arrow::acero::TracedNode(this), 
        ordering_(options.ordering), k_(options.k) {}

    ~TopKNode() {}

    static arrow::Result<arrow::acero::ExecNode*> Make(arrow::acero::ExecPlan* plan, 
            std::vector<arrow::acero::ExecNode*> inputs,
            const arrow::acero::ExecNodeOptions& options);

    const arrow::compute::Ordering& ordering() const override { return ordering_; }

    const char* kind_name() const override { return "TopKNode"; }

    arrow::Status InputFinished(arrow::acero::ExecNode* input, int total_batches) override;

    arrow::Status InputReceived(arrow::acero::ExecNode* input, arrow::compute::ExecBatch batch) override;

    arrow::Status DoSort();

    arrow::Status StartProducing() override {
        NoteStartProducing(ToStringExtra());
        return arrow::Status::OK();
    }

    void PauseProducing(arrow::acero::ExecNode* output, int32_t counter) override {
        if (inputs_.size() > 0) {
            inputs_[0]->PauseProducing(this, counter);
        }
    }

    void ResumeProducing(arrow::acero::ExecNode* output, int32_t counter) override {
        if (inputs_.size() > 0) {
            inputs_[0]->ResumeProducing(this, counter);
        }
    }

    arrow::Status StopProducingImpl() override { return arrow::Status::OK(); }
protected:
    std::string ToStringExtra(int indent = 0) const override { return "TopKNode"; }

private:
    arrow::compute::Ordering ordering_;
    int64_t k_ = -1;
    std::vector<std::shared_ptr<arrow::RecordBatch>> accumulation_queue_;
    std::mutex mutex_;
    arrow::acero::AtomicCounter counter_;
};

class DebugPrintNode : public AceroBaseNode {
public:
    DebugPrintNode(arrow::acero::ExecPlan* plan, 
                    std::vector<arrow::acero::ExecNode*> inputs, 
                    std::shared_ptr<arrow::Schema> output_schema)
      : AceroBaseNode(plan, std::move(inputs), output_schema) {}

    ~DebugPrintNode() {}

    static arrow::Result<arrow::acero::ExecNode*> Make(arrow::acero::ExecPlan* plan, 
                                    std::vector<arrow::acero::ExecNode*> inputs,
                                    const arrow::acero::ExecNodeOptions& options);

    const char* kind_name() const override { return "DebugPrintNode"; }

    arrow::Result<arrow::compute::ExecBatch> ProcessBatch(arrow::compute::ExecBatch batch) override;

    int Finish(int& total_output_batch_count) override;

    std::string ToStringExtra(int indent = 0) const override {
        return "DebugPrintNode";
    }
private:
    int64_t _passed_row_cnt = 0;
    bthread::Mutex _mutex;
};

}  // namespace baikal
/* vim: set ts=4 sw=4 sts=4 tw=100 */
