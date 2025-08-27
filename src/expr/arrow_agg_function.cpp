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

#include "internal_functions.h"
#include "arrow_function.h"
#include <arrow/visit_data_inline.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/array/builder_binary.h>
#include <arrow/compute/registry.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/kernels/codegen_internal.h>
#include <arrow/util/int_util_overflow.h>
#include "slot_ref.h"
#include "agg_fn_call.h"
namespace baikaldb {

// same with hash_aggregate.cpp in arrow/compute
/// C++ abstract base class for the HashAggregateKernel interface.
/// Implementations should be default constructible and perform initialization in
/// Init().
struct GroupedAggregator : arrow::compute::KernelState {
    virtual arrow::Status Init(arrow::compute::ExecContext*, const arrow::compute::KernelInitArgs& args) = 0;

    virtual arrow::Status Resize(int64_t new_num_groups) = 0;

    virtual arrow::Status Consume(const arrow::compute::ExecSpan& batch) = 0;

    virtual arrow::Status Merge(GroupedAggregator&& other, const arrow::ArrayData& group_id_mapping) = 0;

    virtual arrow::Result<arrow::Datum> Finalize() = 0;

    virtual std::shared_ptr<arrow::DataType> out_type() const = 0;
};

template <typename Impl>
arrow::Result<std::unique_ptr<arrow::compute::KernelState>> HashAggregateInit(arrow::compute::KernelContext* ctx,
                                                       const arrow::compute::KernelInitArgs& args) {
    auto impl = std::make_unique<Impl>();
    RETURN_NOT_OK(impl->Init(ctx->exec_context(), args));
    return std::move(impl);
}

arrow::Status HashAggregateResize(arrow::compute::KernelContext* ctx, int64_t num_groups) {
    return arrow::internal::checked_cast<GroupedAggregator*>(ctx->state())->Resize(num_groups);
}
arrow::Status HashAggregateConsume(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch) {
    return arrow::internal::checked_cast<GroupedAggregator*>(ctx->state())->Consume(batch);
}
arrow::Status HashAggregateMerge(arrow::compute::KernelContext* ctx, arrow::compute::KernelState&& other,
                          const arrow::ArrayData& group_id_mapping) {
    return arrow::internal::checked_cast<GroupedAggregator*>(ctx->state())
        ->Merge(arrow::internal::checked_cast<GroupedAggregator&&>(other), group_id_mapping);
}
arrow::Status HashAggregateFinalize(arrow::compute::KernelContext* ctx, arrow::Datum* out) {
    return arrow::internal::checked_cast<GroupedAggregator*>(ctx->state())->Finalize().Value(out);
}

arrow::Result<arrow::TypeHolder> ResolveGroupOutputType(arrow::compute::KernelContext* ctx,
                                          const std::vector<arrow::TypeHolder>&) {
    return arrow::internal::checked_cast<GroupedAggregator*>(ctx->state())->out_type();
}

arrow::compute::HashAggregateKernel MakeKernel(std::shared_ptr<arrow::compute::KernelSignature> signature,
                               arrow::compute::KernelInit init, const bool ordered = false) {
    arrow::compute::HashAggregateKernel kernel(std::move(signature), std::move(init), 
                        HashAggregateResize,
                        HashAggregateConsume, 
                        HashAggregateMerge,
                        HashAggregateFinalize, 
                        ordered);
    return kernel;
}

arrow::compute::HashAggregateKernel MakeKernel(arrow::compute::InputType argument_type, arrow::compute::KernelInit init,
                               const bool ordered = false) {
    return MakeKernel(
        arrow::compute::KernelSignature::Make({std::move(argument_type), arrow::compute::InputType(arrow::Type::UINT32)},
                        arrow::compute::OutputType(ResolveGroupOutputType)),
                        std::move(init), 
                        ordered);
}

arrow::compute::HashAggregateKernel MakeUnaryKernel(arrow::compute::KernelInit init) {
    return MakeKernel(arrow::compute::KernelSignature::Make({arrow::compute::InputType(arrow::Type::UINT32)},
                        arrow::compute::OutputType(ResolveGroupOutputType)),
                        std::move(init));
}

arrow::Status AddHashAggKernels(
        const std::vector<std::shared_ptr<arrow::DataType>>& types,
        arrow::Result<arrow::compute::HashAggregateKernel> make_kernel(const std::shared_ptr<arrow::DataType>&),
        arrow::compute::HashAggregateFunction* function) {
    for (const auto& ty : types) {
        ARROW_ASSIGN_OR_RAISE(auto kernel, make_kernel(ty));
        RETURN_NOT_OK(function->AddKernel(std::move(kernel)));
    }
    return arrow::Status::OK();
}

template <typename T>
struct GroupedValueTraits {
  using CType = typename arrow::TypeTraits<T>::CType;

  static CType Get(const CType* values, uint32_t g) { return values[g]; }
  static void Set(CType* values, uint32_t g, CType v) { values[g] = v; }
  static arrow::Status AppendBuffers(arrow::TypedBufferBuilder<CType>* destination,
                              const uint8_t* values, int64_t offset, int64_t num_values) {
    RETURN_NOT_OK(
        destination->Append(reinterpret_cast<const CType*>(values) + offset, num_values));
    return arrow::Status::OK();
  }
};

template <>
struct GroupedValueTraits<arrow::BooleanType> {
  static bool Get(const uint8_t* values, uint32_t g) {
    return arrow::bit_util::GetBit(values, g);
  }
  static void Set(uint8_t* values, uint32_t g, bool v) {
    arrow::bit_util::SetBitTo(values, g, v);
  }
  static arrow::Status AppendBuffers(arrow::TypedBufferBuilder<bool>* destination,
                              const uint8_t* values, int64_t offset, int64_t num_values) {
    RETURN_NOT_OK(destination->Reserve(num_values));
    destination->UnsafeAppend(values, offset, num_values);
    return arrow::Status::OK();
  }
};

template <typename Type, typename ConsumeValue, typename ConsumeNull>
typename arrow::internal::call_traits::enable_if_return<ConsumeValue, arrow::Status>::type
VisitGroupedValues(const arrow::compute::ExecSpan& batch, ConsumeValue&& valid_func,
                ConsumeNull&& null_func) {
    auto g = batch[1].array.GetValues<uint32_t>(1);
    if (batch[0].is_array()) {
        return arrow::compute::internal::VisitArrayValuesInline<Type>(
            batch[0].array,
            [&](typename arrow::compute::internal::GetViewType<Type>::T val) { return valid_func(*g++, val); },
            [&]() { return null_func(*g++); });
    }
    const arrow::Scalar& input = *batch[0].scalar;
    if (input.is_valid) {
        const auto val = arrow::compute::internal::UnboxScalar<Type>::Unbox(input);
        for (int64_t i = 0; i < batch.length; i++) {
            RETURN_NOT_OK(valid_func(*g++, val));
        }
    } else {
        for (int64_t i = 0; i < batch.length; i++) {
            RETURN_NOT_OK(null_func(*g++));
        }
    }
    return arrow::Status::OK();
}
// ----------------------------------------------------------------------
// Helpers for more easily implementing hash aggregates

/*
 * AVG intermediate to string
 */
template <typename Type, typename Enable = void>
struct AvgIntermediateImpl final : public GroupedAggregator {
    // using GetSet = GroupedValueTraits<Type>;

    arrow::Status Init(arrow::compute::ExecContext* ctx, const arrow::compute::KernelInitArgs&) override {
        ctx_ = ctx;
        null_bitmap_ = arrow::TypedBufferBuilder<bool>(ctx->memory_pool());
        return arrow::Status::OK();
    }

    arrow::Status Resize(int64_t new_num_groups) override {
        auto added_groups = new_num_groups - num_groups_;
        DCHECK_GE(added_groups, 0);
        num_groups_ = new_num_groups;
        avgs_.resize(new_num_groups);
        RETURN_NOT_OK(null_bitmap_.Append(added_groups, false));
        return arrow::Status::OK();
    }
    
    arrow::Status Consume(const arrow::compute::ExecSpan& batch) override {
        if constexpr (std::is_same_v<Type, arrow::LargeBinaryType>) {
            return VisitGroupedValues<Type>(
                batch,
                [&](uint32_t g, std::string_view val) -> arrow::Status {
                    AvgIntermediate* other = (AvgIntermediate*)val.data();
                    if (other->count == 0) {
                        return arrow::Status::OK();
                    }
                    avgs_[g].sum += other->sum;
                    avgs_[g].count += other->count;
                    arrow::bit_util::SetBit(null_bitmap_.mutable_data(), g);
                    return arrow::Status::OK();
                },
            [&](uint32_t g) -> arrow::Status { return arrow::Status::OK(); });
        } else {
            using CType = typename arrow::TypeTraits<Type>::CType;
            return VisitGroupedValues<Type>(
                batch,
                [&](uint32_t g, CType val) -> arrow::Status {
                    avgs_[g].sum += val;
                    avgs_[g].count += 1;
                    arrow::bit_util::SetBit(null_bitmap_.mutable_data(), g);
                    return arrow::Status::OK();
                },
                [&](uint32_t g) -> arrow::Status { return arrow::Status::OK(); });
        }
    }

    arrow::Status Merge(GroupedAggregator&& raw_other, const arrow::ArrayData& group_id_mapping) override {
        auto other = arrow::internal::checked_cast<AvgIntermediateImpl<Type>*>(&raw_other);
        auto g = group_id_mapping.GetValues<uint32_t>(1);
        for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length; ++other_g, ++g) {
            if (other->avgs_[other_g].count == 0) { // null
                continue;
            }
            avgs_[*g].sum += other->avgs_[other_g].sum;
            avgs_[*g].count += other->avgs_[other_g].count;
            arrow::bit_util::SetBit(null_bitmap_.mutable_data(), *g);
        }
        return arrow::Status::OK();
    }

    arrow::Result<arrow::Datum> Finalize() override {
        using offset_type = int64_t;
        ARROW_ASSIGN_OR_RAISE(auto nulls, null_bitmap_.Finish());
        auto avgs_out = arrow::ArrayData::Make(out_type(), num_groups_, {std::move(nulls), nullptr});
        ARROW_ASSIGN_OR_RAISE(
            auto raw_offsets,
            AllocateBuffer((1 + avgs_.size()) * sizeof(offset_type), ctx_->memory_pool()));
        auto* offsets = reinterpret_cast<offset_type*>(raw_offsets->mutable_data());
        offsets[0] = 0;
        offsets++;
        const uint8_t* null_bitmap = avgs_out->buffers[0]->data();
        offset_type total_length = 0;
        for (size_t i = 0; i < avgs_.size(); i++) {
            if (arrow::bit_util::GetBit(null_bitmap, i)) {
                if (arrow::internal::AddWithOverflow(
                        total_length, static_cast<offset_type>(sizeof(AvgIntermediate)), &total_length)) {
                    return arrow::Status::Invalid("Result is too large to fit in ", *avgs_out->type,
                                            " cast to large_ variant of type");
                }
            }
            offsets[i] = total_length;
        }
        ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
        int64_t offset = 0;
        for (size_t i = 0; i < avgs_.size(); i++) {
            if (arrow::bit_util::GetBit(null_bitmap, i)) {
                const AvgIntermediate& value = avgs_[i];
                std::memcpy(data->mutable_data() + offset, &value, sizeof(AvgIntermediate));
                offset += sizeof(AvgIntermediate);
            }
        }
        avgs_out->buffers[1] = std::move(raw_offsets);
        avgs_out->buffers.push_back(std::move(data));
        return avgs_out;
    }
    
    std::shared_ptr<arrow::DataType> out_type() const override { 
        return arrow::large_binary();
    }

    int64_t num_groups_ = 0;
    std::vector<AvgIntermediate> avgs_;
    arrow::TypedBufferBuilder<bool> null_bitmap_;
    arrow::compute::ExecContext* ctx_;
};

template <typename T>
arrow::Result<std::unique_ptr<arrow::compute::KernelState>> GroupedAvgIntermidateInit(arrow::compute::KernelContext* ctx,
                                                    const arrow::compute::KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<AvgIntermediateImpl<T>>(ctx, args));
  return std::move(impl);
}

struct GroupedAvgIntermediateFactory {
    template <typename T>
    arrow::enable_if_integer<T, arrow::Status> Visit(const T&) {
        using PhysicalType = typename T::PhysicalType;
        kernel = MakeKernel(std::move(argument_type), GroupedAvgIntermidateInit<T>);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::FloatType&) {
        kernel = MakeKernel(std::move(argument_type), GroupedAvgIntermidateInit<arrow::FloatType>);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::DoubleType&) {
        kernel = MakeKernel(std::move(argument_type), GroupedAvgIntermidateInit<arrow::DoubleType>);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::BooleanType&) {
        kernel = MakeKernel(std::move(argument_type), GroupedAvgIntermidateInit<arrow::BooleanType>);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::LargeBinaryType&) {
        kernel = MakeKernel(std::move(argument_type), GroupedAvgIntermidateInit<arrow::LargeBinaryType>);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::DataType& type) {
        return arrow::Status::NotImplemented("Computing avg of data of type ", type);
    }

    static arrow::Result<arrow::compute::HashAggregateKernel> Make(const std::shared_ptr<arrow::DataType>& type) {
        GroupedAvgIntermediateFactory factory;
        factory.argument_type = type->id();
        RETURN_NOT_OK(VisitTypeInline(*type, &factory));
        return factory.kernel;
    }

    arrow::compute::HashAggregateKernel kernel;
    arrow::compute::InputType argument_type;
};

template <typename Type>
struct AvgFinalizeImpl final : public GroupedAggregator {
    arrow::Status Init(arrow::compute::ExecContext* ctx, const arrow::compute::KernelInitArgs&) override {
        ctx_ = ctx;
        null_bitmap_ = arrow::TypedBufferBuilder<bool>(ctx->memory_pool());
        return arrow::Status::OK();
    }

    arrow::Status Resize(int64_t new_num_groups) override {
        auto added_groups = new_num_groups - num_groups_;
        DCHECK_GE(added_groups, 0);
        num_groups_ = new_num_groups;
        avgs_.resize(new_num_groups);
        RETURN_NOT_OK(null_bitmap_.Append(added_groups, false));
        return arrow::Status::OK();
    }
    
    arrow::Status Consume(const arrow::compute::ExecSpan& batch) override {
        return VisitGroupedValues<Type>(
            batch,
            [&](uint32_t g, std::string_view val) -> arrow::Status {
                AvgIntermediate* other = (AvgIntermediate*)val.data();
                if (other->count == 0) {
                    return arrow::Status::OK();
                }
                avgs_[g].sum += other->sum;
                avgs_[g].count += other->count;
                arrow::bit_util::SetBit(null_bitmap_.mutable_data(), g);
                return arrow::Status::OK();
            },
            [&](uint32_t g) -> arrow::Status { return arrow::Status::OK(); });
    }

    arrow::Status Merge(GroupedAggregator&& raw_other, const arrow::ArrayData& group_id_mapping) override {
        auto other = arrow::internal::checked_cast<AvgFinalizeImpl<Type>*>(&raw_other);
        auto g = group_id_mapping.GetValues<uint32_t>(1);
        for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length; ++other_g, ++g) {
            if (other->avgs_[other_g].count == 0) {  // null
                continue;
            }
            avgs_[*g].sum += other->avgs_[other_g].sum;
            avgs_[*g].count += other->avgs_[other_g].count;
            arrow::bit_util::SetBit(null_bitmap_.mutable_data(), *g);
        }
        return arrow::Status::OK();
    }

    arrow::Result<arrow::Datum> Finalize() override {
        ARROW_ASSIGN_OR_RAISE(auto nulls, null_bitmap_.Finish());
        auto avgs_out = arrow::ArrayData::Make(out_type(), num_groups_, {std::move(nulls), nullptr});
        const uint8_t* null_bitmap = avgs_out->buffers[0]->data();
        ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(num_groups_ * sizeof(double), ctx_->memory_pool()));
        double* double_avgs = reinterpret_cast<double*>(data->mutable_data());
        for (size_t i = 0; i < avgs_.size(); i++) {
            if (arrow::bit_util::GetBit(null_bitmap, i)) {
                AvgIntermediate* val = &avgs_[i];
                if (val->count > 0) {
                    double_avgs[i] = val->sum / val->count;
                }
            }
        }
        avgs_out->buffers[1] = std::move(data);
        return avgs_out;
    }
    
    std::shared_ptr<arrow::DataType> out_type() const override { 
        return arrow::float64(); 
    }

    int64_t num_groups_ = 0;
    std::vector<AvgIntermediate> avgs_;
    arrow::TypedBufferBuilder<bool> null_bitmap_;
    arrow::compute::ExecContext* ctx_;
};

template <typename T>
arrow::Result<std::unique_ptr<arrow::compute::KernelState>> GroupedAvgFinalizeInit(arrow::compute::KernelContext* ctx,
                                                    const arrow::compute::KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<AvgFinalizeImpl<T>>(ctx, args));
  return std::move(impl);
}

struct GroupedAvgFinalizeFactory {
    template <typename T>
    arrow::enable_if_base_binary<T, arrow::Status> Visit(const T&) {
        kernel = MakeKernel(std::move(argument_type), GroupedAvgFinalizeInit<T>);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::DataType& type) {
        return arrow::Status::NotImplemented("Computing avg of data of type ", type);
    }

    static arrow::Result<arrow::compute::HashAggregateKernel> Make(const std::shared_ptr<arrow::DataType>& type) {
        GroupedAvgFinalizeFactory factory;
        factory.argument_type = type->id();
        RETURN_NOT_OK(VisitTypeInline(*type, &factory));
        return factory.kernel;
    }

    arrow::compute::HashAggregateKernel kernel;
    arrow::compute::InputType argument_type;
};

arrow::Status ArrowFunctionManager::RegisterAllHashAggFunction() {
    auto registry = arrow::compute::GetFunctionRegistry();
    {
        // avg原始值(整型+浮点数)聚合成中间结果string
        auto func = std::make_shared<arrow::compute::HashAggregateFunction>("hash_avg_intermediate", arrow::compute::Arity::Binary(), 
            /*doc=*/arrow::compute::FunctionDoc::Empty());
        DCHECK_OK(AddHashAggKernels({arrow::boolean()}, GroupedAvgIntermediateFactory::Make, func.get()));
        DCHECK_OK(AddHashAggKernels(arrow::SignedIntTypes(), GroupedAvgIntermediateFactory::Make, func.get()));
        DCHECK_OK(AddHashAggKernels(arrow::UnsignedIntTypes(), GroupedAvgIntermediateFactory::Make, func.get()));
        DCHECK_OK(AddHashAggKernels({arrow::float32(), arrow::float64()}, GroupedAvgIntermediateFactory::Make, func.get()));
        DCHECK_OK(AddHashAggKernels({arrow::large_binary()}, GroupedAvgIntermediateFactory::Make, func.get()));
        DCHECK_OK(registry->AddFunction(std::move(func)));
    }
    {
        // avg中间结果聚合成double
        auto func = std::make_shared<arrow::compute::HashAggregateFunction>("hash_avg_finalize", arrow::compute::Arity::Binary(), 
            /*doc=*/arrow::compute::FunctionDoc::Empty());
        DCHECK_OK(AddHashAggKernels(arrow::BaseBinaryTypes(), GroupedAvgFinalizeFactory::Make, func.get())); // 一阶段agg产出中间结果
        DCHECK_OK(registry->AddFunction(std::move(func)));
    }
    return arrow::Status::OK();
}
}