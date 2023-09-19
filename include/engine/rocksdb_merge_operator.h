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
#include "rocksdb/merge_operator.h"
#include "schema_factory.h"
#include "mut_table_key.h"
#include "table_key.h"
#include "table_record.h"

namespace baikaldb {
class OLAPMergeOperator : public rocksdb::MergeOperator {
public:
    bool FullMergeV2(const rocksdb::MergeOperator::MergeOperationInput& merge_in,
                   rocksdb::MergeOperator::MergeOperationOutput* merge_out) const override;

    bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
                    const rocksdb::Slice& right_operand, std::string* new_value,
                    rocksdb::Logger* /*logger*/) const override {
        return false;
    }
    const char* Name() const override { return "OLAPMergeOperator"; }
};
}  // namespace baikaldb