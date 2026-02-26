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

#include <gtest/gtest.h>
#include "parquet_writer.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
void build_arrow_record_batch(std::shared_ptr<arrow::RecordBatch> record_batch) {
    // 定义Schema
    auto schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("score", arrow::float32())
    });

    // 创建ArrayBuilder
    arrow::Int32Builder id_builder;
    arrow::FloatBuilder score_builder;

    // 添加数据到ArrayBuilder
    id_builder.Append(1);
    id_builder.Append(2);
    id_builder.Append(3);

    score_builder.Append(4.5);
    score_builder.Append(8.9);
    score_builder.Append(7.2);

    // 从ArrayBuilder创建Array
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> score_array;
    id_builder.Finish(&id_array);
    score_builder.Finish(&score_array);

    // 将Array包装成ArrayData
    std::vector<std::shared_ptr<arrow::ArrayData>> arrays = {
        id_array->data(),
        score_array->data()
    };

    // 创建RecordBatch
    record_batch = arrow::RecordBatch::Make(schema, 3, arrays);
}


TEST(test_arrow_vector_execute, case_all) {
    // std::shared_ptr<arrow::RecordBatch> record_batch;
    // build_arrow_record_batch(record_batch);
    
    // ParquetWriteOptions options;
    // ParquetWriter writer;
    // writer.init(options);
    // writer.write_batch(record_batch);
    
    // std::vector<std::shared_ptr<ColumnFileInfo> > file_infos = writer.get_file_infos();
    // ASSERT_EQ(file_infos.size(), 1);
    // writer.finish();


}   
}  // namespace baikal