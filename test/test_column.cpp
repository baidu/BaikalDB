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
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <vectorize_helpper.h>
// #include "proto/meta.interface.pb.h"
// #include "proto/plan.pb.h"
#include "parquet_writer.h"
#include "sort_merge.h"
#include "file_manager.h"
#include "mut_table_key.h"
// #include "arrow/testing/gtest_util.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
DECLARE_string(db_path);
/*
struct ColumnSchemaInfo {
    std::vector<FieldInfo> key_fields;
    std::vector<FieldInfo> value_fields;
    SmartIndex index_info = nullptr;
    SmartTable table_info = nullptr;
    int uniq_size = 0; // 主键或索引key的字段个数key_fields.size(); 在parquet中的idx为0~uniq_size-1
    int keytype_idx = -1; // key_fields.size() + value_fields.size();
    int raft_index_idx = -1; // keytype_idx + 1
    int batch_pos_idx = -1; // raft_index_idx + 1
    std::set<int> need_sum_idx;
    std::shared_ptr<arrow::Schema> schema = nullptr; // 包含表中所有字段 + __key_type__; 顺序为key + value + __key_type__; key为主键序，value为非主键字段按field_idx顺序
    std::shared_ptr<arrow::Schema> schema_with_order_info = nullptr;  // 比schema多包含__raft_index__, __batch_pos__放在__key_type__后面
};
*/

std::shared_ptr<ColumnSchemaInfo> test_make_column_schema(int64_t tableid) {
    auto schema_ptr = std::make_shared<ColumnSchemaInfo>();
    FieldInfo key;
    key.id = 1;
    key.type = pb::INT32;
    key.lower_short_name = "k1";
    schema_ptr->key_fields.emplace_back(key);

    FieldInfo v1;
    v1.id = 2;
    v1.type = pb::INT32;
    v1.lower_short_name = "v1";
    schema_ptr->value_fields.emplace_back(v1);

    FieldInfo v2;
    v2.id = 3;
    v2.type = pb::INT32;
    v2.lower_short_name = "v2";
    schema_ptr->value_fields.emplace_back(v2);

    schema_ptr->uniq_size = schema_ptr->key_fields.size();
    schema_ptr->keytype_idx = schema_ptr->key_fields.size() + schema_ptr->value_fields.size();
    schema_ptr->raft_index_idx = schema_ptr->keytype_idx + 1;
    schema_ptr->batch_pos_idx = schema_ptr->raft_index_idx + 1;

    schema_ptr->need_sum_idx.insert(1);
    schema_ptr->need_sum_idx.insert(2);
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    arrow_fields.reserve(schema_ptr->batch_pos_idx + 1);
    for (const auto& field : schema_ptr->key_fields) {
        auto arrow_type = primitive_to_arrow_type(field.type);
        if (arrow_type < 0) {
            DB_COLUMN_FATAL("field: %s primitive type:%d to arrow type failed", field.lower_short_name.c_str(), field.type);
            return nullptr;
        }
        auto arrow_field = VectorizeHelpper::make_field(field.lower_short_name, arrow::Type::type(arrow_type));
        if (arrow_field == nullptr) {
            DB_COLUMN_FATAL("field: %s make arrow schema failed", field.lower_short_name.c_str());
            return nullptr;
        }
        arrow_fields.emplace_back(arrow_field);
    }

    for (const auto& field : schema_ptr->value_fields) {
        auto arrow_type = primitive_to_arrow_type(field.type);
        if (arrow_type < 0) {
            DB_COLUMN_FATAL("field: %s primitive type:%d to arrow type failed", field.lower_short_name.c_str(), field.type);
            return nullptr;
        }
        auto arrow_field = VectorizeHelpper::make_field(field.lower_short_name, arrow::Type::type(arrow_type));
        if (arrow_field == nullptr) {
            DB_COLUMN_FATAL("field: %s make arrow schema failed", field.lower_short_name.c_str());
            return nullptr;
        }
        arrow_fields.emplace_back(arrow_field);
    }

    arrow_fields.emplace_back(VectorizeHelpper::make_field(KEY_TYPE_NAME, arrow::Type::type::INT32));
    schema_ptr->schema = std::make_shared<arrow::Schema>(arrow_fields);
    arrow_fields.emplace_back(VectorizeHelpper::make_field(RAFT_INDEX_NAME, arrow::Type::type::INT64));
    arrow_fields.emplace_back(VectorizeHelpper::make_field(BATCH_POS_NAME, arrow::Type::type::INT32));
    schema_ptr->schema_with_order_info = std::make_shared<arrow::Schema>(arrow_fields);
    return schema_ptr;
}

struct TESTRecordBatchReaderOptions {
    std::shared_ptr<arrow::Schema> schema = nullptr;
    int batch = 0;
};

class TESTRecordBatchReader : public ::arrow::RecordBatchReader {
public:
    TESTRecordBatchReader(std::shared_ptr<arrow::Schema> schema) : _schema(schema) {
        _column_record = std::make_shared<ColumnRecord>(schema, 1024);
        _column_record->init();
        _column_record->reserve(1024);

    }
    virtual ~TESTRecordBatchReader() { }

    void append_row(const std::vector<int>& row, int64_t raft_index = -1, int32_t batch_pos = -1) {
        std::vector<ExprValue> expr_row;
        expr_row.reserve(row.size() + 2);
        for (int i = 0; i < row.size(); ++i) {
            ExprValue value;
            value.type = pb::INT32;
            value._u.int32_val = row[i];
            expr_row.push_back(value);
        }
        if (raft_index != -1 && batch_pos != -1) {
            ExprValue raft_index_value;
            raft_index_value.type = pb::INT64;
            raft_index_value._u.int64_val = raft_index;
            expr_row.push_back(raft_index_value);
            ExprValue batch_pos_value;
            batch_pos_value.type = pb::INT32;
            batch_pos_value._u.int32_val = batch_pos;
            expr_row.push_back(batch_pos_value);
        }

        _column_record->append_row(expr_row);
    }

    std::shared_ptr<arrow::Schema> schema() const override { return _schema; }

    virtual ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* batch) override {
        if (_column_record->size() <= 0) {
            batch->reset();
            return ::arrow::Status::OK();
        }

        int ret = _column_record->finish_and_make_record_batch(batch);
        if (ret < 0) {
            return arrow::Status::IOError("make record batch fail");
        }
        DB_WARNING("record batch size:%ld", (*batch)->num_rows());
        return ::arrow::Status::OK();
    }
private:
    std::shared_ptr<arrow::Schema> _schema = nullptr;
    std::shared_ptr<ColumnRecord> _column_record = nullptr;
};

TEST(test_column_record, case_all) {
    std::shared_ptr<ColumnSchemaInfo> schema_ptr = test_make_column_schema(123);
    auto reader = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema_with_order_info);
    reader->append_row({1,1,1,0},0,0);
    reader->append_row({2,2,2,0},0,0);
    reader->append_row({3,3,3,0},0,0);
    reader->append_row({4,4,4,0},0,0);
    std::shared_ptr<arrow::RecordBatch> record_batch;
    reader->ReadNext(&record_batch);
    ColumnRecord::TEST_print_record_batch(record_batch);
} 
TEST(test_sort_merge, unorder_sort) {
    int ret = 0;
    std::shared_ptr<ColumnSchemaInfo> schema_ptr = test_make_column_schema(123);
    auto reader1 = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema_with_order_info);
    reader1->append_row({1,1,1,COLUMN_KEY_PUT},0,0);
    reader1->append_row({3,3,3,COLUMN_KEY_PUT},0,0);
    reader1->append_row({2,2,2,COLUMN_KEY_MERGE},1,0);
    reader1->append_row({2,2,2,COLUMN_KEY_PUT},0,0);
    reader1->append_row({1,1,1,COLUMN_KEY_DELETE},1,0);
    reader1->append_row({1,1,1,COLUMN_KEY_MERGE},2,0);

    std::vector<std::shared_ptr<SingleRowReader>> readers;
    readers.reserve(2);
    auto usrr = std::make_shared<UnOrderSingleRowReader>(reader1, schema_ptr.get(), 5);
    readers.emplace_back(usrr);
    SortMergeOptions merge_options;
    merge_options.batch_size = 10;
    merge_options.is_base_compact = false;
    merge_options.schema_info = schema_ptr;
    auto sort_merge = std::make_shared<SortMerge>(merge_options, readers);
    std::shared_ptr<arrow::RecordBatch> record_batch;
    auto s = sort_merge->ReadNext(&record_batch);
    EXPECT_EQ(s.ok(), true);
    ColumnRecord::TEST_print_record_batch(record_batch);

    auto result = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema);
    result->append_row({1,1,1,COLUMN_KEY_PUT},-1,-1);
    result->append_row({2,4,4,COLUMN_KEY_PUT},-1,-1);
    result->append_row({3,3,3,COLUMN_KEY_PUT},-1,-1);
    std::shared_ptr<arrow::RecordBatch> result_rb;
    result->ReadNext(&result_rb);
    
    EXPECT_EQ(ColumnRecord::TEST_record_batch_diff(result_rb, record_batch), false);

}

TEST(test_sort_merge, minor_compact) {
    int ret = 0;
    std::shared_ptr<ColumnSchemaInfo> schema_ptr = test_make_column_schema(123);
    auto reader1 = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema_with_order_info);
    reader1->append_row({1,1,1,2},2,0);
    reader1->append_row({3,3,3,2},2,0);
    reader1->append_row({2,2,2,2},2,0);

    auto reader2 = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema_with_order_info);
    reader2->append_row({1,1,1,2},1,0);
    reader2->append_row({2,2,2,2},1,0);
    reader2->append_row({3,3,3,2},1,0);
    reader2->append_row({4,4,4,2},1,0);

    auto reader3 = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema_with_order_info);
    reader3->append_row({2,2,2,2},0,0);
    reader3->append_row({3,3,3,2},0,0);
    reader3->append_row({4,1,1,2},0,0);
    reader3->append_row({5,5,5,2},0,0);

    std::vector<std::shared_ptr<SingleRowReader>> readers;
    readers.reserve(2);
    auto usrr = std::make_shared<UnOrderSingleRowReader>(reader1, schema_ptr.get(), 0);
    auto osrr = std::make_shared<OrderSingleRowReader>(reader2, schema_ptr.get());
    auto osrr2 = std::make_shared<OrderSingleRowReader>(reader3, schema_ptr.get());

    readers.emplace_back(usrr);
    readers.emplace_back(osrr);
    readers.emplace_back(osrr2);

    SortMergeOptions merge_options;
    merge_options.batch_size = 10;
    merge_options.is_base_compact = false;
    merge_options.schema_info = schema_ptr;
    auto sort_merge = std::make_shared<SortMerge>(merge_options, readers);
    std::shared_ptr<arrow::RecordBatch> record_batch;
    auto s = sort_merge->ReadNext(&record_batch);
    EXPECT_EQ(s.ok(), true);
    ColumnRecord::TEST_print_record_batch(record_batch);

    auto result = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema);
    result->append_row({1,2,2,COLUMN_KEY_MERGE},-1,-1);
    result->append_row({2,6,6,COLUMN_KEY_MERGE},-1,-1);
    result->append_row({3,9,9,COLUMN_KEY_MERGE},-1,-1);
    result->append_row({4,5,5,COLUMN_KEY_MERGE},-1,-1);
    result->append_row({5,5,5,COLUMN_KEY_MERGE},-1,-1);
    std::shared_ptr<arrow::RecordBatch> result_rb;
    result->ReadNext(&result_rb);
    
    EXPECT_EQ(ColumnRecord::TEST_record_batch_diff(result_rb, record_batch), false);
}  

TEST(test_acero_merge, acero_merge) {
    int ret = 0;
    std::shared_ptr<ColumnSchemaInfo> schema_ptr = test_make_column_schema(123);
    auto reader1 = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema_with_order_info);
    reader1->append_row({1,1,1,COLUMN_KEY_MERGE},2,0);
    reader1->append_row({3,3,3,COLUMN_KEY_MERGE},2,0);
    reader1->append_row({2,2,2,COLUMN_KEY_MERGE},2,0);
    reader1->append_row({1,1,1,COLUMN_KEY_MERGE},2,0);
    reader1->append_row({2,2,2,COLUMN_KEY_MERGE},2,0);

    AceroMergeOptions acero_options;
    acero_options.batch_size = 10;
    acero_options.schema_info = schema_ptr;
    std::shared_ptr<AceroMerge> acero_merge(new AceroMerge(acero_options, {reader1}));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    auto s = acero_merge->ReadNext(&record_batch);
    EXPECT_EQ(s.ok(), true);
    ColumnRecord::TEST_print_record_batch(record_batch);
    auto result = std::make_shared<TESTRecordBatchReader>(schema_ptr->schema);
    result->append_row({1,2,2,COLUMN_KEY_MERGE},-1,-1);
    result->append_row({2,4,4,COLUMN_KEY_MERGE},-1,-1);
    result->append_row({3,3,3,COLUMN_KEY_MERGE},-1,-1);
    std::shared_ptr<arrow::RecordBatch> result_rb;
    result->ReadNext(&result_rb);
    ColumnRecord::TEST_print_record_batch(result_rb);
    // EXPECT_EQ(ColumnRecord::TEST_record_batch_diff(result_rb, record_batch), false);
}

TEST(parquet_read_write, write_read) {
    int64_t table_id = 123;
    int64_t region_id = 456;
    int ret = 0;
    std::shared_ptr<ColumnSchemaInfo> schema_info = test_make_column_schema(table_id);
    auto reader1 = std::make_shared<TESTRecordBatchReader>(schema_info->schema);
    reader1->append_row({1,1,1,0});
    reader1->append_row({2,2,2,0});
    reader1->append_row({3,3,3,0});
    std::shared_ptr<::arrow::RecordBatch> record_batch;
    auto s = reader1->ReadNext(&record_batch);
    EXPECT_EQ(s.ok(), true);
    EXPECT_EQ(record_batch->num_columns(), 4);
    EXPECT_EQ(record_batch->num_rows(), 3);
    FLAGS_db_path = "./test_data";
    boost::filesystem::path output_path(FLAGS_db_path + "_tmp");
    if (!boost::filesystem::exists(output_path)) {
        // 创建目录
        if (!boost::filesystem::create_directories(output_path)) {
            DB_COLUMN_FATAL("FATAL create output_path fail.");
            return;
        }
    }

    boost::filesystem::path output_path2(FLAGS_db_path + "_column/" + std::to_string(table_id) + "/" + std::to_string(region_id));
    if (!boost::filesystem::exists(output_path2)) {
        // 创建目录
        if (!boost::filesystem::create_directories(output_path2)) {
            DB_COLUMN_FATAL("FATAL create output_path2 fail.");
            return;
        }
    }

    ParquetWriteOptions parquet_options;
    parquet_options.write_batch_length = 10;
    parquet_options.max_file_rows = 1000;
    parquet_options.max_row_group_length = 100; // 保证row group长度是write_batch_length的整数倍
    parquet_options.schema_info = schema_info;
    auto writer = std::make_shared<ParquetWriter>(parquet_options);
    s = writer->init();
    EXPECT_EQ(s.ok(), true);

    RecordBatchInfo rb_info;
    rb_info.row_count = record_batch->num_rows();
    s = writer->write_batch(record_batch, rb_info);
    EXPECT_EQ(s.ok(), true);

    s = writer->finish();
    EXPECT_EQ(s.ok(), true);

    auto file_infos = writer->get_file_infos();
    EXPECT_EQ(file_infos.size(), 1);

    std::shared_ptr<ColumnFileInfo> new_file = std::make_shared<ColumnFileInfo>(table_id, region_id, 0, 123, 0, file_infos[0]);

    ret = ::link(file_infos[0].file_name.c_str(), new_file->full_path().c_str());
    EXPECT_EQ(ret, 0);
    
    auto file = std::make_shared<ParquetFile>(new_file);
    ret = file->open();
    EXPECT_EQ(ret, 0);
    if (ret < 0) {
        DB_COLUMN_FATAL("open file:%s failed, ret:%d", new_file->full_path().c_str(), ret);
        return;
    }
    ParquetFileReaderOptions options;
    options.schema = schema_info->schema;
    std::shared_ptr<ParquetFileReader> parquet_reader = std::make_shared<ParquetFileReader>(options, file);
    ret = parquet_reader->init();
    EXPECT_EQ(ret, 0);
    std::shared_ptr<::arrow::RecordBatch> record_batch2;
    s = parquet_reader->ReadNext(&record_batch2);
    EXPECT_EQ(s.ok(), true);
    ColumnRecord::TEST_print_record_batch(record_batch2);
    ColumnRecord::TEST_print_record_batch(record_batch);
    EXPECT_EQ(ColumnRecord::TEST_record_batch_diff(record_batch2, record_batch), false);

}
DEFINE_string(test_parquet_file, "", "rocks db path");
struct TESTColumnFileInfo : public ColumnFileInfo {
    TESTColumnFileInfo() : ColumnFileInfo(123, 245, 0, 123, 0, ColumnFileMeta()) {

    }

    std::string full_path() {
        return FLAGS_test_parquet_file;
    }
};

TEST(parquet_read_write, read) {
    std::shared_ptr<ColumnFileInfo> new_file(new TESTColumnFileInfo());
    if (new_file->full_path().empty()) {
        DB_COLUMN_FATAL("Fail to get parquet file");
        return;
    }

    auto parquet_file = std::make_shared<ParquetFile>(new_file);

    int ret = parquet_file->open();
    if (ret < 0) {
        DB_COLUMN_FATAL("Fail to open parquet file");
        return;
    }
    std::unique_ptr<::arrow::RecordBatchReader> reader;
    auto s = parquet_file->GetRecordBatchReader(&reader);
    if (!s.ok()) {
        DB_COLUMN_FATAL("Fail to get_record_batch_reader");
        return;
    }

    std::shared_ptr<::arrow::RecordBatch> record_batch;
    s = reader->ReadNext(&record_batch);
    if (!s.ok()) {
        DB_COLUMN_FATAL("Fail to ReadNext");
        return;
    }

    ColumnRecord::TEST_print_record_batch(record_batch);
}
struct TestRow {
    int64_t v = 0;
    bool operator<(const TestRow& other) const {
        DB_WARNING("v:%ld, other.v:%ld", v, other.v);
        return v < other.v;
    }
};
TEST(heap_test, all) {
    Heap<TestRow> heap;
    std::vector<int64_t> v = {1, 2, 3, 5, 8, 13, 7, 6, 4, 10};
    for (auto& i : v) {
        TestRow r;
        r.v = i;
        heap.push(r);
    }
    DB_WARNING("make heap size:%lu", heap.size());
    heap.make_heap();
    // auto& h = heap.heap();
    // for (auto& t : h) {
    //     DB_WARNING("v:%ld", t.v);
    // }

    // TestRow r;
    // r.v = 15;
    // heap.replace_top(r);
    // for (auto& t : h) {
    //     DB_WARNING("v:%ld", t.v);
    // }


}

TEST(check_interval_overlapped, all) {
{
    pb::PossibleIndex::Range range;
    MutTableKey index_start_key;
    index_start_key.append_u64(1).append_u64(2);
    MutTableKey index_end_key;
    index_end_key.append_u64(1).append_u64(2);
    range.set_left_key(index_start_key.data());
    range.set_left_open(false);
    range.set_right_key(index_end_key.data());
    range.set_right_open(false);
    MutTableKey file_start_key;
    file_start_key.append_u64(1).append_u64(2).append_u64(3);
    MutTableKey file_end_key;
    file_end_key.append_u64(1).append_u64(2).append_u64(3);
    EXPECT_EQ(ParquetFile::check_interval_overlapped(range, false, false, false, file_start_key.data(), file_end_key.data()), true);
}

{
    pb::PossibleIndex::Range range;
    MutTableKey index_start_key;
    index_start_key.append_u64(1).append_u64(1);
    MutTableKey index_end_key;
    index_end_key.append_u64(1).append_u64(2);
    range.set_left_key(index_start_key.data());
    range.set_left_open(false);
    range.set_right_key(index_end_key.data());
    range.set_right_open(false);
    MutTableKey file_start_key;
    file_start_key.append_u64(1).append_u64(2);
    MutTableKey file_end_key;
    file_end_key.append_u64(1).append_u64(2);
    EXPECT_EQ(ParquetFile::check_interval_overlapped(range, false, false, false, file_start_key.data(), file_end_key.data()), true);
}

{
    pb::PossibleIndex::Range range;
    MutTableKey index_start_key;
    index_start_key.append_u64(1).append_u64(1);
    MutTableKey index_end_key;
    index_end_key.append_u64(1).append_u64(2);
    range.set_left_key(index_start_key.data());
    range.set_left_open(false);
    range.set_right_key(index_end_key.data());
    range.set_right_open(true);
    MutTableKey file_start_key;
    file_start_key.append_u64(1).append_u64(2);
    MutTableKey file_end_key;
    file_end_key.append_u64(1).append_u64(2);
    EXPECT_EQ(ParquetFile::check_interval_overlapped(range, false, false, true, file_start_key.data(), file_end_key.data()), false);
}

{
    pb::PossibleIndex::Range range;
    MutTableKey index_start_key;
    index_start_key.append_u64(1).append_u64(1);
    MutTableKey index_end_key;
    index_end_key.append_u64(1).append_u64(3);
    range.set_left_key(index_start_key.data());
    range.set_left_open(false);
    range.set_right_key(index_end_key.data());
    range.set_right_open(true);
    MutTableKey file_start_key;
    file_start_key.append_u64(1).append_u64(2).append_u64(3);
    MutTableKey file_end_key;
    file_end_key.append_u64(1).append_u64(2).append_u64(3);
    EXPECT_EQ(ParquetFile::check_interval_overlapped(range, false, false, true, file_start_key.data(), file_end_key.data()), true);
}

}

struct TestBlockContents {
    int size = 0;
    std::string value;
    char* data = nullptr;
    TestBlockContents(int size, const std::string& value) : size(size), value(value), data(new char[size]) {
        DB_WARNING("size:%d, value:%s", size, value.c_str());
    }
    ~TestBlockContents() {
        DB_WARNING("delete data size:%d, value:%s", size, value.c_str());
        delete[] data;
    }
    static void delete_fn(void* value, rocksdb::MemoryAllocator* allocator) {
        TestBlockContents* block = static_cast<TestBlockContents*>(value);
        DB_WARNING("delete data size:%d, value:%s", block->size, block->value.c_str());
        delete block;
        return;
    } 

    static rocksdb::Cache::CacheItemHelper kBasicHelper;
};

rocksdb::Cache::CacheItemHelper TestBlockContents::kBasicHelper{
    rocksdb::CacheEntryRole::kOtherBlock, &TestBlockContents::delete_fn
};

//   // Returns the maximum configured capacity of the cache
//   virtual size_t GetCapacity() const = 0;

//   // Returns the memory size for the entries residing in the cache.
//   virtual size_t GetUsage() const = 0;

//   // Returns the number of entries currently tracked in the table. SIZE_MAX
//   // means "not supported." This is used for inspecting the load factor, along
//   // with GetTableAddressCount().
//   virtual size_t GetOccupancyCount() const { return SIZE_MAX; }

//   // Returns the number of ways the hash function is divided for addressing
//   // entries. Zero means "not supported." This is used for inspecting the load
//   // factor, along with GetOccupancyCount().
//   virtual size_t GetTableAddressCount() const { return 0; }

//   // Returns the memory size for a specific entry in the cache.
//   virtual size_t GetUsage(Handle* handle) const = 0;

//   // Returns the memory size for the entries in use by the system
//   virtual size_t GetPinnedUsage() const = 0;

//   // Returns the charge for the specific entry in the cache.
//   virtual size_t GetCharge(Handle* handle) const = 0;

TEST(lru_cache, all) {
{
    std::vector<std::string> keys = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    std::shared_ptr<rocksdb::Cache> block_cache = rocksdb::NewLRUCache(128 * 1024 * 1024LL, 8);
    auto block = new TestBlockContents(1024, "abc");
    rocksdb::Cache::Handle* handle = nullptr;
    rocksdb::Slice key = rocksdb::Slice("abc");
    auto s = block_cache->Insert(key, block, &TestBlockContents::kBasicHelper, 1024,&handle);
    if (s.ok()) {
        DB_WARNING("insert ok");
        EXPECT_NE(handle, nullptr);
        auto value = block_cache->Value(handle);
        EXPECT_EQ(value, block);
        TestBlockContents* v = static_cast<TestBlockContents*>(value);
        block_cache->Release(handle);
        DB_WARNING("value:%s, GetUsage:%lu,GetCharge:%lu", v->value.c_str(), block_cache->GetUsage(handle), block_cache->GetCharge(handle));
    }
    rocksdb::Cache::Handle* handle2 = nullptr;
    auto block2 = new TestBlockContents(1024, "abcd");
    s = block_cache->Insert(key, block2, &TestBlockContents::kBasicHelper, 1024,&handle2);
    if (s.ok()) {
        block_cache->Release(handle2);
        DB_WARNING("insert ok abcd");
    } else {
        DB_WARNING("insert fail abcd %s", s.ToString().c_str());
    }

}
{
    std::vector<std::string> keys = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    std::shared_ptr<rocksdb::Cache> block_cache = rocksdb::NewLRUCache(128 * 1024 * 1024LL, 8);
    int i = 0;
    for (auto& k : keys) {
        auto block = new TestBlockContents(1024, k);
        rocksdb::Slice key = rocksdb::Slice(k);
        rocksdb::Cache::Handle* handle = nullptr;
        auto s = block_cache->Insert(key, block, &TestBlockContents::kBasicHelper, 1024 + ++i, &handle);
        EXPECT_EQ(s.ok(), true);
        auto value = block_cache->Value(handle);
        EXPECT_EQ(value, block);
        TestBlockContents* v = static_cast<TestBlockContents*>(value);
        block_cache->Release(handle);
        DB_WARNING("value:%s, GetUsage:%lu,GetCharge:%lu", v->value.c_str(), block_cache->GetUsage(handle), block_cache->GetCharge(handle));
    }
    DB_WARNING("GetCapacity:%lu, GetUsage:%lu", block_cache->GetCapacity(), block_cache->GetUsage());
    for (auto& k : keys) {
        rocksdb::Slice key = rocksdb::Slice(k);
        auto handle = block_cache->BasicLookup(key, nullptr);
        EXPECT_NE(handle, nullptr);
        auto value = block_cache->Value(handle);
        TestBlockContents* v = static_cast<TestBlockContents*>(value);
        EXPECT_EQ(k, v->value);
        block_cache->Release(handle);
    }
}
}
}  // namespace baikal