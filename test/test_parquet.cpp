// Copyright (c) 2022 Baidu, Inc. All Rights Reserved.
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

#include "parquet/stream_writer.h"
#include "file_executor.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

// ParquetWriter
class ParquetStreamWriter : public ::parquet::StreamWriter {
public:
    ParquetStreamWriter(std::unique_ptr<::parquet::ParquetFileWriter> writer) : 
            ::parquet::StreamWriter(std::move(writer)) {}

    template <typename WriterType, typename T>
    ::parquet::StreamWriter& Write(const T v) {
        return ::parquet::StreamWriter::Write<WriterType, T>(v);
    }
};

std::shared_ptr<::parquet::schema::GroupNode> get_schema() {
    using namespace parquet;
    schema::NodeVector fields;

    // ::arrow::Type::type::BOOL
    fields.push_back(schema::PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                                    Type::BOOLEAN, ConvertedType::NONE));

    // ::arrow::Type::type::INT32
    fields.push_back(schema::PrimitiveNode::Make("int32_field", Repetition::REQUIRED,
                                                    Type::INT32, ConvertedType::INT_32));

    // ::arrow::Type::type::FLOAT
    fields.push_back(schema::PrimitiveNode::Make("float_field", Repetition::REQUIRED,
                                                    Type::FLOAT, ConvertedType::NONE));

    // ::arrow::Type::type::DOUBLE
    fields.push_back(schema::PrimitiveNode::Make("double_field", Repetition::REQUIRED,
                                                    Type::DOUBLE, ConvertedType::NONE));

    // ::arrow::Type::type::DECIMAL
    fields.push_back(schema::PrimitiveNode::Make("decimal_field", Repetition::REQUIRED,
                                                    Type::INT32, ConvertedType::DECIMAL, -1, 8, 4));

    // ::arrow::Type::type::TIME32
    fields.push_back(schema::PrimitiveNode::Make("time32_field", Repetition::REQUIRED,
                                                    Type::INT32, ConvertedType::TIME_MILLIS));

    // ::arrow::Type::type::TIME64
    fields.push_back(schema::PrimitiveNode::Make("time64_field", Repetition::REQUIRED,
                                                    Type::INT64, ConvertedType::TIME_MICROS));
    
    // ::arrow::Type::type::TIMESTAMP
    fields.push_back(schema::PrimitiveNode::Make("ts_field", Repetition::REQUIRED,
                                                    Type::INT64, ConvertedType::TIMESTAMP_MICROS));

    // ::arrow::Type::type::DATE32
    fields.push_back(schema::PrimitiveNode::Make("date32_field", Repetition::REQUIRED,
                                                    Type::INT32, ConvertedType::DATE));

    // ::arrow::Type::type::BINARY
    fields.push_back(schema::PrimitiveNode::Make("binary_field", Repetition::REQUIRED,
                                                    Type::BYTE_ARRAY, ConvertedType::NONE));

    // ::arrow::Type::type::STRING
    fields.push_back(schema::PrimitiveNode::Make("string_field", Repetition::REQUIRED,
                                                    Type::BYTE_ARRAY, ConvertedType::UTF8));
    
    // ::arrow::Type::type::FIEXD_SIZED_ARRAY
    fields.push_back(schema::PrimitiveNode::Make("fixed_len_field", Repetition::REQUIRED,
                                                    Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE, 4));

    return std::static_pointer_cast<schema::GroupNode>(
        schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

void make_parquet_file(const std::string& file_path, const int32_t& num_row) {
    using namespace parquet;
    PARQUET_ASSIGN_OR_THROW(auto outfile, ::arrow::io::FileOutputStream::Open(file_path));
    StreamWriter writer = StreamWriter { ParquetFileWriter::Open(outfile, get_schema()) };

    bool     bool_val      = true;
    int32_t  int32_val     = 100;
    float    float_val     = 1.23;
    double   double_val    = 1.234;
    int32_t  decimal_val   = 1234567;
    int32_t  time32_val    = 43200000;
    int64_t  time64_val    = 43200000000;
    int64_t  ts_val        = 1665504000000;
    int32_t  date32_val    = 10;
    std::string string_val = "parquet_test";
    char fixed_len_val[4]  = {'p', 'a', 'r', 'q'};

    ByteArray binary_val;
    const std::string str_data = "test_parquet";
    binary_val.ptr = reinterpret_cast<const uint8_t*>(str_data.data());
    binary_val.len = static_cast<uint32_t>(str_data.size());

    for (int i = 0; i < num_row; ++i) {
        writer.Write<BoolWriter>(bool_val);
        writer.Write<Int32Writer>(int32_val);
        writer.Write<FloatWriter>(float_val);
        writer.Write<DoubleWriter>(double_val);
        writer.Write<Int32Writer>(decimal_val);
        writer.Write<Int32Writer>(time32_val);
        writer.Write<Int64Writer>(time64_val);
        writer.Write<Int64Writer>(ts_val);
        writer.Write<Int32Writer>(date32_val);
        writer.Write<ByteArrayWriter>(binary_val);
        writer << string_val << fixed_len_val << EndRow;
    }
}

TEST(test_parquet_writer, case_all) {
    make_parquet_file("./data/parquet/test1.parquet", 1);
    make_parquet_file("./data/parquet/test2.parquet", 100);
}

// ParquetExecutor
TEST(test_parquet_executor_recur, case_all) {
    const std::string path = "./data/parquet";
    FileSystem* fs = new PosixFileSystem();
    ASSERT_TRUE(fs->init() == 0);

    int64_t start_pos = 0;
    int64_t end_pos = 0;
    int result = 0;
    std::ostringstream import_ret;

    auto fields_func = 
        [] (const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) {
            const size_t FIRST_FILE_ROW_SIZE = 1;  
            const size_t SECOND_FILE_ROW_SIZE = 10; // insert_values_count
            const size_t COLUMN_SIZE = 12;

            if (path == "./data/parquet/test1.parquet") {
                EXPECT_TRUE(fields_vec.size() == FIRST_FILE_ROW_SIZE);
            } else if (path == "./data/parquet/test2.parquet") {
                EXPECT_TRUE(fields_vec.size() == SECOND_FILE_ROW_SIZE);
            } else {
                return;
            }

            for (auto& fields : fields_vec) {
                EXPECT_TRUE(fields.size() == COLUMN_SIZE);
            }
        };

    auto split_func = 
        [] (std::string& line, std::vector<std::string>& split_vec) { return true; };

    auto progress_func = [] (const std::string& str) { 
        std::cout << "progress_func: " << str << std::endl;
        return; 
    };

    auto convert_func = [] (std::string& str) -> bool { 
        return true; 
    };

    ImporterImpl importer_impl(
                    path, fs, fields_func, split_func, convert_func, "",
                    start_pos, end_pos, nullptr, false, FileType::Parquet);

    EXPECT_TRUE(importer_impl.run(progress_func) == 0);
}

// ParquetReader
TEST(test_parquet_reader_local, case_all) {
    const std::string path = "./data/parquet/test1.parquet";
    FileSystem* fs = new PosixFileSystem();
    ASSERT_TRUE(fs->init() == 0);

    FileMode mode;
    size_t file_size;
    ASSERT_TRUE(fs->file_mode(path, &mode, &file_size, nullptr) == 0);
    
    int64_t file_start_pos = 0;
    int64_t file_end_pos = file_size;
    int32_t insert_values_count = 10;
    int result = 0;
    std::ostringstream import_ret;
    std::mutex ret_mtx;
    int64_t all_row_group_count = ImporterUtils::all_row_group_count(fs, path);
    std::atomic<int64_t> handled_row_group_count { 0 };

    BthreadCond file_concurrency_cond(-3);

    auto fields_func = 
        [] (const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) {
            const size_t ROW_SIZE    = 1;
            const size_t COLUMN_SIZE = 12;
            if (fields_vec.size() != ROW_SIZE || fields_vec[0].size() != COLUMN_SIZE) {
                return;
            }
            const auto& field = fields_vec[0];
            EXPECT_TRUE(field[0] == "1");
            EXPECT_TRUE(field[1] == "100");
            EXPECT_TRUE(field[2] == "1.23");
            EXPECT_TRUE(field[3] == "1.234");
            EXPECT_TRUE(field[4] == "123.4567");
            EXPECT_TRUE(field[5] == "43200000");
            EXPECT_TRUE(field[6] == "43200000000");
            EXPECT_TRUE(field[7] == "1665504000000");
            EXPECT_TRUE(field[8] == "1970-01-11");
            EXPECT_TRUE(field[9] == "test_parquet");
            EXPECT_TRUE(field[10] == "parquet_test");
            EXPECT_TRUE(field[11] == "parq");
        };

    auto convert_func = [] (std::string& str) -> bool { 
        return true; 
    };

    std::shared_ptr<ParquetReader> p_reader { new (std::nothrow) ParquetReader(
                    path, fs, file_start_pos, file_end_pos, file_size, file_concurrency_cond, 
                    fields_func, insert_values_count, all_row_group_count, handled_row_group_count, 
                    convert_func, result, import_ret, ret_mtx) };

    ASSERT_TRUE(p_reader != nullptr);
    ASSERT_TRUE(p_reader->open() == 0);
    ASSERT_TRUE(p_reader->run() == 0);
    ASSERT_TRUE(result == 0);

    file_concurrency_cond.wait(-3);
}

TEST(DISABLED_test_parquet_reader_afs, case_all) {
    const std::string path = "file_path";
    FileSystem* fs = new AfsFileSystem("cluster_name", "user", "password", "./conf/client.conf");
    ASSERT_TRUE(fs->init() == 0);

    FileMode mode;
    size_t file_size;
    ASSERT_TRUE(fs->file_mode(path, &mode, &file_size, nullptr) == 0);
    
    int64_t file_start_pos = 0;
    int64_t file_end_pos = file_size;
    int32_t insert_values_count = 10;
    int result = 0;
    std::ostringstream import_ret;
    std::mutex ret_mtx;
    int64_t all_row_group_count = ImporterUtils::all_row_group_count(fs, path);
    std::atomic<int64_t> handled_row_group_count { 0 };

    BthreadCond file_concurrency_cond(-3);

    auto fields_func = 
        [] (const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) {
            for (const auto& fields : fields_vec) {
                std::string line;
                for (const auto& field : fields) {
                    line += field;
                    line += "\t";
                }
                if (!line.empty()) {
                    line.pop_back();
                }
                std::cout << line << std::endl;
            }
        };

    auto convert_func = [] (std::string& str) -> bool { 
        return true; 
    };

    std::shared_ptr<ParquetReader> p_reader { new (std::nothrow) ParquetReader(
                    path, fs, file_start_pos, file_end_pos, file_size, file_concurrency_cond, 
                    fields_func, insert_values_count, all_row_group_count, handled_row_group_count, 
                    convert_func, result, import_ret, ret_mtx) };

    ASSERT_TRUE(p_reader != nullptr);
    ASSERT_TRUE(p_reader->open() == 0);
    ASSERT_TRUE(p_reader->run() == 0);
    ASSERT_TRUE(result == 0);

    file_concurrency_cond.wait(-3);
}

TEST(test_parquet_reader_error, case_all) {
    const std::string path = "./data/parquet/test_error.parquet";
    FileSystem* fs = new PosixFileSystem();
    ASSERT_TRUE(fs->init() == 0);

    size_t file_size = 0;
    int64_t file_start_pos = 0;
    int64_t file_end_pos = 0;
    int32_t insert_values_count = 10;
    int result = 0;
    std::ostringstream import_ret;
    std::mutex ret_mtx;
    int64_t all_row_group_count = 0;
    std::atomic<int64_t> handled_row_group_count { 0 };
    
    BthreadCond file_concurrency_cond(-3);

    auto fields_func = 
        [] (const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) { return; };

    auto convert_func = [] (std::string& str) -> bool { 
        return true; 
    };

    std::shared_ptr<ParquetReader> p_reader { new (std::nothrow) ParquetReader(
                    path, fs, file_start_pos, file_end_pos, file_size, file_concurrency_cond, 
                    fields_func, insert_values_count, all_row_group_count, handled_row_group_count, 
                    convert_func, result, import_ret, ret_mtx) };

    ASSERT_TRUE(p_reader != nullptr);
    ASSERT_TRUE(p_reader->open() == -2);
    std::cout << "import_ret: " << import_ret.str() << std::endl;

    file_concurrency_cond.wait(-3);
}

TEST(test_parquet_reader_not_exist, case_all) {
    const std::string path = "./data/parquet/test_not_exist.parquet";
    FileSystem* fs = new PosixFileSystem();
    ASSERT_TRUE(fs->init() == 0);

    size_t file_size = 0;
    int64_t file_start_pos = 0;
    int64_t file_end_pos = 0;
    int32_t insert_values_count = 10;
    int result = 0;
    std::ostringstream import_ret;
    std::mutex ret_mtx;
    int64_t all_row_group_count = 0;
    std::atomic<int64_t> handled_row_group_count { 0 };
    
    BthreadCond file_concurrency_cond(-3);

    auto fields_func = 
        [] (const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) { return; };

    auto convert_func = [] (std::string& str) -> bool { 
        return true; 
    };

    std::shared_ptr<ParquetReader> p_reader { new (std::nothrow) ParquetReader(
                    path, fs, file_start_pos, file_end_pos, file_size, file_concurrency_cond, 
                    fields_func, insert_values_count, all_row_group_count, handled_row_group_count, 
                    convert_func, result, import_ret, ret_mtx) };

    ASSERT_TRUE(p_reader != nullptr);
    ASSERT_TRUE(p_reader->open() == -1);
    std::cout << "import_ret: " << import_ret.str() << std::endl;

    file_concurrency_cond.wait(-3);
}

TEST(DISABLED_test_file_pos_to_row_group_idx, case_all) {
    FileSystem* fs = 
        new AfsFileSystem("cluster_name", "user", "password", "./conf/client.conf");
    ASSERT_TRUE(fs != nullptr);
    ASSERT_TRUE(fs->init() == 0);

    std::string input_file_path = "file_path";
    int64_t cut_size = 1 * 1024 * 1024 * 1024LL; // 1G
    std::vector<std::string> output_file_paths;
    std::vector<int64_t> file_start_pos_vec;
    std::vector<int64_t> file_end_pos_vec;
    ASSERT_TRUE(ImporterUtils::cut_files(
        fs, input_file_path, cut_size, output_file_paths, file_start_pos_vec, file_end_pos_vec) == 0);
    ASSERT_TRUE(output_file_paths.size() == file_start_pos_vec.size());
    ASSERT_TRUE(file_start_pos_vec.size() == file_end_pos_vec.size());

    int32_t insert_values_count = 10;
    int result = 0;
    std::ostringstream import_ret;
    std::mutex ret_mtx;
    int64_t all_row_group_count = ImporterUtils::all_row_group_count(fs, input_file_path);
    std::atomic<int64_t> handled_row_group_count { 0 };
    BthreadCond file_concurrency_cond(-3);

    auto fields_func = 
        [] (const std::string& path, const std::vector<std::vector<std::string>>& fields_vec) {
            for (const auto& fields : fields_vec) {
                std::string line;
                for (const auto& field : fields) {
                    line += field;
                    line += "\t";
                }
                if (!line.empty()) {
                    line.pop_back();
                }
                std::cout << line << std::endl;
            }
        };

    auto convert_func = [] (std::string& str) -> bool { 
        return true; 
    };

    for (int i = 0; i < output_file_paths.size(); ++i) {
        std::vector<std::string> sub_file_paths;
        boost::split(sub_file_paths, output_file_paths[i], boost::is_any_of(";"));

        std::cout << "split path[" << i << "]: " << output_file_paths[i] << std::endl;

        for (int j = 0; j < sub_file_paths.size(); ++j) {
            std::string path = sub_file_paths[j];

            FileMode mode;
            size_t file_size;
            ASSERT_TRUE(fs->file_mode(path, &mode, &file_size, nullptr) == 0);

            int64_t file_start_pos = (j == 0 ? file_start_pos_vec[i] : 0);
            int64_t file_end_pos = (j == sub_file_paths.size() - 1 ? file_end_pos_vec[i] : file_size);
            if (file_end_pos > file_size) {
                file_end_pos = file_size;
            }

            std::shared_ptr<ParquetReader> p_reader { new (std::nothrow) ParquetReader(
                        path, fs, file_start_pos, file_end_pos, file_size, file_concurrency_cond, 
                        fields_func, insert_values_count, all_row_group_count, handled_row_group_count, 
                        convert_func, result, import_ret, ret_mtx) };
            ASSERT_TRUE(p_reader != nullptr);
            ASSERT_TRUE(p_reader->open() == 0);

            std::cout << "subtask[" << i << "], sub_file_paths[" << j << "]: " << path << ", " << file_size
                      << ", row_group_cnt: " << ImporterUtils::all_row_group_count(fs, path)
                      << ", [file_start_pos, file_end_pos): [" << file_start_pos << ", " << file_end_pos
                      << ") => [row_start_idx, row_end_idx): [" << p_reader->file_pos_to_row_group_idx(file_start_pos)
                      << ", " << p_reader->file_pos_to_row_group_idx(file_end_pos) << ")" << std::endl;
        }
    }

    file_concurrency_cond.wait(-3); 
}

} // namespace baikaldb