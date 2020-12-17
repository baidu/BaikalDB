// Copyright (c) 2020 Baidu, Inc. All Rights Reserved.
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

#ifdef SNAPPY
#undef SNAPPY
#endif
#ifdef LZ4
#undef LZ4
#endif
#ifdef ZSTD
#undef ZSTD
#endif
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>

#include <cstdint>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <memory>

#include "common.h"
#include "proto/reverse.pb.h"

namespace baikaldb {

using ArrowReverseNode = pb::CommonReverseNode;

inline std::shared_ptr<arrow::Schema> get_arrow_schema () {
    static auto schema = std::make_shared<arrow::Schema>(
        std::vector<std::shared_ptr<arrow::Field>> {
            arrow::field("key", arrow::binary()),
            arrow::field("flag", arrow::uint8()),
            arrow::field("weight", arrow::float32())
        }
    );
    return schema;
}

class ArrowReverseList {
public:
    ArrowReverseList() = default;
    ~ArrowReverseList() = default;

    int64_t num_rows() const {
        return _rows;
    }

    bool ParseFromString(const std::string& val) {
        _buffer = val;
        arrow::io::BufferReader buf_reader((const uint8_t*)(_buffer.data()), _buffer.size());

        auto rex = arrow::ipc::ReadRecordBatch(get_arrow_schema(), nullptr, &buf_reader, &_result);
        if (rex.ok()) {
            set_internal_info();
        } else {
            DB_WARNING("parser from string error [%s].", rex.ToString().c_str());
        }
        return rex.ok();
    }

    //不拷贝，data生命周期必须比该类长
    bool ParseFromArray(const char* data, size_t size) {
        arrow::io::BufferReader buf_reader((unsigned char*)data, size);
        auto rex = arrow::ipc::ReadRecordBatch(get_arrow_schema(), nullptr, &buf_reader, &_result);
        if (rex.ok()) {
            set_internal_info();
        } else {
            DB_WARNING("parser from array error [%s].", rex.ToString().c_str());
        }
        return rex.ok();
    }

    bool SerializeToString(std::string* val) const {
        std::shared_ptr<arrow::Buffer> buffer;
        auto status = arrow::ipc::SerializeRecordBatch(*_result, arrow::default_memory_pool(), &buffer);
        if (!status.ok()) {
            DB_WARNING("serializeToString error [%s].", status.ToString().c_str());
            return false;
        }
        *val = buffer->ToString();
        return true;
    }

    int64_t reverse_nodes_size() const {
        if (_result == nullptr) {
            return 0;
        }
        return _result->num_rows();
    }

    ArrowReverseNode reverse_nodes(int index) const {
        if (index != _current_node_index) {
            ArrowReverseNode node;
            node.set_key(std::string(_keys_ptr->GetView(index)));
            node.set_flag(baikaldb::pb::ReverseNodeType(_flags_ptr->Value(index)));
            node.set_weight(_weights_ptr->Value(index));
            return node;
        } else {
            return _inner_node;
        }

    }

    void add_node(const std::string& key, int8_t flag, double weight) {
        _key_builder.Append(key);
        _flag_builder.Append(flag);
        _weight_builder.Append(weight);
        _current_node_index++;
        ++_rows;
    }

    void add_node(ArrowReverseNode& node) {
        _key_builder.Append(node.key());
        _flag_builder.Append(node.flag());
        _weight_builder.Append(node.weight());
        _current_node_index++;
        ++_rows;
    }

    void finish() {
        std::shared_ptr<arrow::Array> key_array;
        std::shared_ptr<arrow::Array> flag_array;
        std::shared_ptr<arrow::Array> weight_array;
        _key_builder.Finish(&key_array);
        _flag_builder.Finish(&flag_array);
        _weight_builder.Finish(&weight_array);
        _result = arrow::RecordBatch::Make(get_arrow_schema(), _rows, {key_array, flag_array, weight_array});
        set_internal_info();
    }

    std::string get_key(int64_t index) const {
        return std::string{_keys_ptr->GetView(index)};
    }

    pb::ReverseNodeType get_flag(int64_t index) const {
        return pb::ReverseNodeType(_flags_ptr->Value(index));
    }

    ArrowReverseNode* mutable_reverse_nodes(int64_t index) {
        if (_current_node_index != index) {
            _inner_node.set_key(std::string(_keys_ptr->GetView(index)));
            _inner_node.set_flag(pb::ReverseNodeType(_flags_ptr->Value(index)));
            _inner_node.set_weight(_weights_ptr->Value(index));
            _current_node_index = index;
        }
        return &_inner_node;
    }

private:
    void set_internal_info() {
        _rows = _result->num_rows();
        _keys_ptr =
            static_cast<arrow::StringArray*>(_result->column(0).get());
        _flags_ptr =
            static_cast<arrow::Int8Array*>(_result->column(1).get());
        _weights_ptr =
            static_cast<arrow::DoubleArray*>(_result->column(2).get());
    }
private:
    std::shared_ptr<arrow::RecordBatch> _result {nullptr};
    arrow::StringArray* _keys_ptr = nullptr;
    arrow::Int8Array* _flags_ptr = nullptr;
    arrow::DoubleArray* _weights_ptr = nullptr;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    arrow::StringBuilder _key_builder {pool};
    arrow::Int8Builder _flag_builder {pool};
    arrow::DoubleBuilder _weight_builder {pool};
    int64_t _rows = 0;
    int64_t _current_node_index = -1;
    ArrowReverseNode _inner_node;
    std::string _buffer;
};

}
