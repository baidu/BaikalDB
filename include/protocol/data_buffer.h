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
#include <memory>
#include "common.h"
#include "expr_value.h"

namespace baikaldb {

const size_t MAX_ALLOC_BUF_SIZE = (1024 * 1024 * 1024 * 2ULL);
const size_t DFT_ALLOC_BUF_SIZE = (1024 * 1024);

class DataBuffer {
public:
    DataBuffer(size_t capacity = DFT_ALLOC_BUF_SIZE);
    ~DataBuffer();

    bool byte_array_append_size(size_t len, int is_pow);
    bool byte_array_insert_len(const uint8_t *data, size_t start_pos, size_t len);
    bool byte_array_append_len(const uint8_t *data, size_t len);
    bool append_text_value(const ExprValue& value);
    bool append_binary_value(const ExprValue& value, uint8_t type, uint8_t* null_map, int field_idx, int null_offset);
    bool byte_array_append_length_coded_binary(uint64_t num);
    bool pack_length_coded_string(const std::string& str, bool is_null);
    bool network_queue_send_append(const uint8_t* data, size_t len, 
                                    int packet_id, int append_data_later);
    void byte_array_clear();
    void datetime_to_buf(uint64_t datetime, uint8_t* buf, int& length, uint8_t type);

public:
    uint8_t*        _data = 0;
    size_t          _size = 0;
    size_t          _capacity = 0;
}; 

typedef std::shared_ptr<DataBuffer> SmartBuffer;

} // namespace baikal
