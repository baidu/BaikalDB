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

#include "data_buffer.h"

namespace baikaldb {
DataBuffer::DataBuffer(uint32_t capacity) {
    if (capacity == 0) {
        return;
    }
    _data = (uint8_t *)malloc(capacity);
    if (_data == nullptr) {
        DB_FATAL("Failed to malloc memory.size:[%lu]", capacity);
        return;
    }
    _capacity = capacity;
    return;
}

DataBuffer::~DataBuffer() {
    if (_data != nullptr) {
        free(_data);
    }
    _data = nullptr;
    _size = 0;
    _capacity = 0;
}

void DataBuffer::byte_array_clear() {
    if (_capacity > DFT_ALLOC_BUF_SIZE) {
        uint8_t *p = nullptr;
        if (nullptr == (p = (uint8_t *)malloc(DFT_ALLOC_BUF_SIZE))) {
            DB_WARNING("byte_array_clear resize buf error.");
        } else {
            if (nullptr != _data) {
                free(_data);
            }
            _data = p;
            _capacity = DFT_ALLOC_BUF_SIZE;
        }
    }
    _size = 0;
    return;
}

bool DataBuffer::byte_array_append_size(int len, int is_pow) {
    if (_size + len <= _capacity) {
        return true;
    }
    int want_alloc = _size + len;
    if (want_alloc > (int)MAX_ALLOC_BUF_SIZE) {
        DB_FATAL("want_alloc size[%d] bigger than max[%d]",
                        want_alloc, MAX_ALLOC_BUF_SIZE);
        return false;
    }
    if (is_pow == 1) {
        int acc_alloc = 1;
        while (acc_alloc < want_alloc) {
            acc_alloc <<= 1;
        }
        want_alloc = acc_alloc;
    }
    _data = (uint8_t *)realloc(_data, want_alloc);
    if (nullptr == _data) {
        DB_FATAL("malloc want_alloc=%d append len=%d failed", want_alloc, len);
        return false;
    }
    _capacity = want_alloc;
    return true;
}
bool DataBuffer::byte_array_append_len(const uint8_t *data, int len) {
    if (data == nullptr) {
        DB_FATAL("data buffer is null");
        return false;
    }
    if (len <= 0) {
        return true;
    }
    if (!byte_array_append_size(len, 1)) {
        DB_FATAL("byte_array_append_size fail");
        return false;
    }
    memcpy(_data + _size, data, len);
    _size += len;
    return true;
}

bool DataBuffer::byte_array_append_value(const ExprValue& value) {
    do {
        // string长度太长，单独处理
        if (value.is_string()) {
            return pack_length_coded_string(value.str_val, false);
        } else if (value.is_datetime() || value.is_timestamp() || 
                value.is_date() || value.is_time()) {
            return pack_length_coded_string(value.get_string(), false);
        }
        
        char* buf = (char*)_data + _size;
        if (_capacity < _size) {
            DB_FATAL("_capacity:%u < size:%u", _capacity, _size);
            return false;
        }
        size_t size = _capacity - _size;
        size_t len = 0;
        SerializeStatus ret = value.serialize_to_mysql_packet(buf, size, len);
        if (ret == STMPS_SUCCESS) {
            _size += len;
            return true;
        } else if (ret == STMPS_NEED_RESIZE) {
            if (!byte_array_append_size(len, 1)) {
                DB_FATAL("byte_array_append_size fail");
                return false;
            }
            continue;
        } else {
            DB_FATAL("serialize_to_mysql_packet ret:%d", ret);
            return false;
        }
    } while (true);
}

bool DataBuffer::byte_array_append_length_coded_binary(unsigned long long num) {
    uint8_t bytes[10];
    if (num < 251LL) {
        bytes[0] = (uint8_t)(num & 0xff);
        return byte_array_append_len(bytes, 1);
    }
    if (num < 0x10000LL) {
        bytes[0] = 252;
        bytes[1] = (uint8_t)(num & 0xff);
        bytes[2] = (uint8_t)((num >> 8) & 0xff);
        return byte_array_append_len(bytes, 3);
    }
    if (num < 0x1000000LL) {
        bytes[0] = 253;
        bytes[1] = (uint8_t)(num & 0xff);
        bytes[2] = (uint8_t)((num >> 8) & 0xff);
        bytes[3] = (uint8_t)((num >> 16) & 0xff);
        return byte_array_append_len(bytes, 4);
    }
    bytes[0] = 254;
    for (uint32_t i = 0; i < 8; i++)
    {
        bytes[i + 1] = (uint8_t)(num & 0xff);
        num = num >> 8;
    }
    return byte_array_append_len(bytes, 9);
}

bool DataBuffer::pack_length_coded_string(const std::string& str, bool is_null) {
    uint8_t null_byte = 0xfb;
    uint8_t zero_byte = 0;
    uint32_t length = str.size();
    if (is_null) {
        if (!byte_array_append_len(&null_byte, 1)) {
            DB_FATAL("Failed to append len.value:[%s],len:[1]", null_byte);
            return false;
        }
    } else if (length == 0) {
        if (!byte_array_append_len(&zero_byte, 1)) {
            DB_FATAL("Failed to append len.value:[%s],len:[1]", zero_byte);
            return false;
        }
    } else {
        if (!byte_array_append_length_coded_binary(length)) {
            DB_FATAL("Failed to append length coded binary.length:[%u]", length);
            return false;
        }
        if (!byte_array_append_len((const uint8_t *)str.c_str(), length)) {
            DB_FATAL("Failed to append len. value:[%s],len:[%llu]", str.c_str(), length);
            return false;
        }
    }
    return true;
}

bool DataBuffer::network_queue_send_append(
        const uint8_t *data,
        int len,
        uint8_t packet_id,
        int append_data_later) {
    if (len <= 0 || (data == nullptr && append_data_later == 0)) {
        DB_FATAL("send_buf==NULL||len=%d<= 0 ||(data == null&& append_data_later=%d == 0)",
            len, append_data_later);
        return false;
    }
    uint8_t header[4];
    header[0] = (len >> 0) & 0xFF;
    header[1] = (len >> 8) & 0xFF;
    header[2] = (len >> 16) & 0xFF;
    header[3] = packet_id; // out of 255 ??

    if (!byte_array_append_len(header, 4)) {
        DB_FATAL("Failed to append length.length:[4]");
        return false;
    }
    if (append_data_later == 0) {
        if (!byte_array_append_len(data, len)) {
            DB_FATAL("Failed to append length.length:[%d]", len);
            return false;
        }
    }
    return true;
}

} // namespace baikal
