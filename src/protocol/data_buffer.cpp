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

#include "data_buffer.h"
#include "key_encoder.h"

namespace baikaldb {
DataBuffer::DataBuffer(size_t capacity) {
    if (capacity == 0) {
        return;
    }
    _data = (uint8_t *)malloc(capacity);
    if (_data == nullptr) {
        DB_FATAL("Failed to malloc memory.size:[%zu]", capacity);
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

bool DataBuffer::byte_array_append_size(size_t len, int is_pow) {
    if (_size + len <= _capacity) {
        return true;
    }
    size_t want_alloc = _size + len;
    if (want_alloc > MAX_ALLOC_BUF_SIZE) {
        DB_FATAL("want_alloc size[%zu] bigger than max[%zu]",
                        want_alloc, MAX_ALLOC_BUF_SIZE);
        return false;
    }
    if (is_pow == 1) {
        size_t acc_alloc = 1;
        while (acc_alloc < want_alloc) {
            acc_alloc <<= 1;
        }
        want_alloc = acc_alloc;
    }
    _data = (uint8_t *)realloc(_data, want_alloc);
    if (nullptr == _data) {
        DB_FATAL("malloc want_alloc=%zu append len=%zu failed", want_alloc, len);
        return false;
    }
    _capacity = want_alloc;
    return true;
}

// packet大于16M时插入4字节包头，导致内存移动，需要后续优化
bool DataBuffer::byte_array_insert_len(const uint8_t *data, size_t start_pos, size_t len) {
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
    memmove(_data + start_pos + len, _data + start_pos, _size - start_pos);
    memcpy(_data + start_pos, data, len);
    _size += len;
    return true;
}

bool DataBuffer::byte_array_append_len(const uint8_t *data, size_t len) {
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

bool DataBuffer::append_text_value(const ExprValue& value) {
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
            DB_FATAL("_capacity:%zu < size:%zu", _capacity, _size);
            return false;
        }
        size_t size = _capacity - _size;
        size_t len = 0;
        SerializeStatus ret = value.serialize_to_mysql_text_packet(buf, size, len);
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

void DataBuffer::datetime_to_buf(uint64_t datetime, uint8_t* buf, int& length, uint8_t type) {
    if (type == MYSQL_TYPE_DATE) {
        uint32_t date = (uint32_t)datetime;
        int year_month = ((date >> 5) & 0x1FFFF);
        int year = year_month / 13;
        int month = year_month % 13;
        int day = (date & 0x1F);
        buf[0] = year & 0xFF;
        buf[1] = (year >> 8) & 0xFF;
        buf[2] = month;
        buf[3] = day;
        length = 4;
    } else if (type == MYSQL_TYPE_TIMESTAMP || type == MYSQL_TYPE_DATETIME) {
        DateTime time_struct;
        datetime_to_time_struct(datetime, time_struct, type);
        length = time_struct.datetype_length();
        if (length == 11 || length == 7 || length == 4) {
            buf[0] = time_struct.year & 0xFF;
            buf[1] = (time_struct.year >> 8) & 0xFF;
            buf[2] = time_struct.month;
            buf[3] = time_struct.day;
            if (length == 11 || length == 7) {
                buf[4] = time_struct.hour;
                buf[5] = time_struct.minute;
                buf[6] = time_struct.second;
                if (length == 11) {
                    buf[7] = time_struct.macrosec & 0xFF;
                    buf[8] = (time_struct.macrosec >> 8) & 0xFF;
                    buf[9] = (time_struct.macrosec >> 16) & 0xFF;
                    buf[10] = (time_struct.macrosec >> 24) & 0xFF;
                }
            }
        }
    } else if (type == MYSQL_TYPE_TIME) {
        DateTime time_struct;
        datetime_to_time_struct(datetime, time_struct, type);
        length = time_struct.timetype_length();
        if (length == 8 || length == 12) {
            if (time_struct.is_negative) {
                buf[0] = 1;
            } else {
                buf[0] = 0;
            }
            buf[1] = time_struct.day & 0xFF;
            buf[2] = (time_struct.day >> 8) & 0xFF;
            buf[3] = (time_struct.day >> 16) & 0xFF;
            buf[4] = (time_struct.day >> 24) & 0xFF;
            buf[5] = time_struct.hour;
            buf[6] = time_struct.minute;
            buf[7] = time_struct.second;
            if (length == 12) {
                buf[8]  = time_struct.macrosec & 0xFF;
                buf[9]  = (time_struct.macrosec >> 8) & 0xFF;
                buf[10] = (time_struct.macrosec >> 16) & 0xFF;
                buf[11] = (time_struct.macrosec >> 24) & 0xFF;
            }
        }
    }
}

// https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
// https://dev.mysql.com/doc/internals/en/null-bitmap.html
// TODO: support other mysql types
bool DataBuffer::append_binary_value(const ExprValue& value, uint8_t type, uint8_t* null_map, int field_idx, int null_offset) {
    // DB_WARNING("result value is: %s type: %d", value.get_string().c_str(), type);
    if (value.is_null()) {
        int byte = ((field_idx + null_offset) / 8);
        int bit  = ((field_idx + null_offset) % 8);
        null_map[byte] |= (1 << bit);
        return true;
    }
    // string长度太长，单独处理
    if (value.is_string()) {
        return pack_length_coded_string(value.str_val, false);
    }
    if (type == MYSQL_TYPE_LONGLONG) {
        uint64_t val = value.get_numberic<uint64_t>();
        return byte_array_append_len((uint8_t*)&val, 8);
    } else if (type == MYSQL_TYPE_DOUBLE) {
        double val = value.get_numberic<double>();
        return byte_array_append_len((uint8_t*)&val, 8);
    } else if (type == MYSQL_TYPE_LONG || type == MYSQL_TYPE_INT24) {
        uint32_t val = value.get_numberic<uint32_t>();
        return byte_array_append_len((uint8_t*)&val, 4);
    } else if (type == MYSQL_TYPE_FLOAT) {
        float val = value.get_numberic<float>();
        return byte_array_append_len((uint8_t*)&val, 4);
    } else if (type == MYSQL_TYPE_SHORT) {
        uint16_t val = value.get_numberic<uint16_t>();
        return byte_array_append_len((uint8_t*)&val, 2);
    } else if (type == MYSQL_TYPE_TINY) {
        uint8_t val = value.get_numberic<uint8_t>();
        return byte_array_append_len((uint8_t*)&val, 1);
    } else if (type == MYSQL_TYPE_DATE || type == MYSQL_TYPE_TIMESTAMP
              || type == MYSQL_TYPE_DATETIME || type == MYSQL_TYPE_TIME) {
        uint8_t buf[12] = {0};
        uint64_t val = value.get_numberic<uint64_t>();
        int length = 0;
        datetime_to_buf(val, buf, length, type);
        if (!byte_array_append_len((uint8_t*)&length, 1)) {
            return false;
        }
        if (length > 0) {
            return byte_array_append_len(buf, length);
        }
    } else {
        DB_WARNING("unsupported binary type: %u", type);
        return false;
    }
    return true;
}

bool DataBuffer::byte_array_append_length_coded_binary(uint64_t num) {
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
    size_t length = str.size();
    if (is_null) {
        if (!byte_array_append_len(&null_byte, 1)) {
            DB_FATAL("Failed to append len.value:[%d],len:[1]", null_byte);
            return false;
        }
    } else if (length == 0) {
        if (!byte_array_append_len(&zero_byte, 1)) {
            DB_FATAL("Failed to append len.value:[%d],len:[1]", zero_byte);
            return false;
        }
    } else {
        if (!byte_array_append_length_coded_binary(length)) {
            DB_FATAL("Failed to append length coded binary.length:[%zu]", length);
            return false;
        }
        if (!byte_array_append_len((const uint8_t *)str.c_str(), length)) {
            DB_FATAL("Failed to append len. value:[%s],len:[%zu]", str.c_str(), length);
            return false;
        }
    }
    return true;
}

bool DataBuffer::network_queue_send_append(
        const uint8_t *data,
        size_t len,
        int packet_id,
        int append_data_later) {
    if (len <= 0 || (data == nullptr && append_data_later == 0)) {
        DB_FATAL("send_buf==NULL||len=%zu<= 0 ||(data == null&& append_data_later=%d == 0)",
            len, append_data_later);
        return false;
    }
    uint8_t header[4];
    header[0] = (len >> 0) & 0xFF;
    header[1] = (len >> 8) & 0xFF;
    header[2] = (len >> 16) & 0xFF;
    header[3] = packet_id & 0xFF;

    if (!byte_array_append_len(header, 4)) {
        DB_FATAL("Failed to append length.length:[4]");
        return false;
    }
    if (append_data_later == 0) {
        if (!byte_array_append_len(data, len)) {
            DB_FATAL("Failed to append length.length:[%zu]", len);
            return false;
        }
    }
    return true;
}

} // namespace baikal
