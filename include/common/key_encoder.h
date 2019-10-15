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
#include <cstdint>

namespace baikaldb {

const uint16_t SIGN_MASK_08 = 0x80;
const uint16_t SIGN_MASK_16 = 0x8000;
const uint32_t SIGN_MASK_32 = 0x80000000;
const uint64_t SIGN_MASK_64 = 0x8000000000000000;

class KeyEncoder {
public:
    virtual ~KeyEncoder() {}

    static bool is_big_endian() {
        int16_t i = 0x1234;
        char *c = reinterpret_cast<char*>(&i);
        return (c[1] == 0x34) && (c[0] == 0x12);
    }

    static uint16_t to_endian_u16(uint16_t in) {
        static bool is_big = is_big_endian();
        if (is_big) {
            return in;
        }
        return ((in & 0x00FF) << 8) | ((in & 0xFF00) >> 8);
    }

    static uint32_t to_endian_u32(uint32_t in) {
        static bool is_big = is_big_endian();
        if (is_big) {
            return in;
        }
        return ((in & 0x000000FF) << 24) |
               ((in & 0x0000FF00) << 8) |
               ((in & 0x00FF0000) >> 8) |
               ((in & 0xFF000000) >> 24);
    }

    static uint64_t to_endian_u64(uint64_t in) {
        static bool is_big = is_big_endian();
        if (is_big) {
            return in;
        }
        return ((in & 0x00000000000000FF) << 56) |
               ((in & 0x000000000000FF00) << 40) |
               ((in & 0x0000000000FF0000) << 24) |
               ((in & 0x00000000FF000000) << 8) |
               ((in & 0x000000FF00000000) >> 8) |
               ((in & 0x0000FF0000000000) >> 24) |
               ((in & 0x00FF000000000000) >> 40) |
               ((in & 0xFF00000000000000) >> 56);
    }

    //mem-comparable transform for int64
    static uint8_t encode_i8(int8_t in) {
        return (static_cast<uint8_t>(in)) ^ SIGN_MASK_08;
    }

    static int8_t decode_i8(uint8_t in) {
        return (static_cast<int8_t>(in ^ SIGN_MASK_08));
    }

    static uint16_t encode_i16(int16_t in) {
        return (static_cast<uint16_t>(in)) ^ SIGN_MASK_16;
    }

    static int16_t decode_i16(uint16_t in) {
        return (static_cast<int16_t>(in ^ SIGN_MASK_16));
    }

    static uint32_t encode_i32(int32_t in) {
        return (static_cast<uint32_t>(in)) ^ SIGN_MASK_32;
    }

    static int32_t decode_i32(uint32_t in) {
        return (static_cast<int32_t>(in ^ SIGN_MASK_32));
    }

    static uint64_t encode_i64(int64_t in) {
        return (static_cast<uint64_t>(in)) ^ SIGN_MASK_64;
    }

    static int64_t decode_i64(uint64_t in) {
        return (static_cast<int64_t>(in ^ SIGN_MASK_64));
    }

    ///1. bit translate to u64
    ///2. if positive, then flip the highest bit
    ///3. if neg, then flip all bits
    static uint32_t encode_f32(float in) {
        uint32_t res = *reinterpret_cast<uint32_t*>(&in);
        if (in > 0) {
            return (res | SIGN_MASK_32);
        } else {
            return ~res;
        }
    }

    static float decode_f32(uint32_t in) {
        if (((in & SIGN_MASK_32) >> 31) > 0) {
            in &= (~SIGN_MASK_32);
        } else {
            in = ~in;
        }
        return *reinterpret_cast<float*>(&in);
    }

    static uint64_t encode_f64(double in) {
        uint64_t res = *reinterpret_cast<uint64_t*>(&in);
        if (in > 0) {
            return (res | SIGN_MASK_64);
        } else {
            return ~res;
        }
    }

    static double decode_f64(uint64_t in) {
        if (((in & SIGN_MASK_64) >> 63) > 0) {
            in &= (~SIGN_MASK_64);
        } else {
            in = ~in;
        }
        return *reinterpret_cast<double*>(&in);
    }

private:
    KeyEncoder();
};
}

