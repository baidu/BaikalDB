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

#include "expr_value.h"
#include "hll_common.h"

namespace baikaldb {
DEFINE_bool(use_double_conversion, true, "use_double_conversion");
DEFINE_bool(double_use_all_precision, false, "Double precision output compatibility with MySQL, eg: 1.003*100=100.29999999999998");
SerializeStatus ExprValue::serialize_to_mysql_text_packet(char* buf, size_t size, size_t& len) const {
    if (size < 1) {
        len = 1;
        return STMPS_NEED_RESIZE;
    }
    switch (type) {
        case pb::NULL_TYPE:
        case pb::TDIGEST: {
            uint8_t null_byte = 0xfb;
            memcpy(buf, (const uint8_t*)&null_byte, 1);
            len = 1;
            return STMPS_SUCCESS;
        }
        case pb::BOOL:
        case pb::INT8:
        case pb::INT16:
        case pb::INT32:
        case pb::INT64: {
            int64_t value = get_numberic<int64_t>();
            size_t body_len = 0;
            SerializeStatus ret = to_string(value, buf + 1, size - 1, body_len);
            len = body_len + 1;
            if (ret != STMPS_SUCCESS) {
                return ret;
            }
            // byte_array_append_length_coded_binary(body_len < 251LL)
            buf[0] = (uint8_t)(body_len & 0xff);
            return STMPS_SUCCESS;
        }
        case pb::UINT8:
        case pb::UINT16:
        case pb::UINT32:
        case pb::UINT64: {
            uint64_t value = get_numberic<uint64_t>();
            size_t body_len = 0;
            SerializeStatus ret = to_string(value, buf + 1, size - 1, body_len);
            len = body_len + 1;
            if (ret != STMPS_SUCCESS) {
                return ret;
            }
            // byte_array_append_length_coded_binary(body_len < 251LL)
            buf[0] = (uint8_t)(body_len & 0xff);
            return STMPS_SUCCESS;
        }
        case pb::FLOAT: {
            size_t body_len = 0;
            char tmp_buf[24] = {0};
            body_len = parser::float_to_string(_u.float_val, float_precision_len, tmp_buf, sizeof(tmp_buf));

            len = body_len + 1;
            if (len > size) {
                return STMPS_NEED_RESIZE;
            }
            // byte_array_append_length_coded_binary(body_len < 251LL)
            buf[0] = (uint8_t)(body_len & 0xff);
            memcpy(buf + 1, tmp_buf, body_len);
            return STMPS_SUCCESS;
        }
        case pb::DOUBLE: {
            size_t body_len = 0;
            char tmp_buf[50] = {0};
            body_len = parser::double_to_string(_u.double_val, float_precision_len, tmp_buf, sizeof(tmp_buf));
            len = body_len + 1;
            if (len > size) {
                return STMPS_NEED_RESIZE;
            }
            // byte_array_append_length_coded_binary(body_len < 251LL)
            buf[0] = (uint8_t)(body_len & 0xff);
            memcpy(buf + 1, tmp_buf, body_len);
            return STMPS_SUCCESS;
        }
        case pb::HLL: {
            int64_t value = hll::hll_estimate(*this);
            size_t body_len = 0;
            SerializeStatus ret = to_string(value, buf + 1, size - 1, body_len);
            len = body_len + 1;
            if (ret != STMPS_SUCCESS) {
                return ret;
            }
            buf[0] = (unsigned char)(body_len & 0xff);
            return STMPS_SUCCESS;
        }
        default: {
            uint8_t null_byte = 0xfb;
            memcpy(buf, (const uint8_t*)&null_byte, 1);
            len = 1;
            return STMPS_SUCCESS;
        }
    }
}
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
