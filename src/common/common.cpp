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

#include "common.h"
#include <unordered_map>
#include <cstdlib>

#ifdef BAIDU_INTERNAL
#include <pb_to_json.h>
#include <json_to_pb.h>
#else
#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>
#endif

#include "rocksdb/slice.h"
#include <boost/algorithm/string.hpp>
#include <google/protobuf/descriptor.pb.h>
#include "rocksdb/slice.h"
#include "expr_value.h"

using google::protobuf::FieldDescriptorProto;

namespace baikaldb {
DEFINE_int32(raft_write_concurrency, 40, "raft_write concurrency, default:40");
DEFINE_int32(service_write_concurrency, 40, "service_write concurrency, default:40");
DEFINE_int32(snapshot_load_num, 8, "snapshot load concurrency, default 8");
DECLARE_string(default_physical_room);
DEFINE_bool(enable_debug, false, "open DB_DEBUG log");
DEFINE_bool(enable_self_trace, true, "open SELF_TRACE log");
int64_t timestamp_diff(timeval _start, timeval _end) {
    return (_end.tv_sec - _start.tv_sec) * 1000000 
        + (_end.tv_usec-_start.tv_usec); //macro second
}

std::string pb2json(const google::protobuf::Message& message) {
    std::string json;
    std::string error;
#ifdef BAIDU_INTERNAL
    if (ProtoMessageToJson(message, &json, &error)) {
#else
    if (json2pb::ProtoMessageToJson(message, &json, &error)) {
#endif
        return json;
    }
    return error;
}

std::string json2pb(const std::string& json, google::protobuf::Message* message) {
    std::string error;
#ifdef BAIDU_INTERNAL
    if (JsonToProtoMessage(json, message, &error)) {
#else
    if (json2pb::JsonToProtoMessage(json, message, &error)) {
#endif
        return "";
    }
    return error;
}

// STMPS_SUCCESS,
// STMPS_FAIL,
// STMPS_NEED_RESIZE
SerializeStatus to_string (int32_t number, char *buf, size_t size, size_t& len) {
    if (number == 0U) {
        len = 1;
        if (size < 1) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    if (number == INT32_MIN) {
        len = 11;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        memcpy(buf, "-2147483648", len);
        return STMPS_SUCCESS;
    }
    len = 0;
    if (number < 0) {
        number = -number;
        buf[0] = '-';
        len++;
    }

    int32_t n = number;
    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }
    return STMPS_SUCCESS;
}

std::string to_string(int32_t number)
{
    char buffer[16];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 16, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (uint32_t number, char *buf, size_t size, size_t& len) {

    if (number == 0U) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    len = 0;
    uint32_t n = number;

    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }

    return STMPS_SUCCESS;
}

std::string to_string(uint32_t number)
{
    char buffer[16];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 16, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (int64_t number, char *buf, size_t size, size_t& len) {
    if (number == 0UL) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }

    if (number == INT64_MIN) {
        len = 20;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        memcpy(buf, "-9223372036854775808", len);
        return STMPS_SUCCESS;
    }
    len = 0;
    if (number < 0) {
        number = -number;
        buf[0] = '-';
        len++;
    }

    int64_t n = number;
    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }

    return STMPS_SUCCESS;
}

std::string to_string(int64_t number)
{
    char buffer[32];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 32, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (uint64_t number, char *buf, size_t size, size_t& len) {
    if (number == 0UL) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    len = 0;
    uint64_t n = number;

    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }

    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }
    return STMPS_SUCCESS;
}

std::string to_string(uint64_t number)
{
    char buffer[32];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 32, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

std::string remove_quote(const char* str, char quote) {
    uint32_t len = strlen(str);
    if (len > 2 && str[0] == quote && str[len-1] == quote) {
        return std::string(str + 1, len - 2);
    } else {
        return std::string(str);
    }
}

std::string str_to_hex(const std::string& str) {
    return rocksdb::Slice(str).ToString(true).c_str();
}

void stripslashes(std::string& str) {
    size_t slow = 0;
    size_t fast = 0;
    bool has_slash = false;
    static std::unordered_map<char, char> trans_map = {
        {'\\', '\\'},
        {'\"', '\"'},
        {'\'', '\''},
        {'r', '\r'},
        {'t', '\t'},
        {'n', '\n'},
        {'b', '\b'},
        {'Z', '\x1A'},
    };
    while (fast < str.size()) {
        if (has_slash) {
            if (trans_map.count(str[fast]) == 1) {
                str[slow++] = trans_map[str[fast++]];
            } else if (str[fast] == '%' || str[fast] == '_') {
                // like中的特殊符号，需要补全'\'
                str[slow++] = '\\';
                str[slow++] = str[fast++];
            }
            has_slash = false;
        } else {
            if (str[fast] == '\\') {
                has_slash = true;
                fast++;
            } else if ((str[fast] & 0x80) != 0) {
                //gbk中文字符处理
                str[slow++] = str[fast++];
                if (fast >= str.size()) {
                    // 去除最后半个gbk中文
                    //--slow;
                    break;
                }
                str[slow++] = str[fast++];
            } else {
                str[slow++] = str[fast++];
            }
        }
    }
    str.resize(slow);
}

int primitive_to_proto_type(pb::PrimitiveType type) {
    using google::protobuf::FieldDescriptorProto;
    static std::unordered_map<int32_t, int32_t> _mysql_pb_type_mapping = {
        { pb::INT8,         FieldDescriptorProto::TYPE_SINT32 },
        { pb::INT16,        FieldDescriptorProto::TYPE_SINT32 },
        { pb::INT32,        FieldDescriptorProto::TYPE_SINT32 },
        { pb::INT64,        FieldDescriptorProto::TYPE_SINT64 },
        { pb::UINT8,        FieldDescriptorProto::TYPE_UINT32 },
        { pb::UINT16,       FieldDescriptorProto::TYPE_UINT32 },
        { pb::UINT32,       FieldDescriptorProto::TYPE_UINT32 },
        { pb::UINT64,       FieldDescriptorProto::TYPE_UINT64 },
        { pb::FLOAT,        FieldDescriptorProto::TYPE_FLOAT  },
        { pb::DOUBLE,       FieldDescriptorProto::TYPE_DOUBLE },
        { pb::STRING,       FieldDescriptorProto::TYPE_BYTES  },
        { pb::DATETIME,     FieldDescriptorProto::TYPE_FIXED64},
        { pb::TIMESTAMP,    FieldDescriptorProto::TYPE_FIXED32},
        { pb::DATE,         FieldDescriptorProto::TYPE_FIXED32},
        { pb::TIME,         FieldDescriptorProto::TYPE_SFIXED32},
        { pb::HLL,          FieldDescriptorProto::TYPE_BYTES},
        { pb::BOOL,         FieldDescriptorProto::TYPE_BOOL   }
    };
    if (_mysql_pb_type_mapping.count(type) == 0) {
        DB_WARNING("mysql_type %d not supported.", type);
        return -1;
    }
    return _mysql_pb_type_mapping[type];
}
int get_physical_room(const std::string& ip_and_port_str, std::string& physical_room) {
#ifdef BAIDU_INTERNAL
    butil::EndPoint point;
    int ret = butil::str2endpoint(ip_and_port_str.c_str(), &point);
    if (ret != 0) {
        DB_WARNING("instance:%s to endpoint fail, ret:%d", ip_and_port_str.c_str(), ret);
        return ret;
    }
    std::string host;
    ret = butil::endpoint2hostname(point, &host);
    if (ret != 0) {
        DB_WARNING("endpoint to hostname fail, ret:%d", ret);
        return ret;
    }
    DB_DEBUG("host:%s", host.c_str());
    auto begin = host.find(".");
    auto end = host.find(":");
    if (begin == std::string::npos) {
        DB_WARNING("host:%s to physical room fail", host.c_str()); 
        return -1;
    }
    if (end == std::string::npos) {
        end = host.size();
    }
    physical_room = std::string(host, begin + 1, end - begin -1);
    return 0;
#else
    physical_room = FLAGS_default_physical_room;  
    return 0;
#endif
}

int get_instance_from_bns(int* ret,
                          const std::string& bns_name,
                          std::vector<std::string>& instances,
                          bool need_alive) {
#ifdef BAIDU_INTERNAL
    instances.clear();
    BnsInput input;
    BnsOutput output;
    input.set_service_name(bns_name);
    input.set_type(0);
    *ret = webfoot::get_instance_by_service(input, &output);
    // bns service not exist
    if (*ret == webfoot::WEBFOOT_RET_SUCCESS ||
            *ret == webfoot::WEBFOOT_SERVICE_BEYOND_THRSHOLD) {
        for (int i = 0; i < output.instance_size(); ++i) {
            if (output.instance(i).status() == 0 || !need_alive) {
                instances.push_back(output.instance(i).host_ip() + ":" 
                        + boost::lexical_cast<std::string>(output.instance(i).port()));
            }   
        }   
        return 0;
    }   
    DB_WARNING("get instance from service fail, bns_name:%s, ret:%d",
            bns_name.c_str(), *ret);
    return -1; 
#else
    return -1;
#endif
}
}  // baikaldb
