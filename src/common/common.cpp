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

#include "common.h"
#include <unordered_map>
#include <cstdlib>
#include <cctype>
#include <sstream>

#ifdef BAIDU_INTERNAL
#include <pb_to_json.h>
#include <json_to_pb.h>
#else
#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>
#endif

#ifdef BAIDU_INTERNAL
#include <base/endpoint.h>
#include <baidu/rpc/channel.h>
#include <baidu/rpc/server.h>
#include <baidu/rpc/controller.h>
#include <base/file_util.h>
#else
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <butil/file_util.h>
#endif

#include "rocksdb/slice.h"
#include <boost/algorithm/string.hpp>
#include <google/protobuf/descriptor.pb.h>
#include "rocksdb/slice.h"
#include "expr_value.h"
#include "re2/re2.h"

#include <boost/algorithm/string.hpp>

using google::protobuf::FieldDescriptorProto;

namespace baikaldb {
DEFINE_int32(raft_write_concurrency, 40, "raft_write concurrency, default:40");
DEFINE_int32(service_write_concurrency, 40, "service_write concurrency, default:40");
DEFINE_int32(snapshot_load_num, 4, "snapshot load concurrency, default 4");
DEFINE_int32(baikal_heartbeat_concurrency, 10, "baikal heartbeat concurrency, default:10");
DEFINE_int64(incremental_info_gc_time, 600 * 1000 * 1000, "time interval to clear incremental info");
DECLARE_string(default_physical_room);
DEFINE_bool(enable_debug, false, "open DB_DEBUG log");
DEFINE_bool(enable_self_trace, true, "open SELF_TRACE log");
DEFINE_bool(servitysinglelog, true, "diff servity message in seperate logfile");
DEFINE_bool(open_service_write_concurrency, true, "open service_write_concurrency, default: true");
DEFINE_int32(baikal_heartbeat_interval_us, 10 * 1000 * 1000, "baikal_heartbeat_interval(us)");
DEFINE_bool(schema_ignore_case, false, "whether ignore case when match db/table name");
DEFINE_bool(disambiguate_select_name, false, "whether use the first when select name is ambiguous, default false");
DEFINE_int32(new_sign_read_concurrency, 10, "new_sign_read concurrency, default:20");
DEFINE_bool(open_new_sign_read_concurrency, false, "open new_sign_read concurrency, default: false");
DEFINE_bool(need_verify_ddl_permission, false, "default true");
DEFINE_bool(use_cond_decrease_signal, false, "default false");


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
    bool negtive = false;
    if (number < 0) {
        number = -number;
        negtive = true;
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
    if (negtive) {
        buf[0] = '-';
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
    bool negtive = false;
    if (number < 0) {
        number = -number;
        negtive = true;
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
    if (negtive) {
        buf[0] = '-';
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
    return rocksdb::Slice(str).ToString(true);
}

bool is_digits(const std::string& str) {
    return std::all_of(str.begin(), str.end(), ::isdigit);
}

void stripslashes(std::string& str, bool is_gbk) {
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
            } else if (is_gbk && (str[fast] & 0x80) != 0) {
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

void update_op_version(pb::SchemaConf* p_conf, const std::string& desc) {
    auto version = p_conf->has_op_version() ? p_conf->op_version() : 0;
    p_conf->set_op_version(version + 1);
    p_conf->set_op_desc(desc);
}

void update_schema_conf_common(const std::string& table_name, const pb::SchemaConf& schema_conf, pb::SchemaConf* p_conf) {
        const google::protobuf::Reflection* src_reflection = schema_conf.GetReflection();
        //const google::protobuf::Descriptor* src_descriptor = schema_conf.GetDescriptor();
        const google::protobuf::Reflection* dst_reflection = p_conf->GetReflection();
        const google::protobuf::Descriptor* dst_descriptor = p_conf->GetDescriptor();
        const google::protobuf::FieldDescriptor* src_field = nullptr;
        const google::protobuf::FieldDescriptor* dst_field = nullptr;

        std::vector<const google::protobuf::FieldDescriptor*> src_field_list;
        src_reflection->ListFields(schema_conf, &src_field_list);
        for (int i = 0; i < (int)src_field_list.size(); ++i) {
            src_field = src_field_list[i];
            if (src_field == nullptr) {
                continue;
            }

            dst_field = dst_descriptor->FindFieldByName(src_field->name());
            if (dst_field == nullptr) {
                continue;
            }

            if (src_field->cpp_type() != dst_field->cpp_type()) {
                continue;
            }

            auto type = src_field->cpp_type();
            switch (type) {
                case google::protobuf::FieldDescriptor::CPPTYPE_INT32: {
                    auto src_value = src_reflection->GetInt32(schema_conf, src_field);
                    dst_reflection->SetInt32(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT32: {
                    auto src_value = src_reflection->GetUInt32(schema_conf, src_field);
                    dst_reflection->SetUInt32(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_INT64: {
                    auto src_value = src_reflection->GetInt64(schema_conf, src_field);
                    dst_reflection->SetInt64(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT64: {
                    auto src_value = src_reflection->GetUInt64(schema_conf, src_field);
                    dst_reflection->SetUInt64(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT: {
                    auto src_value = src_reflection->GetFloat(schema_conf, src_field);
                    dst_reflection->SetFloat(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE: {
                    auto src_value = src_reflection->GetDouble(schema_conf, src_field);
                    dst_reflection->SetDouble(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_BOOL: {
                    auto src_value = src_reflection->GetBool(schema_conf, src_field);
                    dst_reflection->SetBool(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
                    auto src_value = src_reflection->GetString(schema_conf, src_field);
                    dst_reflection->SetString(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
                    auto src_value = src_reflection->GetEnum(schema_conf, src_field);
                    dst_reflection->SetEnum(p_conf, dst_field, src_value);
                } break;
                default: {
                    break;
                }
            }

        }
        DB_WARNING("%s schema conf UPDATE TO : %s", table_name.c_str(), schema_conf.ShortDebugString().c_str());
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
        { pb::BOOL,         FieldDescriptorProto::TYPE_BOOL},
        { pb::BITMAP,       FieldDescriptorProto::TYPE_BYTES},
        { pb::TDIGEST,      FieldDescriptorProto::TYPE_BYTES},
        { pb::NULL_TYPE,    FieldDescriptorProto::TYPE_BOOL}
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
                          bool need_alive,
                          bool white_list) {
#ifdef BAIDU_INTERNAL
    instances.clear();
    BnsInput input;
    BnsOutput output;
    input.set_service_name(bns_name);
    input.set_type(0);
    if (white_list) {
        input.set_type(1);
    }
    *ret = webfoot::get_instance_by_service(input, &output);
    // bns service not exist
    if (*ret == webfoot::WEBFOOT_RET_SUCCESS ||
            *ret == webfoot::WEBFOOT_SERVICE_BEYOND_THRSHOLD) {
        for (int i = 0; i < output.instance_size(); ++i) {
            if (output.instance(i).status() == 0 || !need_alive) {
                instances.push_back(output.instance(i).host_ip() + ":" 
                        + std::to_string(output.instance(i).port()));
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

bool same_with_container_id_and_address(const std::string& container_id, const std::string& address) {
#ifdef BAIDU_INTERNAL
    if (container_id.empty()) {
        return true;
    }
    int bns_ret = 0;
    std::vector<std::string> instances;
    int ret = get_instance_from_bns(&bns_ret, container_id, instances, false);
    if (ret != 0) {
        return true;
    }
    if (instances.size() != 1) {
        return true;
    }
    if (instances[0] == address) {
        return true;
    } else {
        DB_WARNING("diff with container_id:%s and address:%s", container_id.c_str(), address.c_str());
        return false;
    }
#else
    return true;
#endif
}

#ifdef BAIDU_INTERNAL
inline int get_dummy_port(std::string& dummy_port, const std::string& multi_port) {
    bool has_dummy_port = false;
    std::vector<std::string> split_vec;
    boost::split(split_vec, multi_port, boost::is_any_of(","));
    for (const std::string& info : split_vec) {
        auto find_pos = info.find("dummy");
        if (find_pos == info.npos) {
            continue;
        }

        std::vector<std::string> vec;
        boost::split(vec, info, boost::is_any_of("="));
        if (vec.size() == 2 && vec[1] != "") {
            dummy_port = vec[1];
        }
    }

    if (!has_dummy_port) {
        return -1;
    } else {
        return 0;
    }
}
#endif
int get_multi_port_from_bns(int* ret,
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
                std::string dummy_port = "";
                if (get_dummy_port(dummy_port, output.instance(i).multi_port()) != 0) {
                    instances.push_back(output.instance(i).host_ip() + ":" + dummy_port);
                }
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

static unsigned char to_hex(unsigned char x)   {   
    return  x > 9 ? x + 55 : x + 48;   
}

static unsigned char from_hex(unsigned char x) {   
    unsigned char y = '\0';  
    if (x >= 'A' && x <= 'Z') { 
        y = x - 'A' + 10;  
    } else if (x >= 'a' && x <= 'z') { 
        y = x - 'a' + 10;  
    } else if (x >= '0' && x <= '9') {
        y = x - '0';  
    }
    return y;  
}  

std::string url_decode(const std::string& str) {
    std::string strTemp = "";  
    size_t length = str.length();  
    for (size_t i = 0; i < length; i++)  {  
        if (str[i] == '+') {
            strTemp += ' ';
        }  else if (str[i] == '%')  {  
            unsigned char high = from_hex((unsigned char)str[++i]);  
            unsigned char low = from_hex((unsigned char)str[++i]);  
            strTemp += high * 16 + low;  
        }  
        else strTemp += str[i];  
    }  
    return strTemp;  
}

std::vector<std::string> string_split(const std::string &s, char delim) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, delim)) {
        elems.emplace_back(item);
        // elems.push_back(std::move(item));
    }
    return elems;
}

int64_t parse_snapshot_index_from_path(const std::string& snapshot_path, bool use_dirname) {
    butil::FilePath path(snapshot_path);
    std::string tmp_path;
    if (use_dirname) {
        tmp_path = path.DirName().BaseName().value();
    } else {
        tmp_path = path.BaseName().value();
    }
    std::vector<std::string> split_vec;
    std::vector<std::string> snapshot_index_vec;
    boost::split(split_vec, tmp_path, boost::is_any_of("/"));
    boost::split(snapshot_index_vec, split_vec.back(), boost::is_any_of("_"));
    int64_t snapshot_index = 0;
    if (snapshot_index_vec.size() == 2) {
        snapshot_index = atoll(snapshot_index_vec[1].c_str());
    }
    return snapshot_index;
}

bool ends_with(const std::string &str, const std::string &ending) {
    if (str.length() < ending.length()) {
        return false;
    }
    return str.compare(str.length() - ending.length(), ending.length(), ending) == 0;
}

std::string string_trim(std::string& str) {
    size_t first = str.find_first_not_of(' ');
    if (first == std::string::npos)
        return "";
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, (last-first+1));
}

const std::string& rand_peer(pb::RegionInfo& info) {
    if (info.peers_size() == 0) {
        return info.leader();
    }
    uint32_t i = butil::fast_rand() % info.peers_size();
    return info.peers(i);
}

void other_peer_to_leader(pb::RegionInfo& info) {
    auto peer = rand_peer(info);
    if (peer != info.leader()) {
        info.set_leader(peer);
        return;
    }
    for (auto& peer : info.peers()) {
        if (peer != info.leader()) {
            info.set_leader(peer);
            break;
        }
    }
}

std::string url_encode(const std::string& str) {
    std::string strTemp = "";  
    size_t length = str.length();  
    for (size_t i = 0; i < length; i++) {  
        if (isalnum((unsigned char)str[i]) ||   
                (str[i] == '-') ||  
                (str[i] == '_') ||   
                (str[i] == '.') ||   
                (str[i] == '~')) {
            strTemp += str[i];  
        } else if (str[i] == ' ') {
            strTemp += "+";  
        } else  {  
            strTemp += '%';  
            strTemp += to_hex((unsigned char)str[i] >> 4);  
            strTemp += to_hex((unsigned char)str[i] % 16);  
        }  
    }  
    return strTemp; 
}

int brpc_with_http(const std::string& host, const std::string& url, std::string& response) {
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    if (channel.Init(host.c_str() /*any url*/, &options) != 0) {
        DB_WARNING("Fail to initialize channel, host: %s, url: %s", host.c_str(), url.c_str());
        return -1;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url;  // 设置为待访问的URL
    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL/*done*/);
    DB_DEBUG("http status code : %d",cntl.http_response().status_code());
    response = cntl.response_attachment().to_string();
    DB_WARNING("host: %s, url: %s, response: %s", host.c_str(), url.c_str(), response.c_str());
    return 0;
}

std::string store_or_db_bns_to_meta_bns(const std::string& bns) {
    auto serv_pos = bns.find(".serv");
    std::string bns_group = bns;
    if (serv_pos != bns.npos) {
        bns_group = bns.substr(0, serv_pos);
    }
    std::string bns_group2 = bns_group;
    auto pos1 = bns_group.find_first_of(".");
    auto pos2 = bns_group.find_last_of(".");
    if (pos1 == bns_group.npos || pos2 == bns_group.npos || pos2 <= pos1) {
        return "";
    }
    bns_group = "group" + bns_group.substr(pos1, pos2 - pos1 + 1) + "all";
    bns_group2 = bns_group2.substr(pos1 + 1);
    bool is_store_bns = false;
    if (bns_group.find("baikalStore") != bns_group.npos || bns_group.find("baikalBinlog") != bns_group.npos) {
        is_store_bns = true;
    }

    std::vector<std::string> instances;
    int retry_times = 0;
    while (true) {
        instances.clear();
        int ret2 = 0;
        int ret = 0;
        if (is_store_bns) {
            ret = get_instance_from_bns(&ret2, bns_group, instances);
        } else {
            ret = get_multi_port_from_bns(&ret2, bns_group, instances);
        }
        if (ret != 0 || instances.empty()) {
            if (++retry_times > 5) {
                return "";
            } else {
                if (retry_times > 3) {
                    bns_group = bns_group2;
                }
                continue;
            }
        } else {
            break;
        }
    }

    std::string meta_bns = "";
    for (const std::string& instance : instances) {
        std::string response;
        int ret = brpc_with_http(instance, instance + "/flags/meta_server_bns", response);
        if (ret != 0) {
            continue;
        }
        auto pos = response.find("(default");
        if (pos != response.npos) {
            response = response.substr(0, pos + 1);
        }
        re2::RE2::Options option;
        option.set_utf8(false);
        option.set_case_sensitive(false);
        option.set_perl_classes(true);

        re2::RE2 reg(".*(group.*baikalMeta.*all).*", option);
        meta_bns.clear();
        if (!RE2::Extract(response, reg, "\\1", &meta_bns)) {
            DB_WARNING("extract commit error. response: %s", response.c_str());
            continue;
        }

        if (meta_bns.empty()) {
            continue;
        } else {
            break;
        }
    }

    DB_WARNING("bns_group: %s; store_bns to meta bns : %s => %s", bns_group.c_str(), bns.c_str(), meta_bns.c_str());

    return meta_bns;
}
void parse_sample_sql(const std::string& sample_sql, std::string& database, std::string& table, std::string& sql) {
    // Remove comments.
    re2::RE2::Options option;
    option.set_utf8(false);
    option.set_case_sensitive(false);
    option.set_perl_classes(true);

    re2::RE2 reg("family_table_tag_optype_plat=\\[(.*)\t(.*)\t.*\t.*\t.*sql=\\[(.*)\\]", option);

    if (!RE2::Extract(sample_sql, reg, "\\1", &database)) {
        DB_WARNING("extract commit error.");
    }
    if (!RE2::Extract(sample_sql, reg, "\\2", &table)) {
        DB_WARNING("extract commit error.");
    }
    if (!RE2::Extract(sample_sql, reg, "\\3", &sql)) {
        DB_WARNING("extract commit error.");
    }
    DB_WARNING("sample_sql: %s, database: %s, table: %s, sql: %s", sample_sql.c_str(), database.c_str(), table.c_str(), sql.c_str());
}

}  // baikaldb
