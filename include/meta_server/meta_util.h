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

#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include "common.h"

#include "datetime.h"
#include "expr_node.h"
#include "expr_value.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
struct IdcInfo {
    std::string resource_tag;
    std::string logical_room;
    std::string physical_room;
    IdcInfo() {};
    IdcInfo(const std::string& resource, const std::string& logical, const std::string& physical)
            : resource_tag(resource), logical_room(logical), physical_room(physical) {};
    IdcInfo(std::string str) {
        std::vector<std::string> split_vec; 
        boost::split(split_vec, str, boost::is_any_of(":"), boost::token_compress_on);
        if (split_vec.size() >= 1) {
            resource_tag = split_vec[0];
        } 
        if (split_vec.size() >= 2) {
            logical_room = split_vec[1];
        }
        if (split_vec.size() == 3) {
            physical_room = split_vec[2];
        }
    }
    std::string to_string() const {
        return resource_tag + ":" + logical_room + ":" + physical_room;
    }
    std::string logical_room_level() const {
        return resource_tag + ":" + logical_room + ":";
    }
    std::string resource_tag_level() const {
        return resource_tag + "::";
    }
    bool match(const IdcInfo& other) const {
        if ((!resource_tag.empty() && !other.resource_tag.empty() && resource_tag != other.resource_tag)
            || (!logical_room.empty() && !other.logical_room.empty() && logical_room != other.logical_room)
            || (!physical_room.empty() && !other.physical_room.empty() && physical_room != other.physical_room)) {
            return false;
        }
        return true;
    }
};

namespace partition_utils {

constexpr char* MIN_DATE = "0000-01-01";
constexpr char* MAX_DATE = "9999-12-31";
constexpr char* MIN_DATETIME = "0000-01-01 00:00:00";
constexpr char* MAX_DATETIME = "9999-12-31 23:59:59";
constexpr char* DATE_FORMAT = "%Y-%m-%d";
constexpr char* DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S";
constexpr char* DAY_FORMAT = "%Y%m%d";
constexpr char* MONTH_FORMAT = "%Y%m";
constexpr char* PREFIX = "p";
constexpr int32_t START_DAY_OF_MONTH = 1;
constexpr int32_t MIN_START_DAY_OF_MONTH = 1;
constexpr int32_t MAX_START_DAY_OF_MONTH = 28;

int64_t compare(const pb::Expr& left, const pb::Expr& right);
pb::Expr min(const pb::Expr& left, const pb::Expr& right);
pb::Expr max(const pb::Expr& left, const pb::Expr& right);
bool is_equal(const pb::RangePartitionInfo& left, const pb::RangePartitionInfo& right);

int create_partition_expr(const pb::PrimitiveType col_type, const std::string& str_val, pb::Expr& expr);
int check_partition_expr(const pb::Expr& expr, bool lower_or_upper_bound);
int get_partition_value(const pb::Expr& expr, ExprValue& value);
int get_min_partition_value(const pb::PrimitiveType col_type, std::string& str_val);
int get_max_partition_value(const pb::PrimitiveType col_type, std::string& str_val);
int create_dynamic_range_partition_info(
    const std::string& partition_prefix, const pb::PrimitiveType partition_col_type, 
    const time_t current_ts, const int32_t offset, const TimeUnit time_unit, 
    pb::RangePartitionInfo& range_partition_info);
bool check_range_partition_info(const pb::RangePartitionInfo& range_partition_info);
bool check_dynamic_partition_attr(const pb::DynamicPartitionAttr& dynamic_partition_attr);
bool check_partition_overlapped(
    const std::vector<pb::RangePartitionInfo>& partitions_vec, 
    const pb::RangePartitionInfo& partition);
bool check_partition_overlapped(
    const ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo>& partitions_vec, 
    const pb::RangePartitionInfo& partition);
int get_specifed_partitions(
    const pb::Expr& specified_expr,
    const ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo>& partitions_vec,
    ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo>& specified_partitions_vec,
    bool is_cold = false);
int convert_to_partition_range(
    const pb::PrimitiveType partition_col_type, 
    pb::RangePartitionInfo& range_partition_info);
int set_partition_col_type(
    const pb::PrimitiveType partition_col_type,
    pb::RangePartitionInfo& range_partition_info);
int get_partition_range(
    const pb::RangePartitionInfo& range_partition_info,
    std::pair<std::string, std::string>& range);

inline bool is_specified_partition(
        const pb::RangePartitionInfo& partition_info, const pb::RangePartitionInfo& new_partition_info) {
    // 获取指定类型的热分区
    if (!partition_info.is_cold() && !new_partition_info.is_cold() && partition_info.type() != new_partition_info.type()) {
        return false;
    }
    return true;
}

// true表示left <= right
struct RangeComparator {
    bool operator() (const pb::RangePartitionInfo& left, const pb::RangePartitionInfo& right) {
        if (!left.has_range() || !right.has_range()) {
            return false;
        }
        const pb::Expr& pre_right_value = left.range().right_value();
        const pb::Expr& next_right_value = right.range().right_value();
        return compare(pre_right_value, next_right_value) <= 0;
    }
};

// true表示left <= right
struct PointerRangeComparator {
    bool operator() (const pb::RangePartitionInfo* left, const pb::RangePartitionInfo* right) {
        if (left == nullptr || !left->has_range() || right == nullptr || !right->has_range()) {
            return false;
        }
        const pb::Expr& pre_right_value = left->range().right_value();
        const pb::Expr& next_right_value = right->range().right_value();
        int64_t res = compare(pre_right_value, next_right_value);
        if (res == 0) {
            return left->type() <= right->type();
        } else {
            return res < 0;
        }
    }
};

} // namespace partition_utils

#define ERROR_SET_RESPONSE(response, errcode, err_message, op_type, log_id) \
    do {\
        DB_FATAL("request op_type:%d, %s ,log_id:%lu",\
                op_type, err_message, log_id);\
        if (response != NULL) {\
            response->set_errcode(errcode);\
            response->set_errmsg(err_message);\
            response->set_op_type(op_type);\
        }\
    }while (0);

#define ERROR_SET_RESPONSE_WARN(response, errcode, err_message, op_type, log_id) \
    do {\
        DB_WARNING("request op_type:%d, %s ,log_id:%lu",\
                op_type, err_message, log_id);\
        if (response != NULL) {\
            response->set_errcode(errcode);\
            response->set_errmsg(err_message);\
            response->set_op_type(op_type);\
        }\
    }while (0);

#define IF_DONE_SET_RESPONSE(done, errcode, err_message) \
    do {\
        if (done && ((MetaServerClosure*)done)->response) {\
            ((MetaServerClosure*)done)->response->set_errcode(errcode);\
            ((MetaServerClosure*)done)->response->set_errmsg(err_message);\
        }\
    }while (0);

#define SET_RESPONSE(response, errcode, err_message) \
    do {\
        if (response) {\
            response->set_errcode(errcode);\
            response->set_errmsg(err_message);\
        }\
    }while (0);

#define RETURN_IF_NOT_INIT(init, response, log_id) \
    do {\
        if (!init) {\
            DB_WARNING("have not init, log_id:%lu", log_id);\
            response->set_errcode(pb::HAVE_NOT_INIT);\
            response->set_errmsg("have not init");\
            return;\
        }\
    } while (0);

}//namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
