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
