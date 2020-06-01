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
#include "common.h"

namespace baikaldb {
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

#define RETURN_IF_NOT_INIT(init, response, log_id) \
    do {\
        if (!init) {\
            DB_FATAL("have not init, log_id:%lu", log_id);\
            response->set_errcode(pb::HAVE_NOT_INIT);\
            response->set_errmsg("have not init");\
            return;\
        }\
    } while (0);

}//namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
