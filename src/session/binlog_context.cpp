// Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
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

#include "binlog_context.h"
#include "meta_server_interact.hpp"

namespace baikaldb {

MetaServerInteract TsoFetcher::tso_meta_inter;
int64_t TsoFetcher::get_tso() {
    pb::TsoRequest request;
    request.set_op_type(pb::OP_GEN_TSO);
    request.set_count(1);
    pb::TsoResponse response;
    int retry_time = 0;
    int ret = 0;
    for (;;) {
        retry_time++;
        ret = TsoFetcher::tso_meta_inter.send_request("tso_service", request, response);
        if (ret < 0) {
            if (response.errcode() == pb::RETRY_LATER && retry_time < 5) {
                bthread_usleep(tso::update_timestamp_interval_ms * 1000LL);
                continue;  
            } else {
                DB_FATAL("get tso failed, response:%s", response.ShortDebugString().c_str());
                return ret;
            }
        }
        break;
    }
    //DB_WARNING("response:%s", response.ShortDebugString().c_str());
    auto&  tso = response.start_timestamp();
    int64_t timestamp = (tso.physical() << tso::logical_bits) + tso.logical();

    return timestamp;
}

int BinlogContext::get_binlog_regions(uint64_t log_id) {
    if (_table_info == nullptr || _partition_record == nullptr) {
        DB_WARNING("wrong state log_id:%lu", log_id);
        return -1;
    }
    if (!_table_info->is_linked) {
        DB_WARNING("table %ld not link to binlog table log_id:%lu", _table_info->id, log_id);
        return -1;      
    }
    int ret = 0;
    if (_table_info->partition_num == 1) {
        ret = _factory->get_binlog_regions(_table_info->id, _binlog_region);
    } else {
        if (_table_info->link_field.empty()) {
            DB_WARNING("table %ld not link to binlog table log_id:%lu", _table_info->id, log_id);
            return -1;      
        }   

        FieldInfo& link_filed = _table_info->link_field[0];
        auto field_desc = _partition_record->get_field_by_idx(link_filed.pb_idx);
        ExprValue value = _partition_record->get_value(field_desc);
        ret = _factory->get_binlog_regions(_table_info->id, _binlog_region, value);
    }

    if (ret < 0) {
        DB_WARNING("get binlog region failed table_id: %ld log_id:%lu",
            _table_info->id, log_id);
        return -1;
    }
    return 0;
}

} // namespace baikaldb