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

// Brief:  truncate table exec node
#pragma once

#include "exec_node.h"
#include "fetcher_store.h"
#include "dml_node.h"
#include "proto/optype.pb.h"

namespace baikaldb {
class DmlManagerNode : public ExecNode {
public:
    DmlManagerNode() {
        _factory = SchemaFactory::get_instance();
    }
    virtual ~DmlManagerNode() {
    }
    virtual int open(RuntimeState* state);

    virtual void close(RuntimeState* state) override {
        ExecNode::close(state);
        _execute_child_idx = 0;
        std::vector<int32_t>().swap(_seq_ids);
        std::vector<SmartRecord>().swap(_insert_scan_records);
        std::vector<SmartRecord>().swap(_del_scan_records);
    }

    void set_op_type(pb::OpType op_type) {
        _op_type = op_type;
    }
    std::vector<int32_t>& seq_ids() {
        return _seq_ids;
    }
    int send_request(RuntimeState* state,
            DMLNode* dml_node,
            const std::vector<SmartRecord>& insert_scan_records,
            const std::vector<SmartRecord>& delete_scan_records);
    int send_request_light(RuntimeState* state,
            DMLNode* dml_node,
            FetcherStore& fetcher_store,
            int seq_id,
            const std::vector<SmartRecord>& insert_scan_records,
            const std::vector<SmartRecord>& delete_scan_records);
    int send_request_concurrency(RuntimeState* state, size_t execute_child_idx);
    int get_region_infos(RuntimeState* state,
            DMLNode* dml_node,
            const std::vector<SmartRecord>& insert_scan_records,
            const std::vector<SmartRecord>& delete_scan_records,
            std::map<int64_t, pb::RegionInfo>& region_infos);
    size_t uniq_index_number() const {
        return _uniq_index_number;
    }
protected:
    FetcherStore _fetcher_store;
    pb::OpType  _op_type;
    SchemaFactory*                  _factory = nullptr;
    std::vector<int32_t>        _seq_ids;
    size_t  _execute_child_idx = 0;
    size_t  _uniq_index_number = 0;
    size_t  _affected_index_num = 0;
    std::map<int32_t, FieldInfo*> _update_fields;
    std::vector<SmartRecord> _insert_scan_records;
    std::vector<SmartRecord> _del_scan_records;
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
