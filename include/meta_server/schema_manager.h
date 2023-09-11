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

#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"
#include "meta_state_machine.h"

namespace baikaldb {
typedef std::shared_ptr<pb::RegionInfo> SmartRegionInfo;
DECLARE_int32(balance_periodicity);
class SchemaManager {
public:
    static const std::string MAX_NAMESPACE_ID_KEY;
    static const std::string MAX_DATABASE_ID_KEY;
    static const std::string MAX_TABLE_ID_KEY;
    static const std::string MAX_REGION_ID_KEY;
    static SchemaManager* get_instance() {
        static SchemaManager instance;
        return &instance;
    }
    ~SchemaManager() {}
    void process_schema_info(google::protobuf::RpcController* controller,
                  const pb::MetaManagerRequest* request, 
                  pb::MetaManagerResponse* response,
                  google::protobuf::Closure* done); 
    
    void process_schema_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
                                            pb::StoreHeartBeatResponse* response);
    void process_peer_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
                                            pb::StoreHeartBeatResponse* response,
                                            uint64_t log_id);
    void process_leader_heartbeat_for_store(const pb::StoreHeartBeatRequest* request,
                                            pb::StoreHeartBeatResponse* response,
                                            uint64_t log_id);
    void process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request,
                                pb::BaikalHeartBeatResponse* response,
                                uint64_t log_id);
    //为权限操作类接口提供输入参数检查和id获取功能
    int check_and_get_for_privilege(pb::UserPrivilege& user_privilege);

    int load_snapshot();
    void set_meta_state_machine(MetaStateMachine* meta_state_machine) {
        _meta_state_machine = meta_state_machine;
    }
    bool get_unsafe_decision() {
        return _meta_state_machine->get_unsafe_decision();
    }
private:
    SchemaManager() {}
    int pre_process_for_create_table(const pb::MetaManagerRequest* request,
                                    pb::MetaManagerResponse* response,
                                    uint64_t log_id);
    int pre_process_for_merge_region(const pb::MetaManagerRequest* request,
                                    pb::MetaManagerResponse* response,
                                    uint64_t log_id);
    int pre_process_for_split_region(const pb::MetaManagerRequest* request, 
                                    pb::MetaManagerResponse* response,
                                    uint64_t log_id);
    int load_max_id_snapshot(const std::string& max_id_prefix, 
                              const std::string& key, 
                              const std::string& value);
    int whether_dists_legal(pb::MetaManagerRequest* request, 
                            pb::MetaManagerResponse* response,
                            std::string& candidate_logical_room,
                            uint64_t log_id);
    int whether_main_logical_room_legal(pb::MetaManagerRequest* request, 
                            pb::MetaManagerResponse* response,
                            uint64_t log_id);
    bool is_table_info_op_type(pb::OpType op_type) {
        if (op_type == pb::OP_CREATE_TABLE
            || op_type == pb::OP_DROP_TABLE
            || op_type == pb::OP_RESTORE_TABLE
            || op_type == pb::OP_RENAME_TABLE) {
            return true;
        }
        return false;
    }
    // Partition
    int pre_process_for_partition(const pb::MetaManagerRequest* request,
                                  pb::MetaManagerResponse* response,
                                  uint64_t log_id);
    int pre_process_for_dynamic_partition(const pb::MetaManagerRequest* request,
                                          pb::MetaManagerResponse* response,
                                          uint64_t log_id,
                                          pb::PrimitiveType partition_col_type);

    MetaStateMachine* _meta_state_machine;
}; //class

}//namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
