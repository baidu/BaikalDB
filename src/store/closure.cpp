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

#include "closure.h"

namespace baikaldb {
DECLARE_int64(print_time_us);
void DMLClosure::Run() {
    int64_t region_id = 0;
    butil::EndPoint leader;
    if (region != nullptr) {
        region->real_writing_decrease();
        region_id = region->get_region_id();
        leader = region->get_leader();
    }
    uint64_t log_id = 0;
    if (cntl != nullptr && cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    if (!status().ok()) {
        response->set_errcode(pb::NOT_LEADER);
        response->set_leader(butil::endpoint2str(leader).c_str());
        response->set_errmsg("leader transfer");
        //  发生切主，回滚当前dml
        if (transaction != nullptr && region != nullptr) {
            uint64_t txn_id = transaction->txn_id();
            if (transaction->primary_region_id_seted()) {
                if (op_type != pb::OP_COMMIT && op_type != pb::OP_ROLLBACK) {
                    int seq_id = transaction->seq_id();
                    transaction->rollback_current_request();
                    DB_WARNING("region_id: %ld log_id:%lu txn_id: %lu:%d, op_type: %s",
                    region_id, log_id, transaction->txn_id(), seq_id, pb::OpType_Name(op_type).c_str());
                }
            } else {
                if (op_type == pb::OP_PREPARE) {
                    region->get_txn_pool().on_leader_stop_rollback(txn_id);
                }
            }
        }
        DB_WARNING("region_id: %ld  status:%s ,leader:%s, log_id:%lu, remote_side: %s",
                    region_id, 
                    status().error_cstr(),
                    butil::endpoint2str(leader).c_str(), 
                    log_id, remote_side.c_str());
    } else {
        if (transaction != nullptr && region != nullptr) {
            transaction->clear_current_req_point_seq();
        }
    }
    if (is_replay) {
        replay_last_log_cond->decrease_signal();
    }
    if (done) {
        done->Run();
    }
    if (region != nullptr && (op_type == pb::OP_INSERT || op_type == pb::OP_DELETE || op_type == pb::OP_UPDATE)) {
        region->update_average_cost(cost.get_time());
    }
    if (cost.get_time() > FLAGS_print_time_us) {
        DB_NOTICE("dml log_id:%lu, type:%s, raft_total_cost:%ld, region_id: %ld, "
                    "qps:%ld, average_cost:%ld, num_prepared:%d remote_side:%s",
                    log_id, 
                    pb::OpType_Name(op_type).c_str(), 
                    cost.get_time(), 
                    region_id, 
                    (region != nullptr)?region->get_qps():0, 
                    (region != nullptr)?region->get_average_cost():0, 
                    (region != nullptr)?region->num_prepared():0, 
                    remote_side.c_str());
    }
    delete this;
}

void AddPeerClosure::Run() {
    if (!status().ok()) {
        DB_WARNING("region add peer fail, new_instance:%s, status:%s, region_id: %ld, cost:%ld", 
                  new_instance.c_str(),
                  status().error_cstr(), 
                  region->get_region_id(),
                  cost.get_time());
        if (response) {
            response->set_errcode(pb::NOT_LEADER);
            response->set_leader(butil::endpoint2str(region->get_leader()).c_str());
            response->set_errmsg("not leader");
        }
        //region->send_remove_region_to_store(region->get_region_id(), new_instance);
    } else {
        DB_WARNING("region add peer success, region_id: %ld, cost:%ld", 
                    region->get_region_id(),
                    cost.get_time());
    }
    region->reset_region_status();
    DB_WARNING("region status was reset, region_id: %ld", region->get_region_id());
    if (done) {
        done->Run();
    }
    cond.decrease_broadcast();
    delete this;
}

void MergeClosure::Run() {

    if (response) {
        response->set_errcode(pb::SUCCESS);
        response->set_errmsg("success");
    }

    if (is_dst_region) {
        if (!status().ok()) {
            if (response) {
                response->set_errcode(pb::NOT_LEADER);
                response->set_leader(butil::endpoint2str(region->get_leader()).c_str());
                response->set_errmsg("not leader");
            }
        }
        //目标region需要返回给源region
        region->copy_region(response->add_regions());
        if (done) {
            done->Run();
        }
        region->reset_region_status();
    } else {
        ScopeMergeStatus merge_status(region);
        if (!status().ok()) {
            //目标region需要回退version和key TODO
        }
    }
    delete this;
}

void SplitClosure::Run() {
    bool split_fail = false;
    ScopeProcStatus split_status(region);
    if (!status().ok()) { 
        DB_FATAL("split step(%s) fail, region_id: %ld status:%s, time_cost:%ld", 
                 step_message.c_str(), 
                 region->get_region_id(), 
                 status().error_cstr(), 
                 cost.get_time());
        split_fail = true;
    } else if (ret < 0) {
        DB_FATAL("split step(%s) fail, region_id: %ld, cost:%ld", 
                  step_message.c_str(),
                  region->get_region_id(),
                  cost.get_time());
        split_fail = true;
    } else {
        split_status.reset();
        Bthread bth(&BTHREAD_ATTR_SMALL);
        bth.run(next_step);
        DB_WARNING("last step(%s) for split, start to next step, "
                    "region_id: %ld, cost:%ld", 
                    step_message.c_str(),
                    region->get_region_id(),
                    cost.get_time());
    }
    // OP_VALIDATE_AND_ADD_VERSION 这步失败了也不能自动删除new region
    // 防止出现flase negative，raft返回失败，实际成功
    // 如果真实失败，需要手工drop new region
    // todo 增加自动删除步骤，删除与分裂解耦
    if (split_fail && op_type != pb::OP_VALIDATE_AND_ADD_VERSION) {
        DB_WARNING("split fail, start remove region, old_region_id: %ld, split_region_id: %ld, new_instance:%s",
                    region->get_region_id(), split_region_id, new_instance.c_str());
        RpcSender::send_remove_region_method(split_region_id, new_instance);
    }
    delete this;
}

void ConvertToSyncClosure::Run() {
    if (!status().ok()) { 
        DB_FATAL("region_id: %ld, asyn step exec fail, status:%s, time_cost:%ld", 
                region_id,
                status().error_cstr(), 
                cost.get_time());
    } else {
        DB_WARNING("region_id: %ld, asyn step exec success, time_cost: %ld", 
                region_id, cost.get_time());
    }
    sync_sign.decrease_signal();
    delete this;
}

void Dml1pcClosure::Run() {
    if (!status().ok()) {
        DB_FATAL("dml 1pc exec fail, status:%s, time_cost:%ld", 
                 status().error_cstr(), 
                 cost.get_time());
        if (txn != nullptr) {
            txn->rollback();
            state->is_fail = true;
            state->raft_error_msg = status().error_cstr();
        }
    } 
    if (done) {
        done->Run();
    }
    txn_cond.decrease_signal();
    delete this;
}
} // end of namespace
