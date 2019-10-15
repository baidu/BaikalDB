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

#include "runtime_state.h"
#include "transaction_manager_node.h"
#include "network_socket.h"
#include "network_server.h"

namespace baikaldb {
DECLARE_int32(retry_interval_us);
DEFINE_int32(wait_after_prepare_us, 0, "wait time after prepare(us)");

int TransactionManagerNode::add_commit_log_entry(
        uint64_t txn_id,
        int32_t  seq_id,
        ExecNode* commit_fetch,
        std::map<int64_t, pb::RegionInfo>& region_infos) {

    pb::CachePlan commit_plan;
    commit_plan.set_op_type(pb::OP_COMMIT);
    commit_plan.set_seq_id(seq_id);
    ExecNode::create_pb_plan(0, commit_plan.mutable_plan(), commit_fetch);

    for (auto& pair : region_infos) {
        commit_plan.add_regions()->CopyFrom(pair.second);
    }
    auto meta_db = RocksWrapper::get_instance();
    MutTableKey txn_key;
    txn_key.append_u8(NetworkServer::transaction_prefix);
    txn_key.append_u64(txn_id);

    std::string txn_value;
    if (!commit_plan.SerializeToString(&txn_value)) {
        DB_WARNING("serialize backup plan error: %s", commit_plan.ShortDebugString().c_str());
        return -1;
    }
    rocksdb::Status ret = meta_db->put(
            rocksdb::WriteOptions(), 
            meta_db->get_meta_info_handle(), 
            txn_key.data(), 
            txn_value);
    if (!ret.ok()) {
        DB_WARNING("write backup plan error: %d, %s", ret.code(), ret.ToString().c_str());
        return -1;
    }
    //DB_WARNING("add_backup_plan success, txn_id: %lu", txn_id);
    return 0;
}

int TransactionManagerNode::remove_commit_log_entry(uint64_t txn_id) {
    auto meta_db = RocksWrapper::get_instance();
    MutTableKey txn_key;
    txn_key.append_u8(NetworkServer::transaction_prefix);
    txn_key.append_u64(txn_id);

    rocksdb::Status ret = meta_db->remove(
            rocksdb::WriteOptions(), 
            meta_db->get_meta_info_handle(), 
            txn_key.data());
    if (!ret.ok()) {
        DB_WARNING("remove backup plan error: %d, %s", ret.code(), ret.ToString().c_str());
        return -1;
    }
    //DB_WARNING("remove_backup_plan success, txn_id: %lu", txn_id);
    return 0;
}

int TransactionManagerNode::exec_begin_node(RuntimeState* state, ExecNode* begin_node) {
    return push_cmd_to_cache(state, pb::OP_BEGIN, begin_node);
}
int TransactionManagerNode::exec_prepared_node(RuntimeState* state, ExecNode* prepared_node, int64_t start_seq_id) {
    uint64_t log_id = state->log_id();
    auto client_conn = state->client_conn();
    if (client_conn->region_infos.size() <= 1) {
        state->set_optimize_1pc(true);
        DB_WARNING("enable optimize_1pc: txn_id: %lu, seq_id: %d, log_id: %lu", 
                state->txn_id, client_conn->seq_id, log_id);
    }
    return _fetcher_store.run(state, client_conn->region_infos, prepared_node, start_seq_id,
                   client_conn->seq_id, pb::OP_PREPARE); 
}

int TransactionManagerNode::exec_commit_node(RuntimeState* state, ExecNode* commit_node) {
    //DB_WARNING("TransactionNote: prepare success, txn_id: %lu log_id:%lu", state->txn_id, state->log_id());
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    // for transaction recovery when BaikalDB crash
    if (0 != add_commit_log_entry(state->txn_id, client_conn->seq_id, commit_node, client_conn->region_infos)) {
        DB_WARNING("TransactionError: add_commit_log_entry failed: %lu log_id:%lu ", state->txn_id, state->log_id());
        return -1;
    }
    if (FLAGS_wait_after_prepare_us != 0) {
        bthread_usleep(FLAGS_wait_after_prepare_us);
    }
    int retry = 0;
    int ret = 0;
    do {
        auto seq_id = client_conn->seq_id;
        ret = _fetcher_store.run(state, client_conn->region_infos, commit_node, seq_id, pb::OP_COMMIT);
        if (ret < 0) {
            DB_WARNING("TransactionWarn: commit failed, retry: %d. txn_id: %ld log_id:%lu", retry, 
                   state->txn_id, state->log_id());
            bthread_usleep(FLAGS_retry_interval_us);
            // refresh the region infos, [start_key end_key) ranges are invarient despite regions splitting
            pb::CachePlan commit_plan;
            for (auto& pair : client_conn->region_infos) {
                commit_plan.add_regions()->CopyFrom(pair.second);
            }
            client_conn->region_infos.clear();
            SchemaFactory::get_instance()->get_region_by_key(commit_plan.regions(), client_conn->region_infos);
            retry++;
        } else {
            break;
        }
    } while (true);
    if (ret < 0) {
         // un-expected case since infinite retry of commit after prepare
         DB_WARNING("TransactionError: commit failed. txn_id: %lu log_id:%lu ", state->txn_id, state->log_id());
     } else {
         remove_commit_log_entry(client_conn->txn_id);
     }
    return ret;
}
int TransactionManagerNode::exec_rollback_node(RuntimeState* state, ExecNode* rollback_node) {
    //DB_WARNING("rollback for single-sql trnsaction with optimize_1pc");
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    int32_t retry = 0;
    auto ret = 0;
    do {
        auto seq_id = client_conn->seq_id;
        ret = _fetcher_store.run(state, client_conn->region_infos, rollback_node, seq_id, pb::OP_ROLLBACK);
        if (ret < 0) {
            DB_WARNING("TransactionWarn: rollback is-single-sql:%d fail, retry: %d, txn_id: %lu log_id:%lu", 
            state->single_sql_autocommit() ,retry, state->txn_id, state->log_id());
            bthread_usleep(FLAGS_retry_interval_us);
            retry++;
        } else {
            break;
        }
    } while (true);
    if (ret < 0) {
        // un-expected case since infinite retry of commit after prepare
        DB_WARNING("TransactionError: rollback failed. txn_id: %lu log_id:%lu",
                state->txn_id, state->log_id());
        return -1;
    }
    return ret;
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
