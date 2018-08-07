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

#include "runtime_state.h"
#include "transaction_node.h"
#include "network_socket.h"
#include "network_server.h"

namespace baikaldb {
DECLARE_int32(retry_interval_us);
DEFINE_int32(wait_after_prepare_us, 0, "wait time after prepare(us)");

int TransactionNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _txn_cmd = node.derive_node().transaction_node().txn_cmd();
    return 0;
}

int TransactionNode::add_commit_log_entry(
        uint64_t txn_id,
        int32_t  seq_id,
        ExecNode* commit_fetch,
        std::unordered_map<int64_t, pb::RegionInfo>& region_infos) {

    pb::CachePlan commit_plan;
    commit_plan.set_op_type(pb::OP_COMMIT);
    commit_plan.set_seq_id(seq_id);
    ExecNode::create_pb_plan(commit_plan.mutable_plan(), commit_fetch);

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
    DB_WARNING("add_backup_plan success, txn_id: %lu", txn_id);
    return 0;
}

int TransactionNode::remove_commit_log_entry(uint64_t txn_id) {
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
    DB_WARNING("remove_backup_plan success, txn_id: %lu", txn_id);
    return 0;
}

//TODO set seq_id after rollback 
int TransactionNode::open(RuntimeState* state) {
    int64_t region_id = 0;
    if (state->resource()) {
        region_id = state->resource()->region_id;
    }
    int ret = 0;
    NetworkSocket* client = state->client_conn();
    if (_txn_cmd == pb::TXN_BEGIN 
            || _txn_cmd == pb::TXN_COMMIT 
            || _txn_cmd == pb::TXN_COMMIT_BEGIN
            || _txn_cmd == pb::TXN_ROLLBACK 
            || _txn_cmd == pb::TXN_ROLLBACK_BEGIN) {
        if (client == nullptr) {
            DB_WARNING("no client for baikaldb txn control cmd");
            return -1;
        }
    }
    TransactionPool* txn_pool = state->txn_pool();
    if (_txn_cmd == pb::TXN_PREPARE 
            || _txn_cmd == pb::TXN_BEGIN_STORE 
            || _txn_cmd == pb::TXN_COMMIT_STORE 
            || _txn_cmd == pb::TXN_ROLLBACK_STORE) {
        if (txn_pool == nullptr) {
            DB_WARNING("no txn_pool for store txn control cmd");
            return -1;
        }
    }
    if (_txn_cmd == pb::TXN_BEGIN) {
        ret = _children[0]->open(state);
        if (ret < 0) {
            DB_WARNING("ExecNode::open fail, ret:%d", ret);
            return ret;
        }
        return 0;
    } else if (_txn_cmd == pb::TXN_COMMIT || _txn_cmd == pb::TXN_COMMIT_BEGIN) {
        uint64_t old_txn_id = client->txn_id;
        uint64_t new_txn_id = client->new_txn_id;

        // starts 2 phase commit
        // the two children node (prepare_fetcher and commit_fetcher) should
        // open one by one rather than in parallel
        if (_txn_cmd == pb::TXN_COMMIT) {
            if ((state->autocommit() && _children.size() != 3)
                    || (!state->autocommit() && _children.size() != 2)) {
                DB_WARNING("error number of children nodes: %lu, autocommit:%d", 
                    _children.size(), state->autocommit());
                return -1;
            }
        } else if (_txn_cmd == pb::TXN_COMMIT_BEGIN && _children.size() != 3) {
            DB_WARNING("error number of children nodes: %lu", _children.size());
            return -1;
        }
        ret = _children[0]->open(state);
        if (state->optimize_1pc() == false) {
            if (ret < 0) {
                DB_WARNING("TransactionNote: prepare failed, rollback txn_id: %lu", state->txn_id);
                if (state->autocommit()) {
                    if (_children.size() < 3) {
                        DB_WARNING("TransactionError: no rollback node: txn_id: %lu", state->txn_id);
                    } else {
                        // auto rollback in autocommit mode
                        state->seq_id++;
                        _children[2]->open(state);
                    }
                } else {
                    // not in autocommit mode, user may retry prepare & commit
                    // should not reset connection transaction status
                }
                ret = -1;
            } else {
                DB_WARNING("TransactionNote: prepare success, txn_id: %lu", state->txn_id);
                state->seq_id++;
                // for transaction recovery when BaikalDB crash
                if (0 != add_commit_log_entry(state->txn_id, state->seq_id, _children[1], client->region_infos)) {
                    DB_WARNING("TransactionError: add_commit_log_entry failed: %lu", state->txn_id);
                    return -1;
                }
                if (FLAGS_wait_after_prepare_us != 0) {
                    bthread_usleep(FLAGS_wait_after_prepare_us);
                }
                int retry = 0;
                int res = -1;
                do {
                    res = _children[1]->open(state);
                    if (res < 0) {
                        DB_WARNING("TransactionWarn: commit failed, retry: %d. txn_id: %ld", retry, state->txn_id);
                        bthread_usleep(FLAGS_retry_interval_us);
                        // refresh the region infos, [start_key end_key) ranges are invarient despite regions splitting
                        pb::CachePlan commit_plan;
                        for (auto& pair : client->region_infos) {
                            commit_plan.add_regions()->CopyFrom(pair.second);
                        }
                        client->region_infos.clear();
                        SchemaFactory::get_instance()->get_region_by_key(commit_plan.regions(), client->region_infos);
                        retry++;
                    } else {
                        break;
                    }
                } while (true);

                if (res < 0) {
                    // un-expected case since infinite retry of commit after prepare
                    DB_WARNING("TransactionError: commit failed. txn_id: %lu", state->txn_id);
                    ret = -1;
                } else {
                    remove_commit_log_entry(state->txn_id);
                }
                client->on_commit_rollback();
            }
        } else {
            DB_WARNING("TransactionNote: optimize_1pc, no commit/rollback: txn_id: %lu", state->txn_id);
        }
        if (ret >= 0 || state->autocommit()) {
            client->on_commit_rollback();
        }

        // prepare may return num_affected_rows
        int result = ret;
        // implicit commit and then start a new txn
        if (_txn_cmd == pb::TXN_COMMIT_BEGIN) {
            if (result < 0) {
                DB_WARNING("TransactionWarn: cannot start new txn since the old commit failed, "
                    "pls rollback first: %lu", state->txn_id);
                return result;
            }
            client->on_begin(new_txn_id);
            state->txn_id = new_txn_id;
            client->seq_id = 1;
            state->seq_id = 1;

            DB_WARNING("client new txn_id: %lu, %d", state->txn_id, state->seq_id);
            ret = _children[2]->open(state);
            if (ret < 0) {
                DB_WARNING("begin new txn failed after commit, txn_id: %lu, new_txn_id: %lu", 
                    old_txn_id, new_txn_id);
                result = -1;
            }
        }
        return result;
    } else if (_txn_cmd == pb::TXN_ROLLBACK || _txn_cmd == pb::TXN_ROLLBACK_BEGIN) {
        _children[0]->open(state);
        uint64_t old_txn_id = client->txn_id;
        uint64_t new_txn_id = client->new_txn_id;
        client->on_commit_rollback();

        // start the new txn
        if (_txn_cmd == pb::TXN_ROLLBACK_BEGIN) {
            client->on_begin(new_txn_id);
            state->txn_id = new_txn_id;
            client->seq_id = 1;
            state->seq_id = 1;
            DB_WARNING("client new txn_id: %lu, %lu, %d", new_txn_id, state->txn_id, state->seq_id);
            ret = _children[1]->open(state);
            if (ret < 0) {
                DB_WARNING("begin new txn failed after rollback, txn_id: %lu, new_txn_id: %lu", 
                    old_txn_id, new_txn_id);
                return -1;
            }
        }
        return 0;
    } else if (_txn_cmd == pb::TXN_PREPARE) {
        // for autocommit dml cmds
        int num_affected_rows = 0;
        for (auto c : _children) {
            int ret = 0;
            ret = c->open(state);
            if (ret < 0) {
                return ret;
            }
            num_affected_rows += ret;
        }
        Transaction* txn = txn_pool->get_txn(state->txn_id);
        if (txn == nullptr) {
            DB_WARNING("get txn failed, no txn in pool, txn_id: %lu", state->txn_id);
            return -1;
        }
        auto res = txn->prepare();
        if (res.ok()) {
            DB_WARNING("prepare success, region_id: %ld, txn_id: %lu:%d", region_id, state->txn_id, state->seq_id);
        } else if (res.IsExpired()) {
            DB_WARNING("txn expired, region_id: %ld, txn_id: %lu:%d", region_id, state->txn_id, state->seq_id);
            return -1;
        } else {
            DB_WARNING("unknown error: txn_id: %lu:%d, errcode:%d, msg:%s", 
                state->txn_id, 
                state->seq_id, 
                res.code(), 
                res.ToString().c_str());
            return -1;
        }
        return num_affected_rows;
    } else if (_txn_cmd == pb::TXN_BEGIN_STORE) {

        Transaction* txn = nullptr;
        int ret = txn_pool->begin_txn(state->txn_id, txn);
        if (ret != 0) {
            DB_WARNING("create txn failed: %lu:%d", state->txn_id, state->seq_id);
            return -1;
        }
        state->set_txn(txn);
        return 0;
    } else if (_txn_cmd == pb::TXN_COMMIT_STORE) {
        // TODO: commit failure requires infinite retry until succeed
        Transaction* txn = txn_pool->get_txn(state->txn_id);
        if (txn == nullptr) {
            DB_WARNING("get txn failed, no txn in pool, txn_id: %lu", state->txn_id);
            return -1;
        }
        auto res = txn->commit();
        int ret = 0;
        if (res.ok()) {
            DB_WARNING("txn commit success, region_id: %ld, txn_id: %lu, seq_id:%d", 
                region_id, state->txn_id, state->seq_id);
        } else if (res.IsExpired()) {
            DB_WARNING("txn expired when commit, region_id: %ld, txn_id: %lu, seq_id:%d", 
                region_id, state->txn_id, state->seq_id);
            ret = -1;
        } else {
            DB_FATAL("unknown error, region_id: %ld, txn_id: %lu, err_code: %d, err_msg: %s", 
                region_id, state->txn_id, res.code(), res.ToString().c_str());
            ret = -1;
        }
        txn_pool->remove_txn(state->txn_id);
        return ret;
    } else if (_txn_cmd == pb::TXN_ROLLBACK_STORE) {
        // TODO: rollback failure can be simply ignored
        Transaction* txn = txn_pool->get_txn(state->txn_id);
        if (txn == nullptr) {
            DB_WARNING("get txn failed, no txn in pool, txn_id: %lu", state->txn_id);
            return -1;
        }
        auto res = txn->rollback();
        if (res.ok()) {
            DB_WARNING("txn rollback success, region_id: %ld, txn_id: %lu, seq_id:%d", 
                region_id, state->txn_id, state->seq_id);
        } else if (res.IsExpired()) {
            DB_WARNING("txn expired when rollback, region_id: %ld, txn_id: %lu, seq_id:%d", 
                region_id, state->txn_id, state->seq_id);
        } else {
            DB_FATAL("unknown error, region_id: %ld, txn_id: %lu, err_code: %d, err_msg: %s", 
                region_id, state->txn_id, res.code(), res.ToString().c_str());
        }
        txn_pool->remove_txn(state->txn_id);
        return 0;
    }
    return 0;
}

// serialize a TransactionNode into protobuf PlanNode
void TransactionNode::transfer_pb(pb::PlanNode* pb_node) {
    ExecNode::transfer_pb(pb_node);
    auto txn_node = pb_node->mutable_derive_node()->mutable_transaction_node();
    txn_node->set_txn_cmd(_txn_cmd);
}
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
