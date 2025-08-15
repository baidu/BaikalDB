#pragma once

#include "proto/db.interface.pb.h"

#ifdef BAIDU_INTERNAL
#include <baidu/rpc/server.h>
#include <baidu/rpc/controller.h>
#else
#include <brpc/server.h>
#include <brpc/controller.h>
#endif
#include <bthread/execution_queue.h>
#include <bthread/condition_variable.h>
#include "common.h"
#include "exec_node.h"
#include "fragment.h"
#include "rocksdb_scan_node.h"

namespace baikaldb {

class DBInteract {
public:
    static const int RETRY_TIMES = 3;
    
    static DBInteract* get_instance() {
        static DBInteract _instance;
        return &_instance;
    }

    int handle_mpp_dag_fragment(const pb::DAGFragmentRequest& request,
                            const std::string& db_address) {
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_db_request_timeout;
        channel_opt.connect_timeout_ms = FLAGS_db_connect_timeout;
        brpc::Channel short_channel;
        if (short_channel.Init(db_address.c_str(), &channel_opt) != 0) {
            DB_WARNING("connect with meta server fail. channel Init fail, leader_addr:%s",
                        db_address.c_str());
            return -1;
        }
        int retry_times = 0;
        bool db_unavailable = false;
        do {
            brpc::Controller cntl;
            pb::DbResponse response;
            cntl.set_log_id(request.log_id());
            pb::DbService_Stub(&short_channel).handle_mpp_dag_fragment(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                db_unavailable = true;
                DB_FATAL("send db server fail, log_id:%lu, db_address: %s, err:%s, retry times: %d", 
                        cntl.log_id(), db_address.c_str(), cntl.ErrorText().c_str(), retry_times);
                continue;
            }
            if (response.errcode() != pb::SUCCESS) {
                DB_FATAL("mpp execute fail: fragment fail, log_id:%lu, db_address: %s, err:%s, retry times: %d", 
                        cntl.log_id(), db_address.c_str(), pb::ErrCode_Name(response.errcode()).c_str(), retry_times);
                continue;
            } else {
                break;
            }
        } while (retry_times++ < RETRY_TIMES);

        if (retry_times >= RETRY_TIMES) {
            DB_FATAL("mpp execute fail: out of retries, log_id:%lu, db_address: %s, retry times: %d", 
                        request.log_id(), db_address.c_str(), retry_times);
            if (db_unavailable) {
                // 删除访问失败的db信息, 后续不会往该db发送fragment, 心跳处理会重置db状态
                SchemaFactory::get_instance()->set_db_unavailable(db_address);
            }
            return -1;
        }
        return 0;
    }
};

class DbService : public pb::DbService {
public:
    virtual ~DbService() {}
    static DbService* get_instance() {
        static DbService instance;
        return &instance;
    }
    int init_after_listen();

    virtual void transmit_data(google::protobuf::RpcController* controller,
                               const pb::TransmitDataParam* request,
                               pb::DbResponse* response,
                               google::protobuf::Closure* done);
    virtual void handle_mpp_dag_fragment(google::protobuf::RpcController* controller,
                                const pb::DAGFragmentRequest* request,
                                pb::DbResponse* response,
                                google::protobuf::Closure* done);
    void fragment_internal_exec(std::vector<std::shared_ptr<FragmentInfo> >& fragment_need_to_exec,
                                bool is_main_db);
    void close() {
        _shutdown = true;
        _query_cancel_bth.join();
    }

private:
    DbService() {}

    void handle_fragment_start(google::protobuf::RpcController* controller,
                                const pb::DAGFragmentRequest* request,
                                pb::DbResponse* response,
                                google::protobuf::Closure* done);
    int get_all_fragment_instance_id(google::protobuf::RpcController* controller,
                                const pb::DAGFragmentRequest* request,
                                std::unordered_map<uint64_t, pb::FragmentInfo>& fragment_need_to_build);
    int fragment_internal_open(google::protobuf::RpcController* controller,
                                const pb::DAGFragmentRequest* request,
                                pb::DbResponse* response,
                                uint64_t fragment_instance_id,
                                const pb::FragmentInfo& pb_fragment,
                                std::vector<std::shared_ptr<FragmentInfo> >& fragment_need_to_exec,
                                std::shared_ptr<UserInfo>& user_info);
    void handle_fragment_stop(google::protobuf::RpcController* controller,
                                const pb::DAGFragmentRequest* request,
                                pb::DbResponse* response,
                                google::protobuf::Closure* done);
    void handle_fragment_fatal(int64_t log_id, 
                                ExecNode* root, 
                                RuntimeState& state,
                                bool need_destory);
    int handle_fragment_scan_nodes(RuntimeState* state, 
                                std::vector<ExecNode*>& scan_nodes);
    void query_cancel_thread();
private:
    bool                                            _shutdown  = false;
    std::map<uint64_t, std::pair<int64_t, int>>     _query_start_time_map; // key: log_id, value: (start_time, fragment_cnt)
    std::unordered_map<std::string, int64_t>        _finished_exchange_receiver_keys; // 执行成功的receiver key, 延迟删除
    Bthread                                         _query_cancel_bth;   // 用于cancel超时的fragment; 同时清理执行成功的receiver key
    bthread::Mutex                                  _query_start_time_map_lock;
    bthread::Mutex                                  _finished_exchange_receiver_keys_lock;
};

} // naemspace baikaldb