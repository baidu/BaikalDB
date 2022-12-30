#include "task_manager.h"
#include "physical_planner.h"
#include "network_socket.h"
#include "ddl_work_planner.h"
#include "network_server.h"

namespace baikaldb {

DEFINE_int32(worker_number, 20, "baikaldb worker number.");
 
int TaskManager::init() {
    _workers.run([this](){
        this->fetch_thread();
    });
    return 0;
}

void TaskManager::fetch_thread() {
    DB_WARNING("fetch ok!");

    while (true) {
        //process ddlwork
        pb::RegionDdlWork work;
        if (TaskFactory<pb::RegionDdlWork>::get_instance()->fetch_task(work) != 0) {
            //DB_WARNING("no write ddl region data task");
        } else {
            _workers.run([work, this]() {
                process_ddl_work(work);
            });
        }

        // 等待事务完成任务
        pb::DdlWorkInfo txn_work;
        if (TaskFactory<pb::DdlWorkInfo>::get_instance()->fetch_task(txn_work) != 0) {
            DB_DEBUG("no wait txn done task");
        } else {
            _workers.run([txn_work, this]() {
                process_txn_ddl_work(txn_work);
            });
        }
        bthread_usleep(5 * 1000 * 1000LL);
    }
}

void TaskManager::process_txn_ddl_work(pb::DdlWorkInfo work) {
    DB_NOTICE("process txn ddl work %s", work.ShortDebugString().c_str());
    TimeCost tc;

    while (true) {
        if (tc.get_time() > 30 * 60 * 1000 * 1000LL) {
            DB_WARNING("time_out txn not ready.");
            work.set_status(pb::DdlWorkFail);
            break;
        }
        int64_t write_only_time = -1;
        {
            auto index_ptr = SchemaFactory::get_instance()->get_index_info_ptr(work.index_id());
            if (index_ptr != nullptr && index_ptr->write_only_time != -1) {
                write_only_time =  index_ptr->write_only_time;          
            } else {
                DB_NOTICE("wait ddl work %s txn done.", work.ShortDebugString().c_str());
                bthread_usleep(30 * 1000 * 1000LL);
                continue;
            }
        }
        auto epool_ptr = NetworkServer::get_instance()->get_epoll_info();
        if (epool_ptr != nullptr) {
            if (epool_ptr->all_txn_time_large_then(write_only_time, work.table_id())) {
                DB_NOTICE("epool time write_only_time %ld", write_only_time);
                work.set_status(pb::DdlWorkDone);
                break;
            } else {
                DB_NOTICE("wait ddl work %s txn done.", work.ShortDebugString().c_str());
                bthread_usleep(30 * 1000 * 1000LL);
                continue;
            }
        }
    }
    if (TaskFactory<pb::DdlWorkInfo>::get_instance()->finish_task(work) != 0) {
        DB_WARNING("finish work %s error", work.ShortDebugString().c_str());
    }
}

void TaskManager::process_ddl_work(pb::RegionDdlWork work) {
    DB_NOTICE("begin ddl work task_%ld_%ld : %s", work.table_id(), work.region_id(), work.ShortDebugString().c_str());
    int ret = 0;
    SmartSocket client(new NetworkSocket);
    client->query_ctx->client_conn = client.get();
    client->is_index_ddl = true;
    client->server_instance_id = NetworkServer::get_instance()->get_instance_id();
    std::unique_ptr<DDLWorkPlanner> planner_ptr(new  DDLWorkPlanner(client->query_ctx.get()));
    ret = planner_ptr->set_ddlwork(work);
    if (ret != 0) {
        DB_FATAL("ddl work[%s] set ddlwork fail.", work.ShortDebugString().c_str());
        work.set_status(pb::DdlWorkFail);
        TaskFactory<pb::RegionDdlWork>::get_instance()->finish_task(work);
        return;
    }
    ret = planner_ptr->plan();
    if (ret !=0) {
        DB_FATAL("ddl work[%s] fail plan error.", work.ShortDebugString().c_str());
        //plan失败，建二级索引初始化region失败。回滚，不重试
        work.set_status(pb::DdlWorkError);
        TaskFactory<pb::RegionDdlWork>::get_instance()->finish_task(work);
        return;
    }
    ret = planner_ptr->execute();
    if (ret == 0) {
        DB_NOTICE("process ddlwork %s success.", work.ShortDebugString().c_str());
    }

    if (TaskFactory<pb::RegionDdlWork>::get_instance()->finish_task(planner_ptr->get_ddlwork()) != 0) {
        DB_WARNING("finish work error");
    }
    DB_NOTICE("ddl work task_%ld_%ld finish ok! %s", work.table_id(), work.region_id(), work.ShortDebugString().c_str());
}

} // namespace baikaldb
