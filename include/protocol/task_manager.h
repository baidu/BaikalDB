#pragma once

#include "common.h"
#include "task_fetcher.h"

namespace baikaldb {

DECLARE_int32(worker_number);

class TaskManager : public Singleton<TaskManager> {
public:
    int init();

    void fetch_thread();

    void process_ddl_work(pb::RegionDdlWork work);
    void process_txn_ddl_work(pb::DdlWorkInfo work);

private:
    ConcurrencyBthread _workers {FLAGS_worker_number};
};
    
} // namespace baikaldb
