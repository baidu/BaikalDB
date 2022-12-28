// copyright (c) 2018-present baidu, inc. all rights reserved.
// 
// licensed under the apache license, version 2.0 (the "license");
// you may not use this file except in compliance with the license.
// you may obtain a copy of the license at
// 
//     http://www.apache.org/licenses/license-2.0
// 
// unless required by applicable law or agreed to in writing, software
// distributed under the license is distributed on an "as is" basis,
// without warranties or conditions of any kind, either express or implied.
// see the license for the specific language governing permissions and
// limitations under the license.

#include "ddl_manager.h"
#include "region_manager.h"
#include "meta_rocksdb.h"
#include "mut_table_key.h"
#include "table_manager.h"
#include "log.h"
#include "meta_util.h"

namespace baikaldb {

DEFINE_int32(baikaldb_max_concurrent, 5, "ddl work baikaldb concurrent");
DEFINE_int32(single_table_ddl_max_concurrent, 50, "ddl work baikaldb concurrent");
DEFINE_int32(submit_task_number_per_round, 20, "submit task number per round");
DEFINE_int32(ddl_status_update_interval_us, 10 * 1000 * 1000, "ddl_status_update_interval(us)");
DEFINE_int32(max_region_num_ratio, 2, "max region number ratio");
DEFINE_int32(max_ddl_retry_time, 30, "max ddl retry time");
DECLARE_int32(baikal_heartbeat_interval_us);

std::string construct_ddl_work_key(const std::string& identify, const std::initializer_list<int64_t>& ids) {
    std::string ddl_key;
    ddl_key = MetaServer::SCHEMA_IDENTIFY + identify;
    for (auto id : ids) {
        ddl_key.append((char*)&id, sizeof(int64_t));
    }
    return ddl_key;
}

bool StatusChangePolicy::should_change(int64_t table_id, pb::IndexState status) {
    BAIDU_SCOPED_LOCK(_mutex);
    size_t index = static_cast<size_t>(status);
    if (_time_costs_map[table_id][index] == nullptr) {
        _time_costs_map[table_id][index].reset(new TimeCost);
        return false;
    } else {
        return _time_costs_map[table_id][index]->get_time() > 5 * FLAGS_ddl_status_update_interval_us;
    }
}

void DBManager::process_common_task_hearbeat(const std::string& address, const pb::BaikalHeartBeatRequest* request,
    pb::BaikalHeartBeatResponse* response) {

    _common_task_map.update(address, [&address, &response](CommonTaskMap& db_task_map) {
        auto todo_iter = db_task_map.to_do_task_map.begin();
        for (; todo_iter != db_task_map.to_do_task_map.end(); ) {
            auto ddl_work_handle = response->add_region_ddl_works();
            auto& region_ddl_info = todo_iter->second.region_info;
            region_ddl_info.set_status(pb::DdlWorkDoing);
            region_ddl_info.set_address(address);
            ddl_work_handle->CopyFrom(region_ddl_info);
            todo_iter->second.update_timestamp = butil::gettimeofday_us(); 
            // 通过raft更新状态为 doing
            auto task_id = std::to_string(region_ddl_info.table_id()) + 
                "_" + std::to_string(region_ddl_info.region_id());
            DB_NOTICE("start_db task_%s work %s", task_id.c_str(), region_ddl_info.ShortDebugString().c_str());
            DDLManager::get_instance()->update_region_ddlwork(region_ddl_info);
            db_task_map.doing_task_map.insert(*todo_iter);
            todo_iter = db_task_map.to_do_task_map.erase(todo_iter);
        }
    });

    //处理已经完成的工作
    for (const auto& region_ddl_info : request->region_ddl_works()) {
        // 删除 _to_launch_task_info_map内任务。
        //DB_NOTICE("update ddlwork %s", region_ddl_info.ShortDebugString().c_str());
        _common_task_map.update(region_ddl_info.address(), [&region_ddl_info](CommonTaskMap& db_task_map) {
            auto task_id = std::to_string(region_ddl_info.table_id()) + 
                "_" + std::to_string(region_ddl_info.region_id());
            if (region_ddl_info.status() == pb::DdlWorkDoing) {
                // 正在运行，跟新时间戳。
                auto iter =  db_task_map.doing_task_map.find(task_id);
                if (iter != db_task_map.doing_task_map.end()) {
                    iter->second.update_timestamp = butil::gettimeofday_us();
                    //DB_NOTICE("task_%s update work %s", task_id.c_str(), region_ddl_info.ShortDebugString().c_str());
                }
            } else {
                auto doing_iter = db_task_map.doing_task_map.find(task_id);
                if (doing_iter != db_task_map.doing_task_map.end()) {
                    DB_NOTICE("task_%s work done %s", task_id.c_str(), region_ddl_info.ShortDebugString().c_str());
                    DDLManager::get_instance()->update_region_ddlwork(region_ddl_info);
                    db_task_map.doing_task_map.erase(doing_iter);
                }
            }
        });
    }
}

void DBManager::process_broadcast_task_hearbeat(const std::string& address, const pb::BaikalHeartBeatRequest* request,
    pb::BaikalHeartBeatResponse* response) {
    {
        std::vector<BroadcastTaskPtr> broadcast_task_tmp_vec;
        broadcast_task_tmp_vec.reserve(4);
        {
            BAIDU_SCOPED_LOCK(_broadcast_mutex);
            for (auto& txn_task : _broadcast_task_map) {
                broadcast_task_tmp_vec.emplace_back(txn_task.second);  
            }
        }

        for (auto& txn_task_ptr : broadcast_task_tmp_vec) {
            bool ret = txn_task_ptr->to_do_task_map.exist(address);
            if (ret) {
                MemDdlWork work;
                work.update_timestamp = butil::gettimeofday_us();
                txn_task_ptr->to_do_task_map.erase(address);
                txn_task_ptr->doing_task_map.set(address, work);
                auto txn_work_handle = response->add_ddl_works();
                txn_work_handle->CopyFrom(txn_task_ptr->work);
                txn_work_handle->set_status(pb::DdlWorkDoing);
            }
        }
    }

    for (const auto& txn_ddl_info : request->ddl_works()) {

        auto table_id = txn_ddl_info.table_id();
        BroadcastTaskPtr txn_ptr;
        {
            BAIDU_SCOPED_LOCK(_broadcast_mutex);
            auto iter = _broadcast_task_map.find(table_id);
            if (iter == _broadcast_task_map.end()) {
                DB_NOTICE("unknown txn task.");
                continue;
            }
            txn_ptr = iter->second;
        }
        
        DB_NOTICE("before number %ld", txn_ptr->number.load());
        //iter->second.done_txn_task_map.insert(std::make_pair(address, txn_ddl_info));
        //判断是否所有的db都返回。
        if (txn_ddl_info.status() == pb::DdlWorkDoing) {
            bool ret = txn_ptr->doing_task_map.update(address, [address](MemDdlWork& ddlwork){
                ddlwork.update_timestamp = butil::gettimeofday_us();
                DB_NOTICE("update txn work timestamp %ld address:%s", ddlwork.update_timestamp, address.c_str());
            });
            if (!ret) {
                txn_ptr->to_do_task_map.update(address, [address](MemDdlWork& ddlwork){
                    ddlwork.update_timestamp = butil::gettimeofday_us();
                    DB_NOTICE("update txn work timestamp %ld address:%s", ddlwork.update_timestamp, address.c_str());
                });
            }
            continue;
        } else if (txn_ddl_info.status() == pb::DdlWorkFail) {
            DB_WARNING("wait txn work %s fail address:%s.", txn_ddl_info.ShortDebugString().c_str(), address.c_str());
            DDLManager::get_instance()->set_txn_ready(txn_ptr->work.table_id(), false);
            {
                BAIDU_SCOPED_LOCK(_broadcast_mutex);
                _broadcast_task_map.erase(table_id);
            }
        } else if (txn_ddl_info.status() == pb::DdlWorkDone) {
            bool ret = txn_ptr->doing_task_map.exist(address);
            if (ret) {
                txn_ptr->number--;
                txn_ptr->doing_task_map.erase(address);
            }
        }
        if (txn_ptr->number == 0) {
            DB_NOTICE("table_%ld txn work done.", table_id);
            DDLManager::get_instance()->set_txn_ready(txn_ptr->work.table_id(), true);
            {
                BAIDU_SCOPED_LOCK(_broadcast_mutex);
                _broadcast_task_map.erase(table_id);
            }
        }
    }
}
void DBManager::process_baikal_heartbeat(const pb::BaikalHeartBeatRequest* request,
    pb::BaikalHeartBeatResponse* response, brpc::Controller* cntl) {
    // 更新baikaldb 信息
    if (!request->can_do_ddlwork()) {
        return;
    }
    TimeCost tc;
    std::string address = butil::endpoint2str(cntl->remote_side()).c_str();
    auto room = request->physical_room();
    update_baikaldb_info(address, room);
    auto update_db_info_ts = tc.get_time();
    tc.reset();

    process_common_task_hearbeat(address, request, response);
    auto common_task_ts = tc.get_time();
    tc.reset();

    process_broadcast_task_hearbeat(address, request, response);
    auto broadcast_task_ts = tc.get_time();

    DB_NOTICE("process ddl baikal heartbeat update biakaldb info %ld, common task time %ld, broadcast task time %ld",
        update_db_info_ts, common_task_ts, broadcast_task_ts);

    DB_DEBUG("ddl_request : %s address %s", request->ShortDebugString().c_str(), address.c_str());
    DB_DEBUG("dll_response : %s address %s", response->ShortDebugString().c_str(), address.c_str());
}

bool DBManager::round_robin_select(std::string* selected_address, bool is_column_ddl) {
    BAIDU_SCOPED_LOCK(_address_instance_mutex);
    auto iter = _address_instance_map.find(_last_rolling_instance);
    if (iter == _address_instance_map.end() || (++iter) == _address_instance_map.end()) {
        iter = _address_instance_map.begin();
    }
    auto instance_count = _address_instance_map.size();
    for (size_t index = 0; index < instance_count; ++index) {
        if (iter == _address_instance_map.end()) {
            iter = _address_instance_map.begin();
        }
        if (iter->second.instance_status.state == pb::FAULTY) {
            DB_NOTICE("address %s is faulty.", iter->first.c_str());
            iter++;
            continue;
        }
        int32_t current_task_number = 0;
        auto find_task_map = _common_task_map.init_if_not_exist_else_update(iter->first, false, [&current_task_number](CommonTaskMap& db_task_map){
            current_task_number = db_task_map.doing_task_map.size() + db_task_map.to_do_task_map.size();
        });
        int32_t max_concurrent = is_column_ddl ? FLAGS_baikaldb_max_concurrent * 5 : FLAGS_baikaldb_max_concurrent;
        if (!find_task_map || current_task_number < max_concurrent) {
            _last_rolling_instance = iter->first;
            *selected_address = iter->first;
            DB_NOTICE("select address %s", iter->first.c_str());
            return true;
        }
        iter++;
    }
    return false;
}

bool DBManager::select_instance(std::string* selected_address, bool is_column_ddl) {
    return round_robin_select(selected_address, is_column_ddl);
}

int DBManager::execute_task(MemRegionDdlWork& work) {
    int32_t all_task_count_by_table_id = 0;
    std::string table_id_prefix = std::to_string(work.region_info.table_id());
    _common_task_map.traverse([&all_task_count_by_table_id, &table_id_prefix](CommonTaskMap& db_task_map) {
        auto iter1 = db_task_map.to_do_task_map.cbegin();
        while (iter1 != db_task_map.to_do_task_map.cend()) {
            if (iter1->first.find(table_id_prefix) == 0) {
                ++all_task_count_by_table_id;
            }
            ++iter1;
        }
        auto iter2 = db_task_map.doing_task_map.cbegin();
        while (iter2 != db_task_map.doing_task_map.cend()) {
            if (iter2->first.find(table_id_prefix) == 0) {
                ++all_task_count_by_table_id;
            }
            ++iter2;
        }
    });
    int32_t max_concurrent = work.region_info.op_type() == pb::OP_MODIFY_FIELD ?
        FLAGS_single_table_ddl_max_concurrent * 10 : FLAGS_single_table_ddl_max_concurrent;
    if (all_task_count_by_table_id > max_concurrent) {
        DB_NOTICE("table %s ddl task count %d reach max concurrency %d", table_id_prefix.c_str(),
            all_task_count_by_table_id, max_concurrent);
        return -1;
    }

    //选择address执行
    auto& region_ddl_info = work.region_info;
    work.update_timestamp = butil::gettimeofday_us();
    std::string address;
    if (select_instance(&address, work.region_info.op_type() == pb::OP_MODIFY_FIELD)) {
        auto task_id = std::to_string(region_ddl_info.table_id()) + "_" + std::to_string(region_ddl_info.region_id());
        auto retry_time = region_ddl_info.retry_time();
        region_ddl_info.set_retry_time(++retry_time);
        region_ddl_info.set_address(address);
        CommonTaskMap map;
        map.to_do_task_map[task_id] = work;
        _common_task_map.init_if_not_exist_else_update(address, false, [&work, &task_id](CommonTaskMap& db_task_map){
            db_task_map.to_do_task_map[task_id] = work;
        }, map);
        DB_NOTICE("choose address_%s for task_%s", address.c_str(), task_id.c_str());
        return 0;
    } else {
        return -1;
    }
}

std::vector<std::string> DBManager::get_faulty_baikaldb() {
    std::vector<std::string> ret;
    ret.reserve(5);
    BAIDU_SCOPED_LOCK(_address_instance_mutex);
    auto iter = _address_instance_map.begin();
    for (; iter != _address_instance_map.end(); ) {
        if (butil::gettimeofday_us() - iter->second.instance_status.timestamp >
            FLAGS_baikal_heartbeat_interval_us * 20) {
            DB_NOTICE("db %s is faulty.", iter->first.c_str());
            iter->second.instance_status.state = pb::FAULTY;
            ret.emplace_back(iter->first);

            if (butil::gettimeofday_us() - iter->second.instance_status.timestamp >
                FLAGS_baikal_heartbeat_interval_us * 90) {
                DB_NOTICE("db %s is dead, delete", iter->first.c_str());
                iter = _address_instance_map.erase(iter);
                continue;
            }
        }
        iter++;
    }
    return ret;
}

void DBManager::init() {
    _bth.run([this]() {
        DB_NOTICE("sleep, wait collect db info.");
        bthread_usleep(2 * 60 * 1000 * 1000LL);
        while (!_shutdown) {
            if (!_meta_state_machine->is_leader()) {
                DB_NOTICE("not leader, sleep.");
                bthread_usleep_fast_shutdown(5 * 1000 * 1000, _shutdown);
                continue;
            }
            DB_NOTICE("db manager working thread.");
            _common_task_map.traverse([this](CommonTaskMap& db_task_map) {
                auto traverse_func = [](std::unordered_map<TaskId, MemRegionDdlWork>& update_map){
                    auto iter = update_map.begin();
                    for (; iter != update_map.end(); ) {
                        if (butil::gettimeofday_us() - iter->second.update_timestamp >
                            FLAGS_baikal_heartbeat_interval_us * 20) {

                            auto task_id = std::to_string(iter->second.region_info.table_id()) + "_" + 
                                std::to_string(iter->second.region_info.region_id());
                            DB_NOTICE("task_%s restart work %s", task_id.c_str(), 
                                iter->second.region_info.ShortDebugString().c_str());                                               

                            iter->second.region_info.set_status(pb::DdlWorkIdle);
                            DDLManager::get_instance()->update_region_ddlwork(iter->second.region_info);
                            iter = update_map.erase(iter);
                        } else {
                            iter++;
                        }
                    }
                };
                traverse_func(db_task_map.to_do_task_map);
                traverse_func(db_task_map.doing_task_map);
                
            });
            std::vector<BroadcastTaskPtr> broadcast_task_tmp_vec;
            {
                broadcast_task_tmp_vec.reserve(4);
                {
                    BAIDU_SCOPED_LOCK(_broadcast_mutex);
                    for (auto& txn_task : _broadcast_task_map) {
                        broadcast_task_tmp_vec.emplace_back(txn_task.second);  
                    }
                }
            }
            for (auto& cast_task_ptr : broadcast_task_tmp_vec) {
                auto delete_heartbeat_timeout_txn_work = [&cast_task_ptr](ThreadSafeMap<std::string, MemDdlWork>& work_map) {
                    std::vector<std::string> timeout_instance_vec;
                    timeout_instance_vec.reserve(5);
                    work_map.traverse_with_key_value([&cast_task_ptr, &timeout_instance_vec](const std::string& instance, MemDdlWork& work) {
                        if (butil::gettimeofday_us() - work.update_timestamp >
                            FLAGS_baikal_heartbeat_interval_us * 30) {
                            DB_WARNING("instance %s txn work heartbeat timeout.", instance.c_str());
                            timeout_instance_vec.emplace_back(instance);
                        }
                    });
                    for (auto& instance : timeout_instance_vec) {
                        cast_task_ptr->number -= work_map.erase(instance);
                    }
                };
                delete_heartbeat_timeout_txn_work(cast_task_ptr->doing_task_map);
                delete_heartbeat_timeout_txn_work(cast_task_ptr->to_do_task_map);
            }
            
            auto faulty_dbs = get_faulty_baikaldb();
            for (const auto& faulty_db : faulty_dbs) {
                _common_task_map.update(faulty_db, [this](CommonTaskMap& db_task_map) {
                    auto re_launch_task_func = [this](std::unordered_map<TaskId, MemRegionDdlWork>& task_map) {
                        for (auto& task : task_map) {
                            auto task_id = std::to_string(task.second.region_info.table_id()) + "_" + 
                                std::to_string(task.second.region_info.region_id());
                            DB_NOTICE("re_launch task_%s %s", task_id.c_str(), task.second.region_info.ShortDebugString().c_str());
                            task.second.region_info.set_status(pb::DdlWorkIdle);
                            DDLManager::get_instance()->update_region_ddlwork(task.second.region_info);
                        }
                        task_map.clear();
                    };
                    re_launch_task_func(db_task_map.to_do_task_map);
                    re_launch_task_func(db_task_map.doing_task_map);
                });

                BAIDU_SCOPED_LOCK(_broadcast_mutex);
                for (auto& txn_work : _broadcast_task_map) {
                    txn_work.second->number -= txn_work.second->to_do_task_map.erase(faulty_db);
                    txn_work.second->number -= txn_work.second->doing_task_map.erase(faulty_db);
                }
            }
            bthread_usleep_fast_shutdown(20 * 1000 * 1000, _shutdown);
        }                
    });
}

int DBManager::restore_task(const pb::RegionDdlWork& region_ddl_info) {
    auto task_id = std::to_string(region_ddl_info.table_id()) + "_" + std::to_string(region_ddl_info.region_id());
    CommonTaskMap map;
    MemRegionDdlWork work;
    work.region_info = region_ddl_info;
    work.update_timestamp = butil::gettimeofday_us();
    map.to_do_task_map[task_id] = work;
    _common_task_map.init_if_not_exist_else_update(region_ddl_info.address(), false, [&work, &task_id](CommonTaskMap& db_task_map){
        db_task_map.doing_task_map[task_id] = work;
    }, map);
    DB_NOTICE("choose address_%s for doing_task_map task_%s", region_ddl_info.address().c_str(), task_id.c_str());
    return 0;
}

void DBManager::update_txn_ready(int64_t table_id) {
    auto is_ready = false;
    {
        BAIDU_SCOPED_LOCK(_broadcast_mutex);
        auto iter = _broadcast_task_map.find(table_id);
        if (iter != _broadcast_task_map.end()) {
            if (iter->second->number == 0) {
                is_ready = true;
                _broadcast_task_map.erase(iter);
            }
        } else {
            DB_WARNING("unknown txn work %ld", table_id);
        }
    }
    if (is_ready) {
        DDLManager::get_instance()->set_txn_ready(table_id, true);
    }
}

int DDLManager::init_del_index_ddlwork(int64_t table_id, const pb::IndexInfo& index_info) {
    DB_NOTICE("init del ddl tid_%ld iid_%ld is_global:%d", table_id,
        index_info.index_id(), index_info.is_global());
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_ddl_mem.count(table_id) == 1) {
        DB_WARNING("table_id_%ld delete index is running..", table_id);
        return -1;
    }
    MemDdlInfo mem_info;
    mem_info.work_info.set_table_id(table_id);
    mem_info.work_info.set_op_type(pb::OP_DROP_INDEX);
    mem_info.work_info.set_index_id(index_info.index_id());
    mem_info.work_info.set_errcode(pb::IN_PROCESS);
    mem_info.work_info.set_global(index_info.is_global());
    _table_ddl_mem.emplace(table_id, mem_info);
    std::string index_ddl_string;
    if (!mem_info.work_info.SerializeToString(&index_ddl_string)) {
        DB_FATAL("serialzeTostring error.");
        return -1;
    }
    if(MetaRocksdb::get_instance()->put_meta_info(
        construct_ddl_work_key(MetaServer::DDLWORK_IDENTIFY, {table_id}), index_ddl_string) != 0) {
        DB_FATAL("put meta info error.");
        return -1;
    }
    return 0;
}

int DDLManager::init_index_ddlwork(int64_t table_id, const pb::IndexInfo& index_info, 
    std::unordered_map<int64_t, std::set<int64_t>>& partition_regions) {
    DB_NOTICE("init ddl tid_%ld iid_%ld", table_id, index_info.index_id());
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_ddl_mem.count(table_id) == 1) {
        DB_WARNING("table_id_%ld add index is running..", table_id);
        return -1;
    }
    MemDdlInfo mem_info;
    mem_info.work_info.set_table_id(table_id);
    mem_info.work_info.set_op_type(pb::OP_ADD_INDEX);
    mem_info.work_info.set_index_id(index_info.index_id());
    mem_info.work_info.set_errcode(pb::IN_PROCESS);
    mem_info.work_info.set_status(pb::DdlWorkIdle);
    mem_info.work_info.set_global(index_info.is_global());
    _table_ddl_mem.emplace(table_id, mem_info);
    std::string index_ddl_string;
    if (!mem_info.work_info.SerializeToString(&index_ddl_string)) {
        DB_FATAL("serialzeTostring error.");
        return -1;
    }
    if(MetaRocksdb::get_instance()->put_meta_info(
        construct_ddl_work_key(MetaServer::DDLWORK_IDENTIFY, {table_id}), index_ddl_string) != 0) {
        DB_FATAL("put meta info error.");
        return -1;
    }
    std::vector<int64_t> region_ids;
    std::unordered_map<int64_t, int64_t> region_partition_map;
    region_ids.reserve(1000);
    for (const auto& partition_region : partition_regions) {
        for (auto& region_id :  partition_region.second) {
            region_ids.emplace_back(region_id);    
            region_partition_map[region_id] = partition_region.first;
        }
    }
    DB_NOTICE("work %s region size %zu", mem_info.work_info.ShortDebugString().c_str(), region_ids.size());
    std::vector<SmartRegionInfo> region_infos;
    RegionManager::get_instance()->get_region_info(region_ids, region_infos);

    MemRegionDdlWorkMapPtr region_map_ptr;
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        _region_ddlwork[table_id].reset(new ThreadSafeMap<int64_t, MemRegionDdlWork>);
        region_map_ptr = _region_ddlwork[table_id];
    }
    std::vector<std::string> region_ddl_work_keys;
    std::vector<std::string> region_ddl_work_values;
    region_ddl_work_keys.reserve(100);
    region_ddl_work_values.reserve(100);
    for (const auto& region_info : region_infos) {
        pb::RegionDdlWork region_work;
        region_work.set_table_id(table_id);
        region_work.set_op_type(pb::OP_ADD_INDEX);
        region_work.set_region_id(region_info->region_id());
        region_work.set_start_key(region_info->start_key());
        region_work.set_end_key(region_info->end_key());
        region_work.set_status(pb::DdlWorkIdle);
        region_work.set_index_id(index_info.index_id());
        region_work.set_partition(region_partition_map[region_info->region_id()]);
        std::string region_work_string;
        if (!region_work.SerializeToString(&region_work_string)) {
            DB_FATAL("serialze region work error.");
            return -1;
        }
        MemRegionDdlWork region_ddl_work;
        region_ddl_work.region_info = region_work;
        
        region_map_ptr->set(region_info->region_id(), region_ddl_work);

        auto task_id = std::to_string(table_id) + "_" + std::to_string(region_work.region_id());
        DB_NOTICE("init region_ddlwork task_%s table%ld region_%ld region_%s", task_id.c_str(), table_id, 
            region_info->region_id(), region_work.ShortDebugString().c_str());
        region_ddl_work_keys.emplace_back(construct_ddl_work_key(MetaServer::INDEX_DDLWORK_REGION_IDENTIFY, 
                    {table_id, region_info->region_id()}));
        region_ddl_work_values.emplace_back(region_work_string);
        if (region_ddl_work_keys.size() == 100) {
            if(MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
                DB_FATAL("put region info error.");
                return -1;
            }
            region_ddl_work_keys.clear();
            region_ddl_work_values.clear();
        }
    }
    if (region_ddl_work_keys.size() != 0) {
        if(MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
            DB_FATAL("put region info error.");
            return -1;
        }
    }
    return 0;
}

int DDLManager::init_column_ddlwork(int64_t table_id, const pb::DdlWorkInfo& work_info, 
    std::unordered_map<int64_t, std::set<int64_t>>& partition_regions) {
    DB_NOTICE("init column ddl tid_%ld opt:%s", table_id, work_info.opt_sql().c_str());
    BAIDU_SCOPED_LOCK(_table_mutex);
    if (_table_ddl_mem.count(table_id) == 1) {
        DB_WARNING("table_id_%ld add index is running..", table_id);
        return -1;
    }
    MemDdlInfo mem_info;
    mem_info.work_info.CopyFrom(work_info);
    mem_info.work_info.set_errcode(pb::IN_PROCESS);
    mem_info.work_info.set_status(pb::DdlWorkIdle);
    mem_info.work_info.set_job_state(pb::IS_NONE);
    _table_ddl_mem.emplace(table_id, mem_info);
    std::string ddl_work_string;
    if (!mem_info.work_info.SerializeToString(&ddl_work_string)) {
        DB_FATAL("serialzeTostring error.");
        return -1;
    }
    if(MetaRocksdb::get_instance()->put_meta_info(
        construct_ddl_work_key(MetaServer::DDLWORK_IDENTIFY, {table_id}), ddl_work_string) != 0) {
        DB_FATAL("put meta info error.");
        return -1;
    }
    std::vector<int64_t> region_ids;
    std::unordered_map<int64_t, int64_t> region_partition_map;
    region_ids.reserve(1000);
    for (const auto& partition_region : partition_regions) {
        for (auto& region_id :  partition_region.second) {
            region_ids.emplace_back(region_id);    
            region_partition_map[region_id] = partition_region.first;
        }
    }
    DB_NOTICE("work %s region size %zu", mem_info.work_info.ShortDebugString().c_str(), region_ids.size());
    std::vector<SmartRegionInfo> region_infos;
    RegionManager::get_instance()->get_region_info(region_ids, region_infos);

    MemRegionDdlWorkMapPtr region_map_ptr;
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        _region_ddlwork[table_id].reset(new ThreadSafeMap<int64_t, MemRegionDdlWork>);
        region_map_ptr = _region_ddlwork[table_id];
    }
    std::vector<std::string> region_ddl_work_keys;
    std::vector<std::string> region_ddl_work_values;
    region_ddl_work_keys.reserve(100);
    region_ddl_work_values.reserve(100);
    for (const auto& region_info : region_infos) {
        pb::RegionDdlWork region_work;
        region_work.set_table_id(table_id);
        region_work.set_op_type(pb::OP_MODIFY_FIELD);
        region_work.set_region_id(region_info->region_id());
        region_work.set_start_key(region_info->start_key());
        region_work.set_end_key(region_info->end_key());
        region_work.set_status(pb::DdlWorkIdle);
        region_work.set_partition(region_partition_map[region_info->region_id()]);
        region_work.mutable_column_ddl_info()->CopyFrom(work_info.column_ddl_info());
        std::string region_work_string;
        if (!region_work.SerializeToString(&region_work_string)) {
            DB_FATAL("serialze region work error.");
            return -1;
        }
        MemRegionDdlWork region_ddl_work;
        region_ddl_work.region_info = region_work;
        
        region_map_ptr->set(region_info->region_id(), region_ddl_work);

        auto task_id = std::to_string(table_id) + "_" + std::to_string(region_work.region_id());
        DB_NOTICE("init region_ddlwork task_%s table%ld region_%ld region_%s", task_id.c_str(), table_id, 
            region_info->region_id(), region_work.ShortDebugString().c_str());
        region_ddl_work_keys.emplace_back(construct_ddl_work_key(MetaServer::INDEX_DDLWORK_REGION_IDENTIFY, 
                    {table_id, region_info->region_id()}));
        region_ddl_work_values.emplace_back(region_work_string);
        if (region_ddl_work_keys.size() == 100) {
            if(MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
                DB_FATAL("put region info error.");
                return -1;
            }
            region_ddl_work_keys.clear();
            region_ddl_work_values.clear();
        }
    }
    if (region_ddl_work_keys.size() != 0) {
        if(MetaRocksdb::get_instance()->put_meta_info(region_ddl_work_keys, region_ddl_work_values) != 0) {
            DB_FATAL("put region info error.");
            return -1;
        }
    }
    return 0;
}

// 定时线程处理所有ddl work。
int DDLManager::work() {
    DB_NOTICE("sleep, wait ddl manager init.");
    bthread_usleep(3 * 60 * 1000 * 1000LL);
    while (!_shutdown) {
        if (!_meta_state_machine->is_leader()) {
            DB_NOTICE("not leader, sleep.");
            bthread_usleep_fast_shutdown(5 * 1000 * 1000, _shutdown);
            continue;
        }
        DB_NOTICE("leader process ddl work.");
        std::unordered_map<int64_t, MemDdlInfo> temp_ddl_mem;
        {
            BAIDU_SCOPED_LOCK(_table_mutex);
            for (auto iter = _table_ddl_mem.begin(); iter != _table_ddl_mem.end(); iter++) {
                if (iter->second.work_info.errcode() == pb::SUCCESS || iter->second.work_info.errcode() == pb::EXEC_FAIL) {
                    pb::MetaManagerRequest clear_request;
                    clear_request.mutable_ddlwork_info()->CopyFrom(iter->second.work_info);
                    clear_request.set_op_type(pb::OP_DELETE_DDLWORK);
                    apply_raft(clear_request);

                    if (iter->second.work_info.errcode() == pb::EXEC_FAIL && iter->second.work_info.op_type() == pb::OP_ADD_INDEX) {
                        DB_NOTICE("ddl add index job fail, drop index %s", iter->second.work_info.ShortDebugString().c_str());
                        TableManager::get_instance()->drop_index_request(iter->second.work_info);
                    }
                    DB_NOTICE("ddl job[%s] finish.", iter->second.work_info.ShortDebugString().c_str());
                } else {
                    if (iter->second.work_info.suspend()) {
                        DB_NOTICE("work %ld is suspend.", iter->second.work_info.table_id());
                    } else {
                        temp_ddl_mem.insert(*iter);
                    }
                }
            }
        }
        
        for (auto& table_ddl_info  : temp_ddl_mem) {
            auto op_type = table_ddl_info.second.work_info.op_type();
            if (op_type == pb::OP_DROP_INDEX) {
                drop_index_ddlwork(table_ddl_info.second.work_info);
            } else if (op_type == pb::OP_ADD_INDEX) {
                add_index_ddlwork(table_ddl_info.second.work_info);
            } else if (op_type == pb::OP_MODIFY_FIELD) {
                add_column_ddlwork(table_ddl_info.second.work_info);
            } else {
                DB_FATAL("unknown optype.");
            }
        }
        bthread_usleep_fast_shutdown(20 * 1000 * 1000, _shutdown);
    }
    
    return 0;
}

int DDLManager::load_table_ddl_snapshot(const pb::DdlWorkInfo& ddl_work) {
    BAIDU_SCOPED_LOCK(_table_mutex);
    DB_NOTICE("load table ddl snapshot %s.", ddl_work.ShortDebugString().c_str());
    MemDdlInfo mem_info;
    mem_info.work_info = ddl_work;
    if (ddl_work.op_type() == pb::OP_MODIFY_FIELD && ddl_work.job_state() == pb::IS_PUBLIC) {
        _table_ddl_done_mem.emplace(ddl_work.table_id(), mem_info);
    } else {
        _table_ddl_mem.emplace(ddl_work.table_id(), mem_info);
    }
    return 0;
}

int DDLManager::load_region_ddl_snapshot(const std::string& region_ddl_info) {
    pb::RegionDdlWork region_work;
    if (!region_work.ParseFromString(region_ddl_info)) {
        DB_FATAL("parse from string error.");
        return 0;
    }
    MemRegionDdlWork region_ddl_work;
    region_ddl_work.region_info = region_work;
    auto task_id = std::to_string(region_ddl_work.region_info.table_id()) + 
        "_" + std::to_string(region_ddl_work.region_info.region_id());
    DB_NOTICE("load region ddl task_%s snapshot %s", 
        task_id.c_str(), region_ddl_work.region_info.ShortDebugString().c_str());
    auto table_id = region_work.table_id();
    BAIDU_SCOPED_LOCK(_region_mutex);
    if (_region_ddlwork[table_id] == nullptr) {
        _region_ddlwork[table_id].reset(new ThreadSafeMap<int64_t, MemRegionDdlWork>);
    }
    _region_ddlwork[table_id]->set(region_work.region_id(), region_ddl_work);
    return 0;
}

void DDLManager::on_leader_start() {
    std::vector<MemRegionDdlWorkMapPtr> region_work_ptrs;
    region_work_ptrs.reserve(5);
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        for (auto& region_map_pair : _region_ddlwork) {
            region_work_ptrs.emplace_back(region_map_pair.second);
        }
    }
    for (auto& region_work_ptr : region_work_ptrs) {
        DB_NOTICE("leader start reload ddl work.");
        region_work_ptr->traverse([this](MemRegionDdlWork& work) {
            auto& region_work = work.region_info;
            if (region_work.status() == pb::DdlWorkDoing) {
                DB_NOTICE("restore ddl work %s.", region_work.ShortDebugString().c_str());
                increase_doing_work_number(region_work.table_id());
                DBManager::get_instance()->restore_task(region_work);
            }
        });
    }
}

void DDLManager::get_ddlwork_info(int64_t table_id, pb::QueryResponse* query_response) {
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto iter = _table_ddl_mem.find(table_id);
        if (iter != _table_ddl_mem.end()) {
            query_response->add_ddlwork_infos()->CopyFrom(iter->second.work_info);
        } else {
            auto iter2 = _table_ddl_done_mem.find(table_id);
            if (iter2 != _table_ddl_done_mem.end()) {
                query_response->add_ddlwork_infos()->CopyFrom(iter2->second.work_info);
            }
        }
    }
    MemRegionDdlWorkMapPtr region_work_ptr;
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        auto iter = _region_ddlwork.find(table_id);
        if (iter != _region_ddlwork.end()) {
            region_work_ptr = iter->second;
        }
    }
    if (region_work_ptr != nullptr) {
        region_work_ptr->traverse([this, query_response](MemRegionDdlWork& work) {
            query_response->add_region_ddl_infos()->CopyFrom(work.region_info);
        });
    }
}

int DDLManager::launch_work() {
    _work_thread.run([this]() {
        this->work();
    });
    return 0;
}

int DDLManager::drop_index_ddlwork(pb::DdlWorkInfo& ddl_work) {
    int64_t table_id = ddl_work.table_id();
    DB_NOTICE("process drop index ddlwork tid_%ld", table_id);
    pb::IndexState current_state;
    if (TableManager::get_instance()->get_index_state(ddl_work.table_id(), ddl_work.index_id(), current_state) != 0) {
        DB_WARNING("ddl index not ready. table_id[%ld] index_id[%ld]", 
            ddl_work.table_id(), ddl_work.index_id());
        return -1;
    }
    if (ddl_work.errcode() == pb::EXEC_FAIL) {
        DB_FATAL("drop index failed");
        return 0;
    }
    switch (current_state)
    {
    case pb::IS_NONE: {
        if (_update_policy.should_change(table_id, current_state)) {
            ddl_work.set_deleted(true);
            ddl_work.set_errcode(pb::SUCCESS);
            ddl_work.set_status(pb::DdlWorkDone);
            TableManager::get_instance()->update_index_status(ddl_work);
            if (ddl_work.global()) {
                pb::MetaManagerRequest request;
                request.mutable_ddlwork_info()->CopyFrom(ddl_work);
                request.set_op_type(pb::OP_REMOVE_GLOBAL_INDEX_DATA);
                apply_raft(request);
            }
            _update_policy.clear(table_id);
            update_table_ddl_mem(ddl_work);
        }
        break;
    }
    case pb::IS_DELETE_LOCAL: {
        if (_update_policy.should_change(table_id, current_state)) {
            ddl_work.set_deleted(true);
            ddl_work.set_errcode(pb::SUCCESS);
            ddl_work.set_status(pb::DdlWorkDone);
            TableManager::get_instance()->update_index_status(ddl_work);
            _update_policy.clear(table_id);
            update_table_ddl_mem(ddl_work);
        }
        break;
    }
    case pb::IS_DELETE_ONLY: {
        if (_update_policy.should_change(table_id, current_state)) {
            if (ddl_work.global()) {
                ddl_work.set_job_state(pb::IS_NONE);
            } else {
                ddl_work.set_job_state(pb::IS_DELETE_LOCAL);
            }
            TableManager::get_instance()->update_index_status(ddl_work);
            update_table_ddl_mem(ddl_work);
        }
        break;
    }
    case pb::IS_WRITE_ONLY: {
        if (_update_policy.should_change(table_id, current_state)) {
            ddl_work.set_job_state(pb::IS_DELETE_ONLY);
            ddl_work.set_status(pb::DdlWorkDoing);
            TableManager::get_instance()->update_index_status(ddl_work);
            update_table_ddl_mem(ddl_work);
        }
        break;
    }
    case pb::IS_WRITE_LOCAL: {
        if (_update_policy.should_change(table_id, current_state)) {
            ddl_work.set_job_state(pb::IS_WRITE_ONLY);
            ddl_work.set_status(pb::DdlWorkDoing);
            TableManager::get_instance()->update_index_status(ddl_work);
            update_table_ddl_mem(ddl_work);
        }
        break;
    }
    case pb::IS_PUBLIC: {
        if (_update_policy.should_change(table_id, current_state)) {
            ddl_work.set_job_state(pb::IS_WRITE_ONLY);
            ddl_work.set_status(pb::DdlWorkDoing);
            TableManager::get_instance()->update_index_status(ddl_work);
            update_table_ddl_mem(ddl_work);
        }
        break;
    }
    default:
        break;
    }
    return 0;

}
//处理单个ddl work
int DDLManager::add_index_ddlwork(pb::DdlWorkInfo& ddl_work) {
    int64_t table_id = ddl_work.table_id();
    int64_t region_table_id = ddl_work.global() ? ddl_work.index_id() : ddl_work.table_id();
    size_t region_size = TableManager::get_instance()->get_region_size(region_table_id);
    DB_NOTICE("table_id:%ld index_id:%ld region size %zu", table_id, ddl_work.index_id(), region_size);
    pb::IndexState current_state;
    if (TableManager::get_instance()->get_index_state(ddl_work.table_id(), ddl_work.index_id(), current_state) != 0) {
        DB_WARNING("ddl index not ready. table_id[%ld] index_id[%ld]", 
            ddl_work.table_id(), ddl_work.index_id());
        return -1;
    }
    if (ddl_work.errcode() == pb::EXEC_FAIL) {
        DB_FATAL("ddl work %s fail", ddl_work.ShortDebugString().c_str());
        return 0;
    }

    switch (current_state) {
    case pb::IS_NONE:
        if (_update_policy.should_change(table_id, current_state)) {
            ddl_work.set_job_state(pb::IS_DELETE_ONLY);
            ddl_work.set_status(pb::DdlWorkDoing);
            update_table_ddl_mem(ddl_work);
            TableManager::get_instance()->update_index_status(ddl_work);
        }
        break;
    case pb::IS_DELETE_ONLY:
        if (_update_policy.should_change(table_id, current_state)) {
            ddl_work.set_job_state(pb::IS_WRITE_ONLY);
            update_table_ddl_mem(ddl_work);
            TableManager::get_instance()->update_index_status(ddl_work);
        }
        break;
    case pb::IS_WRITE_ONLY:
        {
            if (!exist_wait_txn_info(table_id)) {
                set_wait_txn_info(table_id, ddl_work);
                DBManager::get_instance()->execute_broadcast_task(ddl_work);
            } else {
                DBManager::get_instance()->update_txn_ready(table_id);
                if (is_txn_done(table_id)) {
                    if (is_txn_success(table_id)) {
                        DB_NOTICE("ddl work %s all txn done", ddl_work.ShortDebugString().c_str());
                        ddl_work.set_job_state(pb::IS_WRITE_LOCAL);
                        update_table_ddl_mem(ddl_work);
                        TableManager::get_instance()->update_index_status(ddl_work);
                        erase_txn_info(table_id);
                    } else {
                        DB_WARNING("ddl work %s wait txn fail.", ddl_work.ShortDebugString().c_str());
                        DB_WARNING("ddl work %s rollback.", ddl_work.ShortDebugString().c_str());
                        ddl_work.set_errcode(pb::EXEC_FAIL);
                        ddl_work.set_status(pb::DdlWorkFail);
                        update_table_ddl_mem(ddl_work);
                        erase_txn_info(table_id);
                        _update_policy.clear(table_id);
                    }
                } else {
                    DB_NOTICE("ddl work wait all txn done.");
                }
            }           
        }
        break;
    case pb::IS_WRITE_LOCAL:
        //遍历任务提交执行，如果全部任务执行完成，设置状态为PUBLIC
        {
            bool done = true;
            bool rollback = false;
            size_t max_task_number = FLAGS_submit_task_number_per_round;
            size_t current_task_number = 0;
            int32_t wait_num = 0;
            MemRegionDdlWorkMapPtr region_map_ptr;
            {
                BAIDU_SCOPED_LOCK(_region_mutex);
                region_map_ptr = _region_ddlwork[table_id];
            }
            if (region_map_ptr == nullptr) {
                DB_WARNING("ddl work table_id %ld is done.", table_id);
                return 0;
            }

            int32_t doing_work_number = get_doing_work_number(table_id);
            if (doing_work_number == -1) {
                return 0;
            } else if (doing_work_number > region_size * FLAGS_max_region_num_ratio) {
                DB_NOTICE("table_%ld not enough region.", table_id);
                return 0;
            }

            region_map_ptr->traverse_with_early_return([&done, &rollback, this, table_id, region_size, 
                &current_task_number, max_task_number, &ddl_work, &wait_num](MemRegionDdlWork& region_work) -> bool {
                auto task_id = std::to_string(region_work.region_info.table_id()) + 
                        "_" + std::to_string(region_work.region_info.region_id());
                if (region_work.region_info.status() == pb::DdlWorkIdle) {
                    done = false;
                    DB_NOTICE("execute task_%s %s", task_id.c_str(), region_work.region_info.ShortDebugString().c_str());
                    if (DBManager::get_instance()->execute_task(region_work) == 0) {
                        //提交任务成功，设置状态为DOING.
                        region_work.region_info.set_status(pb::DdlWorkDoing);
                        if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                            DB_NOTICE("table_%ld not enough region.", table_id);
                            return false;
                        }
                        current_task_number++;
                        if (current_task_number > max_task_number) {
                            DB_NOTICE("table_%ld launch task next round.", table_id);
                            return false;
                        }
                    } else {
                        DB_NOTICE("table_%ld not enough baikaldb to execute.", table_id);
                        return false;
                    }
                }
                if (region_work.region_info.status() != pb::DdlWorkDone) {
                    DB_NOTICE("wait task_%s %s", task_id.c_str(), region_work.region_info.ShortDebugString().c_str());
                    wait_num++;
                    done = false;
                }
                if (region_work.region_info.status() == pb::DdlWorkFail) {
                    auto retry_time = region_work.region_info.retry_time();
                    if (retry_time < FLAGS_max_ddl_retry_time) {
                        if (DBManager::get_instance()->execute_task(region_work) == 0) {
                            region_work.region_info.set_status(pb::DdlWorkDoing);
                            if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                                DB_NOTICE("not enough region.");
                                return false;
                            }
                            DB_NOTICE("retry task_%s %s", task_id.c_str(), 
                                region_work.region_info.ShortDebugString().c_str());
                        }
                    } else {
                        rollback = true;
                        DB_NOTICE("rollback task_%s %s", task_id.c_str(), 
                            region_work.region_info.ShortDebugString().c_str());
                    }
                    done = false;
                } else if (region_work.region_info.status() == pb::DdlWorkDupUniq ||
                           region_work.region_info.status() == pb::DdlWorkError) {
                    DB_FATAL("region task_%s %s dup uniq or create index region error.", task_id.c_str(), 
                        region_work.region_info.ShortDebugString().c_str());
                    done = false;
                    rollback = true;
                }

                if (rollback) {
                    DB_FATAL("ddl work %s rollback.", ddl_work.ShortDebugString().c_str());
                    ddl_work.set_errcode(pb::EXEC_FAIL);
                    ddl_work.set_status(pb::DdlWorkFail);
                    update_table_ddl_mem(ddl_work);
                    _update_policy.clear(table_id);
                    return false;
                }
                return true;
            });
            if (done) {
                DB_NOTICE("done");
                ddl_work.set_job_state(pb::IS_PUBLIC);
                ddl_work.set_errcode(pb::SUCCESS);
                ddl_work.set_status(pb::DdlWorkDone);
                update_table_ddl_mem(ddl_work);
                TableManager::get_instance()->update_index_status(ddl_work);   
            } else {
                DB_NOTICE("wait %d ddl work to finish.", wait_num);
            }
        }
        break;
    case pb::IS_PUBLIC:
        if (ddl_work.errcode() != pb::SUCCESS) {
            ddl_work.set_job_state(pb::IS_PUBLIC);
            ddl_work.set_errcode(pb::SUCCESS);
            ddl_work.set_status(pb::DdlWorkDone);
            update_table_ddl_mem(ddl_work);
            TableManager::get_instance()->update_index_status(ddl_work);
        }
        DB_NOTICE("work done.");
        break;
    default:
        break;
    }
    return 0;
}

int DDLManager::add_column_ddlwork(pb::DdlWorkInfo& ddl_work) {
    bool done = true;
    bool rollback = false;
    const size_t max_task_number = FLAGS_submit_task_number_per_round * 10;
    size_t current_task_number = 0;
    int32_t wait_num = 0;
    int64_t table_id = ddl_work.table_id();
    size_t region_size = TableManager::get_instance()->get_region_size(table_id);
    if (ddl_work.errcode() == pb::EXEC_FAIL) {
        DB_FATAL("ddl work %s fail", ddl_work.ShortDebugString().c_str());
        return 0;
    }
    MemRegionDdlWorkMapPtr region_map_ptr;
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        region_map_ptr = _region_ddlwork[table_id];
    }
    if (region_map_ptr == nullptr) {
        DB_WARNING("ddl work table_id %ld is done.", table_id);
        return 0;
    }

    int32_t doing_work_number = get_doing_work_number(table_id);
    if (doing_work_number == -1) {
        return 0;
    } else if (doing_work_number > region_size * FLAGS_max_region_num_ratio) {
        DB_NOTICE("table_%ld not enough region.", table_id);
        return 0;
    }
    ddl_work.set_job_state(pb::IS_WRITE_LOCAL);
    ddl_work.set_status(pb::DdlWorkDoing);
    update_table_ddl_mem(ddl_work);
    region_map_ptr->traverse_with_early_return([&done, &rollback, this, table_id, region_size, 
        &current_task_number, max_task_number, &ddl_work, &wait_num](MemRegionDdlWork& region_work) -> bool {
        auto task_id = std::to_string(region_work.region_info.table_id()) + 
                "_" + std::to_string(region_work.region_info.region_id());
        if (region_work.region_info.status() == pb::DdlWorkIdle) {
            done = false;
            DB_NOTICE("execute task_%s %s", task_id.c_str(), region_work.region_info.ShortDebugString().c_str());
            if (DBManager::get_instance()->execute_task(region_work) == 0) {
                //提交任务成功，设置状态为DOING.
                region_work.region_info.set_status(pb::DdlWorkDoing);
                if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                    DB_NOTICE("table_%ld not enough region.", table_id);
                    return false;
                }
                current_task_number++;
                if (current_task_number > max_task_number) {
                    DB_NOTICE("table_%ld launch task next round.", table_id);
                    return false;
                }
            } else {
                DB_NOTICE("table_%ld not enough baikaldb to execute.", table_id);
                return false;
            }
        }
        if (region_work.region_info.status() != pb::DdlWorkDone) {
            DB_NOTICE("wait task_%s %s", task_id.c_str(), region_work.region_info.ShortDebugString().c_str());
            wait_num++;
            done = false;
        }
        if (region_work.region_info.status() == pb::DdlWorkFail) {
            auto retry_time = region_work.region_info.retry_time();
            if (retry_time < FLAGS_max_ddl_retry_time) {
                if (DBManager::get_instance()->execute_task(region_work) == 0) {
                    region_work.region_info.set_status(pb::DdlWorkDoing);
                    if (increase_doing_work_number(table_id) > region_size * FLAGS_max_region_num_ratio) {
                        DB_NOTICE("not enough region.");
                        return false;
                    }
                    DB_NOTICE("retry task_%s %s", task_id.c_str(), 
                        region_work.region_info.ShortDebugString().c_str());
                }
            } else {
                rollback = true;
                DB_NOTICE("rollback task_%s %s", task_id.c_str(), 
                    region_work.region_info.ShortDebugString().c_str());
            }
            done = false;
            } else if (region_work.region_info.status() == pb::DdlWorkDupUniq ||
                        region_work.region_info.status() == pb::DdlWorkError) {
                DB_FATAL("region task_%s %s dup uniq or create index region error.", task_id.c_str(), 
                    region_work.region_info.ShortDebugString().c_str());
                done = false;
                rollback = true;
            }

            if (rollback) {
                DB_FATAL("ddl work %s rollback.", ddl_work.ShortDebugString().c_str());
                ddl_work.set_errcode(pb::EXEC_FAIL);
                ddl_work.set_job_state(pb::IS_PUBLIC);
                ddl_work.set_status(pb::DdlWorkFail);
                ddl_work.set_end_timestamp(butil::gettimeofday_s());
                update_table_ddl_mem(ddl_work);
                return false;
            }
            return true;
        });
    if (done) {
        ddl_work.set_job_state(pb::IS_PUBLIC);
        ddl_work.set_errcode(pb::SUCCESS);
        ddl_work.set_status(pb::DdlWorkDone);
        ddl_work.set_end_timestamp(butil::gettimeofday_s());
        update_table_ddl_mem(ddl_work);
    } else {
        DB_NOTICE("wait %d ddl work to finish.", wait_num);
    }
    return 0;
}

int DDLManager::update_region_ddlwork(const pb::RegionDdlWork& work) {
    auto table_id = work.table_id();
    if (work.status() != pb::DdlWorkDoing) {
        decrease_doing_work_number(table_id);
    }
    pb::MetaManagerRequest request;
    request.mutable_index_ddl_request()->mutable_region_ddl_work()->CopyFrom(work);
    request.set_op_type(pb::OP_UPDATE_INDEX_REGION_DDL_WORK);
    apply_raft(request);

    return 0;
}

int DDLManager::delete_index_ddlwork_region_info(int64_t table_id) {
    DB_NOTICE("delete ddl region info.");
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        _region_ddlwork.erase(table_id);
    }
    rocksdb::WriteOptions write_options;
    std::string begin_key = construct_ddl_work_key(MetaServer::INDEX_DDLWORK_REGION_IDENTIFY, 
                        {table_id});
    std::string end_key = begin_key;
    end_key.append(8, 0xFF);
    RocksWrapper* db = RocksWrapper::get_instance();
    auto res = db->remove_range(write_options, db->get_meta_info_handle(), begin_key, end_key, true);
    if (!res.ok()) {
        DB_FATAL("DDL_LOG remove_index error: code=%d, msg=%s", 
            res.code(), res.ToString().c_str());
    }

    return 0; 
}

int DDLManager::delete_index_ddlwork_info(int64_t table_id,
            const pb::DdlWorkInfo& work_info) {
    DB_NOTICE("delete ddl table info.");
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        _table_ddl_mem.erase(table_id);
        _table_ddl_done_mem.erase(table_id);
        if (work_info.op_type() == pb::OP_MODIFY_FIELD) {
            MemDdlInfo mem_info;
            mem_info.work_info.CopyFrom(work_info);
            _table_ddl_done_mem[table_id] = mem_info;
        }
    }
    _update_policy.clear(table_id);
    {
        BAIDU_SCOPED_LOCK(_txn_mutex);
        _wait_txns.erase(table_id);
    }
    std::string ddlwork_key = construct_ddl_work_key(MetaServer::DDLWORK_IDENTIFY, {table_id});
    // 保存最新一条column ddl任务信息
    if (work_info.op_type() == pb::OP_MODIFY_FIELD) {
        std::string ddl_string;
        if (!work_info.SerializeToString(&ddl_string)) {
            DB_FATAL("serialzeTostring error.");
            return -1;
        }
        if (MetaRocksdb::get_instance()->put_meta_info(ddlwork_key, ddl_string) != 0) {
            DB_FATAL("put meta info error.");
            return -1;
        }
    } else {
        std::vector<std::string> keys {ddlwork_key};
        if (MetaRocksdb::get_instance()->delete_meta_info(keys) != 0) {
            DB_FATAL("delete meta info error.");
            return -1;
        } 
    }
    return 0;
}

int DDLManager::update_ddl_status(bool is_suspend, int64_t table_id) {
    pb::DdlWorkInfo mem_info;
    if (get_ddl_mem(table_id, mem_info)) {
        mem_info.set_suspend(is_suspend);
        update_table_ddl_mem(mem_info);
        std::string index_ddl_string;
        if (!mem_info.SerializeToString(&index_ddl_string)) {
            DB_FATAL("serialzeTostring error.");
            return -1;
        }
        if (MetaRocksdb::get_instance()->put_meta_info(
            construct_ddl_work_key(MetaServer::DDLWORK_IDENTIFY, {table_id}), index_ddl_string) != 0) {
            DB_FATAL("put meta info error.");
            return -1;
        }   
    }
    return 0;
}

int DDLManager::raft_update_info(const pb::MetaManagerRequest& request,
    const int64_t apply_index,
    braft::Closure* done) {
    auto& ddl_request = request.index_ddl_request();
    auto table_id = ddl_request.table_id();
    switch (request.op_type()) {
        case pb::OP_UPDATE_INDEX_REGION_DDL_WORK: {
            update_index_ddlwork_region_info(request.index_ddl_request().region_ddl_work());
            break;
        }
        case pb::OP_SUSPEND_DDL_WORK: {
            DB_NOTICE("suspend ddl work %ld", table_id);
            update_ddl_status(true, table_id);
            break;
        }
        case pb::OP_RESTART_DDL_WORK: {
            DB_NOTICE("restart ddl work %ld", table_id);
            update_ddl_status(false, table_id);
            break;
        }
        default:
            break;
    }
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
    return 0;
}

void DDLManager::delete_ddlwork(const pb::MetaManagerRequest& request, braft::Closure* done) {
    DB_NOTICE("delete ddlwork %s", request.ShortDebugString().c_str());
    pb::DdlWorkInfo del_work;
    bool find = false;
    int64_t table_id = request.ddlwork_info().table_id();
    {
        BAIDU_SCOPED_LOCK(_table_mutex);
        auto iter = _table_ddl_mem.find(table_id);
        if (iter != _table_ddl_mem.end()) {
            find = true;
            iter->second.work_info.CopyFrom(request.ddlwork_info());
            del_work = iter->second.work_info;
        }
    }
    if (find && request.ddlwork_info().drop_index()) {
        TableManager::get_instance()->drop_index_request(del_work);
    }
    delete_index_ddlwork_region_info(table_id);
    delete_index_ddlwork_info(table_id, del_work);
    Bthread _rm_th;
    _rm_th.run([table_id](){
        DBManager::get_instance()->clear_task(table_id);
    });
    IF_DONE_SET_RESPONSE(done, pb::SUCCESS, "success");
}

int DDLManager::apply_raft(const pb::MetaManagerRequest& request) {
    SchemaManager::get_instance()->process_schema_info(NULL, &request, NULL, NULL);
    return 0;
}

int DDLManager::update_index_ddlwork_region_info(const pb::RegionDdlWork& work) {
    auto table_id = work.table_id();
    MemRegionDdlWorkMapPtr region_map_ptr;
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        region_map_ptr = _region_ddlwork[table_id];
    }
    auto task_id = std::to_string(table_id) + "_" + std::to_string(work.region_id());
    if (region_map_ptr != nullptr) {
        DB_NOTICE("update region task_%s %s", task_id.c_str(), work.ShortDebugString().c_str());
        MemRegionDdlWork region_work;
        region_work.region_info = work;
        region_map_ptr->set(work.region_id(), region_work);
    }
    std::string region_ddl_string;
    if (!work.SerializeToString(&region_ddl_string)) {
        DB_FATAL("serialzeTostring error.");
        return -1;
    }
    if(MetaRocksdb::get_instance()->put_meta_info(
            construct_ddl_work_key(MetaServer::INDEX_DDLWORK_REGION_IDENTIFY, 
                {work.table_id(), work.region_id()}), region_ddl_string) != 0) {
        DB_FATAL("put region info error.");
        return -1;
    }
    return 0;
}

void DDLManager::get_index_ddlwork_info(const pb::QueryRequest* request, pb::QueryResponse* response) {
    auto table_id = request->table_id();
    MemRegionDdlWorkMapPtr region_map_ptr;
    {
        BAIDU_SCOPED_LOCK(_region_mutex);
        region_map_ptr = _region_ddlwork[table_id];
    }
    if (region_map_ptr != nullptr) {
        region_map_ptr->traverse([&response](MemRegionDdlWork& region_work){
            auto iter = response->add_region_ddl_infos();
            iter->CopyFrom(region_work.region_info);
        });
    }
}
}
