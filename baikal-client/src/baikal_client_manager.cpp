// Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
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

/**
 * @file baikal_client_manager.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/04 17:42:51
 * @brief 
 *  
 **/
#include "baikal_client_manager.h"
#include "baikal_client_define.h"
#include "baikal_client_bns_connection_pool.h"
#ifdef BAIDU_INTERNAL
#include "com_log.h"
#endif

using std::string;
using std::map;
using std::vector;

namespace baikal {
namespace client {
void* healthy_check_thread(void* pd) {
    CLIENT_WARNING("healthy check thread start");
    Manager* mg = static_cast<Manager*>(pd);
    if (mg == NULL) {
        CLIENT_FATAL("Manager is NULL");
        return NULL;
    }
    int seconds = mg->get_healthy_check_seconds();
    uint64_t count = 0;
    int time = 0;
    bool need_detect_dead = false;
    while (mg->get_healthy_check_run()) {
        //进行健康检查操作
        if (time >= seconds * 1000 / 500) {
            need_detect_dead = true;
            CLIENT_WARNING("start healthy_check_thread, manager_name:%s, count:%llu", 
                    mg->get_manager_name().c_str(), count + 1);
            time = 0;
            ++count;
        } else {
            need_detect_dead = false;
            ++time;
        }
        bthread_usleep(500000); //500毫秒
        mg->healthy_check_function(need_detect_dead);
    } 
    return NULL;
}

void* hang_check_thread(void* pd) {
    Manager* mg = static_cast<Manager*>(pd);
    if (mg == NULL) {
        CLIENT_FATAL("Manager is NULL");
        return NULL;
    }
    int count = 0;
    while (mg->get_hang_check_run()) {
        bthread_usleep(1000000);
        TimeCost time_cost;
        mg->hang_check_function();
        CLIENT_WARNING("start hang_check_thread, manager_name:%s, count:%llu, time_cost:%lu",
                     mg->get_manager_name().c_str(), count, time_cost.get_time());
        ++count;
    }
    return NULL;
}

void* bns_syn_thread(void* pd) {
    CLIENT_WARNING("bns syn thread start");
    Manager* mg = static_cast<Manager*>(pd);
    if (mg == NULL) {
        CLIENT_FATAL("Manager is NULL");
        return NULL;
    }
    int seconds = mg->get_bns_syn_seconds();
    uint64_t count = 0;
    while (mg->get_bns_syn_run()) {
        int time = 0;
        while (time < seconds) {
            if (!mg->get_bns_syn_run()) {
                break;
            }
            bthread_usleep(1000000);
            ++time;
        }
        CLIENT_WARNING("start bns_syn_thread, manager_name:%s, count:%llu",
                mg->get_manager_name().c_str(), count);
        //进行健康检查操作，先等待一段时间后再检查
        mg->bns_syn_function();
        ++count;
    }
    return NULL;
}
void* event_loop_thread(void* arg) {
    EpollServer* _epoll = static_cast<EpollServer*>(arg);
    _epoll->start_server();
    return NULL;
}

Manager::Manager() :
        _hang_check(true),
        _manager_name("default"),
        _is_init_ok(false),
        _healthy_check_seconds(0),
        _single_healthy_check_thread(false),
        _healthy_check_run(false),
        _healthy_check_tid(0),
        _bns_syn_seconds(0),
        _single_bns_syn_thread(false),
        _bns_syn_run(false),
        _bns_syn_tid(0),
        _single_hang_check_thread(false),
        _hang_check_run(false),
        _hang_check_tid(0),
        _single_event_loop_thread(false),
        _event_loop_run(false),
        _event_loop_tid(0),
        _use_epoll(false),
        _no_permission_wait_s(0),
        _faulty_exit(true),
        _async(false) {}

#ifdef BAIDU_INTERNAL
int Manager::init(const std::string& file_path,
                  const std::string& file_name,
                  const std::string& manager_name) {
    _manager_name = manager_name;
    return init(file_path, file_name);
}
int Manager::init(const string& file_path, const string& file_name) {
    if (file_path.empty() || file_name.empty()) {
        CLIENT_WARNING("Configure file path or file name is wrong");
        return INPUTPARAM_ERROR;
    }
    comcfg::Configure conf;
    int ret = conf.load(file_path.c_str(), file_name.c_str(), NULL);
    if (ret < 0) {
        CLIENT_WARNING("Load configre file failed!");
        return CONFPARAM_ERROR;
    }
    try {
        _healthy_check_seconds = conf["healthy_check_time"].to_int32();
        _bns_syn_seconds = conf["bns_syn_time"].to_int32();
        if (conf["use_epoll"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _use_epoll = (conf["use_epoll"].to_uint32() != 0);
        }
        if (conf["hang_check"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _hang_check = (conf["hang_check"].to_uint32() != 0);
        }
        if (conf["faulty_exit"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _faulty_exit = (conf["faulty_exit"].to_uint32() != 0);
        }
        if (conf["async"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _async = (conf["async"].to_uint32() != 0);
        }
        if (conf["no_permission_wait_s"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _no_permission_wait_s = conf["no_permission_wait_s"].to_int32();
        }
    } catch (comcfg::ConfigException& e) {
        CLIENT_WARNING("configure file format is wrong, exception:%s", e.what());
        return CONFPARAM_ERROR;
    }
    _epoll = NULL;
    if (_use_epoll) {
        CLIENT_WARNING("use epoll event loop");
        _epoll = EpollServer::get_instance();
        if (_epoll->init() != true) {
            CLIENT_FATAL("create epoll server failed");
            return -1;
        }
        int ret = _start_event_loop_thread();
        if (ret < 0) {
            CLIENT_WARNING("start event loop thread failed");
            return ret;
        }
    } else {
        CLIENT_WARNING("use bthread default event loop");
    }
    if (mysql_library_init(0, NULL, NULL)) {
        CLIENT_WARNING("init mysql library fail");
        return CONFPARAM_ERROR;
    }
    // 初始化分库分表计算管理器
    ShardOperatorMgr::get_s_instance();
    if (conf["service"].size() == 0) {
        CLIENT_WARNING("this is a empty configure file ,no service");
        return CONFPARAM_ERROR;
    }
    // 加载每个service配置
    for (size_t i = 0; i < conf["service"].size(); ++i) {
        ret = _load_service_conf(conf["service"][i]);
        if (ret < 0) {
            CLIENT_WARNING("load %dth service conf error", i);
            return ret;
        }
    }
    if (_healthy_check_seconds > 0) {
        ret = _start_healthy_check_thread();
        if (ret < 0) {
            CLIENT_WARNING("start healthy check thread failed");
            return ret;
        }
    }

    if (_bns_syn_seconds > 0) {
        ret = _start_bns_syn_thread();
        if (ret < 0) {
            CLIENT_WARNING("start bns syn thread failed");
            return ret;
        }
    }
    if (_hang_check) {
        ret = _start_hang_check_thread();
        if (ret < 0) {
            CLIENT_WARNING("start hang check thread failed"); 
            return ret;
        }
    } else {
        CLIENT_WARNING("hang check is false");
    }
    //随机种子，全局初始化一次
    srand(time(0));
    _is_init_ok = true;
    CLIENT_NOTICE("manager init successfully, manager_name:%s",
                _manager_name.c_str());
    return SUCCESS; 
}
#else
int Manager::init(const Option& option) {
    _healthy_check_seconds = option.healthy_check_secs;
    _use_epoll = option.use_epoll;
    _hang_check = option.hang_check;
    _faulty_exit = option.faulty_exit;
    _async = option.async;
    _no_permission_wait_s = option.no_permission_wait_s;

    _epoll = NULL;
    if (_use_epoll) {
        CLIENT_WARNING("use epoll event loop");
        _epoll = EpollServer::get_instance();
        if (!_epoll->init()) {
            CLIENT_FATAL("create epoll server failed");
            return -1;
        }
        int ret = _start_event_loop_thread();
        if (ret < 0) {
            CLIENT_WARNING("start event loop thread failed");
            return ret;
        }
    } else {
        CLIENT_WARNING("use bthread default event loop");
    }
    if (mysql_library_init(0, NULL, NULL)) {
        CLIENT_WARNING("init mysql library fail");
        return CONFPARAM_ERROR;
    }
    // 初始化分库分表计算管理器
    ShardOperatorMgr::get_s_instance();
    if (option.services.empty()) {
        CLIENT_WARNING("this is a empty configure file ,no service");
        return CONFPARAM_ERROR;
    }

    // 加载每个service配置
    int idx = 0;
    for (auto& service : option.services) {
        int ret = _load_service_conf(service);
        if (ret < 0) {
            CLIENT_WARNING("load %dth service conf error", idx++);
            return ret;
        }
    }

    int ret = 0;
    if (_healthy_check_seconds > 0) {
        ret = _start_healthy_check_thread();
        if (ret < 0) {
            CLIENT_WARNING("start healthy check thread failed");
            return ret;
        }
    }

    if (_bns_syn_seconds > 0) {
        ret = _start_bns_syn_thread();
        if (ret < 0) {
            CLIENT_WARNING("start bns syn thread failed");
            return ret;
        }
    }
    if (_hang_check) {
        ret = _start_hang_check_thread();
        if (ret < 0) {
            CLIENT_WARNING("start hang check thread failed");
            return ret;
        }
    } else {
        CLIENT_WARNING("hang check is false");
    }
    //随机种子，全局初始化一次
    srand(time(0));
    _is_init_ok = true;
    CLIENT_NOTICE("manager init successfully, manager_name:%s", _manager_name.c_str());
    return SUCCESS;
}
#endif

int Manager::query(uint32_t partition_key, const std::string& sql, ResultSet* result) {
    if (_db_service_map.size() != 1) {
        CLIENT_WARNING("service num is not one, this method cann't be called, service num:%d", 
                _db_service_map.size());
        return GET_SERVICE_FAIL;
    }
    string name = _db_service_map.begin()->first; 
    Service* service = get_service(name);
    if (service == NULL) {
        CLIENT_WARNING("get service in query fail");
        return GET_SERVICE_FAIL;
    }
    return service->query(partition_key, sql, result);
}
std::string Manager::get_manager_name() {
    return _manager_name;
}
Service* Manager::get_service(const string& service_name) {
    if (!_is_init_ok) {
        CLIENT_WARNING("Manager is not inited");
        return NULL;
    }
    map<string, Service*>::iterator iter = _db_service_map.find(service_name);
    if (iter == _db_service_map.end()) {
        CLIENT_WARNING("input service is not exist, service_name:%s", service_name.c_str());
        return NULL;
    }
    return iter->second;
}

void Manager::bns_syn_function() {
#ifdef BAIDU_INTERNAL
    map<string, BnsInfo*>::iterator iter = _bns_infos.begin();
    for (; iter != _bns_infos.end(); ++iter) {
        vector<InstanceInfo> result;
        int ret = get_instance_from_bns(iter->first, result);
        // 如果从bns同步信息失败，则继续保存上次同步得到的信息，此次不更新
        if (ret < 0) {
            CLIENT_WARNING("get instance from bns fail, bns_name:%s", iter->first.c_str());
            CLIENT_WARNING("this bns will keep last info, bns_name:%s", iter->first.c_str());
        } else {
            boost::mutex::scoped_lock lock(iter->second->mutex_lock);
            iter->second->instance_infos.clear();
            for (vector<InstanceInfo>::size_type i = 0; i < result.size(); ++i) {
                iter->second->instance_infos[result[i].id] = result[i];
            }
        }
    }
#endif
}

void  Manager::healthy_check_function(bool need_detect_dead) {
    //对每个servie做循环
    map<string, Service*>::iterator db_service_it = _db_service_map.begin();
    for (; db_service_it != _db_service_map.end(); ++db_service_it) {
        //每个service的bns 存储在map<unsigned, BnsConnectionPool*>
        Service* service = db_service_it->second;
        if (!service->is_inited()) {
            continue;
        }
        map<int, BnsConnectionPool*> id_bns_map = service->get_id_bns_map();
        map<int, BnsConnectionPool*>::iterator pool_it = id_bns_map.begin();
        //对sevice里的每个bns做循环
        for (; pool_it != id_bns_map.end(); ++pool_it) {
            int ret = pool_it->second->healthy_check(need_detect_dead);
            if (ret < 0) {
                CLIENT_WARNING("bns pool healthy check fail, service_name: %s, bns_name: %s",
                             service->get_name().c_str(), pool_it->second->get_name().c_str());
            } 
        }
    }
}

void  Manager::hang_check_function() {
    //对每个servie做循环
    map<string, Service*>::iterator db_service_it = _db_service_map.begin();
    for (; db_service_it != _db_service_map.end(); ++db_service_it) {
        //每个service的bns 存储在map<unsigned, BnsConnectionPool*>
        Service* service = db_service_it->second;
        if (!service->get_hang_check()) {
            continue;
        }
        map<int, BnsConnectionPool*> id_bns_map = service->get_id_bns_map();
        map<int, BnsConnectionPool*>::iterator pool_it = id_bns_map.begin();
        //对sevice里的每个bns做循环
        for (; pool_it != id_bns_map.end(); ++pool_it) {
            int ret = pool_it->second->hang_check();
            if (ret < 0) {
                CLIENT_WARNING("bns pool hang check fail, service_name: %s, bns_name: %s",
                             service->get_name().c_str(), pool_it->second->get_name().c_str());
            } 
        }
    }
}

Manager::~Manager() {
    this->_clear();
}

int Manager::get_healthy_check_seconds() const {
    return _healthy_check_seconds;
}

int Manager::get_bns_syn_seconds() const {
    return _bns_syn_seconds;
}

bool Manager::get_healthy_check_run() const {
    return _healthy_check_run;
}

bool Manager::get_bns_syn_run() const {
    return _bns_syn_run;
}
bool Manager::get_hang_check_run() const {
    return _hang_check_run;
}

#ifdef BAIDU_INTERNAL
int Manager::_load_service_conf(comcfg::ConfigUnit& conf_unit) {
    if (conf_unit.selfType() == comcfg::CONFIG_ERROR_TYPE) {
        CLIENT_WARNING("configure file format is error");
        return CONFPARAM_ERROR;
    }
    string service_name(conf_unit["service_name"].to_cstr());
    // service_name 不能重复
    if (_db_service_map.count(service_name) != 0) {
        CLIENT_WARNING("service configuration is repeat, service_name:%s",
                    service_name.c_str());
        return CONFPARAM_ERROR;
    }
    Service* service = new(std::nothrow) Service(_bns_infos, 
            _no_permission_wait_s, _faulty_exit, _async);
    if (NULL == service) {
        CLIENT_WARNING("New ProxyService error");
        return NEWOBJECT_ERROR;
    }
    _db_service_map[service_name] = service;
    int ret = service->init(conf_unit);
    if (ret < 0) {
        CLIENT_WARNING("service init failed, service name: %s", service_name.c_str());
        return ret;
    }
    return SUCCESS;
}
#else
int Manager::_load_service_conf(const Service::Option& option) {
    const string& service_name = option.service_name;
    // service_name 不能重复
    if (_db_service_map.count(service_name) != 0) {
        CLIENT_WARNING("service configuration is repeat, service_name:%s",
                service_name.c_str());
        return CONFPARAM_ERROR;
    }
    Service* service = new(std::nothrow) Service(_bns_infos,
            _no_permission_wait_s, _faulty_exit, _async);
    if (NULL == service) {
        CLIENT_WARNING("New ProxyService error");
        return NEWOBJECT_ERROR;
    }
    _db_service_map[service_name] = service;
    int ret = service->init(option);
    if (ret < 0) {
        CLIENT_WARNING("service init failed, service name: %s", service_name.c_str());
        return ret;
    }
    return SUCCESS;
}
#endif

void Manager::_clear() {
    CLIENT_WARNING("start clearn manager resource");
    _healthy_check_run = false;
    _bns_syn_run = false;
    _hang_check_run = false;
    _join_healthy_check_thread();
    _join_bns_syn_thread();
    _join_hang_check_thread();
    _join_event_loop_thread();
    _single_healthy_check_thread = false;
    _single_bns_syn_thread = false;
    _single_hang_check_thread = false;
    _single_event_loop_thread = false;
    map<string, Service*>::iterator iter = _db_service_map.begin();
    for (; iter != _db_service_map.end(); ++iter) {
        delete iter->second;
        iter->second = NULL;
    }
    _db_service_map.clear();
    map<string, BnsInfo*>::iterator bns_iter = _bns_infos.begin();
    for (; bns_iter != _bns_infos.end(); ++bns_iter) {
        delete bns_iter->second;
        bns_iter->second = NULL;
    }
    _bns_infos.clear();
    mysql_library_end();
    CLIENT_WARNING("clearn manager resource end");
}

int Manager::_start_healthy_check_thread() {
    if (!_single_healthy_check_thread) {
        _healthy_check_run = true;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        int ret = bthread_start_background(&_healthy_check_tid, &attr, healthy_check_thread, this);
        if (ret != 0) {
            CLIENT_WARNING("Start new healthy check thread failed!");
            _healthy_check_run = false;
            return THREAD_START_ERROR;
        }
        _single_healthy_check_thread = true;
        return SUCCESS;
    } 
    CLIENT_WARNING("Another healthy check thread is already started");
    return THREAD_START_REPEAT;
}

int Manager::_start_bns_syn_thread() {
    if (!_single_bns_syn_thread) {
        _bns_syn_run = true;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        int ret = bthread_start_background(&_bns_syn_tid, &attr, bns_syn_thread, this);
        if (ret != 0) {
            CLIENT_WARNING("Start new bns syn thread failed!");
            _bns_syn_run = false;
            return THREAD_START_ERROR;
        }
        _single_bns_syn_thread = true;
        return SUCCESS;
    } 
    CLIENT_WARNING("Another bns syn thread is already started");
    return THREAD_START_REPEAT;
}
int Manager::_start_hang_check_thread() {
    if (!_single_hang_check_thread) {
        _hang_check_run = true;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        int ret = bthread_start_background(&_hang_check_tid, &attr, hang_check_thread, this);
        if (ret != 0) {
            CLIENT_WARNING("Start new hang check thread failed!");
            _hang_check_run = false;
            return THREAD_START_ERROR;
        }
        _single_hang_check_thread = true;
        return SUCCESS;
    }
    CLIENT_WARNING("Another hang check thread is already started");
    return THREAD_START_REPEAT;  
}
int Manager::_start_event_loop_thread() {
    if (!_single_event_loop_thread) {
        _event_loop_run = true;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        int ret = bthread_start_background(&_event_loop_tid, &attr, event_loop_thread, _epoll);
        if (ret != 0) {
            CLIENT_WARNING("Start new event loop thread failed!");
            _event_loop_run = false;
            return THREAD_START_ERROR;
        }
        _single_event_loop_thread = true;
        return SUCCESS;
    }
    CLIENT_WARNING("Another event loop thread is already started");
    return THREAD_START_REPEAT;
}
void Manager::_join_healthy_check_thread() {
    if (_single_healthy_check_thread) {
        bthread_join(_healthy_check_tid, NULL);
        _single_healthy_check_thread = false; 
        CLIENT_WARNING("join healthy check thread successfully");
    }
}

void Manager::_join_bns_syn_thread() {
    if (_single_bns_syn_thread) {
        bthread_join(_bns_syn_tid, NULL);
        _single_bns_syn_thread = false; 
        CLIENT_WARNING("join bns syn thread successfully");
    }
}

void Manager::_join_hang_check_thread() {
    if (_single_hang_check_thread) {
        bthread_join(_hang_check_tid, NULL);
        _single_hang_check_thread = false;
        CLIENT_WARNING("join hang check thread successfully");
    }
}

void Manager::_join_event_loop_thread() {
    if (_single_event_loop_thread) {
        _epoll->set_shutdown();
        bthread_join(_event_loop_tid, NULL);
        _single_event_loop_thread = false;
        CLIENT_WARNING("join event loop thread successfully");
    }
}
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
