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
 * @file baikal_client_manager.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/04 15:48:37
 * @brief
 *
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_MANAGER_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_MANAGER_H

#include <pthread.h>
#include <vector>
#include <string>
#include <cstddef>

#ifdef BAIDU_INTERNAL
#include "Configure.h"
#endif

#include "baikal_client_service.h"
#include "baikal_client_epoll.h"
#include "shard_operator_mgr.h"
#include "baikal_client_util.h"

namespace baikal {
namespace client {
// @brief 健康检查线程
void* healthy_check_thread(void*);

// @brief bns同步线程
void* bns_syn_thread(void*);

// @brief 时间循环探测线程
void* event_loop_thread(void*);

void* hang_check_thread(void*);

// @brief 控制类，单例模式
class Manager {
public:
    // @breif 构造函数
    Manager();

#ifdef BAIDU_INTERNAL
    // @brief 初始化
    // @file_path : 配置文件路径
    // @file_name : 配置文件名字
    // @return : 返回0表示初始化正确
    int init(const std::string& file_path, const std::string& file_name);

    int init(const std::string& file_path,
            const std::string& file_name,
            const std::string& manager_name);
#endif
    struct Option {
        int healthy_check_secs = 0;
        bool use_epoll = false;
        bool hang_check = true;
        bool faulty_exit = true;
        bool async = false;
        int no_permission_wait_s = 0;

        std::vector<Service::Option> services;
    };

    int init(const Option& option);

    // @brief 若client只配置了一个service，可直接调用该接口执行sql语句
    int query(uint32_t partition_key, const std::string& sql, ResultSet* result);

    std::string get_manager_name();

    // @brief 根据客户端输入的db_name,得到相应的service
    // @return : - 非NULL：service_name对应的service
    //           - NULL：service_name不存在或输入有错误
    Service* get_service(const std::string& service_name);

    // @brief 从bns服务同步得到每个实例的信息
    void bns_syn_function();

    // @brief 健康检查
    void healthy_check_function(bool need_detect_dead);

    void hang_check_function();

    // @brief join 健康检查线程和bns同步线程，在析构函数前调用
    ~Manager();

    int get_healthy_check_seconds() const;

    int get_bns_syn_seconds() const;

    int get_hang_check_seconds() const;

    bool get_healthy_check_run() const;

    bool get_bns_syn_run() const;

    bool get_hang_check_run() const;

private:

    // @brief 从配置文件加载每个service的配置
#ifdef BAIDU_INTERNAL
    int _load_service_conf(comcfg::ConfigUnit& conf_unit);
#else
    int _load_service_conf(const Service::Option& option);
#endif

    void _clear();

    int _start_healthy_check_thread();

    int _start_bns_syn_thread();

    int _start_event_loop_thread();

    int _start_hang_check_thread();

    void _join_healthy_check_thread();

    void _join_bns_syn_thread();

    void _join_event_loop_thread();

    void _join_hang_check_thread();

private:
    bool _hang_check;
    std::string _manager_name;

    // service_name 与service的对应关系
    std::map<std::string, Service*> _db_service_map;

    // bns同步线程得到的数据, key值为bns_name
    std::map<std::string, BnsInfo*> _bns_infos;

    bool _is_init_ok;

    // 健康检查的时间间隔，单位为s
    int _healthy_check_seconds;
    bool _single_healthy_check_thread;
    bool _healthy_check_run;
    bthread_t _healthy_check_tid;

    // bns同步的时间间隔，单位为s
    int _bns_syn_seconds;
    bool _single_bns_syn_thread;
    bool _bns_syn_run;
    bthread_t _bns_syn_tid;

    //hang_check时间间隔
    bool _single_hang_check_thread;
    bool _hang_check_run;
    bthread_t _hang_check_tid;

    bool _single_event_loop_thread;
    bool _event_loop_run;
    bthread_t _event_loop_tid;
    bool _use_epoll;
    int _no_permission_wait_s;
    bool _faulty_exit;
    bool _async;

    EpollServer* _epoll;

    Manager(const Manager&);

    Manager& operator=(const Manager&);
};
}
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_MANAGER_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
