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
 * @file baikal_client_instance.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/09 16:03:59
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_INSTANCE_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_INSTANCE_H

#include <string>
#include <vector>
#include <limits>
#include "mysql.h"
#include "boost/thread/shared_mutex.hpp"
#include "boost/atomic/atomic.hpp"
#ifdef BAIDU_INTERNAL
#include <base/containers/bounded_queue.h>
#include <base/time.h>
#else
#include "butil/containers/bounded_queue.h"
#include "butil/time.h"
#endif
#include "baikal_client_util.h"
#include "baikal_client_define.h"
#include "baikal_client_connection.h"
#include "baikal_client_bns_connection_pool.h"
#include "baikal_client_mysql_connection.h"

namespace baikal {
namespace client {
class BnsConnectionPool;
struct TimeInfo {
    int64_t latency_sum;         // us
    int64_t end_time_us;
    double squared_latency_sum;  // for calculating deviation
};
// @权重类
class Weight {
public:
    static const int32_t RECV_QUEUE_SIZE = 128;
    static const int64_t DEFAULT_QPS = 1;
    static int64_t _s_weight_scale;
    static const int64_t MIN_WEIGHT = 1000;
    static const int64_t DEFAULT_AVG_LATENCY = 5000; //us 
    static int64_t _s_default_weight;
    explicit Weight(int64_t initial_weight, 
                    Instance* instance, 
                    int32_t max_connection_per_instance); 
    ~Weight() {}

    // Called in Feedback() to recalculate _weight.
    // Returns diff of _weight.
    int64_t update(const int64_t& begin_time_us);

    // Weight of self. Notice that this value may change at any time.
    int64_t volatile_value() const {
        return _weight; 
    }
    struct AddInflightResult {
        bool chosen;
        int64_t weight_diff;
    };
    AddInflightResult recal_inflight(int64_t now_time_us, int64_t dice);
    void add_inflight(int64_t begin_time_us);
    void sub_inflight(int64_t begin_time_us);
    //int64_t punish_weight_for_max_conneciton(int32_t times);
    int64_t get_qps() const {
        return _qps;
    }
    int64_t get_avg_latency() const {
        return _avg_latency;
    }
    int64_t clear_weight() {
        boost::mutex::scoped_lock lock(_lock); 
        int64_t old_weight = _weight;
        _weight = 0;
        _base_weight = 0;
        return _weight - old_weight;
    }
    int64_t reset_weight() {
        boost::mutex::scoped_lock lock(_lock);
        int64_t old_weight = _weight;
        _weight = _s_default_weight; 
        _base_weight = _s_default_weight;
        return _weight - old_weight;
    }
    void print_weight(); 
    int64_t get_begin_time_count() {
        return _begin_time_count;
    }
private:
    int64_t _cal_inflight_weight(int64_t now_us);
    
    int64_t _weight;
    int64_t _base_weight;
    boost::mutex _lock;
    int64_t _begin_time_sum;
    int64_t _begin_time_count;
    int64_t _avg_latency;
    int64_t _dev;
    int64_t _qps;
    Instance* _instance;
    butil::BoundedQueue<baikal::client::TimeInfo> _time_q;
    // content of _time_q
    TimeInfo _time_q_items[RECV_QUEUE_SIZE];
    int32_t _max_connection_per_instance;
};

// @实例类，代表一个物理数据库
class Instance{
public:
    static const int DEFAULT_TIMES_FOR_AVG_AND_QUARTILE = 100;
    static const int CAL_SELECT1_SIZE = 60 / 10 * 30; // number of select_1 in 30 minutes
    static const int DEFAULT_SELECT1_TIME = 50000; //select1默认的耗时，单位us

    //static const int DEFAULT_QUARTILE_VALUE = 500000; //默认分位值, 单位us
    static const int MULTIPLE_OF_AVG_LATENCY = 100;
    static const int TOTAL_COUNT = 1000000; //统计时间访问内请求的总数
    
    static const int DELAY_DETECT_TIMES = 100; //统计时间访问内请求的总数

    static const int WRITE_TIMEOUT = 1; //s
    static const int READ_TIMEOUT = 1; //s
    static const int TIMEOUT_CONN_MAX = 3;
    Instance(
            std::string ip,
            int port,
            int max_connection_per_instance,
            BnsConnectionPool* pool,
            InstanceStatus status,
            ConnectionConf& conn_conf,
            int32_t index,
            int64_t init_weight);
    ~Instance();

    // @brief 从该实例上选择一条可用连接
    SmartConnection fetch_connection();
    
    // @brief 在该实例上新建conn_num连接
    // 初始化和健康检查线程中调用，不涉及多线程
    int add_connection(int conn_num);
   
    //建立一个专有的连接，建完之后可直接返回使用
    SmartConnection new_special_connection();

    void hang_check();
   
    // @brief 在bns中该实例的状态为可用
    void bns_available_healthy_check(bool need_detect_dead);
    
    // @brief 在bns中该实例的状态为不可用
    void bns_not_available_healthy_check();

    void delete_connection_not_used(int conn_num);
    
    bool has_not_used_connection();

    int32_t delete_all_connection_not_used();

    // @探测功能，供探死探活操作使用，新建一个连接，简单查询，断开
    int detect_fun(int64_t& select1_time);

    // @探测功能，循环建立多个连接，比较延时的最大值和最小值
    int detect_delay_fun(); 
    
    // @breif 状态转换
    bool status_online_to_faulty();
    bool status_offline_to_faulty();
    bool status_online_to_offline();
    bool status_faulty_to_offline();
    bool status_delay_to_offline();
    bool status_faulty_to_online();

    std::string get_ip() const {
        return _ip;
    }

    int get_port() const {
        return _port;
    }

    int32_t get_total_connection_num() const {
        return _total_connection_num.load();
    }
   
    int get_not_used_connection_num(); 

    InstanceStatus get_status() const {
        return _status.load();
    }

    ConnectionConf& get_conn_conf() {
        return _conn_conf;
    };

    int64_t get_select1_avg() const {
        return _select1_avg.load();
    }

    int64_t get_quartile_value() const {
        return _quartile_value.load();
    }
    void set_quartile_value(int64_t quartile_value) {
        _quartile_value.store(quartile_value);
    }
    bool get_first_update_quartile() const {
        return _first_update_quartile;
    }
    void set_first_update_quartile(bool value) {
        _first_update_quartile = value;
    } 
    void update_quartile_value(const int64_t& response_time);
    int64_t update(const int64_t& start_time);
    int64_t left_fetch_add(int64_t diff);
    int64_t reset_weight();
    int64_t clear_weight();
    int64_t get_weight() const {
        return _weight.volatile_value();
    }
    int64_t get_left() const {
        return _left.load();
    }
    Weight::AddInflightResult recal_inflight(int64_t begin_time_us, int64_t dice) {
        return _weight.recal_inflight(begin_time_us, dice);
    }

    //int64_t punish_weight_for_max_connection(); 
    BnsConnectionPool* get_bns_pool() const {
        return _bns_pool;
    }
    void add_inflight(int64_t begin_time_us) {
        _weight.add_inflight(begin_time_us);
    }
    void sub_inflight(int64_t begin_time_us) {
        _weight.sub_inflight(begin_time_us);
    }
    int32_t get_index() const {
        return _index;
    }
    void status_to_online();
    void status_to_delay();
    void print_weight() {
        _weight.print_weight();
    }
    int64_t get_qps() const {
        return _weight.get_qps();
    }
private:

    // @brief 当实例故障或离线时删除所有的连接 
    int _delete_all_connection();
    
    // @brief 对状态为在线实例的所有空闲连接做ping,防止被服务器踢掉
    void _ping();
   
    //探测是否有链接坏掉，目前只有一种close的时候事务回滚失败会坏掉
    void _detect_bad(); 
    // @对离线实例或者故障实例上的连接做重分配
    // conn_num: 连接个数
    void _realloc(int conn_num);
   
private:
    std::string _ip;
    int _port;

    // store values for calcuting avg of select1    
    butil::BoundedQueue<int64_t> _cal_select1_avg;
    int64_t _cal_select1_avg_items[CAL_SELECT1_SIZE];
    boost::atomic<int64_t> _select1_avg;
    
    boost::atomic<int64_t> _left;
    Weight _weight;
    int32_t _index;

    //cal 99.99分位
    HeapSort _min_heap;
    boost::atomic<int64_t> _quartile_value;
    boost::mutex _lock; 
    int64_t _total_count;
    boost::atomic<int64_t> _times_for_avg_and_quartile;

    // 实例状态
    boost::atomic<InstanceStatus> _status;
    
    // 该实例上建立连接的配置
    ConnectionConf _conn_conf;

    ConnectionConf _conn_conf_healthy_check; //healthy_check时使用的读写延迟
    // 该实例上最大的连接数
    int _max_connection_per_instance;
    
    // 增加删除连接只在初始化和健康检查线程中，不涉及多线程问题
    // int _total_connection_num;
    // 新版中在工作线程也增加实例，所以用atomic
    boost::atomic<int32_t> _total_connection_num;

    //操作connections时加读写锁
    std::vector<SmartConnection> _connections;
    boost::shared_mutex _rw_lock;

    BnsConnectionPool* _bns_pool;
    bool _first_update_quartile;
};

}
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_INSTANCE_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
