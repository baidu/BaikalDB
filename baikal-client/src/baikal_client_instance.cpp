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
 * @file baikal_client_instance.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/13 17:21:10
 * @brief
 *
 **/

#include "baikal_client_instance.h"
#ifdef BAIDU_INTERNAL
#include "com_log.h"
#endif
#include "baikal_client_bns_connection_pool.h"
using std::string;
using std::vector;

namespace baikal {
namespace client {
const int32_t Weight::RECV_QUEUE_SIZE;
const int64_t Weight::DEFAULT_QPS;
const int64_t Weight::DEFAULT_AVG_LATENCY;
const int64_t Weight::MIN_WEIGHT;
int64_t Weight::_s_weight_scale = 
    std::numeric_limits<int64_t>::max() / 72000000 / (RECV_QUEUE_SIZE - 1);
int64_t Weight::_s_default_weight = 
    _s_weight_scale * DEFAULT_QPS / DEFAULT_AVG_LATENCY * 100000 / DEFAULT_AVG_LATENCY;
Weight::Weight(int64_t inital_weight, 
                Instance* instance, 
                int32_t max_connection_per_instance): 
                _weight(inital_weight),
                _base_weight(inital_weight),
                _begin_time_sum(0),
                _begin_time_count(0),
                _avg_latency(DEFAULT_AVG_LATENCY),
                _dev(0),
                _qps(DEFAULT_QPS),
                _instance(instance),
                _time_q(_time_q_items, 
                        RECV_QUEUE_SIZE * sizeof(TimeInfo), 
                        butil::NOT_OWN_STORAGE),
                _max_connection_per_instance(max_connection_per_instance){}
int64_t Weight::update(const int64_t& begin_time_us) {
    const int64_t end_time_us = butil::gettimeofday_us();
    boost::mutex::scoped_lock lock(_lock);
    // _weight == 0 代表实例状态为DELAY, FATULT, OFFLINE
    if (_weight == 0) {
        return 0;
    }
    const int64_t latency = end_time_us - begin_time_us;
    if (latency < 0) {
        // error happens or time skews, ignore the sample.
        return 0;
    }
    TimeInfo tm_info = {latency, end_time_us, latency * (double)latency};
    if (!_time_q.empty()) {
        tm_info.latency_sum += _time_q.bottom()->latency_sum;
        tm_info.squared_latency_sum += _time_q.bottom()->squared_latency_sum;
    }
    _time_q.elim_push(tm_info);
    const int64_t top = _time_q.top()->end_time_us;
    const size_t n = _time_q.size();
    int64_t scaled_qps = DEFAULT_QPS * _s_weight_scale;
    if (end_time_us > top) {
        _qps = (n - 1) * 1000000L / (end_time_us - top);
        scaled_qps = _qps * _s_weight_scale;
        if (scaled_qps < _s_weight_scale) {
            scaled_qps = _s_weight_scale;
        }
        _avg_latency = (tm_info.latency_sum - _time_q.top()->latency_sum) / (n - 1);
        double avg_squared_latency = (tm_info.squared_latency_sum -
                                _time_q.top()->squared_latency_sum) / (n - 1);
        _dev = (int64_t)sqrt(avg_squared_latency - _avg_latency * (double)_avg_latency);
        if (_instance->get_first_update_quartile() && _time_q.full()) {
            _instance->set_first_update_quartile(false);
            _instance->set_quartile_value(Instance::MULTIPLE_OF_AVG_LATENCY * _avg_latency);
            CLIENT_WARNING("quartile value is updated by avg_latency,"
                        "instance:%s:%d, quartile_value:%d",
                        _instance->get_ip().c_str(), 
                        _instance->get_port(), 
                        _instance->get_quartile_value());
        }
    } else {
        _avg_latency = latency;
        _dev = _avg_latency;
    }
    _base_weight = scaled_qps / _avg_latency * 100000 / _avg_latency;
    return _cal_inflight_weight(end_time_us);
}

//int64_t Weight::punish_weight_for_max_conneciton(int32_t times) {
//    boost::mutex::scoped_lock lock(_lock);
//    if (_weight == 0) {
//        return 0;
//    }
//    int64_t old_weight = _weight;
//    _weight = _weight / times;
//    if (_weight < MIN_WEIGHT) {
//        _weight = MIN_WEIGHT;
//    }
//    _base_weight = _weight;
//    CLIENT_WARNING("weight was punished for max conneiton,"
//                "instance:%s:%d, new_weight:%ld, old_weight:%ld",
//                _instance->get_ip().c_str(), _instance->get_port(), 
//                _weight, old_weight);
//    return _weight - old_weight; 
//}
Weight::AddInflightResult Weight::recal_inflight(int64_t now_time_us, int64_t dice) {
    boost::mutex::scoped_lock lock(_lock);
    if (_weight == 0) {
        AddInflightResult r = { false, 0 };
        return r;
    }
    const int64_t diff = _cal_inflight_weight(now_time_us);
    if (_weight < dice) {
        AddInflightResult r = { false, diff };
        return r;
    }
    AddInflightResult r = { true, diff };
    return r;
}

void Weight::add_inflight(int64_t begin_time_us) {
    boost::mutex::scoped_lock lock(_lock); 
    _begin_time_sum += begin_time_us; 
    ++_begin_time_count;
    return; 
}

void Weight::sub_inflight(int64_t begin_time_us) {
    boost::mutex::scoped_lock lock(_lock);
    _begin_time_sum -= begin_time_us;
    --_begin_time_count;
    return;
}
void Weight::print_weight() {
        CLIENT_WARNING("instance:%s:%d, _weight:%ld, _base_weight:%ld," 
                    "_begin_time_sum:%ld, _begin_time_count:%ld," 
                    "_avg_latency:%ld, _dev:%ld, _qps:%ld", 
                    _instance->get_ip().c_str(), _instance->get_port(),
                    _weight, _base_weight,
                    _begin_time_sum, _begin_time_count, 
                    _avg_latency, _dev, _qps);

}
int64_t Weight::_cal_inflight_weight(int64_t now_us) {
    int64_t new_weight = _base_weight;
    if (_begin_time_count > 0) {
        const int64_t inflight_delay = now_us - _begin_time_sum / _begin_time_count;
        int64_t punish_latency = _avg_latency * 2 + 3 * _dev;
        //int64_t punish_latency = _avg_latency * 3;
        if (inflight_delay > punish_latency) {
            new_weight = new_weight * _avg_latency / inflight_delay;
            CLIENT_WARNING("instance: %s:%d,  new weight is punished:%ld,"
                    "_begin_time_count:%d, now_us:%ld, _begin_time_sum:%ld, _begin_time_count:%d,"
                    "inflight_delay:%ld, punish_latency:%ld, _avg_latency:%ld",
                    _instance->get_ip().c_str(), _instance->get_port(), new_weight, 
                    _begin_time_count, now_us, _begin_time_sum, _begin_time_count,
                    inflight_delay, punish_latency, _avg_latency);
        }
        if (_instance->get_total_connection_num() >= _max_connection_per_instance) {
            int32_t punish_scale =                       
                divide_ceil(_max_connection_per_instance, 
                            _instance->get_bns_pool()->get_connection_num_per_instance(),
                            NULL);
            new_weight = new_weight / punish_scale;
            CLIENT_WARNING("instance:%s:%d was punished for max conneciton, "
                        "punish_scale:%d, new weight:%ld", 
                        _instance->get_ip().c_str(), _instance->get_port(),
                        punish_scale, new_weight); 
        }
    }
    if (new_weight < MIN_WEIGHT) {
        new_weight = MIN_WEIGHT;
    }
    const int64_t old_weight = _weight;
    _weight = new_weight;
    return new_weight - old_weight;
}
const int Instance::CAL_SELECT1_SIZE;
const int Instance::DEFAULT_SELECT1_TIME;

const int Instance::MULTIPLE_OF_AVG_LATENCY;
const int Instance::TOTAL_COUNT;

const int Instance::DELAY_DETECT_TIMES;

const int Instance::WRITE_TIMEOUT;
const int Instance::READ_TIMEOUT;
const int Instance::DEFAULT_TIMES_FOR_AVG_AND_QUARTILE;
const int Instance::TIMEOUT_CONN_MAX;
Instance::Instance(
            string ip,
            int port,
            int max_connection_per_instance,
            BnsConnectionPool* pool,
            InstanceStatus status,
            ConnectionConf& conn_conf,
            int32_t index,
            int64_t init_weight):
            _ip(ip),
            _port(port),
            _cal_select1_avg(_cal_select1_avg_items,
                            CAL_SELECT1_SIZE * sizeof(int64_t),
                            butil::NOT_OWN_STORAGE),
            _select1_avg(DEFAULT_SELECT1_TIME),
            _left(0),
            _weight(init_weight, this, max_connection_per_instance),
            _index(index),
            _quartile_value(conn_conf.read_timeout * 1000),
            _total_count(0),
            _times_for_avg_and_quartile(DEFAULT_TIMES_FOR_AVG_AND_QUARTILE),
            _status(status),
            _conn_conf(conn_conf),
            _max_connection_per_instance(max_connection_per_instance),
            _total_connection_num(0),
            _bns_pool(pool),
            _first_update_quartile(true) {
    _min_heap.init_heap(TOTAL_COUNT / 10000, 0); // 99.99分位
    _conn_conf_healthy_check = _conn_conf;
    _conn_conf_healthy_check.read_timeout = READ_TIMEOUT;
    _conn_conf_healthy_check.write_timeout = WRITE_TIMEOUT;
}

Instance::~Instance() {
    _bns_pool = NULL;
}

SmartConnection Instance::fetch_connection() {
    //if (_status.load() != ON_LINE) {
    if (_status.load() == FAULTY) {
        CLIENT_WARNING("instance id: %s:%d, status:%d", _ip.c_str(), _port, _status.load());
        return SmartConnection();
    }
    boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
    vector<SmartConnection>::iterator iter = _connections.begin();
    for (; iter != _connections.end(); ++iter) {
        ConnectionStatus expected = IS_NOT_USED;
        if ((*iter)->compare_exchange_strong(expected, IS_USING)) {
            return *iter;
        }
    }
    //CLIENT_WARNING("there is no not_used connection, instance_id: %s:%d, total_conn_num:%d",
    //            _ip.c_str(), _port, _total_connection_num.load());
    return SmartConnection();
}

int Instance::add_connection(int conn_num) {
    vector<SmartConnection> vec;
    for (int i = 0; i < conn_num; ++i) {
        SmartConnection conn(new MysqlConnection(this, _bns_pool));
        int ret = conn->connect(_conn_conf, true);
        if (ret == INSTANCE_FAULTY_ERROR) {
            CLIENT_WARNING("connection connect fail, instance faulty, id: %s:%d",
                        _ip.c_str(), _port);
            // 实例如果为在线置为故障状态
            status_online_to_faulty();
            return INSTANCE_FAULTY_ERROR;
        }
        if (ret < 0) {
            CLIENT_WARNING("connection connect fail, instance illegal, id: %s:%d",
                        _ip.c_str(), _port);
            return ret;
        }
        vec.push_back(conn);
    }
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    vector<SmartConnection>::iterator iter = vec.begin();
    for (; iter != vec.end(); ++iter) {
        _connections.push_back(*iter);
    }
    _total_connection_num.store(_connections.size());
    return SUCCESS;
}
SmartConnection Instance::new_special_connection() {
    TimeCost cost;
    //if (_status.load() != ON_LINE) {
    //    CLIENT_WARNING("instance id: %s:%d, status:%d cost:%ld us",
    //            _ip.c_str(), _port, _status.load(), cost.get_time());
    //    return SmartConnection();
    //}
    if (_total_connection_num.load() >= _max_connection_per_instance) {
        CLIENT_WARNING("instance conn nums exceeds max num, instance id: %s:%d cost:%ld us",
                     _ip.c_str(), _port, cost.get_time());
        return SmartConnection();
    }
    SmartConnection conn;
    SmartConnection mysql_conn(new MysqlConnection(this, _bns_pool));
    conn = mysql_conn;
    int ret = conn->connect(_conn_conf, true);
    if (ret == INSTANCE_FAULTY_ERROR) {
        CLIENT_WARNING("connection connect fail, instance faulty, id: %s:%d cost:%ld us",
                _ip.c_str(), _port, cost.get_time());
        // 实例如果为在线置为故障状态
        status_online_to_faulty();
        return SmartConnection();
    }
    if (ret < 0) {
        CLIENT_WARNING("connection connect fail, instance illegal, id: %s:%d cost:%ld us",
                    _ip.c_str(), _port, cost.get_time());
        return SmartConnection();
    }
    ConnectionStatus expected = IS_NOT_USED;
    if (!conn->compare_exchange_strong(expected, IS_USING)) {
        CLIENT_WARNING("new special connection fail, id: %s:%d cost:%ld us",
                _ip.c_str(), _port, cost.get_time());
        return SmartConnection();
    }
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    _connections.push_back(conn);
    _total_connection_num.store(_connections.size());
    CLIENT_WARNING("new special connection success, id: %s:%d cost:%ld us,"
                 "total_connection_num:%d",
            _ip.c_str(), _port, cost.get_time(), _total_connection_num.load());
    return conn;
}

void Instance::hang_check() {
    int32_t count = 0;
    int64_t quartile_value =  _quartile_value.load();
    int64_t avg_latency = _weight.get_avg_latency();
    //统计实例状态是否为delay
    {
        boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
        int64_t max_delay_time = (quartile_value > 
                                    (avg_latency * _times_for_avg_and_quartile.load())) ?
                                    quartile_value : 
                                    (avg_latency * _times_for_avg_and_quartile.load());
        //int64_t max_delay_time = quartile_value;
        for (size_t i = 0; i < _connections.size(); ++i) {
            //CLIENT_WARNING("connection:%d, _begin_execute:%d, begin_time_us:%lu, inflight_cost:%lu, _quartile_value:%lu", 
            //    i, _connections[i]->get_begin_execute(), _connections[i]->get_begin_time_us(), 
            //    butil::gettimeofday_us() - _connections[i]->get_begin_time_us(),
            //    _quartile_value.load());
            if (_connections[i]->get_begin_execute() 
                    && ((butil::gettimeofday_us() - _connections[i]->get_begin_time_us())
                        //> _quartile_value.load())) {
                        > max_delay_time)) {
                ++count;
            }
        }
    }
    
    //CLIENT_WARNING("instance:%s.%d, hang_check hang count of connection:%d, begin_time_count:%d", 
    //        _ip.c_str(), _port, count, _weight.get_begin_time_count());
    int64_t max_time_count = (_weight.get_qps() / 10 > 3) ? (_weight.get_qps() / 10) : 3;
    max_time_count = (max_time_count < _total_connection_num.load()) ? 
                    max_time_count : 
                    _total_connection_num.load();
    if (_status.load() == ON_LINE && max_time_count < 3) {
        max_time_count = 3;
    } 

    //CLIENT_WARNING("instance:%s.%d, hang_check hang count of connection:%d, begin_time_count:%d, max_time_count:%d", 
    //        _ip.c_str(), _port, count, _weight.get_begin_time_count(), max_time_count);
    
    //if (count > TIMEOUT_CONN_MAX) {
    if (count >= max_time_count && count != 0) {
        CLIENT_FATAL("instance:%s:%d is delay!!!, _quartile_value:%ld,"
                    "avg_latency:%ld, times_for_avg_and_quartile:%d,"
                    "count:%d >= max_time_count:%ld, qps:%ld, total_conneciton_num:%d",
                _ip.c_str(), _port, quartile_value,
                avg_latency, _times_for_avg_and_quartile.load(),
                count, max_time_count, _weight.get_qps(), _total_connection_num.load());
        status_to_delay();
    }
    if (_status.load() != DELAY) {
        return;
    }
    //超时连接从connections中删除
    int counter = 0;
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    vector<SmartConnection>::iterator iter = _connections.begin();
    while (iter != _connections.end()) {
        CLIENT_WARNING("connection:%d, _begin_execute:%d, "
                        "begin_time_us:%lu, inflight_cost:%lu,"
                        "_quartile_value:%lu",
                        counter, (*iter)->get_begin_execute(), 
                        (*iter)->get_begin_time_us(),
                        butil::gettimeofday_us() - (*iter)->get_begin_time_us(),
                        _quartile_value.load());
        (*iter)->kill();
        iter = _connections.erase(iter);
        ++counter;
        //if ((*iter)->get_begin_execute() 
        //        && (butil::gettimeofday_us() - (*iter)->get_begin_time_us())
        //            > _quartile_value.load()) {
        //    (*iter)->kill();
        //    iter = _connections.erase(iter);                                              
        //    ++counter;                                                                    
        //} else {
        //    ++iter;                                                                       
        //}                                                                                 
    }
    _total_connection_num.store(_connections.size());
    return; 
}

//若pool中存在该实例，则根据pool中实例的状态，做不同的操作
void Instance::bns_available_healthy_check(bool need_detect_dead) {
    //若为在线，则做探死，若死，则删除连接，将连接分配到其他实例上;
    //若活着则对空闲连接做ping；
    if (_status.load() == ON_LINE && need_detect_dead) {
        int64_t latency = 0;
        int ret = detect_fun(latency);
        if (ret == 0) {
            //cal select1_avg
            //debug
            if (!_cal_select1_avg.empty()) {
                latency += *(_cal_select1_avg.bottom());
            } 
            _cal_select1_avg.elim_push(latency);
            if (_cal_select1_avg.size() > 1) {
                _select1_avg.store((latency - *(_cal_select1_avg.top())) / 
                                    (_cal_select1_avg.size() -1));
            } else {
                _select1_avg.store(latency);
            }
            //探测是否有bad的链接，如果有则关闭连接
            _detect_bad();
            _ping();
        } else {
            CLIENT_WARNING("detect live fail, instance faulty, lantency:%ld instance:%s:%d",
                        latency, _ip.c_str(), _port);
            status_online_to_faulty();
        }
        CLIENT_WARNING("select1_avg:%ld instance:%s:%d", _select1_avg.load(), _ip.c_str(), _port);
    }
    //若为离线，则标记为故障status_to_faulty
    if (_status.load() == OFF_LINE) {
        CLIENT_WARNING("instance status offline to faulty(offline to online), instance:%s:%d",
                     _ip.c_str(), _port);
        status_offline_to_faulty();
    }
    //若为故障则删除连接，做探活
    if (_status.load() == FAULTY) {
        int num = delete_all_connection_not_used();
        _realloc(num);
        int64_t latency = 0;
        int ret = detect_fun(latency);
        if (ret == 0) {
            CLIENT_WARNING("faulty instance been detected alive, instance:%s:%d", _ip.c_str(), _port);
            int conn_num = divide_ceil(_bns_pool->get_total_conn_num(),
                     _bns_pool->get_online_instance_num() + 1, NULL);
            if (conn_num > _max_connection_per_instance) {
                conn_num = _max_connection_per_instance;
            }
            ret = add_connection(conn_num);
            if (ret == 0) {
                CLIENT_WARNING("faulty instance status change to ON_LINE, instance:%s:%d",
                             _ip.c_str(), _port);
                status_faulty_to_online(); 
                _bns_pool->print_conn_info();
            }
        } else {
            CLIENT_WARNING("faulty instance is still faulty, instance:%s:%d", _ip.c_str(), _port);
        }
    } //if FAULTY
    if (_status.load() == DELAY) {
        int num = delete_all_connection_not_used();
        _realloc(num);
        int ret = detect_delay_fun();
        if (ret == 0) {
            CLIENT_WARNING("delay instance been detected alive, instance:%s:%d", _ip.c_str(), _port);
            int conn_num = divide_ceil(_bns_pool->get_total_conn_num(),
                     _bns_pool->get_online_instance_num() + 1, NULL);
            if (conn_num > _max_connection_per_instance) {
                conn_num = _max_connection_per_instance;
            }
            ret = add_connection(conn_num);
            if (ret == 0) {
                CLIENT_WARNING("delay instance status change to ON_LINE, instance:%s:%d",
                             _ip.c_str(), _port);
                status_to_online();
                _bns_pool->print_conn_info(); 
            }
        } else {
            CLIENT_WARNING("delay instance is still delay, instance:%s:%d", _ip.c_str(), _port);
        }
    } 
}
//int64_t Instance::punish_weight_for_max_connection() {
//    int32_t times = divide_ceil(_max_connection_per_instance, 
//                                _bns_pool->get_connection_num_per_instance(), 
//                                NULL);  
//    return _weight.punish_weight_for_max_conneciton(times);
//}
void Instance::bns_not_available_healthy_check() {
    if (_status.load() == ON_LINE) {
        CLIENT_WARNING("instance status online to offline, instance:%s:%d", _ip.c_str(), _port);
        status_online_to_offline();
    }
    //若为故障，status_to_offline
    if (_status.load() == FAULTY) {
        CLIENT_WARNING("instance status faulty to offline, instance:%s:%d", _ip.c_str(), _port);
        status_faulty_to_offline();
    }
    if (_status.load() == DELAY) {
        CLIENT_WARNING("instance status DELAY to offline, instance:%s:%d", _ip.c_str(), _port);
        status_delay_to_offline();
    }
    int num = delete_all_connection_not_used();
    _realloc(num);
}

void Instance::delete_connection_not_used(int conn_num) {
    int counter = 0;
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    vector<SmartConnection>::iterator iter = _connections.begin();
    while (counter < conn_num && iter != _connections.end()) {
        if ((*iter)->compare_exchange_strong(IS_NOT_USED, IS_NOT_CONNECTED)) {
            iter = _connections.erase(iter);
            ++counter;
        } else {
            ++iter;
        }
    }
    _total_connection_num.store(_connections.size());
}

int32_t Instance::delete_all_connection_not_used() {
    int counter = 0;
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    vector<SmartConnection>::iterator iter = _connections.begin();
    while (iter != _connections.end()) {
        if ((*iter)->compare_exchange_strong(IS_NOT_USED, IS_NOT_CONNECTED)) {
            iter = _connections.erase(iter);
            ++counter;
        } else {
            ++iter;
        }
    }
    _total_connection_num.store(_connections.size());
    return counter;
}

int Instance::detect_fun(int64_t& select1_time) {
    SmartConnection conn(new MysqlConnection(this, _bns_pool));
    int ret = conn->connect(_conn_conf_healthy_check, true);
    //if (ret == INSTANCE_FAULTY_ERROR) {
    if (ret != SUCCESS) {
        CLIENT_WARNING("connection connect fail, instance faulty, id: %s:%d", 
                _ip.c_str(), _port);
        // 实例如果为在线置为故障状态
        return INSTANCE_FAULTY_ERROR;            
    }
    int64_t start_time = butil::gettimeofday_us();
    string sql = "select 1";
    ret = conn->execute(sql, NULL);
    select1_time = butil::gettimeofday_us() - start_time;
    if (ret != SUCCESS) {
        CLIENT_WARNING("connect successfully, but [select 1] fail, instance:%s:%d", 
                _ip.c_str(), _port);
        return INSTANCE_FAULTY_ERROR;   
    }
    return SUCCESS;
}

int Instance::detect_delay_fun() {
    int64_t select1_min = std::numeric_limits<int64_t>::max();
    int64_t select1_max = 0;
    int64_t total_delay = 0;
    for (int i = 0; i < DELAY_DETECT_TIMES; ++i) {
        int64_t latency_time;
        int ret = detect_fun(latency_time);
        if (ret == INSTANCE_FAULTY_ERROR) {
            CLIENT_WARNING("instance:%s:%d is still delay, select1 fail", _ip.c_str(), _port);
            return INSTANCE_FAULTY_ERROR;
        }
        total_delay += latency_time;
        if (latency_time < select1_min) {
            select1_min = latency_time;
        }
        if (latency_time > select1_max) {
            select1_max = latency_time;
        }
    }
    if (total_delay / DELAY_DETECT_TIMES > 3 * _select1_avg.load()) {
        CLIENT_WARNING("instance:%s:%d is still delay, avg of select1 is greater 3 times "
                    "total_delay:%lu, DELAY_DETECT_TIMES:%d, _select1_avg:%lu",
                    _ip.c_str(), _port,
                    total_delay, DELAY_DETECT_TIMES, _select1_avg.load());
        return INSTANCE_FAULTY_ERROR;
    }
    if (select1_max > 5000 && (select1_max / select1_min >= 10)) {
        CLIENT_WARNING("instance:%s:%d is still delay, select1_max / select1_min > 10,"
                     "select1_max:%ld, select1_min:%ld",
                    _ip.c_str(), _port, select1_max, select1_min);
        return INSTANCE_FAULTY_ERROR;
    }
    return SUCCESS;
}

bool Instance::status_online_to_faulty() {
    InstanceStatus expected = ON_LINE;
    bool result = _status.compare_exchange_strong(expected, FAULTY);
    if (result) {
        int64_t diff = clear_weight();
        _bns_pool->update_parent_weight(diff, _index);
        _bns_pool->total_fetch_add(diff);
        CLIENT_WARNING("stauts online to faulty, instance:%s.%d", _ip.c_str(), _port);
        print_weight();
    }
    return result;
}

bool Instance::status_offline_to_faulty() {
    InstanceStatus expected = OFF_LINE;
    bool result =  _status.compare_exchange_strong(expected, FAULTY); 
    if (result) {
        int64_t diff = clear_weight();
        _bns_pool->update_parent_weight(diff, _index);
        _bns_pool->total_fetch_add(diff);
        CLIENT_WARNING("stauts offline to faulty, instance:%s.%d", _ip.c_str(), _port);
        print_weight();
    }
    return result;
}

bool Instance::status_faulty_to_online() {
    InstanceStatus expected = FAULTY;
    bool result =  _status.compare_exchange_strong(expected, ON_LINE);
    if (result) {
        int64_t diff = reset_weight();
        _bns_pool->update_parent_weight(diff, _index);
        _bns_pool->total_fetch_add(diff);
        CLIENT_WARNING("stauts faulty to online, instance:%s.%d", _ip.c_str(), _port);
        print_weight();
    }
    return result;
}

bool Instance::status_online_to_offline() {
    InstanceStatus expected = ON_LINE;
    bool result = _status.compare_exchange_strong(expected, OFF_LINE); 
    if (result) {
        int64_t diff = clear_weight();
        _bns_pool->update_parent_weight(diff, _index);
        _bns_pool->total_fetch_add(diff);
        CLIENT_WARNING("stauts online to offline, instance:%s.%d", _ip.c_str(), _port);
        print_weight();
    }
    return result;
}

bool Instance::status_faulty_to_offline() {
    InstanceStatus expected = FAULTY;
    bool result = _status.compare_exchange_strong(expected, OFF_LINE); 
    if (result) {
        int64_t diff = clear_weight();
        _bns_pool->update_parent_weight(diff, _index);
        _bns_pool->total_fetch_add(diff);
        CLIENT_WARNING("stauts faulty to offline, instance:%s.%d", _ip.c_str(), _port);
        print_weight();
    }
    return result;
}

bool Instance::status_delay_to_offline() {
    InstanceStatus expected = DELAY;
    bool result = _status.compare_exchange_strong(expected, OFF_LINE);
    if (result) {
        int64_t diff = clear_weight();
        _bns_pool->update_parent_weight(diff, _index);
        _bns_pool->total_fetch_add(diff);
        CLIENT_WARNING("stauts faulty to offline, instance:%s.%d", _ip.c_str(), _port);
        print_weight();
    }
    return result;
}
void Instance::status_to_online() {
    _status.store(ON_LINE);
    int64_t diff = reset_weight();
    _bns_pool->update_parent_weight(diff, _index); 
    _bns_pool->total_fetch_add(diff);
    CLIENT_WARNING("stauts to online, instance:%s.%d", _ip.c_str(), _port);
    print_weight();
}

void Instance::status_to_delay() {
    int64_t diff = clear_weight();
    _bns_pool->update_parent_weight(diff, _index);
    _bns_pool->total_fetch_add(diff);
    _status.store(DELAY);
    CLIENT_WARNING("stauts to DELAY, instance:%s.%d", _ip.c_str(), _port);
    print_weight();
}

int Instance::get_not_used_connection_num() {
    boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
    int num = 0;
    vector<SmartConnection>::iterator iter = _connections.begin();
    for (; iter!= _connections.end(); ++iter) {
        if ((*iter)->get_status() == IS_NOT_USED) {
            ++num;
        }
    }
    return num;
}
bool Instance::has_not_used_connection() {
    boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
    vector<SmartConnection>::iterator iter = _connections.begin();
    for (; iter!= _connections.end(); ++iter) {
        if ((*iter)->get_status() == IS_NOT_USED) {
            return true;
        }
    }
    return false;
}
void Instance::update_quartile_value(const int64_t& response_time) {
    boost::mutex::scoped_lock lock(_lock);
    ++_total_count;
    if (response_time > _min_heap.get_min_value()) {
        _min_heap.heap_down(response_time);
    }
    if (_total_count > TOTAL_COUNT) {
        _first_update_quartile = false;
        _quartile_value.store(_min_heap.get_min_value());
        _times_for_avg_and_quartile.store(_quartile_value.load() / _weight.get_avg_latency()); 
        CLIENT_WARNING("quartile_value is:%ld,_times_for_avg_and_quartile:%d, instance:%s:%d", 
                    _quartile_value.load(), _times_for_avg_and_quartile.load(),
                    _ip.c_str(), _port);
        _min_heap.clear();
        _total_count = 0;
    }
}

int64_t Instance::update(const int64_t& start_time) {
    if (_status.load() != ON_LINE) {
        return 0;
    } 
    return _weight.update(start_time);
}

int64_t Instance::left_fetch_add(int64_t diff) {
    return _left.fetch_add(diff); 
}

int64_t Instance::reset_weight() {
    return _weight.reset_weight();
}

int64_t Instance::clear_weight() {
    return _weight.clear_weight();
}

int Instance::_delete_all_connection() {
    int counter = 0;
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    counter = _connections.size();
    _connections.clear();
    _total_connection_num.store(0);
    //清空weight相关值，否则容易出问题,同时更新父节点值
    return counter;
}
void Instance::_detect_bad() {
    if (_status.load() != ON_LINE) {
        return;
    }
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    vector<SmartConnection>::iterator iter = _connections.begin();
    while (iter != _connections.end()) {
        if ((*iter)->get_status() == IS_BAD) {
            iter = _connections.erase(iter);
        } else {
            ++iter;
        }
    }
    _total_connection_num.store(_connections.size());
}
void Instance::_ping() {
    if (_status.load() != ON_LINE) {
        return;
    }
    int counter = 0;
    int ret = 0;
    // ping过程中迭代器可能失效，
    // 加锁也可以解决，但是ping的时间较长，加锁可能导致工作线程一直等待
    // 所以先加锁把vector复制出来
    vector<SmartConnection> connections_copy;
    {
        boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
        connections_copy = _connections;
    }
    vector<SmartConnection>::iterator iter_copy = connections_copy.begin();
    while (iter_copy != connections_copy.end()) {
        if ((*iter_copy)->compare_exchange_strong(IS_NOT_USED, IS_CHECKING)) {
            (*iter_copy)->set_read_timeout(READ_TIMEOUT);
            ret = (*iter_copy)->ping();
            (*iter_copy)->set_read_timeout(_conn_conf.read_timeout);
            if (ret == 0) {
                (*iter_copy)->compare_exchange_strong(IS_CHECKING, IS_NOT_USED);
            } else {
                int ret = (*iter_copy)->reconnect(_conn_conf, true);
                if (ret != SUCCESS) {
                    CLIENT_WARNING("instance status online to faulty when ping, instance:%s:%d",
                                    _ip.c_str(), _port);
                    status_online_to_faulty();
                    break;
                } else {
                    CLIENT_WARNING("connection reconnect successfully when ping, instance:%s:%d",
                                _ip.c_str(), _port);
                }
            }
            ++counter;
        }
        ++iter_copy;
    }
}

void Instance::_realloc(int conn_num) {
    if (_bns_pool == NULL) {
        return;
    }
    if (conn_num <= 0) {
        return ;
    }
    int ret = _bns_pool->alloc_connection(conn_num);
    if (ret == INSTANCE_NOT_ENOUGH) {
        CLIENT_FATAL("there are not enough instance to add connection, instance:%s:%d, bns name:%s",
                 _ip.c_str(), _port, _bns_pool->get_name().c_str());
    }
}
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
