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
 * @file baikal_client_bns_connection_pool.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/16 16:00:13
 * @brief 
 *  
 **/

#include "baikal_client_bns_connection_pool.h"
#include "global.h"
#include <stdlib.h>
#ifdef BAIDU_INTERNAL
#include "com_log.h"
#include <base/time.h>
#include <base/fast_rand.h>
#else
#include "butil/time.h"
#include "butil/fast_rand.h"
#endif
#include "baikal_client_util.h"
#include "baikal_client_instance.h"

using std::string;
using std::map;
using std::vector;

namespace baikal {
namespace client {
BnsConnectionPool::BnsConnectionPool(
            const string& bns_name,
            const std::string& tag,
            int total_conn_num,
            int max_connection_per_instance,
            int id,
            ConnectionConf conn_conf, 
            map<string, LogicDB*>::iterator begin,
            map<string, LogicDB*>::iterator end,
            BnsInfo* bns_info_ptr,
            string comment_format,
            SelectAlgo select_algo) :
            _total_weight(0),
            _is_used(false),
            _id(id),
            _bns_name(bns_name),
            _tag(tag),
            _total_conn_num(total_conn_num),
            _conf_connection_per_instance(2),
            _max_connection_per_instance(max_connection_per_instance),
            _conn_conf(conn_conf),
            _name_logic_db_map(begin, end),
            _bns_info_ptr(bns_info_ptr),
            _comment_format(comment_format),
            _select_algo(select_algo) {}

BnsConnectionPool::~BnsConnectionPool() {
    _clear();
}

int BnsConnectionPool::init(bool connect_all, vector<InstanceInfo>* list) {
    vector<InstanceInfo> result;
    int ret = 0;
#ifdef BAIDU_INTERNAL
    if (list == NULL) {
        ret = get_instance_from_bns(_bns_name, result);
        if (ret < 0) {
            CLIENT_WARNING("get instance from bns fail when pool init, bns_name:%s", 
                    _bns_name.c_str());
            return ret;
        }
    } else {
#endif
        result = *list;
        _bns_info_ptr = new BnsInfo();
        vector<InstanceInfo>::iterator iter = result.begin();
        for (; iter != result.end(); ++iter) {
            _bns_info_ptr->instance_infos[iter->id] = *iter;
        }
#ifdef BAIDU_INTERNAL
    }
#endif
    vector<InstanceInfo>::iterator it = result.begin();
    for (; it != result.end(); ++it) {
        int id = get_shard_id_by_bns_tag(it->tag);
        if (_tag != "" && id != _id) {
            //不属于本pool的instance
            continue;
        }
        CLIENT_WARNING("instance id:%s tag %s == pool_tag %s",
                it->id.c_str(), it->tag.c_str(), _tag.c_str());
        if (it->is_available) {
            Instance* instance = new(std::nothrow) Instance(
                    it->ip,
                    it->port,
                    _max_connection_per_instance,
                    this,
                    ON_LINE,
                    _conn_conf,
                    _instances_index.size(),
                    Weight::_s_default_weight);
            if (instance == NULL) {
                CLIENT_WARNING("new instance fail, bns_name:%s tag:%s, instance:%s:%d",
                             _bns_name.c_str(), _tag.c_str(), it->ip.c_str(), it->port);
                return NEWOBJECT_ERROR;
            }
            _instances_map[it->id] = instance;
            _instances_index.push_back(it->id);
            update_parent_weight(Weight::_s_default_weight, _instances_index.size() - 1);
            _total_weight.fetch_add(Weight::_s_default_weight);
            int64_t time_cost;
            ret = instance->detect_fun(time_cost);
            if (ret == 0) {
                CLIENT_DEBUG("new available instance successfully, instance id :%s", it->id.c_str());
                CLIENT_DEBUG("instance id :%s, conn_conf.username:%s, conn_conf.password",
                        it->id.c_str(), _conn_conf.username.c_str(), _conn_conf.password.c_str());
            } else {
                instance->status_online_to_faulty();
                CLIENT_WARNING("new instance is faulty, detect fail, instance id:%s", it->id.c_str());
            }
        } else {
            CLIENT_WARNING("instance is not available, instance id:%s, bns_status:%d, enable:%d",
                        it->id.c_str(), it->status, it->enable);
        } 
    }
    int32_t num_online_instance = get_online_instance_num();
    // 可以配置故障不退出程序
    if (_conn_conf.faulty_exit && 
            (num_online_instance * _max_connection_per_instance) < _total_conn_num) {
        CLIENT_WARNING("bns:%s tag:%s num of instance[%d] * "
                     "max conn per instance[%d] < toatal conn num[%d]",
                    _bns_name.c_str(), _tag.c_str(), num_online_instance, 
                    _max_connection_per_instance, _total_conn_num);
        return CONFPARAM_ERROR;
    }
    _conf_connection_per_instance = divide_ceil(_total_conn_num, num_online_instance, NULL);
    //初始化rolling_instance值
    //_rolling_instance = _instances_map.begin()->first;
    _rolling_num.store(0);
    if (connect_all) {
        _is_used = true;
        int num = divide_ceil(_total_conn_num, num_online_instance, NULL);
        map<string, Instance*>::iterator iter1 = _instances_map.begin();
        for (; iter1 != _instances_map.end(); ++iter1) {
            if (iter1->second->get_status() != ON_LINE) {
                continue;
            }
            ret = iter1->second->add_connection(num);
            if (ret < 0) {
                CLIENT_WARNING("create connection fail, bns_name:%s, instance:%s",
                            _bns_name.c_str(), iter1->first.c_str());
            }
        }
    }
    CLIENT_DEBUG("bns init successfully, bns_name:%s", _bns_name.c_str());
    print_conn_info();
    return SUCCESS;
}

SmartConnection BnsConnectionPool::fetch_connection(const std::string& ip_port) {
    _is_used = true;
    TimeCost cost;
    int try_times = 0;
    do {
        //选实例
        Instance* instance = _select_instance(ip_port);
        if (instance == NULL) {
            CLIENT_WARNING("there is no available instance, bns_name:%s", _bns_name.c_str());
            return SmartConnection();
        }
        // 选择已有连接
        SmartConnection connection = instance->fetch_connection();
        if (connection != NULL) {
            //CLIENT_WARNING("fetch_connection, bns_name:%s, instance:%s:%d, cost:%ld us",
            //        _bns_name.c_str(), instance->get_ip().c_str(),
            //        instance->get_port(), cost.get_time());
            return connection;
        }
        // 新建一个专用连接
        //return instance->new_special_connection();
        SmartConnection result = instance->new_special_connection();
        if (result != NULL) {
            CLIENT_WARNING("fetch connection success, new connection, bns_name:%s cost:%ld us",
                     _bns_name.c_str(), cost.get_time());
            return result;
        }
        ++try_times;
    } while (try_times <= 1);
    CLIENT_WARNING("fetch connection fail, new connection, bns_name:%s cost:%ld us",
            _bns_name.c_str(), cost.get_time());
    return SmartConnection();
}

Instance* BnsConnectionPool::select_instance(const std::string& ip_port) {
    if (ip_port.empty()) {
        return NULL;
    }

    // 加读锁
    boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
    //选择这个ip_port上的实例
    if (_instances_map.count(ip_port) == 1 &&
        _instances_map[ip_port]->get_status() == ON_LINE) {
        return _instances_map[ip_port];
    }
    return NULL;
}

Instance* BnsConnectionPool::_select_instance(const std::string& ip_port) {
    // 加读锁
    // boost::mutex::scoped_lock lock(_lock);
    boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
    if (ip_port != "") {
        //选择这个ip_port上的实例
        if (_instances_map.count(ip_port) == 1 &&
                _instances_map[ip_port]->get_status() == ON_LINE) {
            return _instances_map[ip_port];
        } else {
            CLIENT_WARNING("bns:%s instance %s not avaliable, rand select another",
                    _bns_name.c_str(), ip_port.c_str());
        }
    }
    // 按配置选实例
    if (_select_algo == LOCAL_AWARE) {
        return _select_instance_local_aware();
    } else if (_select_algo == RANDOM) {
        return _select_instance_random();
    } else if (_select_algo == ROLLING) {
        return _select_instance_rolling(false);
    } else {
        CLIENT_WARNING("unsupport select algorithm");
        return NULL;
    }
}
Instance* BnsConnectionPool::_select_instance_local_aware() {
    int64_t total = _total_weight.load();
    int64_t dice = butil::fast_rand_less_than(total);
    size_t index = 0;
    int64_t self = 0;
    const size_t n = _instances_index.size();
    int nloop = 0;
    //CLIENT_WARNING("rand weight is :%ld, total weight is : %ld", dice, total);
    while (total > 0) {
        if (++nloop > 50) {
            CLIENT_WARNING("select algo run time too long, maybe has bug");
            break;
        }
        const int64_t left = _instances_map[_instances_index[index]]->get_left();
        self = _instances_map[_instances_index[index]]->get_weight();
        if (dice < left) {
            index = index * 2 + 1;
            if (index < n) {
                continue;
            }
        } else if (dice >= left + self) {
            dice -= left + self;
            index = index * 2 + 2;
            if (index < n) {
                continue;
            }
        } else {
            const Weight::AddInflightResult r =
                    _instances_map[_instances_index[index]]->recal_inflight(
                                                butil::gettimeofday_us(),
                                                dice - left);
            update_parent_weight(r.weight_diff, index);
            _total_weight.fetch_add(r.weight_diff);
            if (r.chosen) {
                if (_instances_map[_instances_index[index]]->get_total_connection_num()
                        < _max_connection_per_instance ||
                        _instances_map[_instances_index[index]]->has_not_used_connection()) {
                    return _instances_map[_instances_index[index]];
                } else {
                    //int64_t diff = _instances_map[_instances_index[index]]->punish_weight_for_max_connection();
                    //update_parent_weight(diff, index);
                    //_total_weight.fetch_add(diff);
                    CLIENT_WARNING("number of connection in instance:%s exceed max connection:%d",
                            _instances_index[index].c_str(), _max_connection_per_instance);
                }
            } else {
                CLIENT_WARNING("instance was not applied, instance:%s",
                        _instances_index[index].c_str());
            }
        }
        total = _total_weight.load();
        dice = butil::fast_rand_less_than(total);
        index = 0;
        //CLIENT_WARNING("rand weight is :%ld, total weight is : %ld", dice, total);
    }
    CLIENT_FATAL("select instance fail when local aware, bns_name:%s, total_weight:%lld",
                _bns_name.c_str(), total);
    return _select_instance_rolling(true);
}
Instance* BnsConnectionPool::_select_instance_random() {
    int rand_num = rand() % (_instances_map.size()) + 1;
    // 选实例
    map<string, Instance*>::iterator iter = _instances_map.begin();
    int i = 0;
    int available_instance = 0;
    do {
        if (iter->second->get_status() == ON_LINE) {
            ++i;
            ++available_instance;
            if (i == rand_num) {
                return iter->second;
            }
        }
        ++iter;
        if (iter == _instances_map.end()) {
            // 该条件满足，遍历所有的实例都没有可用的实例，
            if (available_instance == 0) {
                CLIENT_WARNING("there is no avaiable instance, bns_name:%s", _bns_name.c_str());
                print_conn_info();
                return _select_instance_rolling(true);
            }
            available_instance = 0;
            iter = _instances_map.begin();
        }
    } while (i < rand_num);
    CLIENT_WARNING("select instance fail when random, bns_name:%s", _bns_name.c_str());
    return _select_instance_rolling(true);
}
Instance* BnsConnectionPool::_select_instance_rolling(bool whether_lower) {
    uint32_t roll_time = 0;
    while (roll_time < _instances_map.size()) {
        _rolling_num.fetch_add(1);
        uint64_t instance_num = _instances_index.size();
        std::string rolling_instance = _instances_index[_rolling_num.load() % instance_num];
        map<string, Instance*>::iterator iter = _instances_map.find(rolling_instance);
        if (iter == _instances_map.end()) {
            iter = _instances_map.begin();
        }
        if ((iter->second->get_status() == ON_LINE ||
                    (whether_lower && iter->second->get_status() == DELAY))
            && (iter->second->get_total_connection_num() < _max_connection_per_instance
                || iter->second->has_not_used_connection())){
            return iter->second;
        } else {
            ++roll_time;
        }
    }
    if (whether_lower) {
        CLIENT_WARNING("select instance fail when rolling, bns_name:%s", _bns_name.c_str());
        print_conn_info();
        return NULL;
    } else {
        return _select_instance_rolling(true);
    }
}

int BnsConnectionPool::get_table_id(
        const string& logic_db_name,
        const string& table_name,
        uint32_t partition_key,
        bool* table_split,
        int* table_id) {
    map<string, LogicDB*>::iterator iter = _name_logic_db_map.find(logic_db_name);
    //逻辑库的分表逻辑没有配置，说明该逻辑库下所有的表都不分表
    if (iter == _name_logic_db_map.end()) {
        *table_split = false;
        *table_id = 0;
        return SUCCESS;
    }
    LogicDB* logic_db = iter->second;
    int ret = logic_db->get_table_id(table_name, partition_key, table_split, table_id);
    if (ret < 0) {
        CLIENT_WARNING("get table id fail, table_name:%s.%s", logic_db_name.c_str(),
table_name.c_str());
        return ret;
    }
    return SUCCESS;
}
int BnsConnectionPool::hang_check() {
    if (!_is_used) {
        return SUCCESS;
    }
    std::vector<Instance*> instance_copy;
    {
        //拷贝出来防止加锁时间太长，instance不会删除
        //boost::mutex::scoped_lock lock(_lock);
        boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
        map<string, Instance*>::iterator iter = _instances_map.begin();
        for (; iter != _instances_map.end(); ++iter) {
            instance_copy.push_back(iter->second);
        }
    }
    for (size_t i = 0; i < instance_copy.size(); ++i) {
        if (instance_copy[i] != NULL) {
            instance_copy[i]->hang_check();
        }
    }
    return SUCCESS;
}

int BnsConnectionPool::healthy_check(bool need_detect_dead){
    if (!_is_used) {
        //CLIENT_WARNING("bns pool:%s is not used", _bns_name.c_str());
        return SUCCESS; 
    }
    if (_bns_info_ptr == NULL) {
        CLIENT_FATAL("bns info ptr is null, bns_name:%s", _bns_name.c_str());
        return CONFPARAM_ERROR;
    }
    ////debug
    if (need_detect_dead) {
        print_conn_info();
    }
    //以从bns服务同步回来的实例信息为基准，做一次循环
    boost::mutex::scoped_lock lock(_bns_info_ptr->mutex_lock);
    map<string, InstanceInfo>::iterator iter1 = _bns_info_ptr->instance_infos.begin();
    for (; iter1 != _bns_info_ptr->instance_infos.end(); ++iter1) {
        int shard_id = get_shard_id_by_bns_tag(iter1->second.tag);
        if (_tag != "" && shard_id != _id) {
            //不属于本pool的instance
            //CLIENT_WARNING("_tag:%s, shard_id:%d", _tag.c_str(), shard_id);
            continue;
        }
        string id = iter1->second.id;
        // 状态为可用
        if (iter1->second.is_available) {//可用
            //判断pool中是否存在该实例，若不存在，则新建实例,状态为在线 
            if (_instances_map.count(id) == 0) {
                Instance* instance = new(std::nothrow) Instance(
                                iter1->second.ip,
                                iter1->second.port,
                                _max_connection_per_instance,
                                this,
                                FAULTY,
                                _conn_conf,
                                _instances_index.size(),
                                0);
                if (instance == NULL) {
                    CLIENT_FATAL("new instance error, bns_name:%s, instance:%s:%d", 
                            _bns_name.c_str(), iter1->second.ip.c_str(), iter1->second.port);
                }
                // 增加实例，加写锁
                // boost::mutex::scoped_lock lock(_lock);
                boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
                _instances_map[iter1->second.id] = instance;
                _instances_index.push_back(iter1->second.id);
                CLIENT_WARNING("bns pool new instance available, bns_name:%s, instance:%s",
                            _bns_name.c_str(), iter1->second.id.c_str());
            }
            Instance* instance = _instances_map[id];
            instance->bns_available_healthy_check(need_detect_dead);
        } else {
            //不存在，不处理
            if (_instances_map.count(id) == 0) {
                CLIENT_DEBUG("bns syn new instance not available, bns_name:%s, instance:%s",
                        _bns_name.c_str(), iter1->second.id.c_str());
                continue;
            }
            Instance* instance = _instances_map[id];
            instance->bns_not_available_healthy_check();
        }
    } 
    //以pool为基准做一次循环
    map<string, Instance*>::iterator iter2 = _instances_map.begin();
    for (; iter2 != _instances_map.end(); ++iter2) {
        string id = iter2->first;
        if (_bns_info_ptr->instance_infos.count(id) != 0) {
            continue;
        }
        CLIENT_WARNING("instance not exist in bns,  bns_name:%s, instance:%s, id:%s",
                _bns_name.c_str(), iter2->first.c_str(), id.c_str());
        iter2->second->bns_not_available_healthy_check();
    }
    if (!need_detect_dead) {
        return SUCCESS;
    }
    bool is_legal = false;
    int avg_conn_num = divide_ceil(_total_conn_num, get_online_instance_num(), &is_legal);
    if (!is_legal) {
        CLIENT_FATAL("There are no online instance, bns_name:%s", _bns_name.c_str());
        return SUCCESS;
    }
    int64_t total_qps = 1;
    iter2 = _instances_map.begin();
    for (; iter2 != _instances_map.end(); ++iter2) {
        total_qps += iter2->second->get_qps();
    }
    iter2 = _instances_map.begin();
    for (; iter2 != _instances_map.end(); ++iter2) {
        if (iter2->second->get_status() == ON_LINE) {
            int32_t conn_num_according_qps =
                _total_conn_num * iter2->second->get_qps() / total_qps;
            int32_t should_conn_num =
                conn_num_according_qps > avg_conn_num ? conn_num_according_qps : avg_conn_num;
            int real_conn_num = iter2->second->get_total_connection_num();
            //CLIENT_WARNING("instance:%s, real_conn_num:%d, conn_num_according_qps:%d, "
            //                "should:%d, total_qps:%d, _qps:%d",
            //                iter2->first.c_str(), real_conn_num,
            //                conn_num_according_qps, should_conn_num,
            //                total_qps, iter2->second->get_qps());
            if (real_conn_num < avg_conn_num) {
                // add_connecion
                int delta_num = avg_conn_num - real_conn_num;
                if (delta_num > (_max_connection_per_instance - real_conn_num)) {
                    delta_num = _max_connection_per_instance - real_conn_num;
                    CLIENT_FATAL("There are no enough instances to add connection, bns_name:%s,"
                                "instance:%s, conn_num:%d ",
                                _bns_name.c_str(), iter2->first.c_str(), real_conn_num);
                }
                //CLIENT_WARNING("instance:%s add conn:%d", iter2->first.c_str(), delta_num);
                iter2->second->add_connection(delta_num);
            }
            if (real_conn_num > should_conn_num) {
                //CLIENT_WARNING("instance:%s deletel conn:%d",
                //            iter2->first.c_str(),
                //            real_conn_num - should_conn_num);
                //delete_connection
                iter2->second->delete_connection_not_used(real_conn_num - should_conn_num);
            }
        } 
    }
    print_conn_info();
    return SUCCESS;
}

string BnsConnectionPool::get_name() const {
    return _bns_name;
}

int BnsConnectionPool::get_online_instance_num() {
    //该方法只在健康检查线程和初始化中调用，所以不用加锁
    int num = 0;
    map<string, Instance*>::iterator iter = _instances_map.begin();
    for (; iter != _instances_map.end(); ++iter) {
        if (iter->second->get_status() == ON_LINE) {
            ++num;
        }
    }
    //_num_online_instance = num;
    return num;
}

int BnsConnectionPool::get_total_conn_num() const {
    return _total_conn_num;
}

string BnsConnectionPool::get_comment_format() const {
    return _comment_format;
}

bool BnsConnectionPool::is_split_table() const {
    return _name_logic_db_map.size();
}

void BnsConnectionPool::print_conn_info() {
    //boost::mutex::scoped_lock lock(_lock);
    boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
    CLIENT_WARNING("bns_name:%s, total_weight:%ld", _bns_name.c_str(), _total_weight.load());
    map<string, Instance*>::iterator iter = _instances_map.begin();
    for (; iter != _instances_map.end(); ++iter) {
        CLIENT_WARNING("Instance:%s, instance_status:%s, bns:%s, tag:%s"
                     ", connection num:%d, not_used_conn:%d,"
                     "index:%d, _left:%ld, weight:%ld",
                    iter->first.c_str(), INSTANCE_STATUS_CSTR[iter->second->get_status()],
                    _bns_name.c_str(), _tag.c_str(), iter->second->get_total_connection_num(), 
                    iter->second->get_not_used_connection_num(),
                    iter->second->get_index(),
                    iter->second->get_left(),
                    iter->second->get_weight());
    }
    iter = _instances_map.begin();
    for (; iter != _instances_map.end(); ++iter) {
        iter->second->print_weight();
    }
}

int BnsConnectionPool::alloc_connection(int conn_num) {
    int size = get_online_instance_num();
    if (size == 0) {
        CLIENT_WARNING("there are no online instance to create conn, bns_name:%s", _bns_name.c_str());
        return INSTANCE_NOT_ENOUGH;
    }
    //此处对instance_map的读操作不需要加锁，因为写操作和该操作都在健康检查统一线程中
    //不涉及多线程问题
    //与fetch_connection时的加锁不同
    int conn_num_per_instance = divide_ceil(conn_num, size, NULL);
    map<string, Instance*>::iterator iter = _instances_map.begin();
    for (; iter != _instances_map.end(); ++iter) {
        if (iter->second->get_status() == ON_LINE) {
            int real_conn_num = iter->second->get_total_connection_num();
            int conn_num_need_add = conn_num_per_instance;
            if (conn_num_need_add > (_max_connection_per_instance - real_conn_num)) {
                conn_num_need_add = _max_connection_per_instance - real_conn_num;
                CLIENT_WARNING("There are not enought instance to add connection,"
                            "conn_num:%d, bns_name:%s, online_instance_num:%d,"
                            "instance:%s:%d, connection_num_for_instance:%d",
                             conn_num, _bns_name.c_str(), size,
                             iter->second->get_ip().c_str(),
                             iter->second->get_port(), real_conn_num);
            }
            iter->second->add_connection(conn_num_need_add);
        }
    }
    CLIENT_WARNING("alloc connection");
    print_conn_info();
    return SUCCESS;
}
void BnsConnectionPool::update_parent_weight(int64_t diff, size_t index) {
    //boost::mutex::scoped_lock lock(_lock);
    boost::shared_lock<boost::shared_mutex> r_lock(_rw_lock);
    while (index != 0) {
        const size_t parent_index = (index - 1) / 2;
        if ((parent_index * 2) + 1 == index) {
            if (parent_index >= _instances_index.size()) {
                CLIENT_FATAL("fatal, parent_index is ilegal, parent_index:%ld", parent_index);
                return;
            }
            std::string id = _instances_index[parent_index];
            if (_instances_map.count(id) == 0) {
                CLIENT_FATAL("fatal, instance_map not exist, instance:%s", id.c_str());
                return;
            }
            _instances_map[id]->left_fetch_add(diff);
            //_instances_map[_instances_index[parent_index]]->left_fetch_add(diff);
        }
        index = parent_index;
    }
}
void BnsConnectionPool::_clear() {
    // 删除所有的实例，加写锁
    // boost::mutex::scoped_lock lock(_lock);
    boost::unique_lock<boost::shared_mutex> w_lock(_rw_lock);
    map<string, Instance*>::iterator iter = _instances_map.begin();
    for (; iter != _instances_map.end(); ++iter) {
        delete iter->second;
        iter->second = NULL;
    }
    _bns_info_ptr = NULL;
}
}
} //namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
