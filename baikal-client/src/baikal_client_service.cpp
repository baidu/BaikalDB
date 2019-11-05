/***************************************************************************
 * 
 * Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file baikal_client_service.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/13 16:46:30
 * @brief 
 *  
 **/

#include "baikal_client_service.h"
#include <boost/algorithm/string.hpp>
#include "baikal_client_util.h"
#include "shard_operator_mgr.h"
#include "global.h"

#ifdef BAIDU_INTERNAL
#include "com_log.h"
#endif

using std::string;
using std::map;
using std::vector;

namespace baikal {
namespace client {
const int Service::fetch_conn_times;

Service::Service(map<string, BnsInfo*>& bns_infos,
        int no_permission_wait_s, bool faulty_exit, bool async) :
        _hang_check(true),
        _conn_num_per_bns(0),
        _max_conn_per_instance(0),
        _select_algo(ROLLING),
        _bns_infos(bns_infos),
        _connect_all(false),
        _is_inited(false),
        _no_permission_wait_s(no_permission_wait_s),
        _faulty_exit(faulty_exit),
        _async(async) {}

Service::~Service() {
    _clear();
}

#ifdef BAIDU_INTERNAL
int Service::init(comcfg::ConfigUnit& conf_unit) {
    if (conf_unit.selfType() == comcfg::CONFIG_ERROR_TYPE) {
        CLIENT_WARNING("service configuration is not right");
        return CONFPARAM_ERROR;
    }
    int ret = 0;
    try{
        _name = conf_unit["service_name"].to_cstr();
        _conn_conf.no_permission_wait_s = _no_permission_wait_s;
        _conn_conf.faulty_exit = _faulty_exit;
        _conn_conf.async = _async;
        _conn_conf.conn_type = MYSQL_CONN;
        _conn_num_per_bns = conf_unit["connection_num"].to_int32();
        // 只有用户配置了comment_format字段，才需要解析该值
        if (conf_unit["comment_format"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _comment_format = conf_unit["comment_format"].to_cstr();
        }
        if (conf_unit["hang_check"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _hang_check = (conf_unit["hang_check"].to_uint32() != 0);
        }
        _max_conn_per_instance = conf_unit["max_connection_per_instance"].to_int32();
        _select_algo = ROLLING;
        if (conf_unit["select_algorithm"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            int algo_int = conf_unit["select_algorithm"].to_int32();
            if (algo_int == 1) {
                _select_algo = RANDOM;
            } else if (algo_int == 2) {
                _select_algo = ROLLING;
            } else if (algo_int == 3) {
                _select_algo = LOCAL_AWARE;
            } else {
                CLIENT_WARNING("service:%s, unsupport select alogrithm, algo_int:%d", 
                        _name.c_str(), algo_int);
                return CONFPARAM_ERROR;
            }
        }
        CLIENT_WARNING("service:%s, alogrithm, algo_int:%d", _name.c_str(), _select_algo);
        _conn_conf.read_timeout = DEFAULT_READ_TIMEOUT; //默认值
        if (conf_unit["read_timeout"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _conn_conf.read_timeout = conf_unit["read_timeout"].to_int32();
        }
        _conn_conf.write_timeout = DEFAULT_WRITE_TIMEOUT; //默认值
        if (conf_unit["write_timeout"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _conn_conf.write_timeout = conf_unit["write_timeout"].to_int32();
        }
        _conn_conf.connect_timeout = DEFAULT_CONNECT_TIMEOUT;
        if (conf_unit["connect_timeout"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _conn_conf.connect_timeout = conf_unit["connect_timeout"].to_int32();
        }
        _conn_conf.charset = DEFAULT_CHARSET;
        if (conf_unit["charset"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _conn_conf.charset = conf_unit["charset"].to_cstr();
        }
        if (conf_unit["username"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _conn_conf.username = conf_unit["username"].to_cstr();
        }
        if (conf_unit["password"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _conn_conf.password = conf_unit["password"].to_cstr();
        }
        _connect_all = false;
        if (conf_unit["connect_all"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            _connect_all = (conf_unit["connect_all"].to_uint32() == 1);
        }
        string split_function_string;
        if (conf_unit["db_split_function"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            split_function_string = string(conf_unit["db_split_function"].to_cstr());
            ShardOperatorMgr* mgr = ShardOperatorMgr::get_s_instance();
            int ret = mgr->split(split_function_string, _db_split_function);
            if (ret < 0) {
                CLIENT_WARNING("db split funciton format is wrong, [%s], service_name:[%s]",
                        split_function_string.c_str(), _name.c_str());
                return ret;
            }
        }
        CLIENT_DEBUG("db_split_function split successfully:%s, service_name:[%s]",
                    split_function_string.c_str(), _name.c_str());
        CLIENT_DEBUG("service_name:%s, service_type:%s, conn_num_per_bns:%d,"
                    " max_conn_per_instance:%d, comment format:%s",
                     _name.c_str(), CONN_TYPE_CSTR[_conn_conf.conn_type], _conn_num_per_bns, 
                     _max_conn_per_instance, _comment_format.c_str());
        CLIENT_DEBUG("conn conf, read_timeout:%d, write_timeout:%d, connect_timeout:%d",
                    _conn_conf.read_timeout, _conn_conf.write_timeout, _conn_conf.connect_timeout);
        CLIENT_DEBUG("conn conf, username:%s, password:%s, charset:%s",
                    _conn_conf.username.c_str(), _conn_conf.password.c_str(),
                    _conn_conf.charset.c_str());
        //加载 logic db的配置
        ret = _load_logic_db_info(conf_unit);
        if (ret < 0) {
            CLIENT_WARNING("load logic db fail, service_name:[%s]", _name.c_str());
            return ret;
        }

        // 识别db_shard配置，如果有该配置，则同步bns信息
        // 如果没有该配置，则需要init2来延迟初始化bns或ip信息
        ret = _syn_instance_info_from_db_shard_conf(conf_unit);
        if (ret < 0) {
            CLIENT_WARNING("load instance info from bns is wrong");
            return ret;
        }

        //加载bns配置
        ret = _load_bns_info(conf_unit);
        if (ret <0) {
            CLIENT_WARNING("load logic db fail, service_name:[%s]", _name.c_str());
            return ret;
        }
    }
    catch (comcfg::ConfigException& e) {
        CLIENT_WARNING("configuration file format is not right, service_name: %s, exception:%s", 
                    _name.c_str(), e.what());
        return CONFPARAM_ERROR;
    }
    CLIENT_DEBUG("service init successfully, service_name:%s", _name.c_str());
    return SUCCESS;
}

int Service::init2(const std::string& naming_service_url,
                   const std::string& username,
                   const std::string& password,
                   const std::string& charset) {
    if (_is_inited) {
        CLIENT_WARNING("service %s has already inited", _name.c_str());
        return SERVICE_INIT2_ERROR;
    }
    int ret = 0;
    _conn_conf.username = username;
    _conn_conf.password = password;
    _conn_conf.charset = charset;
    if (naming_service_url.compare(0, 6, "bns://") == 0) {
        std::string bns_name = naming_service_url.substr(6);
        //init增加获取bns实例重试
        int retrys = 10;
        do {
            ret = _syn_instance_info_by_bns_name(bns_name);
            bthread_usleep(1000000); //sleep 1s
        } while (--retrys > 0 && ret < 0);
        if (ret < 0) {
            CLIENT_WARNING("_syn_instace_info_by_bns_name is wrong,"
                    "service_name: %s,bns_name: %s",
                    _name.c_str(), bns_name.c_str());
            return ret;
        }
        //init2调用时，健康检查线程已经开启，需要注意竞争问题
        boost::mutex::scoped_lock lock(_bns_infos[bns_name]->mutex_lock);
        std::map<std::string, InstanceInfo>& instance_infos = _bns_infos[bns_name]->instance_infos;
        std::map<std::string, InstanceInfo>::iterator iter = instance_infos.begin();
        for (; iter != instance_infos.end(); ++iter) {
            std::string tag = iter->second.tag;
            int id = get_shard_id_by_bns_tag(tag);
            if (_id_bns_map.count(id) != 0) {
                continue;
            }
            BnsConnectionPool* pool = new(std::nothrow) BnsConnectionPool(
                    bns_name,
                    tag,
                    _conn_num_per_bns,
                    _max_conn_per_instance,
                    id,
                    _conn_conf,
                    _name_logic_db_map.begin(),
                    _name_logic_db_map.end(),
                    _bns_infos[bns_name],
                    _comment_format,
                    _select_algo);
            if (pool == NULL) {
                CLIENT_WARNING("new pool fail, pool name: %s", bns_name.c_str());
                return NEWOBJECT_ERROR;
            }
            int ret = pool->init(_connect_all, NULL);
            if (ret < 0) {
                CLIENT_WARNING("pool init failed, pool name: %s", bns_name.c_str());
                return ret;
            }
            _id_bns_map[id] = pool;
            CLIENT_DEBUG("bns pool connection successfully, pool name: %s, pool id:%d",
                    bns_name.c_str(), id);
        }
    } else if (naming_service_url.compare(0, 7, "list://") == 0) {
        std::string list = naming_service_url.substr(7);
        std::vector<std::string> list_shards;
        boost::split(list_shards, list, boost::is_any_of(";"));
        for (uint32_t i = 0; i < list_shards.size(); i++) {
            int id = 0;
            std::string shard;
            std::size_t shard_pos = list_shards[i].find('$');
            if (shard_pos == std::string::npos) {
                shard = list_shards[i];
            } else {
                id = atoi(list_shards[i].substr(0, shard_pos).c_str());
                shard = list_shards[i].substr(shard_pos + 1);
            }
            std::vector<std::string> instances_str;
            vector<InstanceInfo> instances;
            boost::split(instances_str, shard, boost::is_any_of(","));
            for (uint32_t j = 0; j < instances_str.size(); j++) {
                InstanceInfo info;
                info.id = instances_str[j];
                std::vector<std::string> items;
                boost::split(items, info.id, boost::is_any_of(":"));
                info.ip = items[0];
                info.port = atoi(items[1].c_str());
                info.enable = true;
                info.status = 0;
                info.is_available = true;
                instances.push_back(info);
            }
            if (_id_bns_map.count(id) != 0) {
                continue;
            }
            BnsConnectionPool* pool = new(std::nothrow) BnsConnectionPool(
                    "ip_list",
                    "",
                    _conn_num_per_bns,
                    _max_conn_per_instance,
                    id,
                    _conn_conf,
                    _name_logic_db_map.begin(),
                    _name_logic_db_map.end(),
                    NULL,
                    _comment_format,
                    _select_algo);
            if (pool == NULL) {
                CLIENT_WARNING("new pool fail, pool name: ip_list");
                return NEWOBJECT_ERROR;
            }
            _id_bns_map[id] = pool;
            int ret = pool->init(_connect_all, &instances);
            if (ret < 0) {
                CLIENT_WARNING("pool init failed, pool name: ip_list");
                return ret;
            }
            CLIENT_DEBUG("bns pool connection successfully, pool name: ip_list, pool id:%d", id);
        }
    } else {
        CLIENT_WARNING("naming_service_url error %s", naming_service_url.c_str());
        return -1;
    }
    _is_inited = true;
    return SUCCESS;
}

int Service::init_bns_healthcheck(const std::string& naming_service_url,
        const std::string& username,
        const std::string& password,
        const std::string& charset) {
    if (_is_inited) {
        CLIENT_WARNING("service %s has already inited", _name.c_str());
        return SERVICE_INIT2_ERROR;
    }

    _conn_conf.username = username;
    _conn_conf.password = password;
    _conn_conf.charset = charset;

    if (naming_service_url.compare(0, 6, "bns://") != 0) {
        CLIENT_WARNING("bns %s syntax invalid", naming_service_url.c_str());
        return -1;
    }

    std::string bns_name = naming_service_url.substr(6);
    int ret = _syn_instance_info_by_bns_name(bns_name);
    if (ret < 0) {
        CLIENT_WARNING("_syn_instace_info_by_bns_name is wrong,"
                "service_name: %s,bns_name: %s",
                _name.c_str(), bns_name.c_str());
        return ret;
    }

    boost::mutex::scoped_lock lock(_bns_infos[bns_name]->mutex_lock);
    std::map<std::string, InstanceInfo>& instance_infos = _bns_infos[bns_name]->instance_infos;
    std::map<std::string, InstanceInfo>::iterator iter = instance_infos.begin();
    for (; iter != instance_infos.end(); ++iter) {
        std::string tag = iter->second.tag;
        int id = get_shard_id_by_bns_tag(tag);
        if (_id_bns_map.count(id) != 0) {
            continue;
        }
        BnsConnectionPool* pool = new(std::nothrow) BnsConnectionPool(
                bns_name,
                tag,
                _conn_num_per_bns,
                _max_conn_per_instance,
                id,
                _conn_conf,
                _name_logic_db_map.begin(),
                _name_logic_db_map.end(),
                _bns_infos[bns_name],
                _comment_format,
                _select_algo);
        if (pool == NULL) {
            CLIENT_WARNING("new pool fail, pool name: %s", bns_name.c_str());
            return NEWOBJECT_ERROR;
        }
        pool->set_is_used(true);
        ret = pool->init(false, NULL);
        if (ret < 0) {
            CLIENT_WARNING("bns health check init failed, pool name: %s", bns_name.c_str());
            return ret;
        }
        _id_bns_map[id] = pool;
        CLIENT_DEBUG("bns health check connection successfully, pool name: %s, pool id:%d",
                bns_name.c_str(), id);
    }
    _is_inited = true;
    return SUCCESS;
}

int Service::_syn_instance_info_by_bns_name(const std::string& bns_name) {
    vector<InstanceInfo> instances;
    if (!_bns_infos.count(bns_name)) {
        int retrys = 3;
        int ret = 0;
        do {
            ret = get_instance_from_bns(bns_name, instances);
        } while (--retrys > 0 && ret < 0);
        if (ret < 0) {
            CLIENT_WARNING("get instance info from bns is wrong,"
                    "service_name: %s,bns_name: %s",
                    _name.c_str(), bns_name.c_str());
            return ret;
        }
        //bns_info保存的是每个bns的信息
        BnsInfo* bns_info = new(std::nothrow) BnsInfo();
        if (bns_info == NULL) {
            CLIENT_WARNING("New BnsInfo error");
            return NEWOBJECT_ERROR;
        }
        _bns_infos[bns_name] = bns_info;
        bns_info->bns_name = bns_name;

        vector<InstanceInfo>::iterator iter = instances.begin();
        for (; iter != instances.end(); ++iter) {
            bns_info->instance_infos[iter->id] = *iter;
            CLIENT_DEBUG("bns_name:%s, id:%s, enable:%d, status:%d, is_available:%d",
                    bns_name.c_str(), iter->id.c_str(), iter->enable,
                    iter->status, iter->is_available);
        }
    }
    return SUCCESS;
}

int Service::_syn_instance_info_from_db_shard_conf(comcfg::ConfigUnit& conf_unit) {
    try {
        int ret = 0;
        string service_name(conf_unit["service_name"].to_cstr());
        if (conf_unit["db_shard"].selfType() == comcfg::CONFIG_ERROR_TYPE) {
            CLIENT_WARNING("service:%s has no db_shard; need calc init2", service_name.c_str());
            return SUCCESS;
        }
        for (size_t i = 0; i < conf_unit["db_shard"].size(); ++i) {
            if (conf_unit["db_shard"][i]["bns_name"].selfType() !=
                    comcfg::CONFIG_ERROR_TYPE) {
                string bns_name = string(conf_unit["db_shard"][i]["bns_name"].to_cstr());
                // 从每个bns服务同步的实例信息
                ret = _syn_instance_info_by_bns_name(bns_name);
                if (ret < 0) {
                    CLIENT_WARNING("_syn_instace_info_by_bns_name is wrong,"
                            "service_name: %s,bns_name: %s",
                            service_name.c_str(), bns_name.c_str());
                    return ret;
                }
            } else if (conf_unit["db_shard"][i]["ip_list"].selfType() !=
                    comcfg::CONFIG_ERROR_TYPE) {
                CLIENT_WARNING("service %s use ip list", _name.c_str());
            } else {
                CLIENT_WARNING("bns name and ip list must choose one");
                return CONFPARAM_ERROR;
            }
        }
    } catch (comcfg::ConfigException& e) {
        CLIENT_WARNING("service configure format is not right, exception:%s", e.what());
        return CONFPARAM_ERROR;
    }
    CLIENT_DEBUG("bns_infos size:%d", _bns_infos.size());
    map<string, BnsInfo*>::iterator iter = _bns_infos.begin();
    for (; iter != _bns_infos.end(); ++iter) {
        map<string, InstanceInfo>::iterator iter_inner = iter->second->instance_infos.begin();
        for (; iter_inner != iter->second->instance_infos.end(); ++iter_inner) {
            CLIENT_DEBUG("id:%s, enable:%d, status:%d, is_available:%d",
                    iter_inner->second.id.c_str(), iter_inner->second.enable,
                    iter_inner->second.status, iter_inner->second.is_available);
        }
    }
    _is_inited = true;
    return SUCCESS;
}
#else

int Service::init(const Option& option) {
    _name = option.service_name;
    _conn_conf.no_permission_wait_s = _no_permission_wait_s;
    _conn_conf.faulty_exit = _faulty_exit;
    _conn_conf.async = _async;
    _conn_conf.conn_type = MYSQL_CONN;
    _conn_num_per_bns = option.connection_num;

    // 只有用户配置了comment_format字段，才需要解析该值
    _comment_format = option.comment_format;
    _hang_check = option.hang_check;
    _max_conn_per_instance = option.max_connection_per_instance;

    _select_algo = ROLLING;
    int algo_int = option.select_algorithm;
    if (algo_int == 1) {
        _select_algo = RANDOM;
    } else if (algo_int == 2) {
        _select_algo = ROLLING;
    } else if (algo_int == 3) {
        _select_algo = LOCAL_AWARE;
    } else {
        CLIENT_WARNING("service:%s, unsupport select alogrithm, algo_int:%d",
                _name.c_str(), algo_int);
        return CONFPARAM_ERROR;
    }

    CLIENT_WARNING("service:%s, alogrithm, algo_int:%d", _name.c_str(), _select_algo);
    _conn_conf.read_timeout = DEFAULT_READ_TIMEOUT; //默认值
    _conn_conf.write_timeout = option.write_timeout;
    _conn_conf.connect_timeout = option.connect_timeout;
    _conn_conf.charset = option.charset;
    _conn_conf.username = option.username;
    _conn_conf.password = option.password;
    _connect_all = option.connect_all;

    string split_function_string;
    if (!option.db_split_function.empty()) {
        split_function_string = option.db_split_function;
        ShardOperatorMgr* mgr = ShardOperatorMgr::get_s_instance();
        int ret = mgr->split(split_function_string, _db_split_function);
        if (ret < 0) {
            CLIENT_WARNING("db split funciton format is wrong, [%s], service_name:[%s]",
                    split_function_string.c_str(), _name.c_str());
            return ret;
        }
    }
    CLIENT_DEBUG("db_split_function split successfully:%s, service_name:[%s]",
            split_function_string.c_str(), _name.c_str());
    CLIENT_DEBUG("service_name:%s, service_type:%s, conn_num_per_bns:%d,"
                 " max_conn_per_instance:%d, comment format:%s",
            _name.c_str(), CONN_TYPE_CSTR[_conn_conf.conn_type], _conn_num_per_bns,
            _max_conn_per_instance, _comment_format.c_str());
    CLIENT_DEBUG("conn conf, read_timeout:%d, write_timeout:%d, connect_timeout:%d",
            _conn_conf.read_timeout, _conn_conf.write_timeout, _conn_conf.connect_timeout);
    CLIENT_DEBUG("conn conf, username:%s, password:%s, charset:%s",
            _conn_conf.username.c_str(), _conn_conf.password.c_str(),
            _conn_conf.charset.c_str());
    //加载 logic db的配置
    int ret = _load_logic_db_info(option.logic_dbs);
    if (ret < 0) {
        CLIENT_WARNING("load logic db fail, service_name:[%s]", _name.c_str());
        return ret;
    }

    // 识别db_shard配置，如果有该配置，则同步bns信息
    // 如果没有该配置，则需要init2来延迟初始化bns或ip信息
    ret = _syn_instance_info_from_db_shard_conf(option);
    if (ret < 0) {
        CLIENT_WARNING("load instance info from bns is wrong");
        return ret;
    }

    //加载bns配置
    ret = _load_bns_info(option.db_shards);
    if (ret < 0) {
        CLIENT_WARNING("load logic db fail, service_name:[%s]", _name.c_str());
        return ret;
    }

    CLIENT_DEBUG("service init successfully, service_name:%s", _name.c_str());
    return SUCCESS;
}

int Service::init2(const std::string& link_url, const std::string& username,
        const std::string& password, const std::string& charset) {
    if (_is_inited) {
        CLIENT_WARNING("service %s has already inited", _name.c_str());
        return SERVICE_INIT2_ERROR;
    }
    _conn_conf.username = username;
    _conn_conf.password = password;
    _conn_conf.charset = charset;
    if (link_url.compare(0, 7, "list://") != 0) {
        CLIENT_WARNING("link_url error %s", link_url.c_str());
        return -1;
    }

    std::string list = link_url.substr(7);
    std::vector<std::string> list_shards;
    boost::split(list_shards, list, boost::is_any_of(";"));
    for (uint32_t i = 0; i < list_shards.size(); i++) {
        int id = 0;
        std::string shard;
        std::size_t shard_pos = list_shards[i].find('$');
        if (shard_pos == std::string::npos) {
            shard = list_shards[i];
        } else {
            id = atoi(list_shards[i].substr(0, shard_pos).c_str());
            shard = list_shards[i].substr(shard_pos + 1);
        }
        std::vector<std::string> instances_str;
        vector<InstanceInfo> instances;
        boost::split(instances_str, shard, boost::is_any_of(","));
        for (uint32_t j = 0; j < instances_str.size(); j++) {
            InstanceInfo info;
            info.id = instances_str[j];
            std::vector<std::string> items;
            boost::split(items, info.id, boost::is_any_of(":"));
            info.ip = items[0];
            info.port = atoi(items[1].c_str());
            info.enable = true;
            info.status = 0;
            info.is_available = true;
            instances.push_back(info);
        }
        if (_id_bns_map.count(id) != 0) {
            continue;
        }
        BnsConnectionPool* pool = new(std::nothrow) BnsConnectionPool(
                "ip_list",
                "",
                _conn_num_per_bns,
                _max_conn_per_instance,
                id,
                _conn_conf,
                _name_logic_db_map.begin(),
                _name_logic_db_map.end(),
                NULL,
                _comment_format,
                _select_algo);
        if (pool == NULL) {
            CLIENT_WARNING("new pool fail, pool name: ip_list");
            return NEWOBJECT_ERROR;
        }
        _id_bns_map[id] = pool;
        int ret = pool->init(_connect_all, &instances);
        if (ret < 0) {
            CLIENT_WARNING("pool init failed, pool name: ip_list");
            return ret;
        }
    }
    _is_inited = true;
    return SUCCESS;
}

int Service::_syn_instance_info_from_db_shard_conf(const Option& option) {

    string service_name = option.service_name;
    if (option.db_shards.empty()) {
        CLIENT_WARNING("service:%s has no db_shard; need calc init2", service_name.c_str());
        return SUCCESS;
    }
    for (auto& db_shard : option.db_shards) {
        if (db_shard.ip_list.empty()) {
            CLIENT_WARNING("ip list must not empty");
            return CONFPARAM_ERROR;
        }
    }

    CLIENT_DEBUG("bns_infos size:%d", _bns_infos.size());
    map<string, BnsInfo*>::iterator iter = _bns_infos.begin();
    for (; iter != _bns_infos.end(); ++iter) {
        map<string, InstanceInfo>::iterator iter_inner = iter->second->instance_infos.begin();
        for (; iter_inner != iter->second->instance_infos.end(); ++iter_inner) {
            CLIENT_DEBUG("id:%s, enable:%d, status:%d, is_available:%d",
                    iter_inner->second.id.c_str(), iter_inner->second.enable,
                    iter_inner->second.status, iter_inner->second.is_available);
        }
    }
    _is_inited = true;
    return SUCCESS;
}

#endif

int Service::query_timeout(int shard_id, const std::string& ip_port,
        const std::string& sql, MYSQL_RES*& res, int second) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SERVICE_NOT_INIT_ERROR;
    }

    uint32_t retry = 0;
    int query_second = 0;
    while (second < 0 || query_second < second) {
        int ret = 0;
        do {
            SmartConnection conn = fetch_connection_by_shard(shard_id, ip_port);
            if (!conn) {
                CLIENT_WARNING("fetch connection in query fail,shard:%d service:%s",
                        shard_id, _name.c_str());
                ret = FETCH_CONNECT_FAIL;
                break;
            }
            ret = conn->execute_raw(sql, res);
            conn->close();
        } while (0);
        if (ret == 0 || ret == CONNECTION_QUERY_FAIL) {
            //成功或sql语法错误则退出
            return ret;
        }
        if (retry < 10) {
            CLIENT_WARNING("mysql gone away,shard:%d, sql:%s ret:%d, retry %u times",
                    shard_id, sql.c_str(), ret, ++retry);
            usleep(10000);
        } else {
            ++query_second;
            CLIENT_FATAL("mysql gone away,shard:%d, sql:%s ret:%d, retry %u times",
                    shard_id, sql.c_str(), ret, ++retry);
            sleep(1);
        }
    }
    return CONNECTION_QUERY_FAIL;
}

int Service::query_timeout(int shard_id, const std::string& ip_port,
        const std::string& sql, MYSQL_RES*& res, int second, std::string& real_ip_port) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SERVICE_NOT_INIT_ERROR;
    }

    uint32_t retry = 0;
    int query_second = 0;
    while (second < 0 || query_second < second) {
        int ret = 0;
        do {
            SmartConnection conn = fetch_connection_by_shard(shard_id, ip_port);
            if (!conn) {
                CLIENT_WARNING("fetch connection in query fail,shard:%d service:%s",
                        shard_id, _name.c_str());
                ret = FETCH_CONNECT_FAIL;
                break;
            }
            ret = conn->execute_raw(sql, res);
            real_ip_port = conn->get_instance_info();
            conn->close();
        } while (0);
        if (ret == 0 || ret == CONNECTION_QUERY_FAIL) {
            //成功或sql语法错误则退出
            return ret;
        }
        if (retry < 10) {
            CLIENT_WARNING("mysql gone away,shard:%d, sql:%s ret:%d, retry %u times",
                    shard_id, sql.c_str(), ret, ++retry);
            usleep(10000);
        } else {
            ++query_second;
            CLIENT_FATAL("mysql gone away,shard:%d, sql:%s ret:%d, retry %u times",
                    shard_id, sql.c_str(), ret, ++retry);
            sleep(1);
        }
    }
    return CONNECTION_QUERY_FAIL;
}

int Service::query(uint32_t partition_key, const std::string& sql, ResultSet* result) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SERVICE_NOT_INIT_ERROR;
    }

    SmartConnection conn = fetch_connection(partition_key);
    if (!conn) {
        CLIENT_WARNING("fetch connection in query fail, service:%s", _name.c_str());
        return FETCH_CONNECT_FAIL;
    }
    int ret = conn->execute(sql, result);
    if (ret == CONNECTION_RECONN_SUCCESS) {
        ret = conn->execute(sql, result);
    }
    conn->close();
    return ret;
}

SmartConnection Service::fetch_connection() {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SmartConnection();
    }
    BnsConnectionPool* pool = _get_pool_no_partition_key();
    if (pool == NULL) {
        CLIENT_WARNING("get conn pool when fetch connection fail, service_name: %s",
                _name.c_str());
        return SmartConnection();
    }
    SmartConnection conn = pool->fetch_connection("");
    if (!conn) {
        //选择连接失败, 在pool->fetch_connection中已经做过n此尝试，所以在这出错直接返回
        CLIENT_WARNING("fetch connection from pool fail, service_name: %s", _name.c_str());
        return conn;
    }
    conn->set_has_partition_key(false);
    conn->set_has_logic_db(false);
    return conn;
}

SmartConnection Service::fetch_connection(uint32_t partition_key) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SmartConnection();
    }
    BnsConnectionPool* pool = _get_pool_partition_key(partition_key);
    if (pool == NULL) {
        CLIENT_WARNING("get conn pool when fetch connection fail, service_name: %s",
                _name.c_str());
        return SmartConnection();
    }
    SmartConnection conn = pool->fetch_connection("");
    if (!conn) {
        //选择连接失败, 在pool->fetch_connection中已经做过n此尝试，所以在这出错直接返回
        CLIENT_WARNING("fetch connection from pool fail, service_name: %s", _name.c_str());
        return conn;
    }
    conn->set_has_partition_key(true);
    conn->set_partition_key(partition_key);
    conn->set_has_logic_db(false);
    return conn;
}

SmartConnection Service::fetch_connection(const string& logic_db_name) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SmartConnection();
    }
    BnsConnectionPool* pool = _get_pool_no_partition_key();
    if (pool == NULL) {
        CLIENT_WARNING("get conn pool when fetch connection fail, service_name: %s",
                _name.c_str());
        return SmartConnection();
    }
    SmartConnection connection = _fetch_and_select_db(pool, logic_db_name, false, 0);
    return connection;
}

SmartConnection Service::fetch_connection(
        const string& logic_db_name,
        uint32_t partition_key) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SmartConnection();
    }
    BnsConnectionPool* pool = _get_pool_partition_key(partition_key);
    if (pool == NULL) {
        CLIENT_WARNING("get conn pool when fetch connection fail, service_name: %s",
                _name.c_str());
        return SmartConnection();
    }
    SmartConnection connection = _fetch_and_select_db(pool, logic_db_name, true, partition_key);
    return connection;
}

SmartConnection Service::fetch_connection_by_shard(int shard_id, const std::string& ip_port) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2", _name.c_str());
        return SmartConnection();
    }
    BnsConnectionPool* pool = NULL;
    if (_id_bns_map.count(shard_id) == 1) {
        pool = _id_bns_map[shard_id];
    }
    if (pool == NULL) {
        CLIENT_WARNING("get conn pool when fetch connection fail, service_name: %s",
                _name.c_str());
        return SmartConnection();
    }
    SmartConnection conn = pool->fetch_connection(ip_port);
    if (!conn) {
        //选择连接失败, 在pool->fetch_connection中已经做过n此尝试，所以在这出错直接返回
        CLIENT_WARNING("fetch connection from pool fail, service_name: %s", _name.c_str());
        return conn;
    }
    return conn;
}

int Service::check_ip_by_shard(int shard_id, const std::string& ip_port) {
    if (!_is_inited) {
        CLIENT_WARNING("service %s is not inited, please use init2, addr:%s",
                _name.c_str(), ip_port.c_str());
        return -1;
    }
    BnsConnectionPool* pool = NULL;
    if (_id_bns_map.count(shard_id) == 1) {
        pool = _id_bns_map[shard_id];
    }
    if (pool == NULL) {
        CLIENT_WARNING("get conn pool when fetch connection fail, service_name: %s, addr:%s",
                _name.c_str(), ip_port.c_str());
        return -1;
    }
    Instance* instance = pool->select_instance(ip_port);
    if (instance == NULL) {
        CLIENT_WARNING("there is no available instance, service_name:%s, addr:%s",
                _name.c_str(), ip_port.c_str());
        return -1;
    }
    return SUCCESS;
}

map<int, BnsConnectionPool*> Service::get_id_bns_map() const {
    return _id_bns_map;
}

string Service::get_name() const {
    return _name;
}

string Service::get_comment_format() const {
    return _comment_format;
}

BnsConnectionPool* Service::_get_pool_no_partition_key() {
    if (_id_bns_map.size() == 1) {
        return _id_bns_map.begin()->second;
    } else {
        CLIENT_WARNING("service:%s has %d shards, it need partition key to choose shard",
                _name.c_str(), _id_bns_map.size());
        return NULL;
    }
    return NULL;
}

BnsConnectionPool* Service::_get_pool_partition_key(
        uint32_t partition_key) {
    if (_db_split_function.size() == 0) { //无分库公式表示不分库
        if (_id_bns_map.size() == 1) {
            return _id_bns_map.begin()->second;
        } else {
            CLIENT_WARNING("sevice:%s has %d shards, no db_split_function, "
                           "use fetch_connection_by_shard", _name.c_str(), _id_bns_map.size());
            return NULL;
        }
    }
    int id = 0;
    int ret = _get_shard_id(partition_key, &id);
    if (ret < 0) {
        CLIENT_WARNING("get shard id fail, partition_key:%d, service:%s",
                partition_key, _name.c_str());
        return NULL;
    }
    map<int, BnsConnectionPool*>::iterator iter = _id_bns_map.find(id);
    if (iter == _id_bns_map.end()) {
        CLIENT_WARNING("there is no pool according to bns id:%d, service:%s",
                id, _name.c_str());
        return NULL;
    }
    return _id_bns_map[id];
}

SmartConnection Service::_fetch_and_select_db(
        BnsConnectionPool* pool,
        string logic_db_name,
        bool has_partition_key,
        uint32_t partition_key) {
    int i = 0;
    do {
        SmartConnection conn = pool->fetch_connection("");
        if (!conn) {
            //选择连接失败, 在pool->fetch_connection中已经做过n此尝试，所以在这出错直接返回
            CLIENT_WARNING("fetch connection from pool fail, service:%s",
                    _name.c_str());
            return conn;
        }
        conn->set_has_logic_db(true);
        conn->set_logic_db(logic_db_name);
        conn->set_has_partition_key(has_partition_key);
        if (has_partition_key) {
            conn->set_partition_key(partition_key);
        }
        string sql = "use " + logic_db_name;
        int ret = conn->execute(sql, NULL);
        if (ret == 0) {
            return conn;
        } else if (ret == INSTANCE_FAULTY_ERROR || ret == CONNECTION_ALREADY_DELETED) {
            conn->close();
            ++i;
        } else {
            conn->close();
            CLIENT_WARNING("select db when fetch connection fail, logic_db_name:%s, service:%s",
                    logic_db_name.c_str(), _name.c_str());
            return SmartConnection();
        }
    } while (i < fetch_conn_times);
    CLIENT_WARNING("instance faulty when fetch conneciton");
    return SmartConnection();
}

int Service::_get_shard_id(uint32_t partition_key, int* shard_id) {
    ShardOperatorMgr* mgr = ShardOperatorMgr::get_s_instance();
    uint32_t id = 0;
    int ret = mgr->evaluate(_db_split_function, partition_key, &id);
    if (ret < 0) {
        CLIENT_WARNING("get shard id fail, service_name:%s, partition_key:%d",
                _name.c_str(), partition_key);
        *shard_id = 0;
        return ret;
    }
    *shard_id = id;
    return SUCCESS;
}

#ifdef BAIDU_INTERNAL
int Service::_load_bns_info(comcfg::ConfigUnit& conf_unit) {
    if (!_is_inited) {
        //后续初始化
        return SUCCESS;
    }
    try {
        for (size_t i = 0; i < conf_unit["db_shard"].size(); ++i) {
            int id = conf_unit["db_shard"][i]["id"].to_int32();
            if (_id_bns_map.count(id) != 0) {
                CLIENT_WARNING("bns configuration repeat, bns_id: %d", id);
                return CONFPARAM_ERROR;
            }
            ConnectionConf conn_conf_tmp = _conn_conf;
            if (conf_unit["db_shard"][i]["read_timeout"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
                conn_conf_tmp.read_timeout = conf_unit["db_shard"][i]["read_timeout"].to_int32();
            }
            if (conf_unit["db_shard"][i]["write_timeout"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
                conn_conf_tmp.write_timeout = conf_unit["db_shard"][i]["write_timeout"].to_int32();
            }
            if (conf_unit["db_shard"][i]["connect_timeout"].selfType() 
                    != comcfg::CONFIG_ERROR_TYPE) {
                conn_conf_tmp.connect_timeout = 
                    conf_unit["db_shard"][i]["connect_timeout"].to_int32();
            }
            if (conf_unit["db_shard"][i]["charset"].selfType() 
                    != comcfg::CONFIG_ERROR_TYPE) {
                conn_conf_tmp.charset = conf_unit["db_shard"][i]["charset"].to_cstr();
            }
            if (conf_unit["db_shard"][i]["username"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
                conn_conf_tmp.username = conf_unit["db_shard"][i]["username"].to_cstr();
            }
            if (conf_unit["db_shard"][i]["password"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
                conn_conf_tmp.password = conf_unit["db_shard"][i]["password"].to_cstr();
            }
            std::string tag;
            if (conf_unit["db_shard"][i]["tag"].selfType() != comcfg::CONFIG_ERROR_TYPE) {
                tag = conf_unit["db_shard"][i]["tag"].to_cstr();
            }
            if (conf_unit["db_shard"][i]["bns_name"].selfType() != 
                    comcfg::CONFIG_ERROR_TYPE) {
                string bns_name(conf_unit["db_shard"][i]["bns_name"].to_cstr());
                BnsConnectionPool* pool = new(std::nothrow) BnsConnectionPool(
                        bns_name,
                        tag,
                        _conn_num_per_bns,
                        _max_conn_per_instance,
                        id,
                        conn_conf_tmp,
                        //_conn_conf,
                        _name_logic_db_map.begin(),
                        _name_logic_db_map.end(),
                        _bns_infos[bns_name],
                        _comment_format,
                        _select_algo);
                if (pool == NULL) {
                    CLIENT_WARNING("new pool fail, pool name: %s", bns_name.c_str());
                    return NEWOBJECT_ERROR;
                }
                _id_bns_map[id] = pool;
                int ret = pool->init(_connect_all, NULL);
                if (ret < 0) {
                    CLIENT_WARNING("pool init failed, pool name: %s", bns_name.c_str());
                    return ret;
                }
                CLIENT_DEBUG("bns pool connection successfully, pool name: %s, pool id:%d",
                        bns_name.c_str(), id);
            } else {
                string ip_list(conf_unit["db_shard"][i]["ip_list"].to_cstr());
                std::vector<std::string> instances_str;
                vector<InstanceInfo> instances;
                boost::split(instances_str, ip_list, boost::is_any_of(","));
                for (uint32_t j = 0; j < instances_str.size(); j++) {
                    InstanceInfo info;
                    info.id = instances_str[j];
                    std::vector<std::string> items;
                    boost::split(items, info.id, boost::is_any_of(":"));
                    info.ip = items[0];
                    info.port = atoi(items[1].c_str());
                    info.enable = true;
                    info.status = 0;
                    info.is_available = true;
                    instances.push_back(info);
                }
                BnsConnectionPool* pool = new(std::nothrow) BnsConnectionPool(
                        "ip_list",
                        "",
                        _conn_num_per_bns,
                        _max_conn_per_instance,
                        id,
                        conn_conf_tmp,
                        _name_logic_db_map.begin(),
                        _name_logic_db_map.end(),
                        NULL,
                        _comment_format,
                        _select_algo);
                if (pool == NULL) {
                    CLIENT_WARNING("new pool fail, pool name: ip_list");
                    return NEWOBJECT_ERROR;
                }
                _id_bns_map[id] = pool;
                int ret = pool->init(_connect_all, &instances);
                if (ret < 0) {
                    CLIENT_WARNING("pool init failed, pool name: ip_list");
                    return ret;
                }
                CLIENT_DEBUG("bns pool connection successfully, pool name: ip_list, pool id:%d", id);
            }
        }
    }
    catch (comcfg::ConfigException& e) {
        CLIENT_WARNING("configuration file format is not right, exception:%s", e.what());
        return CONFPARAM_ERROR;
    }
    CLIENT_DEBUG("load bns info successfully, bns size: %d", conf_unit["db_shard"].size());
    return SUCCESS;
}

int Service::_load_logic_db_info(comcfg::ConfigUnit& conf_unit) {
    try {
        for (size_t i = 0; i < conf_unit["logic_db"].size(); ++i) {
            string logic_db_name(conf_unit["logic_db"][i]["name"].to_cstr());     
            if (_name_logic_db_map.count(logic_db_name) != 0) { 
                CLIENT_WARNING("logic db configuration repeat, logic_name:%s",
                             logic_db_name.c_str()); 
                return CONFPARAM_ERROR; 
            }  
            LogicDB* logic_db = new(std::nothrow) LogicDB(logic_db_name); 
            if (logic_db == NULL) { 
                CLIENT_WARNING("new logic db fail, db name:%s", logic_db_name.c_str()); 
                return NEWOBJECT_ERROR;  
            }
            _name_logic_db_map[logic_db_name] = logic_db;
            int ret = _load_table_split_info(conf_unit, i, logic_db);
            if (ret < 0) {
                CLIENT_WARNING("load table split info fail, logic_name:%s", logic_db_name.c_str());
                return ret;    
            }
        } 
    }
    catch (comcfg::ConfigException& e) {
        CLIENT_WARNING("configuration file format is not right, exception:%s", e.what());
        return CONFPARAM_ERROR;
    }
    CLIENT_DEBUG("load logic db info successfully, logic size:%d",
                conf_unit["logic_db"].size());
    map<string, LogicDB*>::iterator iter = _name_logic_db_map.begin();
    for (; iter != _name_logic_db_map.end(); ++iter) {
        CLIENT_DEBUG("logic db name:%s", iter->first.c_str());
        vector<TableSplit> table_infos = iter->second->get_table_infos();
        for (size_t k = 0; k < table_infos.size(); ++k) {
            CLIENT_DEBUG("%d, table_name:%s, table_count:%d",
                        k, table_infos[k].table_name.c_str(), table_infos[k].table_count);
        }
    }
    return SUCCESS;
}

int Service::_load_table_split_info(
        comcfg::ConfigUnit& conf_unit,
        int i,
        LogicDB* logic_db) { 
    size_t j = 0;
    try { 
        for (; j < conf_unit["logic_db"][i]["table_split"].size(); ++j) {
            string table_name(conf_unit["logic_db"][i]["table_split"][j]["name"].to_cstr());
            comcfg::ConfigUnit& conf_unit_tmp = conf_unit["logic_db"][i]["table_split"][j]; 
            int table_split_count(conf_unit_tmp["sub_tables"].to_int32());
            // 只有分表数量大于1时才需要加载分表公式
            if (table_split_count > 1) {
                vector<string> table_split_function;
                string split_string(conf_unit_tmp["table_split_function"].to_cstr());
                ShardOperatorMgr* mgr = ShardOperatorMgr::get_s_instance();
                int ret = mgr->split(split_string, table_split_function);
                if (ret < 0) {
                    CLIENT_WARNING("table split function split fail, table_name:%s",
                                 table_name.c_str());
                    return ret;
                }
                ret = logic_db->add_table_split(table_name,
                                                table_split_function,
                                                table_split_count);
                if (ret < 0) {
                    CLIENT_WARNING("Add table split fail, table_name:[%s]", table_name.c_str());     
                    return CONFPARAM_ERROR;
                }
                CLIENT_DEBUG("logic db add table_split successfully, table_name:%s, sub_tables:%d",
                            table_name.c_str(), table_split_count);
            } 
        }
    }
    catch (comcfg::ConfigException& e) {
        CLIENT_WARNING("configuration file format is not right, exception:%s", e.what());
        return CONFPARAM_ERROR;
    }
    CLIENT_DEBUG("load table split info successfully, table_split size:%d", j);
    return SUCCESS;
}
#else

int Service::_load_bns_info(const std::vector<DbShardOption>& options) {
    if (!_is_inited) {
        //后续初始化
        return SUCCESS;
    }

    for (auto& option : options) {
        int id = option.id;
        if (_id_bns_map.count(id) != 0) {
            CLIENT_WARNING("bns configuration repeat, bns_id: %d", id);
            return CONFPARAM_ERROR;
        }
        ConnectionConf conn_conf_tmp = _conn_conf;
        conn_conf_tmp.read_timeout = option.read_timeout;
        conn_conf_tmp.write_timeout = option.write_timeout;
        conn_conf_tmp.connect_timeout = option.connect_timeout;
        conn_conf_tmp.charset = option.charset;
        conn_conf_tmp.username = option.username;
        conn_conf_tmp.password = option.password;

        const string& ip_list = option.ip_list;
        std::vector<std::string> instances_str;
        vector<InstanceInfo> instances;
        boost::split(instances_str, ip_list, boost::is_any_of(","));
        for (uint32_t j = 0; j < instances_str.size(); j++) {
            InstanceInfo info;
            info.id = instances_str[j];
            std::vector<std::string> items;
            boost::split(items, info.id, boost::is_any_of(":"));
            info.ip = items[0];
            info.port = atoi(items[1].c_str());
            info.enable = true;
            info.status = 0;
            info.is_available = true;
            instances.push_back(info);
        }
        BnsConnectionPool* pool = new(std::nothrow) BnsConnectionPool(
                "ip_list",
                "",
                _conn_num_per_bns,
                _max_conn_per_instance,
                id,
                conn_conf_tmp,
                _name_logic_db_map.begin(),
                _name_logic_db_map.end(),
                NULL,
                _comment_format,
                _select_algo);
        if (pool == NULL) {
            CLIENT_WARNING("new pool fail, pool name: ip_list");
            return NEWOBJECT_ERROR;
        }
        _id_bns_map[id] = pool;
        int ret = pool->init(_connect_all, &instances);
        if (ret < 0) {
            CLIENT_WARNING("pool init failed, pool name: ip_list");
            return ret;
        }
        CLIENT_DEBUG("bns pool connection successfully, pool name: ip_list, pool id:%d", id);

    }

    CLIENT_DEBUG("load bns info successfully, bns size: %d", conf_unit["db_shard"].size());
    return SUCCESS;
}

int Service::_load_logic_db_info(const std::vector<LogicDbOption>& options) {
    for (auto& option : options) {
        const string& logic_db_name = option.name;
        if (_name_logic_db_map.count(logic_db_name) != 0) {
            CLIENT_WARNING("logic db configuration repeat, logic_name:%s",
                    logic_db_name.c_str());
            return CONFPARAM_ERROR;
        }
        LogicDB* logic_db = new(std::nothrow) LogicDB(logic_db_name);
        if (logic_db == NULL) {
            CLIENT_WARNING("new logic db fail, db name:%s", logic_db_name.c_str());
            return NEWOBJECT_ERROR;
        }
        _name_logic_db_map[logic_db_name] = logic_db;
        int ret = _load_table_split_info(option.table_splits, logic_db);
        if (ret < 0) {
            CLIENT_WARNING("load table split info fail, logic_name:%s", logic_db_name.c_str());
            return ret;
        }
    }

    map<string, LogicDB*>::iterator iter = _name_logic_db_map.begin();
    for (; iter != _name_logic_db_map.end(); ++iter) {
        CLIENT_DEBUG("logic db name:%s", iter->first.c_str());
        vector<TableSplit> table_infos = iter->second->get_table_infos();
        for (size_t k = 0; k < table_infos.size(); ++k) {
            CLIENT_DEBUG("%d, table_name:%s, table_count:%d",
                    k, table_infos[k].table_name.c_str(), table_infos[k].table_count);
        }
    }
    return SUCCESS;
}

int Service::_load_table_split_info(const std::vector<TableSplitOption>& options,
        LogicDB* logic_db) {
    for (auto& option : options) {
        const std::string& table_name = option.name;
        int table_split_count = option.sub_tables;
        // 只有分表数量大于1时才需要加载分表公式
        if (table_split_count > 1) {
            vector<string> table_split_function;
            ShardOperatorMgr* mgr = ShardOperatorMgr::get_s_instance();
            int ret = mgr->split(option.table_split_function, table_split_function);
            if (ret < 0) {
                CLIENT_WARNING("table split function split fail, table_name:%s",
                        table_name.c_str());
                return ret;
            }
            ret = logic_db->add_table_split(table_name,
                    table_split_function,
                    table_split_count);
            if (ret < 0) {
                CLIENT_WARNING("Add table split fail, table_name:[%s]", table_name.c_str());
                return CONFPARAM_ERROR;
            }
            CLIENT_DEBUG("logic db add table_split successfully, table_name:%s, sub_tables:%d",
                    table_name.c_str(), table_split_count);
        }
    }
    return SUCCESS;
}

#endif

void Service::_clear() {
    map<int, BnsConnectionPool*>::iterator iter1 = _id_bns_map.begin();
    for (; iter1 != _id_bns_map.end(); ++iter1) {
        delete iter1->second;
        iter1->second = NULL;
    }
    map<string, LogicDB*>::iterator iter2 = _name_logic_db_map.begin();
    for (; iter2 != _name_logic_db_map.end(); ++iter2) {
        delete iter2->second;
        iter2->second = NULL;
    }
}
}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
