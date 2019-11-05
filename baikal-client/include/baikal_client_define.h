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
 * @file baikal_client_define.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/04 10:35:00
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_DEFINE_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_DEFINE_H

#undef CR
#include <boost/thread/mutex.hpp>
#include <vector>
#include <string>
#include <map>
#include "bthread/butex.h"

#ifdef BAIDU_INTERNAL
#include "bthread.h"
#include <com_log.h>
namespace butil = base;
#else
#include "bthread/bthread.h"
#include "butil/logging.h"
#endif


namespace baikal {
namespace client {

//从bns服务同步得到的每个实例信息
struct InstanceInfo {
    std::string id; // id:port
    std::string ip;
    std::string tag;
    int port;
    bool enable; //干预状态
    int status; //非干预状态
    bool is_available; //干预状态、非干预状态同是为0时，is_avalibale才有效，为true
};

//每个bns信息
struct BnsInfo {
    std::map<std::string, InstanceInfo> instance_infos; //key为ip:port
    std::string bns_name;
    boost::mutex mutex_lock;//对instance_infos的修改操作需要加互斥锁
};

enum ConnectionType {
    NONE = 0,
    MYSQL_CONN = 1
};
//选择实例算法， 目前支持随机和轮询两种
enum SelectAlgo {
    RANDOM = 1,
    ROLLING = 2,
    LOCAL_AWARE = 3
};
//从配置文件读取的连接的参数信息
struct ConnectionConf {
    ConnectionType conn_type;
    int read_timeout;
    int write_timeout;
    int connect_timeout;
    std::string username;
    std::string password;
    std::string charset;
    int no_permission_wait_s;
    bool faulty_exit;
    bool async;
};

struct TableSplit {
    std::string table_name;
    int table_count; //分表的数量
    std::vector<std::string> table_split_function;
    TableSplit(const std::string& name, int count, const std::vector<std::string>& function) :
            table_name(name),
            table_count(count),
            table_split_function(function) {}
};

enum InstanceStatus {
    ON_LINE = 1,
    OFF_LINE = 2,
    FAULTY = 3,
    DELAY = 4
};

enum ConnectionStatus {
    IS_NOT_CONNECTED = 1,
    IS_NOT_USED = 2,
    IS_USING = 3,
    IS_CHECKING = 4,
    IS_BAD      = 5
};

//数据类型
enum ValueType {
    VT_INT32 = 1,
    VT_UINT32 = 2,
    VT_INT64 = 3,
    VT_UINT64 = 4,
    VT_FLOAT = 5,
    VT_DOUBLE = 6,
    VT_STRING = 7,
    ERROR_TYPE = -1
};
/**
 * 系统错误码
 * */
enum ErrorCode {
        SUCCESS = 0,
        INPUTPARAM_ERROR = -1,
        FILESYSTEM_ERROR = -2,
        CONFPARAM_ERROR = -3,
        NEWOBJECT_ERROR = -4,
        GETSERVICE_ERROR = -5,
        COMPUTE_ERROR = -6, // 分库分表号计算失败
        SERVICE_NOT_INIT_ERROR = -7,
        INSTANCE_NOT_ENOUGH = -8,
        INSTANCE_ALREADY_DELETED = -9,
        GET_VALUE_ERROR = -10,
        VALUE_IS_NULL = -11,
        FETCH_CONNECT_FAIL = -12,
        GET_SERVICE_FAIL = -13,
        EXECUTE_FAIL = -14,
        SERVICE_INIT2_ERROR = -15,
        //bns syn eror -100~-200
        BNS_ERROR = -101,
        //BNS_INSTANCE_REPEAT = -102,
        BNS_NO_ONLINE_INSTANCE = -102,
        BNS_GET_INFO_ERROR = -103,
        //connection error -200~-300
        CONNECTION_CONNECT_FAIL = -201,
        CONNECTION_NOT_INIT = -202,
        CONNECTION_PING_FAIL = -203,
        CONNECTION_HANDLE_NULL = -204,
        INSTANCE_FAULTY_ERROR = -205,
        CONNECTION_ALREADY_DELETED = -206,
        NO_PARTITION_KEY_ERROR = -207,
        CONNECTION_QUERY_FAIL = -208,
        CONNECTION_RECONN_SUCCESS = -209,
        CONNECTION_IS_KILLED = -210,
        //thread error -300~-400
        THREAD_START_ERROR = -301,
        THREAD_START_REPEAT = -302,
        THREAD_FINAL_ERROR = -303
};

class BthreadCond {
public:
    BthreadCond() {
        bthread_cond_init(&_cond, NULL);
        bthread_mutex_init(&_mutex, NULL);
        _count = 1;
    }
    ~BthreadCond() {
        bthread_mutex_destroy(&_mutex);
        bthread_cond_destroy(&_cond);
    }

    void init(int count = 1) {
        _count = count;
    }

    void reset() {
        _count = 1;
        bthread_mutex_destroy(&_mutex);
        bthread_cond_destroy(&_cond);

        bthread_cond_init(&_cond, NULL);
        bthread_mutex_init(&_mutex, NULL);
    }

    int signal() {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        _count--;
        bthread_cond_signal(&_cond);
        bthread_mutex_unlock(&_mutex);
        return ret;
    }

    int wait() {
        int ret = 0;
        bthread_mutex_lock(&_mutex);
        while (_count > 0) {
            ret = bthread_cond_wait(&_cond, &_mutex);
            //CWARNING_LOG("bthread signal wait return: %d", ret);
        }
        bthread_mutex_unlock(&_mutex);
        return ret;
    }
private:
    int _count;
    bthread_cond_t _cond;
    bthread_mutex_t _mutex;
};

class TimeCost {
public:
    TimeCost() {
        gettimeofday(&_start, NULL);
    }
    ~TimeCost() {}
    void reset() {
        gettimeofday(&_start, NULL);
    }
    int64_t get_time() {
        gettimeofday(&_end, NULL);
        return (_end.tv_sec - _start.tv_sec) * 1000000
            + (_end.tv_usec-_start.tv_usec); //macro second
    }

private:
    timeval _start;
    timeval _end;
};

struct MysqlEventInfo {
    BthreadCond     cond;
    TimeCost        cost;
    int             event_in;
    int             event_out;
    int             timeout;
};

#ifndef BAIDU_INTERNAL
#define CLIENT_DEBUG(_fmt_, args...)
#else
#define CLIENT_DEBUG(_fmt_, args...) \
    do {\
        com_writelog("DEBUG", "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    }while (0);
#endif

#ifndef BAIDU_INTERNAL
#define CLIENT_TRACE(_fmt_, args...) \
    do {\
        char buf[2048]; \
        snprintf(buf, sizeof(buf) - 1, "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
        LOG(INFO) << buf; \
    }while (0);
#else
#define CLIENT_TRACE(_fmt_, args...) \
    do {\
        com_writelog("TRACE", "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    }while (0);
#endif

#ifndef BAIDU_INTERNAL
#define CLIENT_NOTICE(_fmt_, args...) \
    do {\
        char buf[2048]; \
        snprintf(buf, sizeof(buf) - 1, "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
        LOG(INFO) << buf; \
    }while (0);
#else
#define CLIENT_NOTICE(_fmt_, args...) \
    do {\
        com_writelog("NOTICE", "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    }while (0);
#endif

#ifndef BAIDU_INTERNAL
#define CLIENT_WARNING(_fmt_, args...) \
    do {\
        char buf[2048]; \
        snprintf(buf, sizeof(buf) - 1, "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
        LOG(WARNING) << buf; \
    }while (0);
#else
#define CLIENT_WARNING(_fmt_, args...) \
    do {\
        com_writelog("WARNING", "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    }while (0);
#endif

#ifndef BAIDU_INTERNAL
#define CLIENT_FATAL(_fmt_, args...) \
    do {\
        char buf[2048]; \
        snprintf(buf, sizeof(buf) - 1, "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
        LOG(ERROR) << buf; \
    }while (0);
#else
#define CLIENT_FATAL(_fmt_, args...) \
    do {\
        com_writelog("FATAL", "[%s][%d][%s][%" PRIu64 "]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    }while (0);
#endif

}
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_DEFINE_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
