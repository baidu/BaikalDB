#include "mysql_interact.h"
#include "common.h"
#include <boost/algorithm/string.hpp>

namespace baikaldb {

DEFINE_int32(mysql_retry_times, 1, "mysql query retry times");
DEFINE_int32(mysql_connect_timeout_s, 10, "mysql connect timeout");
DEFINE_int32(mysql_read_timeout_s, 60, "mysql read timeout");
DEFINE_int32(mysql_write_timeout_s, 60, "mysql write timeout");
DEFINE_int32(mysql_no_permission_wait_s, 60, "mysql wait time for data source no permission when connecting ");

int MysqlInteract::query(const std::string& sql, baikal::client::ResultSet* result, bool store) {
    int ret = -1;
    int retry_times = 0;
    if (_mysql_conn == nullptr) {
        _mysql_conn = fetch_connection();
    }
    do {
        // 重试时，重新获取连接
        if (retry_times > 0 && ret != 0) { 
            bthread_usleep(1000000);
            _mysql_conn = fetch_connection();
        }
        if (_mysql_conn == nullptr) {
            continue;
        }
        ret = _mysql_conn->execute(sql, store, result);
    } while (++retry_times < FLAGS_mysql_retry_times && ret != 0);
    if (ret != 0) {
        int error_code = -1;
        std::string error_des;
        if (_mysql_conn != nullptr) {
            _mysql_conn->get_error_code(&error_code);
            error_des = _mysql_conn->get_error_des();
        }
        DB_WARNING("Fail to query, ret: %d, error_code: %d, error_des: %s, mysql_info: %s, sql: %s", 
                    ret, error_code, error_des.c_str(), _mysql_info.ShortDebugString().c_str(), sql.c_str());
        return -1;
    }
    return 0;
}

int MysqlInteract::get_error_code() {
    if (_mysql_conn == nullptr) {
        DB_WARNING("_mysql_conn is nullptr");
        return -1;
    }
    int error_code = 0;
    _mysql_conn->get_error_code(&error_code);
    return error_code;
}

baikal::client::MysqlShortConnection* MysqlInteract::get_connection() {
    if (_mysql_conn == nullptr) {
        _mysql_conn = fetch_connection();
    }
    return _mysql_conn.get();
}

std::string MysqlInteract::mysql_escape_string(baikal::client::MysqlShortConnection* conn, const std::string& value) {
    char* str = new char[value.size() * 2 + 1];
    std::string escape_value;
    if (conn != nullptr) {
        MYSQL* RES = conn->get_mysql_handle();
#ifdef BAIDU_INTERNAL
        mysql_real_escape_string(RES, str, value.c_str(), value.size());
#endif
        escape_value = str;
    } else {
        delete[] str;
        return boost::replace_all_copy(value, "\"", "\\\"");
    }   
    delete[] str;
    return escape_value;
}

std::unique_ptr<baikal::client::MysqlShortConnection> MysqlInteract::fetch_connection() {
    std::unique_ptr<baikal::client::MysqlShortConnection> conn(new baikal::client::MysqlShortConnection());
    if (conn == nullptr) {
        DB_WARNING("Fail to new MysqlShortConnection");
        return nullptr;
    }
#ifdef BAIDU_INTERNAL
    baikal::client::ConnectionConf conn_conf;
    conn_conf.username = _mysql_info.username();
    conn_conf.password = _mysql_info.password();
    conn_conf.charset = _mysql_info.charset();
    conn_conf.connect_timeout = FLAGS_mysql_connect_timeout_s;
    conn_conf.read_timeout = FLAGS_mysql_read_timeout_s;
    conn_conf.write_timeout = FLAGS_mysql_write_timeout_s;
    conn_conf.no_permission_wait_s = FLAGS_mysql_no_permission_wait_s;

    std::string addr = _mysql_info.addr();
    boost::trim(addr);
    std::vector<std::string> split_vec;
    boost::split(split_vec, addr, boost::is_any_of(":"));
    if (split_vec.size() == 0) {
        DB_WARNING("mysql addr is invalid");
        return nullptr;
    }
    int ret = 0;
    if (is_valid_ip(split_vec[0])) {
        // ip格式
        if (split_vec.size() != 2) {
            DB_WARNING("Invalid ip_port: %s", addr.c_str());
            return nullptr;
        }
        const std::string& ip = split_vec[0];
        int port = -1;
        try {
            port = std::stoi(split_vec[1].c_str());
        } catch (...) {
            DB_WARNING("Invalid port: %s", split_vec[1].c_str());
            return nullptr;
        }
        ret = conn->connect(ip, port, conn_conf);
    } else {
        // bns格式
        ret = conn->connect(addr, conn_conf);
    }
    if (ret != 0) {
        return nullptr;
    }
#endif
    return conn;
}

} // namespace baikaldb
