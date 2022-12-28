// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#include "network_server.h"
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#else
#include <brpc/channel.h>
#endif
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <unistd.h>
#include "log.h"
#include <gflags/gflags.h>
#include <time.h>

namespace bthread {
DECLARE_int32(bthread_concurrency); //bthread.cpp
}

namespace baikaldb {

DEFINE_int32(backlog, 1024, "Size of waitting queue in listen()");
DEFINE_int32(baikal_port, 28282, "Server port");
DEFINE_int32(epoll_timeout, 2000, "Epoll wait timeout in epoll_wait().");
DEFINE_int32(check_interval, 10, "interval for conn idle timeout");
DEFINE_int32(connect_idle_timeout_s, 1800, "connection idle timeout threshold (second)");
DEFINE_int32(slow_query_timeout_s, 60, "slow query threshold (second)");
DEFINE_int32(print_agg_sql_interval_s, 10, "print_agg_sql_interval_s");
DEFINE_int32(backup_pv_threshold, 50, "backup_pv_threshold");
DEFINE_double(backup_error_percent, 0.5, "use backup table if backup_error_percent > 0.5");
DEFINE_int64(health_check_interval_us, 10 * 1000 * 1000, "health_check_interval_us");
DECLARE_bool(need_health_check);
DEFINE_int64(health_check_store_timeout_ms, 2000, "health_check_store_timeout_ms");
DEFINE_bool(fetch_instance_id, false, "fetch baikaldb instace id, used for generate transaction id");
DEFINE_string(hostname, "HOSTNAME", "matrix instance name");
DEFINE_bool(insert_agg_sql, false, "whether insert agg_sql");
DEFINE_int32(batch_insert_agg_sql_size, 50, "batch size for insert");
DEFINE_int32(batch_insert_sign_sql_interval_us, 10 * 60 * 1000 * 1000, "batch_insert_sign_sql_interval_us default 10min");
DEFINE_bool(enable_tcp_keep_alive, false, "enable tcp keepalive flag");
DECLARE_int32(baikal_heartbeat_interval_us);
DEFINE_bool(open_to_collect_slow_query_infos, false, "open to collect slow_query_infos, default: false");
DEFINE_int32(limit_slow_sql_size, 50, "each sign to slow query sql counts, default: 50");
DEFINE_int32(slow_query_batch_size, 100, "slow query sql batch size, default: 100");

static const std::string instance_table_name = "INTERNAL.baikaldb.__baikaldb_instance";

void NetworkServer::report_heart_beat() {
    while (!_shutdown) {
        TimeCost cost;
        pb::BaikalHeartBeatRequest request;
        pb::BaikalHeartBeatResponse response;
        _heart_beat_count << 1;
        //1、construct heartbeat request
        BaikalHeartBeat::construct_heart_beat_request(request);
        request.set_physical_room(_physical_room);
        int64_t construct_req_cost = cost.get_time();
        cost.reset();
        //2、send heartbeat request to meta server
        if (MetaServerInteract::get_instance()->send_request("baikal_heartbeat", request, response) == 0) {
            //处理心跳
            BaikalHeartBeat::process_heart_beat_response(response);
            DB_WARNING("report_heart_beat, construct_req_cost:%ld, process_res_cost:%ld",
                    construct_req_cost, cost.get_time());
        } else {
            DB_WARNING("send heart beat request to meta server fail");
        }

        if (MetaServerInteract::get_backup_instance()->is_inited()) {
            request.Clear();
            response.Clear();
            //1、construct heartbeat request
            BaikalHeartBeat::construct_heart_beat_request(request, true);
            int64_t construct_req_cost = cost.get_time();
            cost.reset();
            //2、send heartbeat request to meta server
            if (MetaServerInteract::get_backup_instance()->send_request("baikal_heartbeat", request, response) == 0) {
                //处理心跳
                BaikalHeartBeat::process_heart_beat_response(response, true);
                DB_WARNING("report_heart_beat, construct_req_cost:%ld, process_res_cost:%ld",
                        construct_req_cost, cost.get_time());
            } else {
                DB_WARNING("send heart beat request to meta server fail");
            }
        }

        _heart_beat_count << -1;
        bthread_usleep_fast_shutdown(FLAGS_baikal_heartbeat_interval_us, _shutdown);
    }
}

void NetworkServer::report_other_heart_beat() {
    while (!_shutdown) {
        TimeCost cost;
        pb::BaikalOtherHeartBeatRequest request;
        pb::BaikalOtherHeartBeatResponse response;
        //1、construct heartbeat request
        construct_other_heart_beat_request(request);
        int64_t construct_req_cost = cost.get_time();
        cost.reset();
        //2、send heartbeat request to meta server
        if (MetaServerInteract::get_instance()->send_request("baikal_other_heartbeat", request, response) == 0) {
            //处理心跳
            process_other_heart_beat_response(response);
            DB_WARNING("report_heart_beat, construct_req_cost:%ld, process_res_cost:%ld",
                    construct_req_cost, cost.get_time());
        } else {
            DB_WARNING("send heart beat request to meta server fail");
        }
        bthread_usleep_fast_shutdown(FLAGS_baikal_heartbeat_interval_us, _shutdown);
    }
}

void NetworkServer::get_field_distinct_cnt(int64_t table_id, std::set<int> fields, std::map<int64_t, int>& distinct_field_map) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (fields.size() <= 0) {
        return;
    }
    for (auto field_id : fields) {
        int64_t distinct_cnt = factory->get_histogram_distinct_cnt(table_id, field_id);
        if (distinct_cnt < 0) {
            continue;
        }
        while (true) {
            auto iter = distinct_field_map.find(distinct_cnt);
            if (iter != distinct_field_map.end()) {
                // 有重复, ++ 避免重复
                distinct_cnt++;
                continue;
            }
            distinct_field_map[distinct_cnt] = field_id;
            break;
        }
    }
}

void NetworkServer::fill_field_info(int64_t table_id, std::map<int64_t, int>& distinct_field_map, std::string type, std::ostringstream& os) {
    if (distinct_field_map.size() <= 0) {
        return;
    }
    SchemaFactory* factory = SchemaFactory::get_instance();
    auto table_ptr = factory->get_table_info_ptr(table_id);
    for (auto iter = distinct_field_map.rbegin(); iter != distinct_field_map.rend(); iter++) {
        auto field_ptr = table_ptr->get_field_ptr(iter->second);
        os << field_ptr->short_name << ":" << type << ":" << iter->first << " ";
    }
}

// 推荐索引
void NetworkServer::index_recommend(const std::string& sample_sql, int64_t table_id, int64_t index_id, std::string& index_info, std::string& desc) {
    BvarMap sample = StateMachine::get_instance()->index_recommend_st.get_value();
    SchemaFactory* factory = SchemaFactory::get_instance();
    
    // 没有统计信息无法推荐索引
    auto st_ptr = factory->get_statistics_ptr(table_id);
    if (st_ptr == nullptr) {
        desc = "no statistics info";
        return;
    }

    auto iter = sample.internal_map.find(sample_sql);
    if (iter == sample.internal_map.end()) {
        return;
    }

    auto sum_iter = iter->second.find(index_id);
    if (sum_iter == iter->second.end()) {
        return;
    }

    // 没有条件不推荐索引
    auto field_range_type = sum_iter->second.field_range_type;
    if (field_range_type.size() <= 0) {
        desc = "no condition";
        return;
    }

    std::set<int> eq_field;
    std::set<int> in_field;
    std::set<int> range_field;

    for (auto pair : field_range_type) {
        if (pair.second == range::EQ) {
            eq_field.insert(pair.first);
        } else if (pair.second == range::IN) {
            in_field.insert(pair.first);
        } else if (pair.second == range::RANGE) {
            range_field.insert(pair.first);
        } else {
            // 非 RANGE EQ IN 条件暂时无法推荐索引
            desc = "not only range eq in";
            return;
        }
    }

    std::map<int64_t, int> eq_distinct_field_map;
    std::map<int64_t, int> in_distinct_field_map;
    std::map<int64_t, int> range_distinct_field_map;
    get_field_distinct_cnt(table_id, eq_field, eq_distinct_field_map);
    get_field_distinct_cnt(table_id, in_field, in_distinct_field_map);
    get_field_distinct_cnt(table_id, range_field, range_distinct_field_map);
    std::ostringstream os;
    fill_field_info(table_id, eq_distinct_field_map, "EQ", os);
    fill_field_info(table_id, in_distinct_field_map, "IN", os);
    fill_field_info(table_id, range_distinct_field_map, "RANGE", os);
    desc = os.str();

    // 平均过滤行数小于100不用推荐索引
    if (sum_iter->second.count == 0 || (sum_iter->second.filter_rows / sum_iter->second.count) < 100) {
        desc = "filter rows < 100";
        return;
    }

    // 过滤率小于10%不推荐索引
    if (sum_iter->second.scan_rows == 0 || sum_iter->second.filter_rows * 1.0 / sum_iter->second.scan_rows < 0.1) {
        desc = "filter ratio < 0.1";
        return;
    }

    std::ostringstream recommend_index;
    auto table_ptr = factory->get_table_info_ptr(table_id);
    for (auto iter = eq_distinct_field_map.rbegin(); iter != eq_distinct_field_map.rend(); iter++) {
        auto field_ptr = table_ptr->get_field_ptr(iter->second);
        recommend_index << field_ptr->short_name << ",";
    }

    bool in_pre = false;
    bool finish = false;
    for (auto iter = in_distinct_field_map.rbegin(); iter != in_distinct_field_map.rend(); iter++) {
        auto field_ptr = table_ptr->get_field_ptr(iter->second);
        if (in_pre) {
            if (range_distinct_field_map.size() <= 0) {
                recommend_index << field_ptr->short_name;
            } else {
                auto range_iter = range_distinct_field_map.rbegin();
                if (range_iter->first > iter->first) {
                    auto range_field_ptr = table_ptr->get_field_ptr(range_iter->second);
                    recommend_index << range_field_ptr->short_name;
                } else {
                    recommend_index << field_ptr->short_name;
                }
            }
            finish = true;
            break;
        }
        recommend_index << field_ptr->short_name << ",";
        in_pre = true;
    }

    if (finish) {
        index_info = recommend_index.str();
        return;
    }

    if (range_distinct_field_map.size() > 0) {
        auto range_iter = range_distinct_field_map.rbegin();
        auto range_field_ptr = table_ptr->get_field_ptr(range_iter->second);
        recommend_index << range_field_ptr->short_name;
    }

    index_info = recommend_index.str();
}

int NetworkServer::insert_agg_sql(const std::string& values) {
    baikal::client::ResultSet result_set;
    TimeCost cost;
    // 构造sql
    std::string sql = "REPLACE INTO BaikalStat.baikaldb_trace_info(`time`,`date`,`hour`,`min`,"
                      "`sum`,`count`,`avg`,`affected_rows`,`scan_rows`,`filter_rows`,`sign`, "
                      "`hostname`,`index_name`,`family`,`tbl`,`resource_tag`,`op_type`,`plat`, "
                      "`op_version`,`op_desc`,`err_count`, `region_count`, `learner_read`) VALUES ";
    sql += values;
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 20);

    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
        return -1;
    }
    DB_NOTICE("affected_rows:%lu, cost:%ld, sql_len:%lu, sql:%s",
              result_set.get_affected_rows(), cost.get_time(), sql.size(), sql.c_str());
    return 0;
}

int NetworkServer::insert_agg_sql_by_sign(std::map<uint64_t, std::string>& sign_sql_map, 
    std::set<std::string>& family_tbl_tag_set,
    std::set<uint64_t>& sign_to_counts) {
    std::string values;
    int values_size = 0;
    for (const auto& it : sign_sql_map) {
        values_size++;
        values += it.second + ",";
        if (values_size >= FLAGS_batch_insert_agg_sql_size) {
            values.pop_back();
            int ret  = insert_agg_sql_by_sign(values);
            if (ret < 0) {
                sign_to_counts.clear();
            }
            values_size = 0;
            values.clear();
        } 
    }

    if (values_size > 0) {
        values.pop_back();
        int ret = insert_agg_sql_by_sign(values);
        if (ret < 0) {
            sign_to_counts.clear();
        }
    }

    values_size = 0;
    values.clear();
    for (const auto& f : family_tbl_tag_set) {
        values_size++;
        values += f + ",";
    }

    if (values_size > 0) {
        values.pop_back();
        int ret = insert_family_table_tag(values);
        if (ret < 0) {
            sign_to_counts.clear();
        }
    }

    sign_sql_map.clear();
    family_tbl_tag_set.clear();
    return 0;
}

int NetworkServer::insert_agg_sql_by_sign(const std::string& values) {
    baikal::client::ResultSet result_set;
    TimeCost cost;
    // 构造sql
    std::string sql = "REPLACE INTO BaikalStat.sign_family_table_sql(`sign`,`family`,`tbl`,`resource_tag`,"
                      "`op_type`,`sql`) VALUES ";
    sql += values;
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 20);

    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
        return -1;
    }
    DB_NOTICE("affected_rows:%lu, cost:%ld, sql_len:%lu, sql:%s",
              result_set.get_affected_rows(), cost.get_time(), sql.size(), sql.c_str());
    return 0;
}

int NetworkServer::insert_family_table_tag(const std::string& values) {
    baikal::client::ResultSet result_set;
    TimeCost cost;
    // 构造sql
    std::string sql = "REPLACE INTO BaikalStat.family_table_tag(`family`,`tbl`,`resource_tag`) VALUES ";
    sql += values;
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 20);

    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
        return -1;
    }
    DB_NOTICE("affected_rows:%lu, cost:%ld, sql_len:%lu, sql:%s",
              result_set.get_affected_rows(), cost.get_time(), sql.size(), sql.c_str());
    return 0;
}

void NetworkServer::print_agg_sql() {
    static std::map<uint64_t, std::string> sign_sql_map;
    static std::set<std::string> family_tbl_tag_set;
    static std::map<uint64_t, std::set<std::uint64_t>> parent_sign_to_subquery_signs;
    static std::set<uint64_t> sign_to_counts;
    static std::set<uint64_t> parent_sign_to_counts;
    TimeCost cost;
    TimeCost reset_counter_cost;
    while (!_shutdown) {
        bool need_reset_counter = false;
        if (reset_counter_cost.get_time() > 24 * 3600 * 1000 * 1000LL) {
            need_reset_counter = true;
            reset_counter_cost.reset();
        }
        int64_t last_cost = cost.get_time();
        cost.reset();

        //更新子查询签名信息
        BvarMap sample = StateMachine::get_instance()->sql_agg_cost.reset();
        for (const auto& pair : sample.internal_map) {
            auto& index_id_sumcount_mp = pair.second;
            auto iter_begin = index_id_sumcount_mp.begin();
            if (iter_begin != index_id_sumcount_mp.end()) {
                auto& set_subquery_signs = iter_begin->second.subquery_signs;
                uint64_t parent_sign = iter_begin->second.parent_sign;
                if (set_subquery_signs.size() > 0 && parent_sign_to_counts.count(parent_sign) == 0) {
                    DB_NOTICE("parent_sign is [%lu] and subquery_sign size is [%ld]", parent_sign, set_subquery_signs.size());
                    parent_sign_to_subquery_signs[parent_sign] = set_subquery_signs;
                    parent_sign_to_counts.insert(parent_sign);
                }
            }
        }
        //写入失败直接清除set, 这样可以保证下次还能写入库表
        if (FLAGS_insert_agg_sql) {
            bool is_insert_success = true;
            insert_subquery_signs_info(parent_sign_to_subquery_signs, is_insert_success);
            if (!is_insert_success) {
                parent_sign_to_counts.clear();
            }
        }

        // 更新慢查询信息 到底层库表内
        process_slow_query_map();

        SchemaFactory* factory = SchemaFactory::get_instance();
        time_t timep;
        struct tm tm;
        time(&timep);
        localtime_r(&timep, &tm);

        struct CountErr {
            int64_t count = 0;
            int64_t err = 0;
            CountErr() {
            }
            CountErr(int64_t count, int64_t err) : count(count), err(err) {
            }
            CountErr& operator+=(const CountErr& other) {
                count += other.count;
                err += other.err;
                return *this;
            }
        };

        std::map<int64_t, CountErr> table_count_err;
        int values_size = 0;
        std::string sql_values;
        if (FLAGS_insert_agg_sql) {
            sql_values.reserve(4096);
        }

        for (auto& pair : sample.internal_map) {
            if (pair.first.empty()) {
                continue;
            }
            for (auto& pair2 : pair.second) {
                std::string hostname = FLAGS_hostname;
                if (hostname == "HOSTNAME") {
                    hostname = butil::my_hostname();
                    if (hostname == "") {
                        DB_WARNING("get hostname failed");
                    }
                }

                uint64_t out_sign = pair2.second.parent_sign;
                int64_t version = 0;
                std::string op_description = "-";
                // factory->get_schema_conf_op_info(pair2.second.table_id, version, op_description);
                std::string recommend_index = "-";
                std::string field_desc = "-";
                //index_recommend(pair.first, pair2.second.table_id, pair2.first, recommend_index, field_desc);
                int learner_read = factory->sql_force_learner_read(pair2.second.table_id, out_sign);
                std::shared_ptr<SqlStatistics> sql_info = factory->get_sql_stat(out_sign);
                int64_t dynamic_timeout_ms = -1;
                if (sql_info == nullptr) {
                    sql_info = factory->create_sql_stat(out_sign);
                }
                if (sql_info != nullptr) {
                    // 24小时，把小于SQL_COUNTS_RANGE的sql重置，维持天级定时任务单并发
                    if (need_reset_counter) {
                        if (sql_info->counter < SqlStatistics::SQL_COUNTS_RANGE) {
                            sql_info->counter = 0;
                        }
                    }
                    int64_t last_cost_s = last_cost / 1000 / 1000;
                    if (last_cost_s > 0) {
                        sql_info->qps = pair2.second.count * 1.0 / last_cost_s;
                    }
                    if (pair2.second.count > 0) {
                        sql_info->avg_scan_rows = pair2.second.scan_rows / pair2.second.count;
                    }
                    //只统计正常请求的平响
                    if (pair2.second.count - pair2.second.err_count > 0) {
                        sql_info->latency_us = (pair2.second.sum - pair2.second.err_sum) 
                            / (pair2.second.count - pair2.second.err_count);
                    }
                    dynamic_timeout_ms = sql_info->dynamic_timeout_ms();
                    SQL_TRACE("sign:%lu qps:%f avg_scan_rows:%ld scan_rows_9999:%ld "
                            "latency_us:%ld latency_us_9999:%ld times:%ld dynamic_timeout_ms:%ld",
                            out_sign, sql_info->qps, sql_info->avg_scan_rows, sql_info->scan_rows_9999, sql_info->latency_us,
                            sql_info->latency_us_9999, sql_info->times_avg_and_9999, dynamic_timeout_ms);
                }
                SQL_TRACE("date_hour_min=[%04d-%02d-%02d\t%02d\t%02d] sum_pv_avg_affected_scan_filter_rgcnt_err="
                        "[%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld] sign_hostname_index=[%lu\t%s\t%s] dynamic_timeout_ms:%ld sql_agg: %s "
                        "op_version_desc=[%ld\t%s\t%s\t%s]",
                    1900 + tm.tm_year, 1 + tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min,
                    pair2.second.sum, pair2.second.count,
                    pair2.second.count == 0 ? 0 : pair2.second.sum / pair2.second.count,
                    pair2.second.affected_rows, pair2.second.scan_rows, pair2.second.filter_rows,
                    pair2.second.region_count, pair2.second.err_count,
                    out_sign, hostname.c_str(), factory->get_index_name(pair2.first).c_str(), dynamic_timeout_ms, 
                    pair.first.c_str(), version, op_description.c_str(), recommend_index.c_str(), field_desc.c_str());
                table_count_err[pair2.second.table_id] += CountErr(pair2.second.count, pair2.second.err_count);

                if (FLAGS_insert_agg_sql) {
                    if (values_size >= FLAGS_batch_insert_agg_sql_size) {
                        sql_values.pop_back();
                        if (insert_agg_sql(sql_values) < 0) {
                            DB_WARNING("insert agg_sql: %s failed", sql_values.c_str());
                        }
                        if (insert_agg_sql_by_sign(sign_sql_map, family_tbl_tag_set, sign_to_counts) < 0) {
                            DB_WARNING("insert agg_sql_by_sign: %s failed", sql_values.c_str());
                        }
                        sql_values.clear();
                        values_size = 0;
                    }

                    // pair.first 格式
                    // family_table_tag_optype_plat=[" << stat_info->family << "\t"
                    //             << stat_info->table << "\t" << resource_tag << "\t" << op_type << "\t"
                    //             << FLAGS_log_plat_name << "] sql=[" << ctx->stmt << "]"

                    // 解析 sql_agg，即key
                    std::string sql_agg = pair.first.substr(pair.first.find_first_of('[') + 1);
                    int pos = 0;
                    std::string family = sql_agg.substr(0, sql_agg.find_first_of('\t'));
                    pos += family.size() + 1;
                    std::string tbl = sql_agg.substr(pos, sql_agg.find_first_of('\t', pos) - pos);
                    pos += tbl.size() + 1;
                    std::string resource_tag = sql_agg.substr(pos, sql_agg.find_first_of('\t', pos) - pos);
                    pos += resource_tag.size() + 1;
                    std::string op_type = sql_agg.substr(pos, sql_agg.find_first_of('\t', pos) - pos);
                    pos += op_type.size() + 1;
                    std::string plat = sql_agg.substr(pos, sql_agg.find_first_of(']', pos) - pos);
                    pos = sql_agg.find_first_of('[') + 1;
                    std::string sql_text = sql_agg.substr(pos, sql_agg.find_first_of(']', pos) - pos);
                    sql_text = boost::replace_all_copy(sql_text, "'", "\\'");
                    // 避免REPLACE INTO的执行发生递归
                    if (family == "BaikalStat" && tbl == "baikaldb_trace_info" &&
                        sql_text.find("REPLACE") != std::string::npos) {
                        continue;
                    }

                    // time
                    char time_str[40];
                    snprintf(time_str, 40, "%04d-%02d-%02d %02d:%02d:%02d:%03d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, 0);
                    sql_values += "('" + std::string(time_str) + "',";                 // time: 06-02 14:33:10:063
                    snprintf(time_str, 40, "%02d-%02d-%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                    sql_values += "'" + std::string(time_str) + "',";                  // date: 2021-06-02
                    snprintf(time_str, 40, "%02d", tm.tm_hour);
                    sql_values += "'" + std::string(time_str) + "',";
                    snprintf(time_str, 40, "%02d", tm.tm_min);
                    sql_values += "'" + std::string(time_str) + "',";
                    sql_values += "'" + std::to_string(pair2.second.sum) + "',";
                    sql_values += "'" + std::to_string(pair2.second.count) + "',";
                    sql_values += "'" +
                                  std::to_string(pair2.second.count == 0 ? 0 : pair2.second.sum / pair2.second.count) +
                                  "',";
                    sql_values += "'" + std::to_string(pair2.second.affected_rows) + "',";
                    sql_values += "'" + std::to_string(pair2.second.scan_rows) + "',";
                    sql_values += "'" + std::to_string(pair2.second.filter_rows) + "',";
                    sql_values += "'" + std::to_string(out_sign) + "',";
                    sql_values += "'" + hostname + "',";
                    sql_values += "'" + factory->get_index_name(pair2.first) + "',";
                    sql_values += "'" + family + "',";
                    sql_values += "'" + tbl + "',";
                    sql_values += "'" + resource_tag + "',";
                    sql_values += "'" + op_type + "',";
                    sql_values += "'" + plat + "',";
                    sql_values += "'" + std::to_string(version) + "',";
                    sql_values += "'" + op_description + "',";
                    sql_values += "'" + std::to_string(pair2.second.err_count) + "',";
                    sql_values += "'" + std::to_string(pair2.second.region_count) + "',";
                    sql_values += "'" + std::to_string(learner_read) + "'),";

                    if (sign_to_counts.count(out_sign) == 0) {
                        std::string sign_sql_value = "('" + std::to_string(out_sign) + "',";
                        sign_sql_value += "'" + family + "',";
                        sign_sql_value += "'" + tbl + "',";
                        sign_sql_value += "'" + resource_tag + "',";
                        sign_sql_value += "'" + op_type + "',";
                        sign_sql_value += "'" + sql_text + "')";
                        sign_sql_map[out_sign] = std::move(sign_sql_value);
                        family_tbl_tag_set.insert("('" + family + "','" + tbl + "','" + resource_tag + "')");
                        sign_to_counts.insert(out_sign);
                    }
                    ++values_size;
                }
            }
        }
        if (FLAGS_insert_agg_sql && values_size > 0) {
            sql_values.pop_back();
            if (insert_agg_sql(sql_values) < 0) {
                DB_WARNING("insert agg_sql: %s failed", sql_values.c_str());
            }
            if (insert_agg_sql_by_sign(sign_sql_map, family_tbl_tag_set, sign_to_counts) < 0) {
                DB_WARNING("insert agg_sql_by_sign: %s failed", sql_values.c_str());
            }
        }
        for (auto& pair : table_count_err) {
            // 10s pv>50 出错率>0.5则读路由去备库
            if (pair.second.count > FLAGS_backup_pv_threshold && 
                    pair.second.err * 1.0 / pair.second.count > FLAGS_backup_error_percent) {
                int64_t table_id = pair.first;
                auto tbl_ptr = factory->get_table_info_ptr(table_id);
                if (tbl_ptr != nullptr && tbl_ptr->have_backup) {
                    tbl_ptr->need_read_backup = true;
                    DB_FATAL("table_id: %ld, %s use backup, count:%ld, err:%ld", 
                            table_id, tbl_ptr->name.c_str(), pair.second.count, pair.second.err);
                }
            }
        }
        bthread_usleep_fast_shutdown(FLAGS_print_agg_sql_interval_s * 1000 * 1000LL, _shutdown);
    }
}

void NetworkServer::insert_subquery_signs_info(std::map<uint64_t, std::set<uint64_t>>& parent_sign_to_subquery_signs, bool is_insert_success) {
    int64_t limit_write_rows = 5000, accumulate_counts = 0;
    if (parent_sign_to_subquery_signs.size() == 0) {
        return;
    }

    std::string values = "";
    for (const auto& pair : parent_sign_to_subquery_signs) {
        // 构造sql
        uint64_t parent_sign = pair.first;
        for (const auto& subquery_sign : pair.second) {
            values += "('" + std::to_string(parent_sign) + "', '" + std::to_string(subquery_sign) + "'),";
            accumulate_counts++;
            if (accumulate_counts > limit_write_rows) {
                accumulate_counts = 0;
                if (values.size() > 0) {
                    values.pop_back();
                }
                int ret = insert_subquery_values(values);
                if (ret < 0) {
                    is_insert_success = false;
                }
                values.clear();
            }
        }
    }
    
    if (accumulate_counts > 0) {
        values.pop_back();
        int ret = insert_subquery_values(values);
        if (ret < 0) {
            is_insert_success = false;
        }
        accumulate_counts = 0;
        values.clear();
    }
    parent_sign_to_subquery_signs.clear();
}


int NetworkServer::insert_subquery_values(const std::string& values) {
    std::string sql = "REPLACE INTO BaikalStat.baikaldb_subquery_sign_info("
                        "`parent_sign`,`subquery_signs`) VALUES ";
    sql += values;
    int ret = 0;
    int retry = 0;
    baikal::client::ResultSet result_set;
    TimeCost cost;
    do {
        ret = _baikaldb->query(0, sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 10);

    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", sql.size(), sql.c_str());
        sql += ";\n";
    }
    DB_NOTICE("affected_rows:%lu, cost:%ld, sql_len:%lu, sql:%s",
            result_set.get_affected_rows(), cost.get_time(), sql.size(), sql.c_str());
    bthread_usleep(1000000);//sleep 1s 
    return ret;
}

static void on_health_check_done(pb::StoreRes* response, brpc::Controller* cntl,
        std::string addr, pb::Status old_status) {
    std::unique_ptr<pb::StoreRes> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    pb::Status new_status = pb::NORMAL;
    if (cntl->Failed()) {
        if (cntl->ErrorCode() == brpc::ERPCTIMEDOUT || 
            cntl->ErrorCode() == ETIMEDOUT) {
            new_status = pb::DEAD;
            DB_WARNING("addr:%s is dead(hang), need rpc cancel, errcode:%d, error:%s", 
                    addr.c_str(), cntl->ErrorCode(), cntl->ErrorText().c_str());
        } else if (cntl->ErrorCode() != brpc::ENOMETHOD) {
            new_status = pb::FAULTY;
            DB_WARNING("addr:%s is faulty, errcode:%d, error:%s", 
                    addr.c_str(), cntl->ErrorCode(), cntl->ErrorText().c_str());
        }
    } else if (response->errcode() == pb::STORE_BUSY) {
        new_status = pb::BUSY;
        DB_WARNING("addr:%s is busy", addr.c_str());
    } else if (response->errcode() == pb::STORE_ROCKS_HANG) {
        new_status = pb::DEAD;
        DB_WARNING("addr:%s is rocks hang", addr.c_str());
    }

    if (old_status != new_status) {
        // double check for dead status
        if (old_status == pb::NORMAL && new_status == pb::DEAD) {
            new_status = pb::FAULTY;
        }
        SchemaFactory::get_instance()->update_instance(addr, new_status, false, true);
    }
}

void NetworkServer::store_health_check() {
    while (!_shutdown) {
        SchemaFactory* factory = SchemaFactory::get_instance();
        std::unordered_map<std::string, InstanceDBStatus> info_map;
        factory->get_all_instance_status(&info_map);
        if (info_map.size() == 0) {
            bthread_usleep_fast_shutdown(FLAGS_health_check_interval_us, _shutdown);
            continue;
        }
        int64_t sleep_once = FLAGS_health_check_interval_us / info_map.size();
        for (auto& pair : info_map) {
            bthread_usleep_fast_shutdown(sleep_once, _shutdown);
            if (_shutdown) {
                break;
            }
            std::string addr = pair.first;
            pb::Status old_status = pair.second.status;
            brpc::Channel channel;
            brpc::ChannelOptions option;
            option.max_retry = 1;
            option.connect_timeout_ms = 1000;
            option.timeout_ms = FLAGS_health_check_store_timeout_ms;
            if (old_status != pb::NORMAL) {
                option.timeout_ms = FLAGS_health_check_store_timeout_ms / 2;
            }
            int ret = channel.Init(addr.c_str(), &option);
            if (ret != 0) {
                DB_WARNING("init failed, addr:%s, ret:%d", addr.c_str(), ret);
                continue;
            }
            pb::HealthCheck req;
            brpc::Controller* cntl = new brpc::Controller();
            pb::StoreRes* res = new pb::StoreRes();
            pb::StoreService_Stub(&channel).health_check(cntl, &req, res, 
                    brpc::NewCallback(on_health_check_done, res, cntl, addr, old_status));
        }
    }
}

//other心跳内容包括：代价统计信息；baikaldb参数动态调整
void NetworkServer::construct_other_heart_beat_request(pb::BaikalOtherHeartBeatRequest& request) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    auto schema_read_recallback = [&request, factory](const SchemaMapping& schema){
        auto& table_statistics_mapping = schema.table_statistics_mapping;
        for (auto& info_pair : schema.table_info_mapping) {
            if (info_pair.second->engine != pb::ROCKSDB &&
                    info_pair.second->engine != pb::ROCKSDB_CSTORE) {
                continue;
            }
            auto req_info = request.add_schema_infos();
            req_info->set_table_id(info_pair.first);
            int64_t version = 0;
            auto iter = table_statistics_mapping.find(info_pair.first);
            if (iter != table_statistics_mapping.end()) {
                version = iter->second->version();
            }
            req_info->set_statis_version(version);
        }
    };
    factory->schema_info_scope_read(schema_read_recallback);
    request.set_baikaldb_resource_tag("__baikaldb"); // baikaldb暂时没有resource_tag，使用"__baikaldb"会全平台同时更新参数
}

void NetworkServer::process_other_heart_beat_response(const pb::BaikalOtherHeartBeatResponse& response) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }

    if (response.has_instance_param()) {
        for (auto& item : response.instance_param().params()) {
            if (!item.is_meta_param()) {
                update_param(item.key(), item.value());
            }
        }
    }
}

void NetworkServer::connection_timeout_check() {
    auto check_func = [this]() {
        std::set<std::string> need_cancel_addrs;
        SchemaFactory* factory = SchemaFactory::get_instance();
        std::unordered_map<std::string, InstanceDBStatus> info_map;
        factory->get_all_instance_status(&info_map);
        for (auto& pair : info_map) {
            if (pair.second.status == pb::DEAD && pair.second.need_cancel) {
                need_cancel_addrs.emplace(pair.first);
            }
        }
        //dead实例不会太多，设置个阈值，太多了则不做处理
        size_t max_dead_cnt = std::min(info_map.size() / 10 + 1, (size_t)5);
        if (need_cancel_addrs.size() > max_dead_cnt) {
            DB_WARNING("too many dead instance, size: %lu/%lu", need_cancel_addrs.size(), info_map.size());
            need_cancel_addrs.clear();
        }
        for (auto& addr : need_cancel_addrs) {
            factory->update_instance_canceled(addr);
        }

        time_t time_now = time(NULL);
        if (time_now == (time_t)-1) {
            DB_WARNING("get current time failed.");
            return;
        }
        if (_epoll_info == NULL) {
            DB_WARNING("_epoll_info not initialized yet.");
            return;
        }

        for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
            SmartSocket sock = _epoll_info->get_fd_mapping(idx);
            if (sock == NULL || sock->is_free || sock->fd == -1) {
                continue;
            }

            // 处理客户端Hang住的情况，server端没有发送handshake包或者auth_result包
            timeval current;
            gettimeofday(&current, NULL);
            int64_t diff_us = (current.tv_sec - sock->connect_time.tv_sec) * 1000000
                + (current.tv_usec - sock->connect_time.tv_usec);
            if (!sock->is_authed && diff_us >= 1000000) {
                // 待现有工作处理完成，需要获取锁
                if (sock->mutex.try_lock() == false) {
                    continue;
                }
                if (sock->is_free || sock->fd == -1) {
                    DB_WARNING("sock is already free.");
                    sock->mutex.unlock();
                    continue;
                }
                DB_WARNING("close un_authed connection [fd=%d][ip=%s][port=%d].",
                        sock->fd, sock->ip.c_str(), sock->port);
                sock->shutdown = true;
                MachineDriver::get_instance()->dispatch(sock, _epoll_info,
                        sock->shutdown || _shutdown);
                continue;
            }
            time_now = time(NULL);
            auto ctx = sock->get_query_ctx();
            if (ctx != nullptr && 
                    ctx->mysql_cmd != COM_SLEEP) {
                if (need_cancel_addrs.size() > 0) {
                    // cancel dead addr 
                    sock->cancel_rpc(need_cancel_addrs, sock->fd);
                    DB_WARNING("%lu addrs is dead, cancel", need_cancel_addrs.size());
                }

                int query_time_diff = time_now - ctx->stat_info.start_stamp.tv_sec;
                if (query_time_diff > FLAGS_slow_query_timeout_s) {
                    DB_NOTICE("query is slow, [cost=%d][fd=%d][ip=%s:%d][now=%ld][active=%ld][user=%s][log_id=%lu][sql=%s]",
                            query_time_diff, sock->fd, sock->ip.c_str(), sock->port,
                            time_now, sock->last_active,
                            sock->user_info->username.c_str(),
                            ctx->stat_info.log_id,
                            ctx->sql.c_str());
                    if (FLAGS_open_to_collect_slow_query_infos) {
                        SlowQueryInfo slow_query_info(ctx->stat_info.log_id, 
                            ctx->stat_info.sign,
                            ctx->stat_info.start_stamp.tv_sec,
                            ctx->stat_info.end_stamp.tv_sec,
                            query_time_diff,
                            ctx->stat_info.num_filter_rows,
                            ctx->stat_info.num_affected_rows,
                            ctx->stat_info.num_scan_rows,
                            ctx->stat_info.num_returned_rows,
                            sock->ip,
                            ctx->stat_info.resource_tag,
                            sock->username,
                            ctx->stat_info.family,
                            ctx->stat_info.table,
                            ctx->sql,
                            false);
                        slow_query_map << BvarSlowQueryMap(slow_query_info);
                    }
                    continue;
                }
            }
            // 处理连接空闲时间过长的情况，踢掉空闲连接
            double diff = difftime(time_now, sock->last_active);
            if ((int32_t)diff < FLAGS_connect_idle_timeout_s) {
                continue;
            }
            // 待现有工作处理完成，需要获取锁
            if (sock->mutex.try_lock() == false) {
                continue;
            }
            if (sock->is_free || sock->fd == -1) {
                DB_WARNING("sock is already free.");
                sock->mutex.unlock();
                continue;
            }
            DB_NOTICE("close idle connection [fd=%d][ip=%s:%d][now=%ld][active=%ld][user=%s]",
                    sock->fd, sock->ip.c_str(), sock->port,
                    time_now, sock->last_active,
                    sock->user_info->username.c_str());
            sock->shutdown = true;
            MachineDriver::get_instance()->dispatch(sock, _epoll_info,
                    sock->shutdown || _shutdown);
        }
    };
    while (!_shutdown) {
        check_func();
        bthread_usleep_fast_shutdown(FLAGS_check_interval * 1000 * 1000LL, _shutdown);
    }
}

// Gracefully shutdown.
void NetworkServer::graceful_shutdown() {
    _shutdown = true;
}

void NetworkServer::process_slow_query_map() {
    int64_t values_size = 0;
    BvarSlowQueryMap bvar_slow_query_map = slow_query_map.reset();
    std::string slow_query_info_values = ""; 
    for (const auto& sign_to_slow_query : bvar_slow_query_map.internal_slow_query_map) {
        const std::vector<SlowQueryInfo>& slow_query_infos = sign_to_slow_query.second;
        for (const auto& slow_query_info : slow_query_infos) {
            //timestamp => localtime
            struct tm local_start_time;
            struct tm local_end_time;
            time_t start_time = slow_query_info.start_time;
            time_t end_time = slow_query_info.end_time;
            localtime_r(&start_time, &local_start_time);
            localtime_r(&end_time, &local_end_time);
            //localtime => format("%Y-%m-%d %H:%M:%S")
            char start_time_str[40], end_time_str[40];
            snprintf(start_time_str, 40, "%04d-%02d-%02d %02d:%02d:%02d:%03d",
                         local_start_time.tm_year + 1900, local_start_time.tm_mon + 1, local_start_time.tm_mday, 
                         local_start_time.tm_hour, local_start_time.tm_min, local_start_time.tm_sec, 0);
            snprintf(end_time_str, 40, "%04d-%02d-%02d %02d:%02d:%02d:%03d",
                         local_end_time.tm_year + 1900, local_end_time.tm_mon + 1, local_end_time.tm_mday, 
                         local_end_time.tm_hour, local_end_time.tm_min, local_end_time.tm_sec, 0);
            //combine sql values
            std::string status_ = slow_query_info.status ? "completed" : "doing";
            slow_query_info_values += "('" + std::string(start_time_str) + "', '" +
                                        std::string(end_time_str) + "', '" +
                                        std::to_string(slow_query_info.log_id) + "', '" +
                                        std::to_string(slow_query_info.sign) + "', '" +
                                        slow_query_info.ip + "', '" +
                                        slow_query_info.user_name + "', '" +
                                        slow_query_info.family+ "', '" +
                                        slow_query_info.table_name + "', '" +
                                        status_ + "', '" +
                                        std::to_string(slow_query_info.exec_time) + "', '" +
                                        std::to_string(slow_query_info.affected_rows) + "', '" +
                                        std::to_string(slow_query_info.scan_rows) + "', '" +
                                        std::to_string(slow_query_info.filtered_rows) + "', '" +
                                        std::to_string(slow_query_info.return_rows) + "', '" +
                                        slow_query_info.sql + "'),";
            values_size++;
        }
        
        if (values_size >= FLAGS_slow_query_batch_size) {
            slow_query_info_values.pop_back();
            int ret = insert_slow_query_infos(slow_query_info_values);
            if (ret < 0) {
                DB_WARNING("insert slow query infos failed, values_size is [%ld] values_info [%s]", 
                    values_size, slow_query_info_values.c_str());
            }
            values_size = 0;
            slow_query_info_values.clear();
        }
    }

    if (values_size > 0) {
        slow_query_info_values.pop_back();
        int ret = insert_slow_query_infos(slow_query_info_values);
        if (ret < 0) {
            DB_WARNING("insert slow query infos failed, values_size is [%ld] values_info [%s]", 
                values_size, slow_query_info_values.c_str());
        }
        values_size = 0;
        slow_query_info_values.clear();
    }
}

int NetworkServer::insert_slow_query_infos(const std::string& slow_query_info_values) {
    baikal::client::ResultSet result_set;
    TimeCost cost;
    // 构造sql
    std::string insert_slow_query_info_sql = "REPLACE INTO BaikalStat.slow_query_sql_info (`start_time`,"
            "`end_time`, `log_id`, `sign`, `ip`, `user_name`, `family`, `table_name`, `status`, `exec_time`,"
            "`affected_rows`, `scan_rows`, `filter_rows`, `return_rows`, `sql`) values";
    insert_slow_query_info_sql += slow_query_info_values;
    int ret = 0;
    int retry = 0;
    do {
        ret = _baikaldb->query(0, insert_slow_query_info_sql, &result_set);
        if (ret == 0) {
            break;
        }
        bthread_usleep(1000000);
    }while (++retry < 20);

    if (ret != 0) {
        DB_FATAL("sql_len:%lu query fail : %s", insert_slow_query_info_sql.size(), insert_slow_query_info_sql.c_str());
        insert_slow_query_info_sql += ";\n";
        return -1;
    }
    DB_NOTICE("affected_rows:%lu, cost:%ld, sql_len:%lu, sql:%s",
              result_set.get_affected_rows(), cost.get_time(), insert_slow_query_info_sql.size(), insert_slow_query_info_sql.c_str());
    return 0; 
}

NetworkServer::NetworkServer():
        _is_init(false),
        _shutdown(false),
        _epoll_info(NULL),
        _heart_beat_count("heart_beat_count") {
}

NetworkServer::~NetworkServer() {
    // Free epoll info.
    _conn_check_bth.join();
    _heartbeat_bth.join();
    _other_heartbeat_bth.join();
    _agg_sql_bth.join();
    _health_check_bth.join();
    if (_epoll_info != NULL) {
        delete _epoll_info;
        _epoll_info = NULL;
    }
}

int NetworkServer::fetch_instance_info() {
    SchemaFactory* factory = SchemaFactory::get_instance();
    int64_t instance_tableid = -1;
    if (0 != factory->get_table_id(instance_table_name, instance_tableid)) {
        DB_WARNING("unknown instance table: %s", instance_table_name.c_str());
        return -1;
    }

    // 请求meta来获取自增id
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_GEN_ID_FOR_AUTO_INCREMENT);
    auto auto_increment_ptr = request.mutable_auto_increment();
    auto_increment_ptr->set_table_id(instance_tableid);
    auto_increment_ptr->set_count(1);
    if (MetaServerInteract::get_instance()->send_request("meta_manager", 
                                                          request, 
                                                          response) != 0) {
        DB_FATAL("fetch_instance_info from meta_server fail");
        return -1;
    }
    if (response.start_id() + 1 != response.end_id()) {
        DB_FATAL("gen id count not equal to 1");
        return -1;
    }
    _instance_id = response.start_id();
    DB_NOTICE("baikaldb instance_id: %lu", _instance_id);
    return 0;
}

bool NetworkServer::init() {
    // init val 
    _driver_thread_num = bthread::FLAGS_bthread_concurrency;
    TimeCost cost;
    butil::EndPoint addr;
    addr.ip = butil::my_ip();
    addr.port = FLAGS_baikal_port; 
    std::string address = endpoint2str(addr).c_str(); 
    int ret = get_physical_room(address, _physical_room);
    if (ret < 0) {
        DB_FATAL("get physical room fail");
        return false;
    }
    DB_NOTICE("get physical_room %s", _physical_room.c_str());
    // 先把meta数据都获取到
    pb::BaikalHeartBeatRequest request;
    pb::BaikalHeartBeatResponse response;
    //1、构造心跳请求
    BaikalHeartBeat::construct_heart_beat_request(request);
    request.set_physical_room(_physical_room);
    //2、发送请求
    if (MetaServerInteract::get_instance()->send_request("baikal_heartbeat", request, response) == 0) {
        //处理心跳
        BaikalHeartBeat::process_heart_beat_response_sync(response);
        //DB_WARNING("req:%s  \nres:%s", request.DebugString().c_str(), response.DebugString().c_str());
    } else {
        DB_FATAL("send heart beat request to meta server fail");
        return false;
    }
    DB_NOTICE("sync time2:%ld", cost.get_time());
    if (FLAGS_fetch_instance_id) {
        if (fetch_instance_info() != 0) {
            return false;
        }        
    }
    DB_WARNING("get instance_id: %lu", _instance_id);
    // for print_agg_sql
    if (FLAGS_insert_agg_sql) {
        int rc = 0;
        rc = _manager.init("conf", "baikal_client.conf");
        if (rc != 0) {
            DB_FATAL("baikal client init fail:%d", rc);
            return false;
        }
        _baikaldb = _manager.get_service("baikaldb");
        if (_baikaldb == NULL) {
            DB_FATAL("baikaldb is null");
            return false;
        }
    }
    _is_init = true;
    return true;
}

void NetworkServer::stop() {
    _heartbeat_bth.join();
    _other_heartbeat_bth.join();

    if (_epoll_info == nullptr) {
        DB_WARNING("_epoll_info not initialized yet.");
        return;
    }
    for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
        SmartSocket sock = _epoll_info->get_fd_mapping(idx);
        if (!sock) {
            continue;
        }
        if (sock == nullptr || sock->fd == 0) {
            continue;
        }

        // 待现有工作处理完成，需要获取锁
        if (sock->mutex.try_lock()) {
            sock->shutdown = true;
            MachineDriver::get_instance()->dispatch(sock, _epoll_info, true);
        }
    }
    return;
}

bool NetworkServer::start() {
    if (!_is_init) {
        DB_FATAL("Network server is not initail.");
        return false;
    }
    DB_WARNING("db server init success");
    if (0 != make_worker_process()) {
        _shutdown = true;
        DB_FATAL("Start event loop failed.");
        return false;
    }
    return true;
}

SmartSocket NetworkServer::create_listen_socket() {
    // Fetch a socket.
    SocketFactory* socket_pool = SocketFactory::get_instance();
    SmartSocket sock = socket_pool->create(SERVER_SOCKET);
    if (sock == NULL) {
        DB_FATAL("Failed to fetch socket from poll.type:[%u]", SERVER_SOCKET);
        return SmartSocket();
    }
    // Bind.
    int val = 1;
    if (setsockopt(sock->fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) != 0) {
        DB_FATAL("setsockopt fail");
        return SmartSocket();
    }
    
    if (FLAGS_enable_tcp_keep_alive && set_keep_tcp_alive(sock->fd) != 0) {
        DB_FATAL("setsockopt fail");
        return SmartSocket();
    }
    struct sockaddr_in listen_addr;
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_addr.s_addr = INADDR_ANY;
    listen_addr.sin_port = htons(FLAGS_baikal_port);
    if (0 > bind(sock->fd, (struct sockaddr *) &listen_addr, sizeof(listen_addr))) {
        DB_FATAL("bind() errno=%d, error=%s", errno, strerror(errno));
        return SmartSocket();
    }

    // Listen.
    if (0 > listen(sock->fd, FLAGS_backlog)) {
        DB_FATAL("listen() failed fd=%d, bakclog=%d, errno=%d, error=%s",
                sock->fd, FLAGS_backlog, errno, strerror(errno));
        return SmartSocket();
    }
    sock->shutdown = false;
    // Set socket attribute.
    if (!set_fd_flags(sock->fd)) {
        DB_FATAL("create listen socket but set fd flags error.");
        return SmartSocket();
    }
    return sock;
}

int32_t NetworkServer::set_keep_tcp_alive(int socket_fd) {
  if (socket_fd < 0) {
    return -1;
  }
  // 打开keepalive功能
  int32_t alive = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&alive, sizeof(alive)) < 0) {
    DB_WARNING("fd:%d setsockopt SO_KEEPALIVE failed: %d (%s)", socket_fd, errno, strerror(errno));
    return -1;
  }
  // 关闭一个非活跃连接之前的最大重试次数
  int32_t probes = 3;
  if (setsockopt(socket_fd, SOL_TCP, TCP_KEEPCNT, (void *)&probes, sizeof(probes)) < 0) {
    DB_WARNING("fd:%d setsockopt SO_KEEPCNT failed: %d (%s)", socket_fd, errno, strerror(errno));
    return -1;
  }
  // 发送 keepalive 报文的时间间隔
  int32_t alivetime = 30;
  if (setsockopt(socket_fd, SOL_TCP, TCP_KEEPIDLE, (void *)&alivetime, sizeof(alivetime)) < 0) {
    DB_WARNING("fd:%d setsockopt SO_KEEPIDLE failed: %d (%s)", socket_fd, errno, strerror(errno));
    return -1;
  }
  // 重试间隔
  int32_t interval = 10;
  if (setsockopt(socket_fd, SOL_TCP, TCP_KEEPINTVL, (void *)&interval, sizeof(interval)) < 0) {
    DB_WARNING("fd:%d setsockopt SO_KEEPINTVL failed: %d (%s)", socket_fd, errno, strerror(errno));
    return -1;
  }

  int32_t nodelay = 1;
  if (setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay)) < 0) {
    DB_WARNING("fd:%d setsockopt TCP_NODELAY failed %d (%s)", socket_fd, errno, strerror(errno));
    return -1;
  }

  struct linger linger = {0};
  linger.l_onoff = 1;
  linger.l_linger = 3;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_LINGER, (void *)&linger, sizeof(linger)) < 0) {
    DB_WARNING("fd:%d setsockopt SO_LINGER failed: %d (%s)", socket_fd, errno, strerror(errno));
    return -1;
  }

  return 0;
}

int NetworkServer::make_worker_process() {
    if (MachineDriver::get_instance()->init(_driver_thread_num) != 0) {
        DB_FATAL("Failed to init machine driver.");
        exit(-1);
    }
    _conn_check_bth.run([this]() {connection_timeout_check();});
    _heartbeat_bth.run([this]() {report_heart_beat();});
    _other_heartbeat_bth.run([this]() {report_other_heart_beat();});
    _agg_sql_bth.run([this]() {print_agg_sql();});
    if (FLAGS_need_health_check) {
        _health_check_bth.run([this]() {store_health_check();});
    }


    // Create listen socket.
    _service = create_listen_socket();
    if (_service == nullptr) {
        DB_FATAL("Failed to create listen socket.");
        return -1;
    }

    // Initail epoll info.
    _epoll_info = new EpollInfo();
    if (!_epoll_info->init()) {
        DB_FATAL("initial epoll info failed.");
        return -1;
    }
    if (!_epoll_info->poll_events_add(_service, EPOLLIN)) {
        DB_FATAL("poll_events_add add socket[%d] error", _service->fd);
        return -1;
    }
    // Process epoll events.
    int listen_fd = _service->fd;
    SocketFactory* socket_pool = SocketFactory::get_instance();
    while (!_shutdown) {
        int fd_cnt = _epoll_info->wait(FLAGS_epoll_timeout);
        if (_shutdown) {
            // Delete event from epoll.
            _epoll_info->poll_events_delete(_service);
        }

        for (int cnt = 0; cnt < fd_cnt; ++cnt) {
            int fd = _epoll_info->get_ready_fd(cnt);
            int event = _epoll_info->get_ready_events(cnt);

            // New connection.
            if (!_shutdown && listen_fd == fd) {
                // Accept and check new client socket.
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(fd, (struct sockaddr*)&client_addr, &client_len);
                if (client_fd <= 0) {
                    DB_WARNING("Wrong fd:[%d] errno:%d", client_fd, errno);
                    continue;
                }
                if (client_fd >= CONFIG_MPL_EPOLL_MAX_SIZE) {
                    DB_WARNING("Wrong fd.fd=%d >= CONFIG_MENU_EPOLL_MAX_SIZE", client_fd);
                    close(client_fd);
                    continue;
                }
                // Set flags of client socket.
                if (!set_fd_flags(client_fd)) {
                    DB_WARNING("client_fd=%d set_fd_flags error close(client)", client_fd);
                    close(client_fd);
                    continue;
                }
                // Create NetworkSocket for new client socket.
                SmartSocket client_socket = socket_pool->create(CLIENT_SOCKET);
                if (client_socket == NULL) {
                    DB_WARNING("Failed to create NetworkSocket from pool.fd:[%d]", client_fd);
                    close(client_fd);
                    continue;
                }

                // Set attribute of client socket.
                char *ip_address = inet_ntoa(client_addr.sin_addr);
                if (NULL != ip_address) {
                    client_socket->ip = ip_address;
                }
                client_socket->fd = client_fd;
                client_socket->state = STATE_CONNECTED_CLIENT;
                client_socket->port = ntohs(client_addr.sin_port);
                client_socket->addr = client_addr;
                client_socket->server_instance_id = _instance_id;

                // Set socket mapping and event.
                if (!_epoll_info->set_fd_mapping(client_socket->fd, client_socket)) {
                    DB_FATAL("Failed to set fd mapping.");
                    return -1;
                }
                _epoll_info->poll_events_add(client_socket, 0);

                // New connection will be handled immediately.
                fd = client_fd;
                //DB_NOTICE("Accept new connect [ip=%s, port=%d, client_fd=%d]",
                DB_WARNING_CLIENT(client_socket, "Accept new connect [ip=%s, port=%d, client_fd=%d]",
                        ip_address,
                        client_socket->port,
                        client_socket->fd);
            }

            // Check if socket in fd_mapping or not.
            SmartSocket sock = _epoll_info->get_fd_mapping(fd);
            if (sock == NULL) {
                DB_DEBUG("Can't find fd in fd_mapping, fd:[%d], listen_fd:[%d], fd_cnt:[%d]",
                            fd, listen_fd, cnt);
                continue;
            }
            if (fd != sock->fd) {
                DB_WARNING_CLIENT(sock, "current [fd=%d][sock_fd=%d]"
                    "[event=%d][fd_cnt=%d][state=%s]",
                    fd,
                    sock->fd,
                    event,
                    fd_cnt,
                    state2str(sock).c_str());
                continue;
            }
            sock->last_active = time(NULL);

            // Check socket event.
            // EPOLLHUP: closed by client. because of protocol of sending package is wrong.
            if (event & EPOLLHUP || event & EPOLLERR) {
                if (sock->socket_type == CLIENT_SOCKET) {
                    if ((event & EPOLLHUP) && sock->shutdown == false) {
                        DB_WARNING_CLIENT(sock, "CLIENT EPOLL event is EPOLLHUP, fd=%d event=0x%x",
                                        fd, event);
                    } else if ((event & EPOLLERR) && sock->shutdown == false) {
                        DB_WARNING_CLIENT(sock, "CLIENT EPOLL event is EPOLLERR, fd=%d event=0x%x",
                                        fd, event);
                    }
                } else {
                    DB_WARNING_CLIENT(sock, "socket type is wrong, fd %d event=0x%x", fd, event);
                }
                sock->shutdown = true;
            }

            // Handle client socket event by status machine.
            if (sock->socket_type == CLIENT_SOCKET) {
                // the socket has just connected, no need to require lock
                if (sock->mutex.try_lock() == false) {
                    continue;
                }
                if (sock->is_free || sock->fd == -1) {
                    DB_WARNING_CLIENT(sock, "sock is already free.");
                    sock->mutex.unlock();
                    continue;
                }
                // close the socket event on epoll when the sock is being process
                // and reopen it when finish process
                _epoll_info->poll_events_mod(sock, 0);
                MachineDriver::get_instance()->dispatch(sock, _epoll_info,
                    sock->shutdown || _shutdown);
            } else {
                DB_WARNING("unknown network socket type[%d].", sock->socket_type);
            }
        }
    }
    DB_NOTICE("Baikal instance exit.");
    return 0;
}

std::string NetworkServer::state2str(SmartSocket client) {
    switch (client->state) {
    case STATE_CONNECTED_CLIENT: {
        return "STATE_CONNECTED_CLIENT";
    }
    case STATE_SEND_HANDSHAKE: {
        return "STATE_SEND_HANDSHAKE";
    }
    case STATE_READ_AUTH: {
        return "STATE_READ_AUTH";
    }
    case STATE_SEND_AUTH_RESULT: {
        return "STATE_SEND_AUTH_RESULT";
    }
    case STATE_READ_QUERY_RESULT: {
        return "STATE_READ_QUERY_RESULT";
    }
    case STATE_ERROR_REUSE: {
        return "STATE_ERROR_REUSE";
    }
    case STATE_ERROR: {
        return "STATE_ERROR";
    }
    default: {
        return "unknown state";
    }
    }
    return "unknown state";
}

bool NetworkServer::set_fd_flags(int fd) {
    if (fd < 0) {
        DB_FATAL("wrong fd:[%d].", fd);
        return false;
    }
    int opts = fcntl(fd, F_GETFL);
    if (opts < 0) {
        DB_FATAL("set_fd_flags() fd=%d fcntl(fd, F_GETFL) error", fd);
        return false;
    }
    opts = opts | O_NONBLOCK;
    if (fcntl(fd, F_SETFL, opts) < 0) {
        DB_FATAL("set_fd_flags() fd=%d fcntl(fd, F_SETFL, opts) error", fd);
        return false;
    }
    struct linger li;
    li.l_onoff = 1;
    li.l_linger = 0;

    int ret = setsockopt(fd, SOL_SOCKET, SO_LINGER, (const char*) &li, sizeof(li));
    if (ret != 0) {
        DB_FATAL("set_fd_flags() fd=%d setsockopt linger error", fd);
        return false;
    }
    int var = 1;
    ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &var, sizeof(var));
    if (ret != 0) {
        DB_FATAL("set_fd_flags() fd=%d setsockopt tcp_nodelay error", fd);
        return false;
    }
    return true;
}


} // namespace baikal
