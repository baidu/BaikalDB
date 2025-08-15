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
#include "olap_pre_split.h"
#include "schema_factory.h"
#include "store_interact.hpp"
#include "ddl_planner.h"
namespace baikaldb {
DEFINE_int64(olap_region_pre_split_min_rows, 10000000, "olap_region_pre_split_min_rows 1000w");
DEFINE_int64(olap_region_pre_split_interval_s, 2 * 24 * 3600LL, "default 2 day");
DECLARE_int32(key_point_collector_interval);
DEFINE_bool(need_olap_pre_split, false, "need_olap_pre_split");
void OlapPreSplit::olap_pre_split() {
    if (!FLAGS_need_olap_pre_split) {
        return;
    }
    static TimeCost cost;
    if (cost.get_time() < FLAGS_olap_region_pre_split_interval_s * 1000 * 1000LL) {
        return;
    }
    cost.reset();
    auto tb_vec = SchemaFactory::get_instance()->get_table_list("", nullptr);
    for(auto& table_info : tb_vec) {
        if (table_info->schema_conf.has_olap_pre_split_cnt() && table_info->schema_conf.olap_pre_split_cnt() > 0) {
            olap_pre_split(table_info->id);
        }
    }
}

int OlapPreSplit::olap_pre_split(int64_t table_id) {
    SmartTable table = SchemaFactory::get_instance()->get_table_info_ptr(table_id);
    if (table == nullptr) {
        DB_FATAL("table_id %ld not exist", table_id);
        return -1;
    }
    DB_NOTICE("BEGIN olap pre split, table: %s", table->name.c_str());
    if (!table->is_range_partition || !table->partition_info.has_dynamic_partition_attr()) {
        return -1;
    }
    const auto& primary_range_partition_type = table->partition_info.primary_range_partition_type();
    const auto& dynamic_partition_attr = table->partition_info.dynamic_partition_attr();
    if (!dynamic_partition_attr.has_enable() || dynamic_partition_attr.enable() == false) {
        return -1;
    }

    const std::string& time_unit = dynamic_partition_attr.time_unit();
    if (!boost::algorithm::iequals(time_unit, "DAY")) {
        return -1;
    }

    const pb::PrimitiveType& partition_col_type = table->partition_info.field_info().mysql_type();
    if (partition_col_type != pb::DATE) {
        return -1;
    }

    RangePartition* partition_ptr = static_cast<RangePartition*>(table->partition_ptr.get());
    const auto& ranges = partition_ptr->ranges();
    int start = dynamic_partition_attr.start();
    if (start < 0 && start > -3) {
        start = start;
    } else {
        start = -3;
    }

    std::string start_str;
    time_t current_ts = ::time(NULL);
    time_t start_ts = current_ts;
    date_add_interval(start_ts, start, TimeUnit::DAY);
    timestamp_to_format_str(start_ts, "%Y-%m-%d", start_str);
    ExprValue field_value(pb::DATE, start_str);
    int64_t partition_id = -1;
    for (const auto& range : ranges) {
        if (range.left_value.compare(field_value) <= 0 && range.right_value.compare(field_value) > 0) {
            if (range.is_cold) {
                partition_id = range.partition_id;
                break;
            } else if (primary_range_partition_type == range.partition_type && !range.is_isolation) {
                partition_id = range.partition_id;
                break;
            } 
        }
    }

    if (partition_id == -1) {
        return -1;
    }

    int pre_split_region_cnt = table->schema_conf.olap_pre_split_cnt();
    if (pre_split_region_cnt <= 0) {
        return -1;
    }

    std::map<int64_t, pb::RegionInfo> region_infos;
    if (SchemaFactory::get_instance()->get_partition_regions(table_id, {partition_id}, region_infos) != 0) {
        DB_WARNING("get_all_partition_regions failed, table_id[%ld]", table_id);
        return -1;
    }

    std::map<int64_t, int64_t> userid_rows;
    for (const auto& pair : region_infos) {
        auto& region = pair.second;
        pb::RegionIds req;
        pb::StoreRes res;
        req.set_query_olap_keypoint(true);
        req.add_region_ids(region.region_id());
        StoreInteract interact(region.leader());
        int ret = interact.send_request("query_region", req, res);
        if (ret < 0) {
            return -1;
        }
        if (!res.extra_res().query_keypoint_succ()) {
            DB_FATAL("query region from store %s failed", region.leader().c_str());
            return -1;
        }

        for (const auto& userid_count : res.extra_res().userid_count()) {
            if (userid_rows.count(userid_count.userid()) > 0) {
                userid_rows[userid_count.userid()] += userid_count.count() * FLAGS_key_point_collector_interval;
            } else {
                userid_rows[userid_count.userid()] = userid_count.count() * FLAGS_key_point_collector_interval;
            }
        }
    }
    
    int64_t total_rows = 0;
    for (const auto& pair : userid_rows) {
        total_rows += pair.second;
    }

    int64_t average_rows = total_rows / pre_split_region_cnt;
    if (average_rows < FLAGS_olap_region_pre_split_min_rows) {
        // 限制自动预分裂的region行数不低于1000w，避免采样异常导致的数据倾斜
        DB_WARNING("table_name: %s, average rows less than min pre split rows, total[%ld], average rows[%ld], pre_split_region_cnt[%d], sample_date: %s", 
            table->name.c_str(), total_rows, average_rows, pre_split_region_cnt, start_str.c_str()); 
        return -1;
    }

    int64_t region_rows = 0;
    std::vector<std::string> pre_split_keys;
    pre_split_keys.reserve(pre_split_region_cnt);
    for (const auto& [userid, rows] : userid_rows) {
        if (region_rows + rows > average_rows) {
            pre_split_keys.emplace_back(std::to_string(userid));
            region_rows = rows;
        } else {
            region_rows += rows;
        }
    }

    std::vector<std::string> split_vec;
    boost::split(split_vec, table->name, boost::is_any_of("."));
    if (split_vec.size() != 2) {
        DB_FATAL("get table_name[%s] fail", table->name.c_str());
        return -1;
    }
    int ret = DDLPlanner::update_specify_split_keys(table->namespace_, split_vec[0], split_vec[1], boost::join(pre_split_keys, ","));
    if (ret < 0) {
        DB_WARNING("update split keys fail table: %s", table->name.c_str());
        return -1;
    }

    DB_NOTICE("END auto pre split success, table: %s, total_rows:%ld, average_rows:%ld, sample_date: %s, split size: %ld, pre_split_region_cnt: %d, keys: %s", 
        table->name.c_str(), total_rows, average_rows, start_str.c_str(), pre_split_keys.size(), pre_split_region_cnt, boost::join(pre_split_keys, ",").c_str());
    return 0;
}
}