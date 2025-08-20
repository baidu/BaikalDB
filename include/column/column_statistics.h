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
#pragma once
#include "common.h"

namespace baikaldb {
struct ColumnVars {
    static ColumnVars* get_instance() {
        static ColumnVars _instance;
        return &_instance;
    }

    bvar::Adder<int64_t>  minor_compaction_write_rows;
    bvar::Adder<int64_t>  major_compaction_write_rows;
    bvar::Adder<int64_t>  base_compaction_write_rows;
    bvar::PerSecond<bvar::Adder<int64_t> > minor_compaction_write_rows_per_second;
    bvar::PerSecond<bvar::Adder<int64_t> > major_compaction_write_rows_per_second;
    bvar::PerSecond<bvar::Adder<int64_t> > base_compaction_write_rows_per_second;

    bvar::Maxer<int64_t>     cumulative_file_max_count;
    bvar::Window<bvar::Maxer<int64_t>> cumulative_file_max_count_minute;

    bvar::Adder<int64_t>     parquet_file_open_count;

    bvar::LatencyRecorder parquet_ssd_read_time_cost;
    bvar::LatencyRecorder parquet_afs_read_time_cost;

    bvar::Adder<int64_t>  parquet_read_bytes;
    bvar::PerSecond<bvar::Adder<int64_t> > parquet_read_bytes_per_second;

    bvar::Adder<int64_t>  parquet_cache_bytes;

    bvar::PassiveStatus<int64_t> parquet_cache_hit_rate;

    void inc_cache_hit(bool hit) {
        if (hit) {
            _cache_hit.fetch_add(1 + (1LL << 32));
        } else {
            _cache_hit.fetch_add(1);
        }
    }

    int64_t get_cache_hit() {
        int64_t cur = _cache_hit.exchange(0);
        int64_t a = cur >> 32;
        int64_t b = cur & 0xffffffff;
        if (b <= 0) {
            return 0;
        } else {
            return a * 100 / b;
        }
    }

    static int64_t calc_cache_hit_rate(void* arg) {
        ColumnVars* self = static_cast<ColumnVars*>(arg);
        return self->get_cache_hit();
    }

private:
    ColumnVars():  minor_compaction_write_rows_per_second("column_minor_compaction_write_rows_per_second", &minor_compaction_write_rows),
                   major_compaction_write_rows_per_second("column_major_compaction_write_rows_per_second", &major_compaction_write_rows),
                   base_compaction_write_rows_per_second("column_base_compaction_write_rows_per_second", &base_compaction_write_rows),
                   cumulative_file_max_count_minute("column_cumulative_file_max_count_minute", &cumulative_file_max_count, 60),
                   parquet_file_open_count("column_parquet_file_open_count"),
                   parquet_ssd_read_time_cost("column_parquet_ssd_read_time_cost"),
                   parquet_afs_read_time_cost("column_parquet_afs_read_time_cost"),
                   parquet_read_bytes_per_second("column_parquet_read_bytes_per_second", &parquet_read_bytes),
                   parquet_cache_bytes("column_parquet_cache_bytes"),
                   parquet_cache_hit_rate("column_parquet_cache_hit_rate", &calc_cache_hit_rate, this) { }
    std::atomic<int64_t> _cache_hit = {0};
};

}