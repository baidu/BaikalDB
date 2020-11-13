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
#include "histogram.h"

namespace baikaldb {
DEFINE_int32(histogram_split_threshold_percent, 50, "histogram_split_threshold default 0.5");
SampleSorter::SampleSorter(std::vector<std::shared_ptr<RowBatch> >& batch_vector, ExprNode* sort_expr) {
    _slot_order_exprs.push_back(sort_expr);
    _is_asc.push_back(true);
    _is_null_first.push_back(false);
    _mem_row_compare = std::make_shared<MemRowCompare>(_slot_order_exprs, _is_asc, _is_null_first);
    _sorter = std::make_shared<Sorter>(_mem_row_compare.get());
    //_mem_row_compare(_slot_order_exprs, _is_asc, _is_null_first);
    //_sorter(_mem_row_compare.get());
    int sample_rows = 0;
    for (auto batch : batch_vector) {
        _sorter->add_batch(batch);
        sample_rows += batch->size();
    }
    _expect_bucket_size = sample_rows / FLAGS_expect_bucket_count;
    if (_expect_bucket_size < 1) {
        _expect_bucket_size = 1;
    }
}

void SampleSorter::insert_row(MemRow* row) {
    if (row == nullptr) {
        return;
    }

    ExprValue value = _slot_order_exprs[0]->get_value(row);
    if (value.is_null()) {
        _null_value_cnt++;
        return;
    }

    if (_cur_value_cnt == 0) {
        _cur_value = value;
        _cur_value_cnt = 1;
        return;
    }

    int64_t k = _cur_value.compare(value);
    if (k == 0) {
        _cur_value_cnt++;
        return;
    }

    if (k > 0) {
        return;
    }

    // _cur_value < value

    // 1.先处理 _cur_value
    insert_distinct_value(_cur_value, _cur_value_cnt);

    // 2.再处理 value，放入 _cur_value
    _cur_value = value;
    _cur_value_cnt = 1;

}

void SampleSorter::insert_distinct_value(const ExprValue& value, const int& cnt) {
    _distinct_cnt_total++;
    if (_bucket_infos.empty()) {
        // 首个桶
        BucketInfo bucket_info;
        bucket_info.distinct_cnt = 1;
        bucket_info.bucket_size = cnt;
        bucket_info.start = value;
        bucket_info.end = value;
        _bucket_infos.push_back(bucket_info);
    } else {
        // 已存在桶
        auto& back_bucket_info = _bucket_infos.back();
        int bucket_split_count = _expect_bucket_size * FLAGS_histogram_split_threshold_percent / 100;
        if (cnt >= bucket_split_count || back_bucket_info.bucket_size >= bucket_split_count || (cnt + back_bucket_info.bucket_size) >= _expect_bucket_size) {
            // 当前_cur_value cnt 大于 bucket_split_count 或者上一个桶已经超过阈值，为该值开辟新桶
            BucketInfo bucket_info;
            bucket_info.distinct_cnt = 1;
            bucket_info.bucket_size = cnt;
            bucket_info.start = value;
            bucket_info.end = value;
            _bucket_infos.push_back(bucket_info);
        } else {
            // 继续加入旧桶
            back_bucket_info.distinct_cnt++;
            back_bucket_info.bucket_size += cnt;
            back_bucket_info.end = value;
        }
    }
}

void SampleSorter::insert_done() {
    if (_cur_value_cnt == 0) {
        return;
    }

    insert_distinct_value(_cur_value, _cur_value_cnt);
    _cur_value_cnt= 0;
}

void SampleSorter::packet_column(pb::ColumnInfo* column_info) {
    column_info->set_distinct_cnt(_distinct_cnt_total);
    column_info->set_null_value_cnt(_null_value_cnt);
    for (auto& bucket_info : _bucket_infos) {
        pb::BucketInfo* pb_bucket_info = column_info->add_bucket_infos();
        pb_bucket_info->set_distinct_cnt(bucket_info.distinct_cnt);
        pb_bucket_info->set_bucket_size(bucket_info.bucket_size);
        pb::ExprValue* start_pb = pb_bucket_info->mutable_start();
        bucket_info.start.to_proto(start_pb);
        pb::ExprValue* end_pb = pb_bucket_info->mutable_end();
        bucket_info.end.to_proto(end_pb);
    }
}
} // namespace baikaldb
