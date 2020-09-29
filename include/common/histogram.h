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
#include "proto/meta.interface.pb.h"
#include "data_buffer.h"
#include "sorter.h"

namespace baikaldb {
DECLARE_int32(expect_bucket_count); 
struct BucketInfo {
    int distinct_cnt = 0;
    int bucket_size  = 0;
    ExprValue start;
    ExprValue end;
};

typedef std::map<ExprValue, std::shared_ptr<BucketInfo>, std::function<bool(const ExprValue&, const ExprValue&)>> HistogramMap;

// 列直方图，处理单列中直方图桶操作
class Histogram {
public:
    Histogram(pb::PrimitiveType type, int32_t field_id, int32_t distinct_cnt, int32_t null_value_cnt) : 
        _type(type), _field_id(field_id), _distinct_cnt(distinct_cnt), _null_value_cnt(null_value_cnt), 
        _bucket_mapping([](const ExprValue& a, const ExprValue& b) { 
            int64_t k = a.compare(b);
            return k >= 0 ? false : true;}) {}

    void add_proto(const pb::ColumnInfo& column_info) {
        for (auto& bucket_pb : column_info.bucket_infos()) {
            auto bucket_mem = std::make_shared<BucketInfo>();
            bucket_mem->distinct_cnt = bucket_pb.distinct_cnt();
            bucket_mem->bucket_size = bucket_pb.bucket_size();
            ExprValue start(bucket_pb.start());
            ExprValue end(bucket_pb.end());
            bucket_mem->start = start;
            bucket_mem->end = end;
            _bucket_mapping[bucket_mem->start] = bucket_mem;
        }
    }

    HistogramMap& get_bucket_mapping() {
        return _bucket_mapping;
    }

    int32_t get_distinct_cnt() {
        return _distinct_cnt;
    }

    int32_t get_null_value_cnt() {
        return _null_value_cnt;
    }

    int32_t get_bucket_count() {
        return _bucket_mapping.size();
    }

    double calc_diff(const ExprValue& start, const ExprValue& end, int prefix_len) {
        ExprValue tmp_start = start;
        ExprValue tmp_end = end;
        double ret = tmp_end.float_value(prefix_len) - tmp_start.float_value(prefix_len);
        DB_DEBUG("start:%s, end:%s, prefix_len:%d, ret:%f", 
               start.get_string().c_str(),end.get_string().c_str(), prefix_len, ret);
        return ret;
    }

    double calc_fraction(const ExprValue& start, const ExprValue& end, 
        const ExprValue& value) {
        int prefix_len = start.common_prefix_length(end);
        ExprValue tmp_start = start;
        ExprValue tmp_end = end;
        ExprValue tmp_value = value;
        double ret = (tmp_value.float_value(prefix_len) - tmp_start.float_value(prefix_len)) / 
            (tmp_end.float_value(prefix_len) - tmp_start.float_value(prefix_len));
        DB_DEBUG("value:%s, start:%s, end:%s, prefix_len:%d, ret:%f", 
               value.get_string().c_str(), start.get_string().c_str(),end.get_string().c_str(), prefix_len, ret);
        return ret;
    }

    int32_t calc_scalar(const ExprValue& lower, const ExprValue& upper, 
        std::shared_ptr<BucketInfo> bucket_ptr) {
        int32_t count = 0;
        double ratio = 0.0;
        double distinct_1 = 1.0 / bucket_ptr->distinct_cnt;
        if (lower.compare(bucket_ptr->start) <= 0 && upper.compare(bucket_ptr->end) >= 0) {
            count = bucket_ptr->bucket_size;
        } else if (lower.compare(bucket_ptr->end) > 0 || upper.compare(bucket_ptr->start) < 0) {
            count = 0;
        } else if (lower.compare(bucket_ptr->start) <= 0) {
            ratio = calc_fraction(bucket_ptr->start, bucket_ptr->end, upper);
            ratio = ratio > 1.0 ? 1.0 : ratio;
            ratio = ratio < distinct_1 ? distinct_1 : ratio;
            count = ceil(bucket_ptr->bucket_size * ratio);
        } else if (upper.compare(bucket_ptr->end) >= 0) {
            ratio = calc_fraction(bucket_ptr->start, bucket_ptr->end, lower);
            ratio = ratio > 1.0 ? 1.0 : ratio;
            ratio = ratio < distinct_1 ? distinct_1 : ratio;
            count =  ceil(bucket_ptr->bucket_size * (1.0 - ratio));
        } else {
            ratio = calc_fraction(bucket_ptr->start, bucket_ptr->end, upper) - calc_fraction(bucket_ptr->start, bucket_ptr->end, lower);
            ratio = ratio > 1.0 ? 1.0 : ratio;
            ratio = ratio < distinct_1 ? distinct_1 : ratio;
            count =  ceil(bucket_ptr->bucket_size * ratio);
        }
        if (count < 0) {
            count = 0;
        }
        DB_DEBUG("tmp_lower:%s, tmp_upper%s,start:%s, end:%s, count:%d", 
               lower.get_string().c_str(), upper.get_string().c_str(),
               bucket_ptr->start.get_string().c_str(),bucket_ptr->end.get_string().c_str(), count);
        return count;
    }

    double get_histogram_ratio_dummy(const ExprValue& lower, const ExprValue& upper, int64_t sample_rows) {
        if (_bucket_mapping.size() <= 0) {
            return 1.0;
        }

        if (lower.is_null()) {
            return _bucket_mapping.begin()->second->bucket_size * 1.0 / sample_rows;
        }

        if (upper.is_null()) {
            return _bucket_mapping.rbegin()->second->bucket_size * 1.0 / sample_rows;
        }

        ExprValue start = _bucket_mapping.begin()->second->start;
        ExprValue end   = _bucket_mapping.rbegin()->second->end;
        if (lower.compare(_bucket_mapping.rbegin()->second->end) > 0) {
            end = upper;
        }
        if (upper.compare(_bucket_mapping.begin()->second->start) < 0) {
            start = lower;
        }

        int prefix_len = start.common_prefix_length(end);
        double ret = calc_diff(lower, upper, prefix_len) / calc_diff(start, end, prefix_len);
        return ret > 1.0 ? 1.0 : ret;
    }

    //为避免无效值：lower is_null时取最小值；upper is_null时取最大值
    int32_t get_count(const ExprValue& lower, const ExprValue& upper) {
        if (_bucket_mapping.empty()) {
            return -1;
        }

        ExprValue tmp_lower = lower;
        ExprValue tmp_upper = upper;
        if (lower.is_null()) {
            auto it = _bucket_mapping.begin();
            tmp_lower = it->second->start;
        }

        if (upper.is_null()) {
            auto it = _bucket_mapping.rbegin();
            tmp_upper = it->second->end;
        }

        //超出取值范围返-2，上层需要特殊处理
        if (tmp_lower.compare(_bucket_mapping.rbegin()->second->end) > 0 
           || tmp_upper.compare(_bucket_mapping.begin()->second->start) < 0) {
               DB_DEBUG("tmp_lower:%s, tmp_upper%s,start:%s, end:%s", 
               tmp_lower.get_string().c_str(), tmp_upper.get_string().c_str(),
               _bucket_mapping.begin()->second->start.get_string().c_str(),_bucket_mapping.rbegin()->second->end.get_string().c_str());
            return -2;
        }

        int32_t count = 0;
        auto iter = _bucket_mapping.upper_bound(tmp_upper);
        while (iter != _bucket_mapping.begin()) {
            --iter;
            int64_t k = tmp_lower.compare(iter->second->end);
            if (k > 0) {
                break;
            }
            count += calc_scalar(tmp_lower, tmp_upper, iter->second);
        }

        return count;
    }

    int32_t get_count(const ExprValue& value) {
        if (_bucket_mapping.empty()) {
            return -1;
        }

        if (value.is_null()) {
            return -1;
        }

        if (value.compare(_bucket_mapping.rbegin()->second->end) > 0
            || value.compare(_bucket_mapping.begin()->second->start) < 0) {
            DB_DEBUG("value:%s, start:%s, end:%s", value.get_string().c_str(), _bucket_mapping.begin()->second->start.get_string().c_str(),_bucket_mapping.rbegin()->second->end.get_string().c_str());
            return -2;
        }

        // 找到第一个大于value的值
        auto iter = _bucket_mapping.upper_bound(value);
        if (iter != _bucket_mapping.begin()) {
            --iter;
            if (value.compare(iter->second->end) > 0
                || value.compare(iter->second->start) < 0) {
                DB_DEBUG("value:%s, start:%s, end:%s", value.get_string().c_str(), iter->second->start.get_string().c_str(),iter->second->end.get_string().c_str());
                return -2;
            }
            if (iter->second->distinct_cnt == 0) {
                return -1;
            } else {
                return iter->second->bucket_size / iter->second->distinct_cnt;
            }
        } else {
            return -2;
        }
    } 

private:
    pb::PrimitiveType _type;
    int32_t           _field_id;
    int32_t           _distinct_cnt;
    int32_t           _null_value_cnt;
    HistogramMap      _bucket_mapping;
};

// 对从store获取的抽样行，按每列进行排序，生成列直方图
class SampleSorter {
public:
    SampleSorter(std::vector<std::shared_ptr<RowBatch> >& batch_vector, ExprNode* sort_expr);

    void sort() {
        _sorter->sort();
    }

    int get_next(RowBatch* batch, bool* eos) {
        return _sorter->get_next(batch, eos);
    }

    
    void insert_row(MemRow* row);

    void packet_column(pb::ColumnInfo* column_info);


    void insert_done();
private:
    void insert_distinct_value(const ExprValue& value, const int& cnt);
    std::vector<ExprNode*> _slot_order_exprs;
    std::vector<bool> _is_asc;
    std::vector<bool> _is_null_first;
    std::shared_ptr<MemRowCompare> _mem_row_compare = nullptr;
    std::shared_ptr<Sorter> _sorter = nullptr;
    ExprValue _cur_value;
    int _cur_value_cnt = 0;
    std::vector<BucketInfo> _bucket_infos;
    int _distinct_cnt_total = 0;
    int _null_value_cnt = 0;
    int _expect_bucket_size = 0;
};

// 处理从store获取的抽样行
class PacketSample {
public:
    PacketSample(std::vector<std::shared_ptr<RowBatch> >& batch_vector, 
        std::vector<ExprNode*>& slot_order_exprs, pb::TupleDescriptor* tuple_desc) : 
            _order_exprs(slot_order_exprs), _tuple_desc(tuple_desc) {
        for (auto batch : batch_vector) {
            _batch_vector.push_back(batch);
        }
    }
    

    int packet_sample(pb::Histogram* histogram) {
        int i = 0;
        for (auto expr : _order_exprs) {
            SampleSorter sample_sorter(_batch_vector, expr);
            sample_sorter.sort();
            _batch_vector.clear();
            pb::ColumnInfo* column_info = histogram->add_column_infos();
            column_info->set_col_type(_tuple_desc->slots(i).slot_type());
            column_info->set_field_id(_tuple_desc->slots(i).field_id());

            bool eos = false;
            do {
                std::shared_ptr<RowBatch> batch = std::make_shared<RowBatch>();
                int ret = sample_sorter.get_next(batch.get(), &eos);
                if (ret < 0) {
                    DB_WARNING("get_next fail:%d", ret);
                    return ret;
                }
                for (batch->reset(); !batch->is_traverse_over(); batch->next()) {
                    sample_sorter.insert_row(batch->get_row().get());
                }
                batch->reset();
                _batch_vector.push_back(batch);
            } while (!eos);

            sample_sorter.insert_done();

            sample_sorter.packet_column(column_info);
            i++;
        }
        DB_WARNING("histogram:%s", histogram->ShortDebugString().c_str());
        return 0;
    }

private:
    std::vector<ExprNode*>& _order_exprs;
    pb::TupleDescriptor*    _tuple_desc;
    std::vector<std::shared_ptr<RowBatch> > _batch_vector;
};
}
