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
#include "cmsketch.h"
#include "histogram.h"

namespace baikaldb {
class Statistics {
public:
    Statistics(const pb::Statistics& statistics) {
        _table_id = statistics.table_id();
        _version = statistics.version();
        init_histogram(statistics.histogram());
        init_cmsketch(statistics.cmsketch());
    }

    int64_t table_id() {
        return _table_id;
    }

    int64_t version() {
        return _version;
    }

    int64_t total_rows() const {
        return _total_rows;
    }

    std::shared_ptr<CMsketchColumn> get_cmsketchcolumn_ptr(int field_id) {
        if (field_id <= 0) {
            return nullptr;
        }
        auto iter = _field_cmsketch.find(field_id);
        if (iter != _field_cmsketch.end()) {
            return iter->second;
        }
        return nullptr;
    }

    std::shared_ptr<Histogram> get_histogram_ptr(int field_id) {
        if (field_id <= 0) {
            return nullptr;
        }
        auto iter = _field_histogram.find(field_id);
        if (iter != _field_histogram.end()) {
            return iter->second;
        }
        return nullptr;
    }

    void histogram_to_string(std::vector<std::vector<std::string>>& rows, std::vector<ResultField>& fields) {
        if (_field_histogram.size() != fields.size()) {
            DB_FATAL("use select * from table_name");
            return;
        }
        int i = 0;
        for (auto iter = _field_histogram.begin(); iter != _field_histogram.end(); iter++) {
            std::vector<std::string> row;
            row.push_back(std::to_string(iter->first));
            row.push_back(fields[i++].name);
            row.push_back(std::to_string(iter->second->get_distinct_cnt()));
            row.push_back(std::to_string(iter->second->get_null_value_cnt()));
            row.push_back(std::to_string(iter->second->get_bucket_count()));
            rows.push_back(row);
        }
    }

    // 如果计算小于某值的个数则lower置为null (< upper)，如果计算大于某值得个数则uppder置为null (> lower)
    int64_t get_histogram_count(const int field_id, const ExprValue& lower, const ExprValue& upper) {
        auto iter = _field_histogram.find(field_id);
        if (iter == _field_histogram.end()) {
            return -1;
        }

        return iter->second->get_count(lower, upper);
    }

    int64_t get_histogram_count(const int field_id, const ExprValue& value) {
        auto iter = _field_histogram.find(field_id);
        if (iter == _field_histogram.end()) {
            return -1;
        }

        int64_t count = iter->second->get_count(value);
        return count;
    }

    //get_histogram_count返-2时说明超出取值范围时，根据need_mapping标记判断是否映射到已存在的范围，默认进行映射
    double get_histogram_ratio(const int field_id, const ExprValue& lower, const ExprValue& upper, bool need_mapping = true) {
        if (_sample_rows == 0) {
            return 1.0;
        }
        
        int64_t cnt = get_histogram_count(field_id, lower, upper);
        if (cnt == -2) {
            if (need_mapping) {
                return _field_histogram[field_id]->get_histogram_ratio_dummy(lower, upper, _sample_rows);
            } else {
                return -1.0;
            }
        } else if (cnt == -1) {
            return 1.0;
        }
        return static_cast<double>(cnt) / static_cast<double>(_sample_rows);
    }

    int64_t get_cmsketch_count(const int field_id, const ExprValue& value) {
        auto iter = _field_cmsketch.find(field_id);
        if (iter == _field_cmsketch.end()) {
            return 0;
        }

        return iter->second->get_value(value.hash());
    }

    int get_cmsketch_width(const int field_id) {
        auto iter = _field_cmsketch.find(field_id);
        if (iter == _field_cmsketch.end()) {
            return -1;
        }

        return iter->second->get_width();
    }

    double get_cmsketch_ratio(const int field_id, const ExprValue& value) {
        if (_total_rows == 0) {
            return 1.0;
        }

        int64_t distinct_cnt = get_distinct_cnt(field_id);
        int64_t value_cnt = get_histogram_count(field_id, value);
        if (value_cnt <= 0) {
            return 1.0 / distinct_cnt;
        } else {
            return value_cnt * 1.0 / _sample_rows;
        }
    }

    int64_t get_sample_cnt() {
        return _sample_rows;
    }

    int64_t get_distinct_cnt(int field_id) {
        auto iter = _field_histogram.find(field_id);
        if (iter == _field_histogram.end()) {
            return -1;
        }

        return iter->second->get_distinct_cnt();
    }

private:
    void init_histogram(const pb::Histogram& histogram) {
        _sample_rows = histogram.sample_rows();
        _total_rows = histogram.total_rows();
        for (auto& column : histogram.column_infos()) {
            auto ptr = std::make_shared<Histogram>(column.col_type(), column.field_id(), column.distinct_cnt(), column.null_value_cnt());
            ptr->add_proto(column);
            _field_histogram[column.field_id()] = ptr;
        }
    }

    void init_cmsketch(const pb::CMsketch& cmsketch) {
        int depth = cmsketch.depth();
        int width = cmsketch.width();
        for (auto& column : cmsketch.cmcolumns()) {
            auto ptr = std::make_shared<CMsketchColumn>(depth, width, column.field_id());
            ptr->add_proto(column);
            _field_cmsketch[column.field_id()] = ptr;
            if (_total_rows <= 0) {
                _total_rows = ptr->get_total_rows();
            }
        }
    }
private:
    int64_t _table_id = 0;
    int64_t _version = 0;
    int64_t _sample_rows = 0;
    int64_t _total_rows = 0;
    std::map<int, std::shared_ptr<Histogram>> _field_histogram;
    std::map<int, std::shared_ptr<CMsketchColumn>> _field_cmsketch;
};

typedef std::shared_ptr<Statistics> SmartStatistics;
}
