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
#include "hll_common.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {
class HyperLogLogColumn {
public:
    explicit HyperLogLogColumn(int field_id): _field_id(field_id) {}
    HyperLogLogColumn(int field_id, uint64_t estimate_count): 
            _field_id(field_id), _estimate_count(estimate_count) {}

    explicit HyperLogLogColumn(const pb::HyperLogLogColumn& hll_pb) {
        _field_id = hll_pb.field_id();
        _estimate_count = hll_pb.estimate_count();
        if (hll_pb.has_hll() && !hll_pb.hll().empty()) {
            _hll = hll_pb.hll();
        }
    }
    
    void init_hll() {
        hll::hll_sparse_init(_hll);
    }

    void add_value(int64_t hash_value) {
        hll::hll_add(_hll, hash_value);
    }

    int add_proto(const pb::HyperLogLogColumn& hll_pb) {
        if (_hll.empty()) {
            init_hll();
        }
        std::string hll2 = hll_pb.hll();
        return add_string(hll2);
    }

    uint64_t estimate() {
        if (_hll.empty()) {
            return _estimate_count;
        }
        return hll::hll_estimate(_hll);
    }

    void to_proto(pb::HyperLogLogColumn* pb_hllcolumn, bool keep_hll_str) {
        pb_hllcolumn->set_field_id(_field_id);
        if (keep_hll_str) {
            pb_hllcolumn->set_hll(_hll);
        }
        pb_hllcolumn->set_estimate_count(estimate());
    }

private:
    int add_string(std::string& hll2) {
        if (_add_failed == -1) {
            return -1;
        }
        // 传入的string不是hll类型可能失败，返回-1
        _add_failed = hll::hll_merge(this->_hll, hll2);
        return _add_failed;
    }

private:
    int _field_id;
    std::string _hll;
    uint64_t _estimate_count;
    int _add_failed = 0;
};

// 表的HyperLogLog统计信息
class HyperLogLog {
public:
    HyperLogLog() {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~HyperLogLog() {
        bthread_mutex_destroy(&_mutex);
    }

    void add_proto(const pb::HyperLogLog& pb_hll) {
        BAIDU_SCOPED_LOCK(_mutex);
        for (const auto& hllcolumn: pb_hll.hllcolumns()) {
            if (0 == _column_hll.count(hllcolumn.field_id())) {
                auto ptr = std::make_shared<HyperLogLogColumn>(hllcolumn.field_id());
                ptr->init_hll();
                _column_hll[hllcolumn.field_id()] = ptr;
            }
            _column_hll[hllcolumn.field_id()]->add_proto(hllcolumn);
        }
    }

    void add_value(int field_id, int64_t hash_value) {
        BAIDU_SCOPED_LOCK(_mutex);
        auto it = _column_hll.find(field_id);
        if (it != _column_hll.end()) {
            it->second->add_value(hash_value);
        } else {
            auto ptr = std::make_shared<HyperLogLogColumn>(field_id);
            _column_hll[field_id] = ptr;
            ptr->init_hll();
            ptr->add_value(hash_value);
        }
    }

    // 只有store和db交互需要保留hll_str，db和meta交互不需要
    void to_proto(pb::HyperLogLog* pb_hll, bool keep_hll_str) {
        bthread_mutex_t _mutex;
        for (auto it: _column_hll){
            pb::HyperLogLogColumn* hllcloumn = pb_hll->add_hllcolumns();
            it.second->to_proto(hllcloumn, keep_hll_str);
        }
    }

private:
    bthread_mutex_t _mutex;
    std::map<int, std::shared_ptr<HyperLogLogColumn>> _column_hll;
};

} // namespace baikal
