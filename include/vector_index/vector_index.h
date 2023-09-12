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
#include <functional> 
#include "key_encoder.h"
#include "table_record.h"
#include "rocks_wrapper.h"
#include "transaction.h"
#include "schema_factory.h"
#include "expr_node.h"
#include <atomic>
#include <map>
#include <faiss/index_io.h>
#include <faiss/index_factory.h>
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {

class VectorIndex {
public:
enum VFlag {
    V_NORMAL = 0,
    V_DELETE = 1
};
    virtual ~VectorIndex() {
        delete _faiss_index;
        _faiss_index = nullptr;
        delete _faiss_flat_index;
        _faiss_flat_index = nullptr;
    }
    void reset() {
        std::lock_guard<bthread::Mutex> lock(_faiss_mutex);
        if (_is_flat) {
            _faiss_index->reset();
        } else {
            _faiss_index->reset();
            _faiss_flat_index->reset();
        }
        _cache_idxs.clear();
        _cache_vectors.clear();
        _faiss_cache_idx = 0;
        _faiss_idx = 0;
        _train_idx = 0;
    }
    int init(const pb::RegionInfo& region_info, int64_t region_id, int64_t index_id, int64_t table_id);
    int restore_index(const pb::RegionInfo& region_info); // 恢复增量
    int write_to_file(const std::string& snapshot_path, std::string& faiss_name) {
        DB_WARNING("dump index:%s", snapshot_path.c_str());
        std::lock_guard<bthread::Mutex> lock(_faiss_mutex);
        _faiss_dump_idx = _faiss_idx;
        char buf[100] = {0};
        snprintf(buf, sizeof(buf), "/faissindex_%ld_%ld_%ld", _index_id, _faiss_dump_idx, _del_count.load());
        faiss_name = buf;
        std::string full_path = snapshot_path + faiss_name;
        faiss::write_index(_faiss_index, full_path.c_str());
        return 0;
    }
    int read_from_file(const std::string& snapshot_path, const std::string& faiss_name) {
        DB_WARNING("read index:%s, faiss_name:%s", snapshot_path.c_str(), faiss_name.c_str());
        std::lock_guard<bthread::Mutex> lock(_faiss_mutex);
        std::vector<std::string> vec;
        vec.reserve(5);
        boost::split(vec, faiss_name, boost::is_any_of("_"));
        if (vec.empty()) {
            return -1;
        }
        if (vec.size() >= 3) {
            _faiss_dump_idx = strtoll(vec[2].c_str(), NULL, 10);
        }
        if (vec.size() >= 4) {
            _del_count = strtoll(vec[3].c_str(), NULL, 10);
        }
        _faiss_idx = _faiss_dump_idx;
        _faiss_cache_idx = _faiss_dump_idx;
        std::string full_path = snapshot_path + faiss_name;
        delete _faiss_index;
        _faiss_index = nullptr;
        _faiss_index = faiss::read_index(full_path.c_str());
        if (_faiss_index == nullptr) {
            return -1;
        }
        DB_WARNING("read index success, index_id:%ld, faiss_dump_idx:%ld, del_count:%ld", _index_id,
                _faiss_dump_idx, _del_count.load());
        return 0;
    }
    int insert_vector(
                       SmartTransaction& txn,
                       const std::string& word, 
                       const std::string& pk,
                       SmartRecord record);
    int delete_vector(
                       SmartTransaction& txn,
                       const std::string& word, 
                       const std::string& pk,
                       SmartRecord record);
    int add_to_faiss(const std::string& word, int64_t cache_idx);
    int search(
                       myrocksdb::Transaction* txn,
                       SmartIndex& pk_info,
                       SmartTable& table_info,
                       const std::string& search_data,
                       int64_t topk,
                       std::vector<SmartRecord>& records);

    int add_to_rocksdb(myrocksdb::Transaction* txn, const std::string& pk, VFlag flag, int64_t& cache_idx);
    int del_to_rocksdb(myrocksdb::Transaction* txn, const std::string& pk, VFlag flag);
    int construct_records(myrocksdb::Transaction* txn, SmartIndex& pk_info, SmartTable& table_info, const std::vector<int64_t>& result_idxs, 
    const std::vector<float>& result_dis, std::vector<SmartRecord>& records);
    int64_t faiss_cache_idx() {
        return _faiss_cache_idx;
    }
    bool need_compact() {
        std::lock_guard<bthread::Mutex> lock(_faiss_mutex);
        int64_t ntotal = 0;
        if (_faiss_index != nullptr) {
            ntotal += _faiss_index->ntotal;
        }
        if (_faiss_flat_index != nullptr) {
            ntotal += _faiss_flat_index->ntotal;
        }
        if (ntotal> 10 && _del_count * 100 / ntotal > 30) {
            DB_WARNING("region_id: %ld, ntotal:%ld, _del_count:%ld", _region_id, ntotal, _del_count.load());
            return true;
        }
        return false;
    }
    // hold rocksdb写
    void diable_write() {
        _disable_write_cond.increase();
    }
    void enable_write() {
        _disable_write_cond.decrease_signal();
    }
    // 交换两个向量，主要用来做compaction
    void swap(VectorIndex* vi) {
        std::lock_guard<bthread::Mutex> lock(_faiss_mutex);
        std::swap(_faiss_index, vi->_faiss_index);
        std::swap(_faiss_flat_index, vi->_faiss_flat_index);
        std::swap(_cache_idxs, vi->_cache_idxs);
        std::swap(_cache_vectors, vi->_cache_vectors);
        _faiss_cache_idx.store(vi->_faiss_cache_idx);
        std::swap(_faiss_idx, vi->_faiss_idx);
        std::swap(_faiss_dump_idx, vi->_faiss_dump_idx);
        std::swap(_train_idx, vi->_train_idx);
        _del_count.store(vi->_del_count);
    }

private:
    faiss::Index* _faiss_index = nullptr;
    faiss::Index* _faiss_flat_index = nullptr; // cache层
    RocksWrapper* _rocksdb = nullptr;
    bthread::Mutex _faiss_mutex;
    std::vector<int64_t> _cache_idxs;
    std::vector<float> _cache_vectors;
    std::string _vector_description; //索引描述，FLAT，IVF1000PQ16，HNSW16等
    std::string _key_prefix;
    std::function<bool(float, float)> _metric_compare = std::less<float>();
    int64_t _region_id = 0;
    int64_t _index_id = 0;
    int64_t _table_id = 0;
    std::atomic<int64_t> _faiss_cache_idx {0}; // cache到了哪个idx(不包含)
    int64_t _faiss_idx = 0; // _faiss_index里保存到哪个idx(不包含)
    int64_t _faiss_dump_idx = 0; // _faiss_index dump到哪个idx(不包含)
    int64_t _train_idx = 0; // ivf索引需要重新train
    std::atomic<int64_t> _del_count {0}; // 已删除向量数，>30%则开始做compaction
    int32_t _dimension = 0; // 维度
    faiss::MetricType _metrix_type = faiss::METRIC_L2;
    int32_t _nprobe = 5;
    bool _is_flat = false;
    bool _is_ivf = false;
    bool _is_l2norm = false;
    bool _need_train = false;
    BthreadCond _disable_write_cond;
};

} // end of namespace

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
