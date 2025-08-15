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
#include <bthread/rwlock.h>

namespace baikaldb {

const std::string FAISS_SCALAR_DATA_PREFIX = "/faissindex_scalardata_";
const std::string FAISS_DEL_BITMAP_PREFIX = "/faissindex_delbitmap_";
const std::string FAISS_NOT_CACHE_FIELDS_PREFIX = "/faissindex_notcachefields_";

class VectorIndex {
    struct FaissIndex;
    using SmartFaissIndex = std::shared_ptr<FaissIndex>;

public:
    enum VFlag { 
        V_NORMAL = 0, 
        V_DELETE = 1,
        // 重启流程，先加载snapshot，增量删除的行如果在snapshot中生成的faiss index中也需要标记bitmap
        // 如果直接使用V_DELETE，第一次重启时，就会将V_DELETE对应的记录删除，第二次重启就恢复不了bitmap，所以需要V_DEELTE_OP类型；
        // TODO - 如何删除无效的V_DELETE_OP记录
        V_DELETE_OP = 2 
    };
    virtual ~VectorIndex() {}
    void reset() {
        if (_is_separate) {
            auto call = [this] (SmartFaissIndex& faiss_index) {
                ScopeGuard guard([&faiss_index] () {
                    if (faiss_index != nullptr) {
                        faiss_index->dec_ref();
                    }
                });
                reset_faiss_index(faiss_index);
            };
            // 空FaissIndex在compaction时删除
            // 空FaissIndex不在这里直接删除，避免此时有数据写入，但是FaissIndex已经从map中删除，导致数据丢失
            _separate_faiss_index_map->traverse_copy(inc_ref_faiss_index, call);
        } else {
            reset_faiss_index(_faiss_index);
        }
    }
    int init(
            const pb::RegionInfo& region_info,
            int64_t region_id,
            int64_t index_id,
            int64_t table_id);
    int restore_index(const pb::RegionInfo& region_info); // 恢复增量
    int write_to_file(const std::string& snapshot_path, std::vector<std::string>& faiss_name_vec) {
        if (_faiss_index == nullptr) {
            DB_WARNING("_faiss_index is nullptr");
            return -1;
        }
        const std::string& scalar_data_file = FAISS_SCALAR_DATA_PREFIX + std::to_string(_index_id);
        const std::string& del_bitmap_file = FAISS_DEL_BITMAP_PREFIX + std::to_string(_index_id);
        const std::string& not_cache_fields_file = FAISS_NOT_CACHE_FIELDS_PREFIX + std::to_string(_index_id);
        const std::string& scalar_data_path = snapshot_path + scalar_data_file;
        const std::string& del_bitmap_path = snapshot_path + del_bitmap_file;
        const std::string& not_cache_fields_path = snapshot_path + not_cache_fields_file;
        bthread::RWLockWrGuard lock(_faiss_index->rwlock);
        if (write_scalar_data_to_file(_faiss_index, scalar_data_path) != 0) {
            DB_WARNING("write scalar data to file failed, index_id:%ld", _index_id);
            return -1;
        }
        if (write_del_bitmap_to_file(_faiss_index, del_bitmap_path) != 0) {
            DB_WARNING("write del bitmap to file failed, index_id:%ld", _index_id);
            return -1;
        }
        if (write_not_cache_fields_to_file(_faiss_index, not_cache_fields_path) != 0) {
            DB_WARNING("write not cache fields to file failed, index_id:%ld", _index_id);
            return -1;
        }
        _faiss_index->dump_idx = _faiss_index->idx;
        char buf[100] = {0};
        snprintf(
                buf,
                sizeof(buf),
                "/faissindex_%ld_%ld_%ld",
                _index_id,
                _faiss_index->dump_idx,
                _faiss_index->del_count.load());
        std::string faiss_name = buf;
        std::string full_path = snapshot_path + faiss_name;
        faiss::write_index(_faiss_index->index, full_path.c_str());
        faiss_name_vec.emplace_back(faiss_name);
        faiss_name_vec.emplace_back(scalar_data_file);
        faiss_name_vec.emplace_back(del_bitmap_file);
        faiss_name_vec.emplace_back(not_cache_fields_file);
        return 0;
    }
    int read_from_file(const std::string& snapshot_path, const std::string& faiss_name) {
        if (_faiss_index == nullptr) {
            DB_WARNING("_faiss_index is nullptr");
            return -1;
        }
        bthread::RWLockWrGuard lock(_faiss_index->rwlock);
        if (_faiss_index->scalar_data->num_rows() == 0) {
            // 如果没有标量数据，直接从Rocksdb中全量恢复
            DB_WARNING("empty scalar_data, faiss_name: %s", faiss_name.c_str());
            return 0;
        }
        std::vector<std::string> vec;
        vec.reserve(5);
        boost::split(vec, faiss_name, boost::is_any_of("_"));
        if (vec.empty()) {
            return -1;
        }
        if (vec.size() >= 3) {
            _faiss_index->dump_idx = strtoll(vec[2].c_str(), NULL, 10);
        }
        if (vec.size() >= 4) {
            _faiss_index->del_count = strtoll(vec[3].c_str(), NULL, 10);
        }
        _faiss_index->idx = _faiss_index->dump_idx;
        _faiss_index->cache_idx = _faiss_index->dump_idx;
        std::string full_path = snapshot_path + faiss_name;
        delete _faiss_index->index;
        _faiss_index->index = nullptr;
        _faiss_index->index = faiss::read_index(full_path.c_str());
        if (_faiss_index->index == nullptr) {
            DB_WARNING("_faiss_index->index is nullptr");
            return -1;
        }
        DB_WARNING(
                "read index success, index_id:%ld, faiss_dump_idx:%ld, del_count:%ld",
                _index_id,
                _faiss_index->dump_idx,
                _faiss_index->del_count.load());
        return 0;
    }
    int write_to_separate_files(const std::string& snapshot_path, std::vector<std::string>& file_name_vec);
    int read_from_separate_file(const std::string& snapshot_path, const std::string& file_name);
    int read_from_cacheinfo_file(const std::string& snapshot_path, const std::string& cacheinfo_file);
    int read_scalar_data_from_file(
            const std::string& snapshot_path, const std::string& file_name);
    int read_del_bitmap_from_file(
            const std::string& snapshot_path, const std::string& file_name);
    int read_not_cache_fields_from_file(
            const std::string& snapshot_path, const std::string& file_name);
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
    int search_vector(
            myrocksdb::Transaction* txn,
            const uint64_t separate_value,
            SmartIndex& pk_info,
            SmartTable& table_info,
            const std::string& search_data,
            int64_t topk,
            int32_t efsearch,
            std::vector<SmartRecord>& records,
            std::vector<ExprNode*>& vector_filter_exprs,
            std::vector<ExprNode*>& scan_filter_exprs,
            std::vector<ExprNode*>& pre_filter_exprs);
    int add_to_faiss(
            SmartFaissIndex faiss_index,
            const std::string& word, 
            int64_t cache_idx,
            SmartRecord record);
    int search(
            myrocksdb::Transaction* txn,
            SmartFaissIndex faiss_index,
            SmartIndex& pk_info,
            SmartTable& table_info,
            const std::string& search_data,
            int64_t topk,
            int32_t efsearch,
            std::vector<SmartRecord>& records,
            std::vector<ExprNode*>& vector_filter_exprs,
            std::vector<ExprNode*>& scan_filter_exprs,
            std::vector<ExprNode*>& pre_filter_exprs);
    int add_to_rocksdb(
            myrocksdb::Transaction* txn,
            SmartFaissIndex faiss_index,
            const std::string& pk,
            VFlag flag,
            int64_t& cache_idx);
    int del_to_rocksdb(
            myrocksdb::Transaction* txn, 
            SmartFaissIndex faiss_index, 
            const std::string& pk, 
            VFlag flag);
    int construct_records(
            myrocksdb::Transaction* txn,
            const uint64_t separate_value,
            SmartIndex& pk_info,
            SmartTable& table_info,
            const std::vector<int64_t>& result_idxs,
            const std::vector<float>& result_dis,
            std::vector<SmartRecord>& records);
    bool is_separate() {
        return _is_separate;
    }
    int compact(const pb::RegionInfo& region_info, const int64_t table_lines, bool is_force);
    int schema_change();
    // snapshot后，truncate掉之前的删除操作记录
    int truncate_del();
    // 根据主表行数和faiss索引行数判断是否需要compact
    bool need_compact(const int64_t table_lines);

private:
    int init_faiss_index(SmartFaissIndex faiss_index);
    int reset_faiss_index(SmartFaissIndex faiss_index);
    int restore_faiss_index(const pb::RegionInfo& region_info, SmartFaissIndex faiss_index);
    int compact_faiss_index(const pb::RegionInfo& region_info, SmartFaissIndex faiss_index, bool is_force);
    int schema_change_faiss_index(SmartFaissIndex faiss_index);
    int truncate_del_faiss_index(SmartFaissIndex faiss_index);
    bool need_compact_faiss_index(SmartFaissIndex faiss_index);
    bool need_erase_faiss_index(SmartFaissIndex faiss_index, bool is_force);
    // 遍历rocksdb，恢复缓存结构_separate_faiss_index_map
    int build_separate_faiss_index_map();
    int mark_del_bitmap(SmartFaissIndex faiss_index, const int64_t idx);
    int read_scalar_data_from_file(SmartFaissIndex faiss_index, const std::string& full_file_name);
    int read_del_bitmap_from_file(SmartFaissIndex faiss_index, const std::string& full_file_name);
    int read_not_cache_fields_from_file(SmartFaissIndex faiss_index, const std::string& full_file_name);
    int write_scalar_data_to_file(SmartFaissIndex faiss_index, const std::string& full_file_name);
    int write_del_bitmap_to_file(SmartFaissIndex faiss_index, const std::string& full_file_name);
    int write_not_cache_fields_to_file(SmartFaissIndex faiss_index, const std::string& full_file_name);

    int get_separate_value(SmartRecord record, uint64_t& separate_value) {
        if (!_is_separate) {
            DB_WARNING("vector index is not separate");
            return -1;
        }
        if (record == nullptr) {
            DB_WARNING("record is nullptr");
            return -1;
        }
        const FieldDescriptor* separate_field_desc = record->get_field_by_tag(_separate_field_id);
        if (separate_field_desc == nullptr) {
            DB_WARNING("separate_field_desc is nullptr");
            return -1;
        }
        separate_value = record->get_value(separate_field_desc).cast_to(pb::UINT64).get_numberic<uint64_t>();
        return 0;
    }

    SmartFaissIndex get_or_create_faiss_index(const uint64_t separate_value) {
        SmartFaissIndex faiss_index = _separate_faiss_index_map->get(separate_value, inc_ref_faiss_index);
        if (faiss_index == nullptr) {
            SmartFaissIndex faiss_index_new = std::make_shared<FaissIndex>();
            if (init_faiss_index(faiss_index_new) != 0) {
                DB_WARNING("Fail to init_faiss_index");
                return nullptr;
            }
            faiss_index_new->separate_value = separate_value;
            auto create_call = [this, separate_value, faiss_index_new] (SmartFaissIndex& faiss_index) -> bool {
                faiss_index = faiss_index_new;
                return true;
            };
            faiss_index = 
                _separate_faiss_index_map->get_or_put_call(separate_value, create_call, inc_ref_faiss_index);
            if (faiss_index == nullptr) {
                DB_FATAL("faiss_index is nullptr");
                return nullptr;
            }
        }
        return faiss_index;
    }

private:
    struct FaissIndex {
        bthread::RWLock rwlock;               // bthread读写锁
        faiss::Index* index = nullptr;        // 索引层
        faiss::Index* flat_index = nullptr;   // cache层
        std::atomic<int64_t> cache_idx {0};   // cache到了哪个idx(不包含)
        int64_t idx {0};                      // faiss_index里保存到哪个idx(不包含)
        int64_t dump_idx {0};                 // faiss_index dump到哪个idx(不包含)
        int64_t train_idx {0};                // ivf索引需要重新train
        std::atomic<int64_t> del_count {0};   // 已删除向量数，>30%则开始做compaction
        uint64_t separate_value {0};
        BthreadCond disable_write_cond;
        BthreadCond real_writing_cond;
        TimeCost empty_time_cost;             // FaissIndex为空的时长
        std::atomic<int64_t> ref_cnt {0};     // 引用计数，用于删除空FaissIndex
        
        std::shared_ptr<Roaring> flat_del_bitmap;             // flat层删除bitmap
        std::shared_ptr<Roaring> del_bitmap;                  // 删除bitmap
        std::shared_ptr<arrow::RecordBatch> flat_scalar_data; // flat层标量数据
        std::shared_ptr<arrow::RecordBatch> scalar_data;      // 标量数据
        std::unordered_set<int32_t> need_not_cache_fields;    // 不需要缓存的标量字段

        TimeCost last_compaction_ts;          // 最后一次compaction时间

        enum class Status {
            IDLE = 0,
            DOING
        };
        std::atomic<Status> status {Status::IDLE};

        ~FaissIndex() {
            if (index != nullptr) {
                delete index;
                index = nullptr;
            }
            if (flat_index != nullptr) {
                delete flat_index;
                flat_index = nullptr;
            }
        }

        void inc_ref() {
            ++ref_cnt;
        }
        void dec_ref() {
            --ref_cnt;
        }
        void swap(SmartFaissIndex faiss_index) {
            if (faiss_index == nullptr) {
                return;
            }
            bthread::RWLockWrGuard lock(rwlock);
            std::swap(index, faiss_index->index);
            std::swap(flat_index, faiss_index->flat_index);
            cache_idx.store(faiss_index->cache_idx);
            std::swap(idx, faiss_index->idx);
            std::swap(dump_idx, faiss_index->dump_idx);
            std::swap(train_idx, faiss_index->train_idx);
            del_count.store(faiss_index->del_count);
            std::swap(separate_value, faiss_index->separate_value);
            std::swap(flat_del_bitmap, faiss_index->flat_del_bitmap);
            std::swap(del_bitmap, faiss_index->del_bitmap);
            std::swap(flat_scalar_data, faiss_index->flat_scalar_data);
            std::swap(scalar_data, faiss_index->scalar_data);
            std::swap(need_not_cache_fields, faiss_index->need_not_cache_fields);
        }
    };

    static void inc_ref_faiss_index(SmartFaissIndex faiss_index) {
        if (faiss_index != nullptr) {
            faiss_index->inc_ref();
        }
    }

    bool _is_separate = false; // 向量索引是否进行隔离
    SmartFaissIndex _faiss_index = nullptr; // 正常向量索引
    std::shared_ptr<ThreadSafeMap<uint64_t, SmartFaissIndex>> _separate_faiss_index_map; // 向量隔离索引

    RocksWrapper* _rocksdb = nullptr;
    std::string _vector_description;  //索引描述，FLAT，IVF1000PQ16，HNSW16等
    std::string _key_prefix;
    std::function<bool(float, float)> _metric_compare = std::less<float>();
    int64_t _region_id = 0;
    int64_t _index_id = 0;
    int64_t _table_id = 0;
    int32_t _dimension = 0;                    // 维度
    faiss::MetricType _metrix_type = faiss::METRIC_L2;
    int32_t _nprobe = 5;
    int32_t _efsearch = 16;
    int32_t _efconstruction = 40;
    bool _is_flat = false;
    bool _is_hnsw = false;
    bool _is_l2norm = false;
    int32_t _separate_field_id = -1;
};

}  // namespace baikaldb

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
