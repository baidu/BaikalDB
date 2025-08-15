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

#include "vector_index.h"
#include "concurrency.h"
#include "vectorize_helpper.h"
#include "transaction.h"
#include <arrow/io/file.h>
#include <faiss/AutoTune.h>
#include <faiss/IndexIDMap.h>
#include <faiss/impl/IDSelector.h>
#include <faiss/impl/HNSW.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include "arrow/vendored/fast_float/fast_float.h"

namespace baikaldb {
DEFINE_int32(efsearch_retry_pow, 4, "efsearch_retry_pow");
DEFINE_int32(max_efsearch, 16384, "max_efsearch");
DEFINE_int64(compaction_interval_s, 24 * 60 * 60, "vector compaction interval(s)");
DEFINE_bool(need_brute_force_when_insufficient, true, "need brute force when sufficient");
DEFINE_double(table_lines_compaction_threshold, 0.7, "compaction threshold");

class IDSelectorBitmap : public faiss::IDSelectorBitmapUserDefine {
public:
    IDSelectorBitmap(std::shared_ptr<arrow::BooleanArray> filter_bitmap, 
                     std::shared_ptr<Roaring> del_bitmap)
                     : _filter_bitmap(filter_bitmap)
                     , _del_bitmap(del_bitmap) {
    }
    virtual ~IDSelectorBitmap() {
    }
    virtual bool is_member(int64_t id) const override {
        if (id < 0) {
            // 不应该出现该场景
            return false;
        } 
        bool is_not_filter = true;
        bool is_not_del = true;
        if (_filter_bitmap != nullptr) {
            if (id >= _filter_bitmap->length()) {
                // 不应该出现该场景
                is_not_filter = false;
            } else {
                is_not_filter = _filter_bitmap->Value(id);
            }
        }
        if (_del_bitmap != nullptr) {
            is_not_del = !_del_bitmap->contains(id);
        }
        return is_not_filter && is_not_del;
    }
    virtual int64_t false_count() const override {
        // TODO - 将del_bitmap的信息也统计上
        if (_filter_bitmap == nullptr) {
            return 0;
        }
        return _filter_bitmap->false_count();
    }

private:
    std::shared_ptr<arrow::BooleanArray> _filter_bitmap;
    std::shared_ptr<Roaring> _del_bitmap;
};

int VectorIndex::init(const pb::RegionInfo& region_info, int64_t region_id, int64_t index_id, int64_t table_id) {
    _rocksdb = RocksWrapper::get_instance();
    _region_id = region_id;
    _index_id = index_id;
    _table_id = table_id;
    auto factory = SchemaFactory::get_instance();
    SmartIndex index_info = factory->get_index_info_ptr(_index_id);
    IndexInfo& info = *index_info;
    if (info.fields.size() != 1 && info.fields.size() != 2) {
        DB_WARNING("Invalid fields size: %lu", info.fields.size());
        return -1;
    }
    if (info.fields.size() > 1) {
        _is_separate = true;
        _separate_field_id = info.fields[0].id;
    }
    _dimension = info.dimension;
    if (_dimension <= 0) {
        DB_WARNING("dimension fail:%d", _dimension);
        return -1;
    }
    // TODO: 后续可以用正则匹配
    if (info.vector_description.empty()) {
        _vector_description = "IDMap2,HNSW16";
        _is_hnsw = true;
    } else if (info.vector_description == "Flat") {
        _vector_description = "IDMap2,Flat";
        _is_flat = true;
    } else if (info.vector_description == "L2norm,Flat") {
        _vector_description = "IDMap2,L2norm,Flat";
        _is_flat = true;
        _is_l2norm = true;
    } else if (boost::istarts_with(info.vector_description, "HNSW")) {
        _vector_description = "IDMap2," + info.vector_description;
        _is_hnsw = true;
    } else if (boost::istarts_with(info.vector_description, "L2norm,HNSW")) {
        _vector_description = "IDMap2," + info.vector_description;
        _is_l2norm = true;
        _is_hnsw = true;
    } else {
        DB_WARNING("not support description:%s", info.vector_description.c_str());
        return -1;
    }
    _metrix_type = static_cast<faiss::MetricType>(info.metric_type);
    if (_metrix_type == faiss::METRIC_INNER_PRODUCT) {
        _metric_compare = std::greater<float>();
    }
    _nprobe = info.nprobe;
    _efsearch = info.efsearch;
    _efconstruction = info.efconstruction;

    // 向量隔离索引运行时动态创建FaissIndex，正常向量索引初始化时创建FaissIndex
    if (_is_separate) {
        _separate_faiss_index_map = std::make_shared<ThreadSafeMap<uint64_t, SmartFaissIndex>>();
        if (_separate_faiss_index_map == nullptr) {
            DB_WARNING("_separate_faiss_index_map is nullptr");
            return -1;
        }
    } else {
        _faiss_index = std::make_shared<FaissIndex>();
        if (_faiss_index == nullptr) {
            DB_WARNING("_faiss_index is nullptr");
            return -1;
        }
        if (init_faiss_index(_faiss_index) != 0) {
            DB_WARNING("Fail to init_faiss_index");
            return -1;
        }
    }

    uint64_t region_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_region_id));
    _key_prefix.append((char*)&region_encode, sizeof(uint64_t));
    uint64_t table_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_index_id));
    _key_prefix.append((char*)&table_encode, sizeof(uint64_t));

    DB_WARNING("region_id: %ld, index_id: %ld, vector_description:%s, metrix_type:%d, "
               "dimension:%d, nprobe:%d, efSearch:%d, efConstruction:%d, _is_separate: %d", 
                _region_id, _index_id, _vector_description.c_str(), _metrix_type, 
                _dimension, _nprobe, _efsearch, _efconstruction, _is_separate);

    return 0;
}

int VectorIndex::restore_index(const pb::RegionInfo& region_info) {
    if (_is_separate) {
        // 重启场景，需要恢复内存结构_separate_faiss_index_map
        if (build_separate_faiss_index_map() != 0) {
            DB_FATAL("Fail to build_separate_faiss_index_map");
            return -1;
        }
        std::atomic<bool> is_succ = true;
        auto call = [this, &is_succ, &region_info] (SmartFaissIndex& faiss_index) {
            ScopeGuard guard([&faiss_index] () {
                if (faiss_index != nullptr) {
                    faiss_index->dec_ref();
                }
            });
            if (!is_succ) {
                return;
            }
            if (restore_faiss_index(region_info, faiss_index) != 0) {
                DB_FATAL("fail restore_faiss_index");
                is_succ = false;
                return;
            }
        };
        _separate_faiss_index_map->traverse_copy(inc_ref_faiss_index, call);
        if (!is_succ) {
            DB_WARNING("fail to restore_faiss_index");
            return -1;
        }
    } else {
        if (restore_faiss_index(region_info, _faiss_index) != 0) {
            DB_WARNING("fail restore_faiss_index");
            return -1;
        }
    }
    return 0;
}

int VectorIndex::write_to_separate_files(
        const std::string& snapshot_path, std::vector<std::string>& file_name_vec) {
    // 将flat缓存信息单独存在一个文件中，避免使用文件名存储缓存信息，导致文件过多
    // flat缓存信息文件名: faissindex_indexid_cacheinfo, 文件内容: userid\tdump_idx\tdel_count
    const std::string& cacheinfo_file = "/faissindex_" + std::to_string(_index_id) + "_cacheinfo";
    const std::string& cacheinfo_path = snapshot_path + cacheinfo_file;
    std::ofstream extra_fs(cacheinfo_path, std::ofstream::out | std::ofstream::trunc);
    if (!extra_fs.is_open()) {
        DB_WARNING("Fail to oepn file: %s", cacheinfo_path.c_str());
        return -1;
    }
    ScopeGuard auto_decrease([&extra_fs] () {
        extra_fs.close();
    });
    std::atomic<bool> is_succ = true;
    auto call = [this, &is_succ, &extra_fs, &file_name_vec, &snapshot_path] (SmartFaissIndex& faiss_index) {
        ScopeGuard guard([&faiss_index] () {
            if (faiss_index != nullptr) {
                faiss_index->dec_ref();
            }
        });
        if (!is_succ) {
            return;
        }
        if (faiss_index == nullptr) {
            DB_WARNING("faiss_index is nullptr, index_id: %ld", _index_id);
            is_succ = false;
            return;
        }
        const std::string& scalar_data_file = FAISS_SCALAR_DATA_PREFIX + 
                                              std::to_string(_index_id) + "_" + 
                                              std::to_string(faiss_index->separate_value);
        const std::string& del_bitmap_file = FAISS_DEL_BITMAP_PREFIX + 
                                             std::to_string(_index_id) + "_" + 
                                             std::to_string(faiss_index->separate_value);
        const std::string& not_cache_fields_file = FAISS_NOT_CACHE_FIELDS_PREFIX + 
                                                   std::to_string(_index_id) + "_" + 
                                                   std::to_string(faiss_index->separate_value);
        const std::string& scalar_data_path = snapshot_path + scalar_data_file;
        const std::string& del_bitmap_path = snapshot_path + del_bitmap_file;
        const std::string& not_cache_fields_path = snapshot_path + not_cache_fields_file;
        // /faissindex_indexid_separatevalue
        char buf[100] = {0};
        snprintf(
                buf,
                sizeof(buf),
                "/faissindex_%ld_%lu",
                _index_id,
                faiss_index->separate_value);
        std::string faiss_name = buf;
        std::string full_path = snapshot_path + faiss_name;
        file_name_vec.emplace_back(faiss_name);
        file_name_vec.emplace_back(scalar_data_file);
        file_name_vec.emplace_back(del_bitmap_file);
        file_name_vec.emplace_back(not_cache_fields_file);
        bthread::RWLockWrGuard lock(faiss_index->rwlock);
        // 写入标量缓存数据和删除bitmap
        if (write_scalar_data_to_file(faiss_index, scalar_data_path) != 0) {
            DB_WARNING("Fail to write_scalar_data_to_file");
            is_succ = false;
            return;
        }
        if (write_del_bitmap_to_file(faiss_index, del_bitmap_path) != 0) {
            DB_WARNING("Fail to write_del_bitmap_to_file");
            is_succ = false;
            return;
        }
        if (write_not_cache_fields_to_file(faiss_index, not_cache_fields_path) != 0) {
            DB_WARNING("Fail to write_not_cache_fields_to_file");
            is_succ = false;
            return;
        }
        faiss_index->dump_idx = faiss_index->idx;
        faiss::write_index(faiss_index->index, full_path.c_str());
        extra_fs << faiss_index->separate_value << "\t" 
                 << faiss_index->dump_idx << "\t"
                 << faiss_index->del_count.load() << "\n";
    };
    _separate_faiss_index_map->traverse_copy(inc_ref_faiss_index, call);
    if (!is_succ) {
        DB_WARNING("fail to write_to_separate_files");
        return -1;
    }
    file_name_vec.emplace_back(cacheinfo_file);
    return 0;
}

int VectorIndex::read_from_separate_file(
        const std::string& snapshot_path, const std::string& file_name) {
    const std::string& cacheinfo_file = "/faissindex_" + std::to_string(_index_id) + "_cacheinfo";
    if (file_name == cacheinfo_file) {
        if (read_from_cacheinfo_file(snapshot_path, cacheinfo_file) != 0) {
            DB_WARNING("Fail to read_from_cacheinfo_file");
            return -1;
        }
    } else {
        std::vector<std::string> vec;
        vec.reserve(3);
        boost::split(vec, file_name, boost::is_any_of("_"));
        if (vec.size() < 3) {
            DB_WARNING("Invalid file_name: %s", file_name.c_str());
            return -1;
        }
        const uint64_t separate_value = strtoull(vec[2].c_str(), NULL, 10);
        SmartFaissIndex faiss_index = get_or_create_faiss_index(separate_value);
        ScopeGuard auto_decrease([this, &faiss_index] () {
            if (_is_separate && faiss_index != nullptr) {
                faiss_index->dec_ref();
            }
        });
        if (faiss_index == nullptr) {
            DB_WARNING("faiss_index is nullptr, index_id: %ld, separate_value: %lu", _index_id, separate_value);
            return -1;
        }
        const std::string& full_path = snapshot_path + file_name;
        bthread::RWLockWrGuard lock(faiss_index->rwlock);
        if (faiss_index->scalar_data->num_rows() == 0) {
            return 0;
        }
        delete faiss_index->index;
        faiss_index->index = nullptr;
        faiss_index->index = faiss::read_index(full_path.c_str());
        if (faiss_index->index == nullptr) {
            DB_WARNING("faiss_index->index is nullptr, full_path: %s", full_path.c_str());
            return -1;
        }
    }
    return 0;
}

int VectorIndex::read_from_cacheinfo_file(const std::string& snapshot_path, const std::string& cacheinfo_file) {
    const std::string& full_path = snapshot_path + cacheinfo_file;
    std::ifstream extra_fs(full_path);
    if (!extra_fs.is_open()) {
        DB_WARNING("Fail to open file: %s", full_path.c_str());
        return -1;
    }
    ScopeGuard auto_decrease([&extra_fs] () {
        extra_fs.close();
    });
    std::string line;
    while (std::getline(extra_fs, line)) {
        std::vector<std::string> vec;
        vec.reserve(3);
        boost::split(vec, line, boost::is_any_of("\t"));
        if (vec.size() != 3) {
            DB_WARNING("Invalid line: %s.", line.c_str());
            continue;
        }
        uint64_t separate_value = strtoull(vec[0].c_str(), NULL, 10);
        int64_t dump_idx = strtoll(vec[1].c_str(), NULL, 10);
        int64_t del_count = strtoll(vec[2].c_str(), NULL, 10);
        SmartFaissIndex faiss_index = get_or_create_faiss_index(separate_value);
        ScopeGuard auto_decrease([this, &faiss_index] () {
            if (_is_separate && faiss_index != nullptr) {
                faiss_index->dec_ref();
            }
        });
        if (faiss_index == nullptr) {
            DB_WARNING("faiss_index is nullptr, index_id: %ld, separate_value: %lu", _index_id, separate_value);
            return -1;
        }
        bthread::RWLockWrGuard lock(faiss_index->rwlock);
        if (faiss_index->scalar_data->num_rows() == 0) {
            return 0;
        }
        faiss_index->dump_idx = dump_idx;
        faiss_index->idx = faiss_index->dump_idx;
        faiss_index->cache_idx = faiss_index->dump_idx;
        faiss_index->del_count = del_count;
        DB_WARNING(
            "read index success, index_id:%ld, faiss_dump_idx:%ld, del_count:%ld",
            _index_id,
            faiss_index->dump_idx,
            faiss_index->del_count.load());
    }
    return 0;
}

int VectorIndex::insert_vector(
                   SmartTransaction& txn,
                   const std::string& word, 
                   const std::string& pk,
                   SmartRecord record) {
    int64_t cache_idx = 0;
    SmartFaissIndex faiss_index;
    ScopeGuard auto_decrease([this, &faiss_index] () {
        if (_is_separate && faiss_index != nullptr) {
            faiss_index->dec_ref();
        }
    });
    uint64_t separate_value = 0;
    if (_is_separate) {
        if (get_separate_value(record, separate_value) != 0) {
            DB_WARNING("Fail to get_separate_value");
            return -1;
        }
        faiss_index = get_or_create_faiss_index(separate_value);
    } else {
        faiss_index = _faiss_index;
    }
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    faiss_index->disable_write_cond.wait();
    faiss_index->real_writing_cond.increase();
    ScopeGuard write_auto_decrease([&faiss_index]() {
        faiss_index->real_writing_cond.decrease_signal();
    });
    // 扩大锁范围，解决乱序问题，TODO - 锁范围是否过大
    bthread::RWLockWrGuard lock(faiss_index->rwlock);
    int ret = add_to_rocksdb(txn->get_txn(), faiss_index, pk, V_NORMAL, cache_idx);
    if (ret != 0) {
        return ret;
    }
    return add_to_faiss(faiss_index, word, cache_idx, record);
}

int VectorIndex::delete_vector(
                   SmartTransaction& txn,
                   const std::string& word, 
                   const std::string& pk,
                   SmartRecord record) {
    SmartFaissIndex faiss_index;
    if (_is_separate) {
        uint64_t separate_value = 0;
        if (get_separate_value(record, separate_value) != 0) {
            DB_WARNING("Fail to get_separate_value");
            return -1;
        }
        faiss_index = _separate_faiss_index_map->get(separate_value);
    } else {
        faiss_index = _faiss_index;
    }
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    faiss_index->del_count++;
    faiss_index->disable_write_cond.wait();
    faiss_index->real_writing_cond.increase();
    ScopeGuard write_auto_decrease([&faiss_index]() {
        faiss_index->real_writing_cond.decrease_signal();
    });
    return del_to_rocksdb(txn->get_txn(), faiss_index, pk, V_DELETE);
}

int VectorIndex::search_vector(
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
            std::vector<ExprNode*>& pre_filter_exprs) {
    SmartFaissIndex faiss_index;
    if (_is_separate) {
        faiss_index = _separate_faiss_index_map->get(separate_value);
        if (faiss_index == nullptr) {
            // 没有数据，直接返回
            return 0;
        }
    } else {
        faiss_index = _faiss_index;
    }
    return search(txn, faiss_index, pk_info, table_info, search_data, topk, efsearch, 
                  records, vector_filter_exprs, scan_filter_exprs, pre_filter_exprs);
}

int VectorIndex::add_to_rocksdb(
                    myrocksdb::Transaction* txn, 
                    SmartFaissIndex faiss_index,
                    const std::string& pk, 
                    VFlag flag, 
                    int64_t& cache_idx) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    // region_id,index_id,0,faiss_idx => flag,pk
    std::string key = _key_prefix;
    uint8_t level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    if (_is_separate) {
        // region_id,index_id,0,separate_value,faiss_idx => flag,pk
        uint64_t separate_value_encode = KeyEncoder::to_endian_u64(faiss_index->separate_value);
        key.append((char*)&separate_value_encode, sizeof(uint64_t));
    }
    cache_idx = faiss_index->cache_idx.fetch_add(1);
    uint64_t idx_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(cache_idx));
    key.append((char*)&idx_encode, sizeof(uint64_t));
    std::string value;
    value.append(1, (char)flag);
    value.append(pk);
    auto data_cf = _rocksdb->get_data_handle();
    auto put_res = txn->Put(data_cf, key, value);
    if (!put_res.ok()) {
        DB_WARNING("rocksdb put error: code=%d, msg=%s",
                    put_res.code(), put_res.ToString().c_str());
        return -1;
    }
    // region_id,index_id,1,pk => faiss_idx
    key = _key_prefix;
    level = 1;
    key.append((char*)&level, sizeof(uint8_t));
    key.append(pk);
    value.clear();
    if (_is_separate) {
        // region_id,index_id,1,pk => separate_value,faiss_idx
        uint64_t separate_value_encode = KeyEncoder::to_endian_u64(faiss_index->separate_value);
        value.append((char*)&separate_value_encode, sizeof(uint64_t));
    }
    value.append((char*)&idx_encode, sizeof(uint64_t));
    put_res = txn->Put(data_cf, key, value);
    if (!put_res.ok()) {
        DB_WARNING("rocksdb put error: code=%d, msg=%s",
                    put_res.code(), put_res.ToString().c_str());
        return -1;
    }
    return 0;
}

int VectorIndex::del_to_rocksdb(
        myrocksdb::Transaction* txn, 
        SmartFaissIndex faiss_index,
        const std::string& pk, 
        VFlag flag) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    std::string key = _key_prefix;
    key = _key_prefix;
    uint8_t level = 1;
    key.append((char*)&level, sizeof(uint8_t));
    key.append(pk);
    rocksdb::ReadOptions read_opt;
    read_opt.fill_cache = true;
    rocksdb::PinnableSlice pin_slice;
    rocksdb::Status res;
    auto data_cf = _rocksdb->get_data_handle();
    std::string value;
    res = txn->Get(read_opt, data_cf, key, &value);
    if (!res.ok()) {
        DB_WARNING("rocksdb get error: code=%d, msg=%s",
                    res.code(), res.ToString().c_str());
        return -1;
    }
    // 获取删除向量的序号
    int del_faiss_idx_pos = 0;
    if (_is_separate) {
        del_faiss_idx_pos += sizeof(uint64_t);
    }
    int del_faiss_idx = TableKey(value).extract_i64(del_faiss_idx_pos);

    key = _key_prefix;
    level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    key.append(value);
    value.clear();
    value.append(1, (char)flag);
    value.append(pk);
    auto put_res = txn->Put(data_cf, key, value);
    if (!put_res.ok()) {
        DB_WARNING("rocksdb put error: code=%d, msg=%s",
                    put_res.code(), put_res.ToString().c_str());
        return -1;
    }
    // faiss_idx实际上变成了操作序号
    // region_id,index_id,0,faiss_idx => V_DELETE_OP,del_faiss_idx
    key = _key_prefix;
    level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    if (_is_separate) {
        // region_id,index_id,0,separate_value,faiss_idx => V_DELETE_OP,del_faiss_idx
        uint64_t separate_value_encode = KeyEncoder::to_endian_u64(faiss_index->separate_value);
        key.append((char*)&separate_value_encode, sizeof(uint64_t));
    }
    bthread::RWLockWrGuard lock(faiss_index->rwlock);
    int64_t faiss_idx = faiss_index->cache_idx.fetch_add(1);
    uint64_t idx_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(faiss_idx));
    key.append((char*)&idx_encode, sizeof(uint64_t));
    value.clear();
    value.append(1, (char)V_DELETE_OP);
    uint64_t value_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(del_faiss_idx));
    value.append((char*)&value_encode, sizeof(uint64_t));
    put_res = txn->Put(data_cf, key, value);
    if (!put_res.ok()) {
        DB_WARNING("rocksdb put error: code=%d, msg=%s",
                    put_res.code(), put_res.ToString().c_str());
        return -1;
    }
    int ret = mark_del_bitmap(faiss_index, del_faiss_idx);
    if (ret != 0) {
        DB_WARNING("Fail to mark_del_bitmap");
        return -1;
    }
    return 0;
}

void from_chars_to_float_vec(const std::string& str, std::vector<float>& vec) {
    using ::arrow_vendored::fast_float::from_chars;
    const char* ptr = str.data();
    size_t i = 0;
    //trim 空格 [
    while (i < str.size() && (str[i] == '[' || isspace(str[i]))) {
        ++ptr;
        ++i;
    }
    size_t len = str.size();
    while (len > 0 && (str[len - 1] == ']' || isspace(str[len - 1]))) {
        --len;
    }
    
    while (1) {
        float flt = 0.0;
        auto res = from_chars(ptr, str.data() + len, flt);
        vec.emplace_back(flt);
        if (res.ptr >= str.data() + len) {
            break;
        }
        ptr = res.ptr;
        while (ptr[0] == ' ') {
            ++ptr;
        }
        ++ptr;
    }
}

int VectorIndex::add_to_faiss(SmartFaissIndex faiss_index, 
                              const std::string& word, 
                              int64_t cache_idx,
                              SmartRecord record) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    if (record == nullptr) {
        DB_WARNING("record is nullptr");
        return -1;
    }
    std::vector<int64_t> cache_idxs;
    std::vector<float> cache_vectors;
    cache_vectors.reserve(_dimension);
    from_chars_to_float_vec(word, cache_vectors);
    if ((int)cache_vectors.size() != _dimension) {
        DB_FATAL("cache_vectors.size:%lu != _dimension:%d", cache_vectors.size(), _dimension);
        return -1;
    }
    cache_idxs.emplace_back(cache_idx++);
    std::unordered_set<int32_t> need_not_cache_fields;
    auto arrow_record_batch = VectorizeHelpper::construct_arrow_record_batch(
                                faiss_index->scalar_data->schema(), record, need_not_cache_fields);
    if (arrow_record_batch == nullptr) {
        DB_WARNING("record_batch is nullptr, table_id: %ld", _table_id);
        return -1;
    }
    if (!need_not_cache_fields.empty()) {
        // 从标量缓存中删除不需要缓存的列，对这些列采用后过滤方式
        std::shared_ptr<arrow::RecordBatch> scalar_data;
        std::shared_ptr<arrow::RecordBatch> flat_scalar_data;
        if (VectorizeHelpper::change_arrow_record_batch_schema(arrow_record_batch->schema(), faiss_index->scalar_data, &scalar_data, true) != 0){
            DB_WARNING("Fail to change arrow record batch schema");
            return -1;
        }
        if (faiss_index->flat_scalar_data != nullptr &&
                VectorizeHelpper::change_arrow_record_batch_schema(arrow_record_batch->schema(), faiss_index->flat_scalar_data, &flat_scalar_data, true) != 0) {
            DB_WARNING("Fail to change arrow record batch schema");
            return -1;
        }
        faiss_index->scalar_data = scalar_data;
        faiss_index->flat_scalar_data = flat_scalar_data;
        for (const auto& field_id : need_not_cache_fields) {
            faiss_index->need_not_cache_fields.insert(field_id);
        }
    }
    if (_is_flat) {
        if (faiss_index->index == nullptr) {
            DB_WARNING("faiss_index->index is nullptr");
            return -1;
        }
        // 添加标量数据到RecordBatch
        std::shared_ptr<arrow::Schema> arrow_schema = arrow_record_batch->schema();
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches = {faiss_index->scalar_data, arrow_record_batch};
        std::shared_ptr<arrow::RecordBatch> concat_record_batch;
        if (VectorizeHelpper::concatenate_record_batches(arrow_schema, record_batches, concat_record_batch) != 0) {
            DB_WARNING("concatenate_record_batches failed");
            return -1;
        }
        faiss_index->scalar_data = concat_record_batch;
        // 添加向量数据到Faiss
        faiss_index->index->add_with_ids(1, 
                                         &cache_vectors[cache_vectors.size() - _dimension], 
                                         &cache_idxs[cache_idxs.size() - 1]);
        faiss_index->idx = cache_idx;
    } else {
        if (faiss_index->index == nullptr || faiss_index->flat_index == nullptr) {
            DB_WARNING("faiss_index->index/faiss_index->flat_index is nullptr");
            return -1;
        }
        // 添加标量数据到flat层RecordBatch
        std::shared_ptr<arrow::Schema> arrow_schema = arrow_record_batch->schema();
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches = {faiss_index->flat_scalar_data, arrow_record_batch};
        std::shared_ptr<arrow::RecordBatch> flat_concat_record_batch;
        if (VectorizeHelpper::concatenate_record_batches(arrow_schema, record_batches, flat_concat_record_batch) != 0) {
            DB_WARNING("concatenate_record_batches failed");
            return -1;
        }
        if (faiss_index->flat_index->ntotal >= 1000) {
            // reconstruct from faiss_index, 减少缓存，降低内存占用
            faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->flat_index);
            std::vector<int64_t> origin_cache_idxs;
            std::vector<float> origin_cache_vectors;
            cache_idxs.swap(origin_cache_idxs);
            cache_vectors.swap(origin_cache_vectors);
            size_t cache_size = idmap2_index->rev_map.size() * _dimension;
            cache_idxs.reserve(cache_size);
            cache_vectors.resize(cache_size);
            int pos_idx = 0;
            // 按照faiss中向量序号升序排序，保证标量数据和向量数据一一对应
            std::map<int64_t, int64_t> idx_sort_map;
            for (const auto& [idx, faiss_idx] : idmap2_index->rev_map) {
                idx_sort_map[faiss_idx] = idx;
            }
            for (const auto& [_, idx] : idx_sort_map) {
                cache_idxs.emplace_back(idx);
                idmap2_index->reconstruct(idx, cache_vectors.data() + pos_idx);
                pos_idx += _dimension;
            }
            cache_idxs.insert(cache_idxs.end(), origin_cache_idxs.begin(), origin_cache_idxs.end());
            cache_vectors.insert(cache_vectors.end(), origin_cache_vectors.begin(), origin_cache_vectors.end());
            // 合并flat层标量数据
            record_batches = {faiss_index->scalar_data, flat_concat_record_batch};
            std::shared_ptr<arrow::RecordBatch> concat_record_batch;
            if (VectorizeHelpper::concatenate_record_batches(arrow_schema, record_batches, concat_record_batch) != 0) {
                DB_WARNING("concatenate_record_batches failed");
                return -1;
            }
            auto make_res = arrow::RecordBatch::MakeEmpty(faiss_index->flat_scalar_data->schema());
            if (!make_res.status().ok()) {
                DB_WARNING("Fail to make empty record batch");
                return -1;
            }
            faiss_index->flat_scalar_data = *make_res;
            faiss_index->scalar_data = concat_record_batch;
            // 合并delete map数据
            int64_t flat_index_ntotal = faiss_index->flat_index->ntotal;
            int64_t index_ntotal = faiss_index->index->ntotal;
            for (int i = 0; i < flat_index_ntotal; ++i) {
                if (faiss_index->flat_del_bitmap->contains(i)) {
                    faiss_index->del_bitmap->add(i + index_ntotal);
                }
            }
            faiss_index->index->add_with_ids(cache_idxs.size(), 
                                             &cache_vectors[0], 
                                             &cache_idxs[0]);
            faiss_index->idx = cache_idx;
            idmap2_index->rev_map.clear();
            faiss_index->flat_del_bitmap = std::make_shared<Roaring>();
            faiss_index->flat_index->reset();
        } else {
            faiss_index->flat_scalar_data = flat_concat_record_batch;
            faiss_index->flat_index->add_with_ids(1, 
                                                  &cache_vectors[cache_vectors.size() - _dimension], 
                                                  &cache_idxs[cache_idxs.size() - 1]);
            //DB_WARNING("insert word:%s", word.c_str());
        }
    }
    return 0;
}

int VectorIndex::search(
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
                   std::vector<ExprNode*>& pre_filter_exprs) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    std::vector<float> search_vector;
    search_vector.reserve(_dimension);
    from_chars_to_float_vec(search_data, search_vector);
    if ((int)search_vector.size() != _dimension) {
        DB_FATAL("search_vector.size:%lu != _dimension:%d", search_vector.size(), _dimension);
        return -1;
    }
    int64_t k = topk;
    std::vector<int64_t> flat_idxs;
    std::vector<float> flat_dis;
    std::vector<int64_t> idxs;
    std::vector<float> dis;
    int64_t least = 10;
    int64_t search_count = k;
    {
        bthread::RWLockRdGuard lock(faiss_index->rwlock);

        // 获取不在标量缓存中的字段，如果过滤条件包含这些字段，则这些过滤条件采用后过滤方式
        std::vector<arrow::compute::Expression> arrow_filter_sub_exprs;
        arrow_filter_sub_exprs.reserve(vector_filter_exprs.size() + scan_filter_exprs.size());
        // 将filter_exprs转化成arrow expression
        auto build_arrow_expr_func = [&faiss_index, &arrow_filter_sub_exprs, &pre_filter_exprs] (std::vector<ExprNode*>& filter_exprs) {
            for (auto iter = filter_exprs.begin(); iter != filter_exprs.end();) {
                auto* filter_expr = *iter;
                if (filter_expr == nullptr) {
                    DB_WARNING("filter_expr is nullptr");
                    return -1;
                }
                if (filter_expr->contains_specified_fields(faiss_index->need_not_cache_fields) || !filter_expr->can_use_arrow_vector()) {
                    filter_expr->set_is_vector_index_use(false);
                    ++iter;
                } else {
                    filter_expr->set_is_vector_index_use(true);
                    if (filter_expr->transfer_to_arrow_expression() != 0) {
                        DB_WARNING("Fail to transfer_to_arrow_expression");
                        return -1;
                    }
                    arrow_filter_sub_exprs.emplace_back(filter_expr->arrow_expr());
                    pre_filter_exprs.emplace_back(*iter);
                    iter = filter_exprs.erase(iter);
                }
            }
            return 0;
        };
        if (build_arrow_expr_func(vector_filter_exprs) != 0 || build_arrow_expr_func(scan_filter_exprs) != 0) {
            DB_WARNING("Fail to build_arrow_expr_func");
            return -1;
        }
        bool has_filter = !arrow_filter_sub_exprs.empty();
        arrow::compute::Expression arrow_filter_expr;
        if (has_filter) {
            auto arrow_res = arrow::compute::and_(arrow_filter_sub_exprs).Bind(*faiss_index->scalar_data->schema());
            if (!arrow_res.status().ok()) {
                DB_WARNING("Fail to bind, %s", arrow_res.status().message().c_str());
                return -1;
            }
            arrow_filter_expr = *arrow_res;
        }
        if (_is_flat) {
            if (faiss_index->index == nullptr) {                
                DB_WARNING("faiss_index->index is nullptr");
                return -1;
            }
            int64_t mink = std::max(std::min(k, faiss_index->index->ntotal), least); //0,1都会core
            idxs.resize(mink, -1);
            dis.resize(mink, -1);
            faiss::SearchParameters search_params;
            std::shared_ptr<IDSelectorBitmap> bitmap_sel;
            int64_t prefilter_cost = 0;
            if (has_filter) {
                TimeCost tm;
                auto arrow_res = arrow::compute::ExecuteScalarExpression(
                        arrow_filter_expr, *(faiss_index->scalar_data->schema()), faiss_index->scalar_data);
                if (!arrow_res.status().ok()) {
                    DB_WARNING("Fail to ExecuteScalarExpression, %s", arrow_res.status().message().c_str());
                    return -1;
                }
                prefilter_cost = tm.get_time();
                auto datum_array = (*arrow_res).array_as<arrow::BooleanArray>();
                bitmap_sel = std::make_shared<IDSelectorBitmap>(datum_array, faiss_index->del_bitmap);
            } else {
                bitmap_sel = std::make_shared<IDSelectorBitmap>(nullptr, faiss_index->del_bitmap);
            }
            search_params.sel = bitmap_sel.get();
            try {
                faiss_index->index->search(1, &search_vector[0], mink, &dis[0], &idxs[0], &search_params);
            } catch (...) {
                DB_WARNING("Fail to search, maybe not support search_params");
                return -1;
            }
            DB_WARNING("flat search:%ld, prefilter_cost:%ld", mink, prefilter_cost);
        } else {
            if (faiss_index->index == nullptr || faiss_index->flat_index == nullptr) {
                DB_WARNING("faiss_index->index/faiss_index->flat_index is nullptr");
                return -1;
            }
            int64_t mink = std::max(std::min(k, faiss_index->flat_index->ntotal), least);
            flat_idxs.resize(mink, -1);
            flat_dis.resize(mink, -1);
            faiss::SearchParameters flat_search_params;
            std::shared_ptr<IDSelectorBitmap> flat_bitmap_sel;
            int64_t flat_prefilter_cost = 0;
            if (has_filter) {
                TimeCost tm;
                auto arrow_res = arrow::compute::ExecuteScalarExpression(
                        arrow_filter_expr, *(faiss_index->flat_scalar_data->schema()), faiss_index->flat_scalar_data);
                if (!arrow_res.status().ok()) {
                    DB_WARNING("Fail to ExecuteScalarExpression, %s", arrow_res.status().message().c_str());
                    return -1;
                }
                flat_prefilter_cost = tm.get_time();
                auto flat_datum_array = (*arrow_res).array_as<arrow::BooleanArray>();
                flat_bitmap_sel = std::make_shared<IDSelectorBitmap>(flat_datum_array, faiss_index->flat_del_bitmap);
            } else {
                flat_bitmap_sel = std::make_shared<IDSelectorBitmap>(nullptr, faiss_index->flat_del_bitmap);
            }
            flat_search_params.sel = flat_bitmap_sel.get();
            try {
                faiss_index->flat_index->search(1, &search_vector[0], mink, &flat_dis[0], &flat_idxs[0], &flat_search_params);
            } catch (...) {
                DB_WARNING("Fail to search, maybe not support search_params");
                return -1;
            }
            int64_t flat_search_cnt = 0;
            for (int64_t i : flat_idxs) {
                if (i == -1) {
                    break;
                } else {
                    ++flat_search_cnt;
                }
            }
            mink = std::max(std::min(k, faiss_index->index->ntotal), least);
            idxs.resize(mink, -1);
            dis.resize(mink, -1);
            bool do_brute_force = false;
            faiss::SearchParametersHNSW search_params;
            search_params.efSearch = std::max(_efsearch, efsearch);
            search_params.need_brute_force = true; // 如果过滤数量大于faiss索引数量的93%，则退化成暴搜；
            search_params.need_brute_force_when_insufficient = FLAGS_need_brute_force_when_insufficient; // hnsw搜索结果不足topK时退化成暴搜；
            search_params.do_brute_force = &do_brute_force; // hnsw搜索过程中是否退化暴搜
            std::shared_ptr<IDSelectorBitmap> bitmap_sel;
            int64_t prefilter_cost = 0;
            if (has_filter) {
                TimeCost tm;
                auto arrow_res = arrow::compute::ExecuteScalarExpression(
                        arrow_filter_expr, *(faiss_index->scalar_data->schema()), faiss_index->scalar_data);
                if (!arrow_res.status().ok()) {
                    DB_WARNING("Fail to ExecuteScalarExpression, %s", arrow_res.status().message().c_str());
                    return -1;
                }
                prefilter_cost = tm.get_time();
                auto datum_array = (*arrow_res).array_as<arrow::BooleanArray>();
                bitmap_sel = std::make_shared<IDSelectorBitmap>(datum_array, faiss_index->del_bitmap);
            } else {
                bitmap_sel = std::make_shared<IDSelectorBitmap>(nullptr, faiss_index->del_bitmap);
            }
            search_params.sel = bitmap_sel.get();
            try {
                faiss_index->index->search(1, &search_vector[0], mink, &dis[0], &idxs[0], &search_params);
            } catch (...) {
                DB_WARNING("Fail to search, maybe not support search_params");
                return -1;
            }
            int64_t search_cnt = 0;
            for (int64_t i : idxs) {
                if (i == -1) {
                    break;
                } else {
                    ++search_cnt;
                }
            }
            DB_WARNING("region_id: %ld, index_id: %ld, separate_value: %lu, search:%ld, faiss.ntotal:%lu,%lu, "
                        "search_cnt: %ld,%ld, prefilter_time_cost: %ld, %ld, efsearch: %d, do_brute_force: %d", 
                        _region_id, _index_id, faiss_index->separate_value, mink, faiss_index->flat_index->ntotal, faiss_index->index->ntotal, 
                        flat_search_cnt, search_cnt, flat_prefilter_cost, prefilter_cost, search_params.efSearch, do_brute_force);
            search_count = flat_search_cnt + search_cnt;
        }
    }
    std::vector<int64_t> result_idxs;
    std::vector<float> result_dis;
    result_idxs.reserve(search_count);
    result_dis.reserve(search_count);
    size_t i = 0;
    size_t j = 0;
    while ((i < flat_idxs.size() || j < idxs.size()) 
            && (int64_t)result_idxs.size() < search_count) {
        if (i < flat_idxs.size() && j < idxs.size()) {
            if (_metric_compare(flat_dis[i], dis[j])) { // METRIC_L2和METRIC_INNER_PRODUCT相反
                result_idxs.emplace_back(flat_idxs[i]);
                result_dis.emplace_back(flat_dis[i]);
                i++;
            } else {
                result_idxs.emplace_back(idxs[j]);
                result_dis.emplace_back(dis[j]);
                j++;
            }
        } else if (i < flat_idxs.size()) {
            result_idxs.emplace_back(flat_idxs[i]);
            result_dis.emplace_back(flat_dis[i]);
            i++;
        } else if (j < idxs.size()) {
            result_idxs.emplace_back(idxs[j]);
            result_dis.emplace_back(dis[j]);
            j++;
        }
    }
    records.clear();
    int ret = construct_records(txn, faiss_index->separate_value, pk_info, table_info, result_idxs, result_dis, records);
    if (ret < 0) {
        DB_WARNING("construct records failed");
        return -1;
    }
    return 0;
}
int VectorIndex::construct_records(myrocksdb::Transaction* txn, 
                                   const uint64_t separate_value, 
                                   SmartIndex& pk_info, 
                                   SmartTable& table_info, 
                                   const std::vector<int64_t>& idxs, 
                                   const std::vector<float>& dis, 
                                   std::vector<SmartRecord>& records) {
    auto factory = SchemaFactory::get_instance();
    std::vector<std::string> keys;
    keys.reserve(idxs.size());
    std::vector<rocksdb::Slice> rocksdb_keys;
    rocksdb_keys.reserve(idxs.size());
    for (int64_t id : idxs) {
        std::string key = _key_prefix;
        uint8_t level = 0;
        key.append((char*)&level, sizeof(uint8_t));
        if (_is_separate) {
            uint64_t separate_value_encode = KeyEncoder::to_endian_u64(separate_value);
            key.append((char*)&separate_value_encode, sizeof(uint64_t));
        }
        uint64_t idx_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(id));
        key.append((char*)&idx_encode, sizeof(uint64_t));
        keys.emplace_back(key);
        rocksdb_keys.emplace_back(key);
    }
    std::vector<rocksdb::PinnableSlice> values(idxs.size());
    std::vector<rocksdb::Status> statuses(idxs.size());
    rocksdb::ReadOptions read_opt;
    read_opt.fill_cache = true;
    auto data_cf = _rocksdb->get_data_handle();
    txn->MultiGet(read_opt, data_cf, rocksdb_keys, values, statuses, false);
    for (size_t i = 0; i < idxs.size(); i++) {
        if (statuses[i].ok()) {
            SmartRecord record = factory->new_record(_table_id);
            rocksdb::Slice value_slice(values[i]);
            if (value_slice[0] == V_NORMAL) {
                int pos = 1;
                record->decode_key(*pk_info, value_slice, pos);
                FieldInfo* weight_field = get_field_info_by_name(table_info->fields, "__weight");
                if (weight_field != nullptr) {
                    auto field = record->get_field_by_idx(weight_field->pb_idx);
                    if (field != nullptr) {
                        MessageHelper::set_float(field, record->get_raw_message(), dis[i]);
                    }
                }
                records.emplace_back(record);
            }
        } else if (statuses[i].IsNotFound()) {
            DB_DEBUG("lock ok but key not exist");
            continue;
        } else {
            DB_WARNING("unknown expect error: %d, %s", statuses[i].code(), statuses[i].ToString().c_str());
            return -1;
        }
    }
    return 0;
}

int VectorIndex::schema_change() {
    if (_is_separate) {
        std::atomic<bool> is_succ = true;
        auto call = [this, &is_succ] (SmartFaissIndex& faiss_index) {
            ScopeGuard guard([&faiss_index] () {
                if (faiss_index != nullptr) {
                    faiss_index->dec_ref();
                }
            });
            if (!is_succ) {
                return;
            }
            if (schema_change_faiss_index(faiss_index) != 0) {
                DB_WARNING("schema_change_faiss_index fail");
                is_succ = false;
                return;
            }
        };
        _separate_faiss_index_map->traverse_copy(inc_ref_faiss_index, call);
        if (!is_succ) {
            DB_WARNING("fail to schema_change_faiss_index");
            return -1;
        }
    } else {
        if (schema_change_faiss_index(_faiss_index) != 0) {
            DB_WARNING("schema_change_faiss_index fail");
            return -1;
        }
    }
    return 0;
}

int VectorIndex::truncate_del() {
    if (_is_separate) {
        std::atomic<bool> is_succ = true;
        auto call = [this, &is_succ] (SmartFaissIndex& faiss_index) {
            ScopeGuard guard([&faiss_index] () {
                if (faiss_index != nullptr) {
                    faiss_index->dec_ref();
                }
            });
            if (!is_succ) {
                return;
            }
            if (truncate_del_faiss_index(faiss_index) != 0) {
                DB_WARNING("truncate_del_faiss_index fail");
                is_succ = false;
                return;
            }
        };
        _separate_faiss_index_map->traverse_copy(inc_ref_faiss_index, call);
        if (!is_succ) {
            DB_WARNING("fail to truncate_del_faiss_index");
            return -1;
        }
    } else {
        if (truncate_del_faiss_index(_faiss_index) != 0) {
            DB_WARNING("truncate_del_faiss_index fail");
            return -1;
        }
    }
    return 0;
}

int VectorIndex::truncate_del_faiss_index(SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    FaissIndex::Status expected_status = FaissIndex::Status::IDLE;
    if (!faiss_index->status.compare_exchange_strong(expected_status, FaissIndex::Status::DOING)) {
        DB_WARNING("status is not idle when add peer, region_id: %ld, index_id: %ld, separate_value: %lu", 
                    _region_id, _index_id, faiss_index->separate_value);
        return 0;
    }
    ScopeGuard guard([&faiss_index] () {
        faiss_index->status = FaissIndex::Status::IDLE;
    });
    auto rocksdb = RocksWrapper::get_instance();
    auto data_cf = rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("data_cf is nullptr, _region_id: %ld, _index_id: %ld, separate_value: %lu", 
                    _region_id, _index_id, faiss_index->separate_value);
        return -1;
    }
    rocksdb::WriteOptions options;
    int cache_idx_pos = sizeof(uint64_t) * 2 + sizeof(uint8_t);
    if (_is_separate) {
        cache_idx_pos += sizeof(uint64_t);
    }
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(rocksdb->new_iterator(read_options, data_cf));
    std::string key = _key_prefix;
    uint8_t level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    if (_is_separate) {
        uint64_t separate_value_encode = KeyEncoder::to_endian_u64(faiss_index->separate_value);
        key.append((char*)&separate_value_encode, sizeof(uint64_t));
    }
    int64_t dump_idx = -1;
    {
        bthread::RWLockWrGuard lock(faiss_index->rwlock);
        dump_idx = faiss_index->dump_idx;
    }
    // 删掉[0, dump_idx]的删除操作记录
    int del_cnt = 0;
    uint64_t idx_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(0));
    key.append((char*)&idx_encode, sizeof(uint64_t));
    for (iter->Seek(key); iter->Valid(); iter->Next()) {
        TableKey key_slice(iter->key());
        uint8_t level = key_slice.extract_u8(sizeof(uint64_t) * 2);
        if (level != 0) {
            break;
        }
        if (_is_separate) {
            uint64_t separate_value = key_slice.extract_u64(sizeof(uint64_t) * 2 + sizeof(uint8_t));
            if (separate_value != faiss_index->separate_value) {
                break;
            }
        }
        int64_t cache_idx = key_slice.extract_i64(cache_idx_pos);
        if (cache_idx <= dump_idx) {
            rocksdb::Slice value_slice(iter->value());
            if (value_slice[0] == V_DELETE_OP) {
                rocksdb::Status res = rocksdb->remove(options, data_cf, iter->key());
                if (!res.ok()) {
                    DB_WARNING("delete fail");
                    return -1;
                }
                ++del_cnt;
            }
        } else {
            break;
        }
    }
    DB_WARNING("truncate del cnt: %d, region_id: %ld, index_id: %ld, separate_value: %lu", 
                del_cnt, _region_id, _index_id, faiss_index->separate_value);
    return 0;
}

int VectorIndex::compact(const pb::RegionInfo& region_info, const int64_t table_lines, bool is_force) {
    if (need_compact(table_lines)) {
        is_force = true;
    }
    if (_is_separate) {
        {
            // compact faiss index
            std::atomic<bool> is_succ = true;
            auto call = [this, &is_succ, &region_info, is_force] (SmartFaissIndex& faiss_index) {
                ScopeGuard guard([&faiss_index] () {
                    if (faiss_index != nullptr) {
                        faiss_index->dec_ref();
                    }
                });
                if (!is_succ) {
                    return;
                }
                if (compact_faiss_index(region_info, faiss_index, is_force) != 0) {
                    DB_WARNING("compact_faiss_index fail");
                    is_succ = false;
                    return;
                }
            };
            _separate_faiss_index_map->traverse_copy(inc_ref_faiss_index, call);
            if (!is_succ) {
                DB_WARNING("fail to compact_faiss_index");
                return -1;
            }
        }
        {
            // 删除空faiss index
            std::vector<uint64_t> need_erase_keys;
            auto call = [this, &need_erase_keys, is_force] (SmartFaissIndex& faiss_index) {
                if (faiss_index == nullptr) {
                    // 此情形不应该出现
                    return;
                }
                if (need_erase_faiss_index(faiss_index, is_force)) {
                    need_erase_keys.emplace_back(faiss_index->separate_value);
                }
            };
            _separate_faiss_index_map->traverse(call);
            auto match_call = [this, is_force] (const SmartFaissIndex& faiss_index) {
                if (faiss_index == nullptr) {
                    // nullptr也删除，此情形不应该出现
                    return true;
                }
                // double check
                int64_t ntotal = 0;
                bthread::RWLockWrGuard lock(faiss_index->rwlock);
                if (faiss_index->index != nullptr) {
                    ntotal += faiss_index->index->ntotal;
                }
                if (faiss_index->flat_index != nullptr) {
                    ntotal += faiss_index->flat_index->ntotal;
                }
                if (ntotal == 0 && faiss_index->ref_cnt <= 0) {
                    DB_WARNING("region_id: %ld, index_id: %ld, separate_value: %lu, faiss_index erase",
                                _region_id, _index_id, faiss_index->separate_value);
                    return true;
                }
                return false;
            };
            for (auto& key : need_erase_keys) {
                _separate_faiss_index_map->erase_if_match_call(key, match_call);
            }
        }
    } else {
        if (compact_faiss_index(region_info, _faiss_index, is_force) != 0) {
            DB_WARNING("compact_faiss_index fail");
            return -1;
        }
    }
    return 0;
}

int VectorIndex::init_faiss_index(SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    faiss_index->index = faiss::index_factory(_dimension, _vector_description.c_str(), _metrix_type);
    if (faiss_index->index == nullptr) {
        DB_WARNING("faiss_index->index init fail");
        return -1;
    }
    if (_is_hnsw) {
        faiss::ParameterSpace().set_index_parameter(faiss_index->index, "efSearch", _efsearch);
        faiss::ParameterSpace().set_index_parameter(faiss_index->index, "efConstruction", _efconstruction);
    }
    if (!_is_flat) {
        if (_is_l2norm) {
            faiss_index->flat_index = faiss::index_factory(_dimension, "IDMap2,L2norm,Flat", _metrix_type);
        } else {
            faiss_index->flat_index = faiss::index_factory(_dimension, "IDMap2,Flat", _metrix_type);
        }
        if (faiss_index->flat_index == nullptr) {
            DB_WARNING("faiss_index->flat_index init fail");
            return -1;
        }
    }
    faiss_index->del_bitmap = std::make_shared<Roaring>();
    SmartTable table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (table_info == nullptr) {
        DB_WARNING("table info not found _table_id:%ld", _table_id);
        return -1;
    }
    for (const auto& field : table_info->fields) {
        if (field.short_name == "__weight") {
            faiss_index->need_not_cache_fields.insert(field.id);
            break;
        }
    }
    for (const auto& field_id : table_info->vector_fields) {
        faiss_index->need_not_cache_fields.insert(field_id);
    }
    auto arrow_schema = VectorizeHelpper::construct_arrow_schema(table_info, faiss_index->need_not_cache_fields);
    if (arrow_schema == nullptr) {
        DB_WARNING("Fail to construct_arrow_schema");
        return -1;
    }
    auto make_res = arrow::RecordBatch::MakeEmpty(arrow_schema);
    if (!make_res.status().ok()) {
        DB_WARNING("Fail to make empty record batch");
        return -1;
    }
    faiss_index->scalar_data = *make_res;
    if (!_is_flat) {
        faiss_index->flat_del_bitmap = std::make_shared<Roaring>();
        auto make_res = arrow::RecordBatch::MakeEmpty(arrow_schema);
        if (!make_res.status().ok()) {
            DB_WARNING("Fail to make empty record batch");
            return -1;
        }
        faiss_index->flat_scalar_data = *make_res;
    }
    faiss_index->last_compaction_ts.reset();
    return 0;
}

int VectorIndex::reset_faiss_index(SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        return -1;
    }
    bthread::RWLockWrGuard lock(faiss_index->rwlock);
    if (_is_flat) {
        auto make_res = arrow::RecordBatch::MakeEmpty(faiss_index->scalar_data->schema());
        if (!make_res.status().ok()) {
            DB_WARNING("Fail to make empty record batch");
            return -1;
        }
        faiss_index->scalar_data = *make_res;
        faiss_index->del_bitmap = std::make_shared<Roaring>();
        faiss_index->index->reset();
        faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->index);
        idmap2_index->rev_map.clear();
    } else {
        auto flat_make_res = arrow::RecordBatch::MakeEmpty(faiss_index->flat_scalar_data->schema());
        if (!flat_make_res.status().ok()) {
            DB_WARNING("Fail to make empty record batch");
            return -1;
        }
        auto make_res = arrow::RecordBatch::MakeEmpty(faiss_index->scalar_data->schema());
        if (!make_res.status().ok()) {
            DB_WARNING("Fail to make empty record batch");
            return -1;
        }
        faiss_index->flat_scalar_data = *flat_make_res;
        faiss_index->scalar_data = *make_res;
        faiss_index->flat_del_bitmap = std::make_shared<Roaring>();
        faiss_index->del_bitmap = std::make_shared<Roaring>();
        faiss_index->index->reset();
        faiss_index->flat_index->reset();
        faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->index);
        idmap2_index->rev_map.clear();
        faiss::IndexIDMap2* flat_idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->flat_index);
        flat_idmap2_index->rev_map.clear();
    }
    faiss_index->cache_idx = 0;
    faiss_index->idx = 0;
    faiss_index->train_idx = 0;
    return 0;
}

int VectorIndex::restore_faiss_index(const pb::RegionInfo& region_info, SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    DB_DEBUG("restore_faiss_index, region_id: %ld, _faiss_dump_idx: %ld, _faiss_cache_idx:%ld, separate_value: %lu", 
              region_info.region_id(), faiss_index->dump_idx, faiss_index->cache_idx.load(), 
              faiss_index->separate_value);
    auto factory = SchemaFactory::get_instance();
    std::string key = _key_prefix;
    uint8_t level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    if (_is_separate) {
        uint64_t separate_value_encode = KeyEncoder::to_endian_u64(faiss_index->separate_value);
        key.append((char*)&separate_value_encode, sizeof(uint64_t));
    }
    // compaction会多次调用restore_faiss_index，第一次是seek faiss_index->dump_idx
    uint64_t idx_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(faiss_index->cache_idx.load()));
    key.append((char*)&idx_encode, sizeof(uint64_t));
    auto data_cf = _rocksdb->get_data_handle();
    SmartIndex index_info = factory->get_index_info_ptr(_index_id);
    SmartIndex pk_info = factory->get_index_info_ptr(_table_id);
    SmartTable table_info = factory->get_table_info_ptr(_table_id);
    if (index_info == nullptr || table_info == nullptr || pk_info == nullptr) {
        DB_WARNING("table info not found _table_id:%ld", _table_id);
        return -1;
    }
    int vector_field_idx = 0;
    if (index_info->fields.size() > 1) {
        vector_field_idx = 1;
    }
    std::shared_ptr<RegionResource> resource(new RegionResource);
    resource->region_info = region_info;
    std::map<int32_t, FieldInfo*> field_ids;
    for (auto& field : table_info->fields) {
        field_ids[field.id] = &field;
    }

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, data_cf));

    rocksdb::TransactionOptions txn_opt;
    txn_opt.lock_timeout = 100;
    SmartTransaction txn(new Transaction(0, nullptr));
    txn->begin(txn_opt);
    txn->set_resource(resource);
    int64_t del_cnt = 0;

    int cache_idx_pos = sizeof(uint64_t) * 2 + sizeof(uint8_t);
    if (_is_separate) {
        cache_idx_pos += sizeof(uint64_t);
    }
    int faiss_idx_pos = 0;
    if (_is_separate) {
        faiss_idx_pos += sizeof(uint64_t);
    }
    int64_t seek_cnt = 0;
    for (iter->Seek(key); iter->Valid(); iter->Next()) {
        // 需要check region 范围
        TableKey key_slice(iter->key());
        uint8_t level = key_slice.extract_u8(sizeof(uint64_t) * 2);
        if (level == 1) {
            break;
        }
        if (_is_separate) {
            uint64_t separate_value = key_slice.extract_u64(sizeof(uint64_t) * 2 + sizeof(uint8_t));
            if (separate_value != faiss_index->separate_value) {
                break;
            }
        }
        faiss_index->cache_idx = key_slice.extract_i64(cache_idx_pos);
        rocksdb::Slice value_slice(iter->value());
        // 最后一行faiss_index->cache_idx+1
        int64_t cache_idx = faiss_index->cache_idx.fetch_add(1);
        bool need_del = false;
        if (value_slice[0] == V_NORMAL) {
            SmartRecord record = factory->new_record(*table_info);
            if (record == nullptr) {
                DB_FATAL("record is null, table_id:%ld", _table_id);
                return -1;
            }
            int pos = 1;
            record->decode_key(*pk_info, value_slice, pos);
            MutTableKey pk_key;
            int ret = pk_key.append_index(*pk_info, record.get(), -1, false);
            if (ret < 0) {
                DB_WARNING("append_index");
                return -1;
            }
            ret = txn->get_update_primary(_region_id, *pk_info, TableKey(pk_key), record, field_ids, GET_ONLY, true);
            if (ret == 0) {
                auto field = record->get_field_by_idx(index_info->fields[vector_field_idx].pb_idx);
                if (record->is_null(field)) {
                    continue;
                }
                std::string word;
                ret = record->get_reverse_word(*index_info, word);
                if (ret < 0) {
                    DB_WARNING("index_info to word fail for index_id: %ld", _index_id);
                    return ret;
                }
                bthread::RWLockWrGuard lock(faiss_index->rwlock);
                ret = add_to_faiss(faiss_index, word, cache_idx, record);
                if (ret < 0) {
                    DB_WARNING("add_to_faiss fail, index_id: %ld", _index_id);
                    return ret;
                }
            } else if (ret == -3) {
                need_del = true;
            }
        } else if (value_slice[0] == V_DELETE_OP) {
            int64_t faiss_idx = TableKey(value_slice).extract_i64(1);
            bthread::RWLockWrGuard lock(faiss_index->rwlock);
            if (mark_del_bitmap(faiss_index, faiss_idx) != 0) {
                DB_WARNING("Fail to mark_del_bitmap");
                return -1;
            }
        } else {
            need_del = true;
        }
        // 清理已删除数据
        if (need_del) {
            rocksdb::Status res = txn->get_txn()->Delete(data_cf, iter->key());
            if (!res.ok()) {
                DB_WARNING("delete fail");
                return -1;
            }
            // 同一个pk更新多次会产生多个idx，只有idx相等才删除
            std::string key = _key_prefix;
            level = 1;
            key.append((char*)&level, sizeof(uint8_t));
            value_slice.remove_prefix(1);
            key.append(value_slice.data(), value_slice.size());
            rocksdb::ReadOptions read_opt;
            read_opt.fill_cache = true;
            std::string value;
            res = txn->get_txn()->GetForUpdate(read_opt, data_cf, key, &value);
            if (!res.ok()) {
                DB_WARNING("rocksdb get error, cache_idx:%ld code=%d, msg=%s",
                        cache_idx, res.code(), res.ToString().c_str());
                continue;
            }
            int64_t idx = TableKey(value).extract_i64(faiss_idx_pos);
            if (idx == cache_idx) {
                res = txn->get_txn()->Delete(data_cf, key);
                if (!res.ok()) {
                    DB_WARNING("delete fail");
                    return -1;
                }
            }
            ++del_cnt;
        }
        // 批量提交，减少锁冲突
        if (++seek_cnt % 100 == 0) {
            auto s = txn->commit();
            if (!s.ok()) {
                DB_WARNING("index %ld, region_id: %ld fail to commit failed, status: %s",
                            _index_id, _region_id, s.ToString().c_str());
                return -1;
            }
            txn.reset(new Transaction(0, nullptr));
            txn->begin(txn_opt);
            txn->set_resource(resource);
        }
    }
    if (seek_cnt % 100 != 0) {
        auto s = txn->commit();
        if (!s.ok()) {
            DB_WARNING("index %ld, region_id: %ld fail to commit failed, status: %s",
                        _index_id, _region_id, s.ToString().c_str());
            return -1;
        }
    }
    DB_WARNING("restore_index success, region_id: %ld, faiss dump_idx: %ld, "
               "faiss cache_idx:%ld, del_cnt:%ld, separate_value: %lu", 
                region_info.region_id(), faiss_index->dump_idx, faiss_index->cache_idx.load(),
                del_cnt, faiss_index->separate_value);
    return 0;
}

int VectorIndex::schema_change_faiss_index(SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    SmartTable table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);
    if (table_info == nullptr) {
        DB_WARNING("table info not exist");
        return -1;
    }
    auto arrow_schema = VectorizeHelpper::construct_arrow_schema(table_info, faiss_index->need_not_cache_fields);
    if (arrow_schema == nullptr) {
        DB_WARNING("construct arrow schema fail");
        return -1;
    }
    bthread::RWLockWrGuard lock(faiss_index->rwlock);
    if ((faiss_index->scalar_data != nullptr && !arrow_schema->Equals(faiss_index->scalar_data->schema(), true)) ||
            (faiss_index->flat_scalar_data != nullptr && !arrow_schema->Equals(faiss_index->flat_scalar_data->schema(), true))) {
        // 直接变更，不需要从Rocksdb重建
        std::shared_ptr<arrow::RecordBatch> scalar_data;
        std::shared_ptr<arrow::RecordBatch> flat_scalar_data;
        if (faiss_index->scalar_data != nullptr && 
                VectorizeHelpper::change_arrow_record_batch_schema(arrow_schema, faiss_index->scalar_data, &scalar_data, true) != 0){
            DB_WARNING("Fail to change arrow record batch schema");
            return -1;
        }
        if (faiss_index->flat_scalar_data != nullptr &&
                VectorizeHelpper::change_arrow_record_batch_schema(arrow_schema, faiss_index->flat_scalar_data, &flat_scalar_data, true) != 0) {
            DB_WARNING("Fail to change arrow record batch schema");
            return -1;
        }
        faiss_index->scalar_data = scalar_data;
        faiss_index->flat_scalar_data = flat_scalar_data;
    }
    return 0;
}

int VectorIndex::compact_faiss_index(const pb::RegionInfo& region_info, SmartFaissIndex faiss_index, bool is_force) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    if (!is_force && !need_compact_faiss_index(faiss_index)) {
        return 0;
    }
    FaissIndex::Status expected_status = FaissIndex::Status::IDLE;
    if (!faiss_index->status.compare_exchange_strong(expected_status, FaissIndex::Status::DOING)) {
        DB_WARNING("status is not idle when add peer, region_id: %ld, index_id: %ld, separate_value: %lu", 
                    _region_id, _index_id, faiss_index->separate_value);
        return 0;
    }
    ScopeGuard scope_guard([&faiss_index] () {
        faiss_index->status = FaissIndex::Status::IDLE;
    });
    TimeCost tm;
    SmartFaissIndex tmp_faiss_index = std::make_shared<FaissIndex>();
    if (tmp_faiss_index == nullptr) {
        DB_WARNING("tmp_faiss_index is nullptr");
        return -1;
    }
    tmp_faiss_index->separate_value = faiss_index->separate_value;
    if (init_faiss_index(tmp_faiss_index) != 0) {
        DB_WARNING("init faiss index fail");
        return -1;
    }
    int ret = 0;
    int retry = 0;
    static const int MAX_RETRY = 5;
    int64_t last_idx = 0;
    do {
        ret = restore_faiss_index(region_info, tmp_faiss_index);
        if (ret < 0) {
            DB_FATAL("Fail to restore_index, region_id: %ld", _region_id);
            return -1;
        }
        if (tmp_faiss_index->cache_idx == last_idx) {
            DB_DEBUG("faiss cache_idx: %ld vs tmp faiss cache_idx: %ld, last_idx:%ld", 
                      faiss_index->cache_idx.load(), tmp_faiss_index->cache_idx.load(), last_idx);
            break;
        }
        last_idx = tmp_faiss_index->cache_idx;
        DB_DEBUG("faiss cache_idx: %ld vs tmp faiss cache_idx: %ld, last_idx:%ld", 
                    faiss_index->cache_idx.load(), tmp_faiss_index->cache_idx.load(), last_idx);
    } while (faiss_index->cache_idx - tmp_faiss_index->cache_idx > 1000 && ++retry < MAX_RETRY);
    if (faiss_index->cache_idx - tmp_faiss_index->cache_idx > 1000) {
        DB_FATAL("vector compaction max retry");
    }

    faiss_index->disable_write_cond.increase();
    ScopeGuard write_auto_decrease([&faiss_index] () {
        faiss_index->disable_write_cond.decrease_broadcast();
    });
    faiss_index->real_writing_cond.wait();
    ret = restore_faiss_index(region_info, tmp_faiss_index);
    if (ret < 0) {
        DB_FATAL("Fail to restore_index, region_id: %ld", _region_id);
        return -1;
    }
    faiss_index->swap(tmp_faiss_index);
    faiss_index->last_compaction_ts.reset();
    DB_WARNING("compact faiss index success, region_id: %ld, index_id: %ld, separate_value: %lu, time_cost: %ld", 
                _region_id, _index_id, faiss_index->separate_value, tm.get_time());
    return 0;
}

bool VectorIndex::need_compact_faiss_index(SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return false;
    }
    bthread::RWLockWrGuard lock(faiss_index->rwlock);
    int64_t ntotal = 0;
    if (faiss_index->index != nullptr) {
        ntotal += faiss_index->index->ntotal;
    }
    if (faiss_index->flat_index != nullptr) {
        ntotal += faiss_index->flat_index->ntotal;
    }
    if (ntotal > 10 && faiss_index->del_count * 100 / ntotal > 30) {
        DB_WARNING(
                "region_id: %ld, _index_id: %ld, ntotal:%ld, _del_count:%ld",
                _region_id,
                _index_id,
                ntotal,
                faiss_index->del_count.load());
        return true;
    }
    if (faiss_index->last_compaction_ts.get_time() > FLAGS_compaction_interval_s * 1000 * 1000LL
            && ntotal > 0 && faiss_index->del_count * 100 / ntotal > 5) {
        DB_WARNING(
                "region_id: %ld, _index_id: %ld, ntotal:%ld, _del_count:%ld",
                _region_id,
                _index_id,
                ntotal,
                faiss_index->del_count.load());
        return true;
    }
    // 用于向量隔离索引删除空FaissIndex
    if (_is_separate) {
        if (ntotal > 0 && faiss_index->del_count >= ntotal) {
            DB_WARNING(
                "region_id: %ld, _index_id: %ld, ntotal:%ld, _del_count:%ld",
                _region_id,
                _index_id,
                ntotal,
                faiss_index->del_count.load());
            return true;
        }
    }
    return false;
}

bool VectorIndex::need_erase_faiss_index(SmartFaissIndex faiss_index, bool is_force) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return false;
    }
    bthread::RWLockWrGuard lock(faiss_index->rwlock);
    int64_t ntotal = 0;
    if (faiss_index->index != nullptr) {
        ntotal += faiss_index->index->ntotal;
    }
    if (faiss_index->flat_index != nullptr) {
        ntotal += faiss_index->flat_index->ntotal;
    }
    // FaissIndex超过1h为空，则删除
    if (ntotal == 0) {
        if (faiss_index->empty_time_cost.get_time() >= 3600 * 1000 * 1000ULL || is_force) { 
            return true;
        }
    } else {
        faiss_index->empty_time_cost.reset();
    }
    return false;
}

int VectorIndex::build_separate_faiss_index_map() {
    DB_WARNING("begin build_separate_faiss_index_map");
    std::string key = _key_prefix;
    uint8_t level = 0;
    key.append((char*)&level, sizeof(uint8_t));

    auto data_cf = _rocksdb->get_data_handle();
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, data_cf));
    bool first_flag = true;
    uint64_t cur_separate_value = 0;
    for (iter->Seek(key); iter->Valid(); iter->Next()) {
        TableKey key_slice(iter->key());
        uint8_t level = key_slice.extract_u8(sizeof(uint64_t) * 2);
        if (level == 1) {
            break;
        }
        rocksdb::Slice value_slice(iter->value());
        if (value_slice[0] == V_NORMAL) {
            uint64_t separate_value = key_slice.extract_u64(sizeof(uint64_t) * 2 + sizeof(uint8_t));
            if (first_flag) {
                first_flag = false;
                cur_separate_value = separate_value;
            } else {
                if (cur_separate_value == separate_value) {
                    continue;
                } else {
                    cur_separate_value = separate_value;
                }
            }
            SmartFaissIndex faiss_index = get_or_create_faiss_index(separate_value);
            ScopeGuard auto_decrease([this, &faiss_index] () {
                if (_is_separate && faiss_index != nullptr) {
                    faiss_index->dec_ref();
                }
            });
            if (faiss_index == nullptr) {
                DB_FATAL("fail to get or create new faiss index");
                return -1;
            }
        }
    }
    DB_WARNING("build_separate_faiss_index_map succ, region_id: %ld, index_id: %ld, size: %u", 
                _region_id, _index_id, _separate_faiss_index_map->size());
    return 0;
}

int VectorIndex::mark_del_bitmap(SmartFaissIndex faiss_index, const int64_t idx) {
    if (faiss_index == nullptr) {
        // 已删除的索引，无需标记
        return 0;
    }
    if (_is_flat) {
        faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->index);
        const auto& rev_map = idmap2_index->rev_map;
        if (rev_map.find(idx) != rev_map.end()) {
            int64_t faiss_idx = rev_map.at(idx);
            faiss_index->del_bitmap->add(faiss_idx);
        }
    } else {
        faiss::IndexIDMap2* flat_idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->flat_index);
        const auto& flat_rev_map = flat_idmap2_index->rev_map; 
        if (flat_rev_map.find(idx) != flat_rev_map.end()) {
            int64_t faiss_idx = flat_rev_map.at(idx);
            faiss_index->flat_del_bitmap->add(faiss_idx);
        } else {
            faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->index);
            const auto& rev_map = idmap2_index->rev_map;
            if (rev_map.find(idx) != rev_map.end()) {
                int64_t faiss_idx = rev_map.at(idx);
                faiss_index->del_bitmap->add(faiss_idx);
            }
        }
    }
    return 0;
}

int VectorIndex::read_scalar_data_from_file(const std::string& path, const std::string& file) {
    SmartFaissIndex faiss_index;
    if (_is_separate) {
        std::vector<std::string> vec;
        vec.reserve(4);
        boost::split(vec, file, boost::is_any_of("_"));
        if (vec.size() < 4) {
            DB_WARNING("Invalid file_name: %s", file.c_str());
            return -1;
        }
        const uint64_t separate_value = strtoull(vec[3].c_str(), NULL, 10);
        faiss_index = get_or_create_faiss_index(separate_value);
    } else {
        faiss_index = _faiss_index;
    }
    return read_scalar_data_from_file(faiss_index, path + file);
}

int VectorIndex::read_del_bitmap_from_file(const std::string& path, const std::string& file) {
    SmartFaissIndex faiss_index;
    if (_is_separate) {
        std::vector<std::string> vec;
        vec.reserve(4);
        boost::split(vec, file, boost::is_any_of("_"));
        if (vec.size() < 4) {
            DB_WARNING("Invalid file_name: %s", file.c_str());
            return -1;
        }
        const uint64_t separate_value = strtoull(vec[3].c_str(), NULL, 10);
        faiss_index = get_or_create_faiss_index(separate_value);
    } else {
        faiss_index = _faiss_index;
    }
    return read_del_bitmap_from_file(faiss_index, path + file);
}

int VectorIndex::read_not_cache_fields_from_file(const std::string& path, const std::string& file) {
    SmartFaissIndex faiss_index;
    if (_is_separate) {
        std::vector<std::string> vec;
        vec.reserve(4);
        boost::split(vec, file, boost::is_any_of("_"));
        if (vec.size() < 4) {
            DB_WARNING("Invalid file_name: %s", file.c_str());
            return -1;
        }
        const uint64_t separate_value = strtoull(vec[3].c_str(), NULL, 10);
        faiss_index = get_or_create_faiss_index(separate_value);
    } else {
        faiss_index = _faiss_index;
    }
    return read_not_cache_fields_from_file(faiss_index, path + file);
}

int VectorIndex::read_scalar_data_from_file(SmartFaissIndex faiss_index, const std::string& full_file_name) { 
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr, index_id: %ld", _index_id);
        return -1;
    }
    auto open_res = ::arrow::io::ReadableFile::Open(full_file_name, arrow::default_memory_pool());
    if (!open_res.status().ok()) {
        DB_WARNING("Fail to open file, %s", open_res.status().message().c_str());
        return -1;
    }
    auto infile = *open_res;
    auto open_reader_res = arrow::ipc::RecordBatchFileReader::Open(infile);
    if (!open_reader_res.status().ok()) {
        DB_WARNING("Fail to open file reader, %s", open_reader_res.status().message().c_str());
        return -1;
    }
    auto ipc_reader = *open_reader_res;
    auto read_res = ipc_reader->ReadRecordBatch(0);
    if (!read_res.status().ok()) {
        DB_WARNING("Fail to read record batch, %s", read_res.status().message().c_str());
        return -1;
    }
    // 空RecordBatch，从Rocksdb中恢复数据
    if ((*read_res)->num_rows() == 0) {
        DB_WARNING("empty RecordBatch, restore all data");
        return 0;
    }
    {
        bthread::RWLockWrGuard lock(faiss_index->rwlock);
        faiss_index->scalar_data = *read_res;
    }
    // schema如果发生变更，需要重构scalar_data
    if (schema_change_faiss_index(faiss_index) != 0) {
        DB_WARNING("schema change faiss index failed");
        return -1;
    }
    DB_WARNING("succ read_scalar_data_from_file, faiss_index->scalar_data num_rows: %ld, file_name: %s", 
                faiss_index->scalar_data->num_rows(), full_file_name.c_str());
    return 0;
}

int VectorIndex::read_del_bitmap_from_file(SmartFaissIndex faiss_index, const std::string& full_file_name) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr, index_id: %ld", _index_id);
        return -1;
    }
    {
        bthread::RWLockWrGuard lock(faiss_index->rwlock);
        if (faiss_index->scalar_data->num_rows() == 0) {
            // scalar_data无数据，从Rocksdb中恢复数据
            return 0;
        }
    }
    std::ifstream extra_fs(full_file_name);
    if (!extra_fs.is_open()) {
        DB_WARNING("Fail to open file: %s", full_file_name.c_str());
        return -1;
    }
    ScopeGuard auto_decrease([&extra_fs] () {
        extra_fs.close();
    });
    std::string serialized_str((std::istreambuf_iterator<char>(extra_fs)), std::istreambuf_iterator<char>());    
    try {
        *faiss_index->del_bitmap = Roaring::readSafe(serialized_str.c_str(), serialized_str.size());
    } catch (...) {
        DB_WARNING("bitmap read from string failed");
        return -1;
    }
    DB_WARNING("succ read_del_bitmap_from_file, cardinality: %lu, file_name: %s", 
                faiss_index->del_bitmap->cardinality(), full_file_name.c_str());
    return 0;
}

int VectorIndex::read_not_cache_fields_from_file(SmartFaissIndex faiss_index, const std::string& full_file_name) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr, index_id: %ld", _index_id);
        return -1;
    }
    std::ifstream extra_fs(full_file_name);
    if (!extra_fs.is_open()) {
        DB_WARNING("Fail to open file: %s", full_file_name.c_str());
        return -1;
    }
    ScopeGuard auto_decrease([&extra_fs] () {
        extra_fs.close();
    });
    std::string not_cache_fileds_str((std::istreambuf_iterator<char>(extra_fs)), std::istreambuf_iterator<char>());
    std::vector<std::string> vec;
    vec.reserve(8);
    boost::split(vec, not_cache_fileds_str, boost::is_any_of(","));
    for (const std::string& field_id : vec) {
        try {
            faiss_index->need_not_cache_fields.insert(std::stoi(field_id));
        } catch (...) {
            DB_WARNING("Invalid field_id: %s", field_id.c_str());
            return -1;
        }
    }
    DB_WARNING("succ read_not_cache_fields_from_file, need_not_cache_fields: size: %lu, str: %s", 
                faiss_index->need_not_cache_fields.size(), not_cache_fileds_str.c_str());
    return 0;
}

int VectorIndex::write_scalar_data_to_file(SmartFaissIndex faiss_index, const std::string& full_file_name) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    auto open_res = ::arrow::io::FileOutputStream::Open(full_file_name);
    if (!open_res.status().ok()) {
        DB_WARNING("Fail to open file, %s", open_res.status().message().c_str());
        return -1;
    }
    std::shared_ptr<::arrow::io::FileOutputStream> outfile = *open_res;
    auto make_writer_res = arrow::ipc::MakeFileWriter(outfile, faiss_index->scalar_data->schema());
    if (!make_writer_res.status().ok()) {
        DB_WARNING("Fail to make file writer, %s", make_writer_res.status().message().c_str());
        return -1;
    }
    std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer = *make_writer_res;
    ScopeGuard auto_decrease([&ipc_writer] () {
        ipc_writer->Close();
    });
    auto status = ipc_writer->WriteRecordBatch(*faiss_index->scalar_data);
    if (!status.ok()) {
        DB_WARNING("Fail to write record batch, %s", status.message().c_str());
        return -1;
    }
    DB_WARNING("succ write_scalar_data_to_file, faiss_index->scalar_data num_rows: %ld, file_name: %s", 
                faiss_index->scalar_data->num_rows(), full_file_name.c_str());
    return 0;
}

int VectorIndex::write_del_bitmap_to_file(SmartFaissIndex faiss_index, const std::string& full_file_name) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    std::ofstream extra_fs(full_file_name, std::ofstream::out | std::ofstream::trunc);
    if (!extra_fs.is_open()) {
        DB_WARNING("Fail to oepn file: %s", full_file_name.c_str());
        return -1;
    }
    ScopeGuard delete_auto_decrease([&extra_fs] () {
        extra_fs.close();
    });
    std::string serialized_str;
    uint32_t expected_size = faiss_index->del_bitmap->getSizeInBytes();
    serialized_str.resize(expected_size);
    char* buff = (char*)serialized_str.data();
    faiss_index->del_bitmap->write(buff);
    extra_fs << serialized_str;
    DB_WARNING("succ write_del_bitmap_to_file, cardinality: %lu, file_name: %s", 
                faiss_index->del_bitmap->cardinality(), full_file_name.c_str());
    return 0;
}

int VectorIndex::write_not_cache_fields_to_file(SmartFaissIndex faiss_index, const std::string& full_file_name) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    std::ofstream extra_fs(full_file_name, std::ofstream::out | std::ofstream::trunc);
    if (!extra_fs.is_open()) {
        DB_WARNING("Fail to oepn file: %s", full_file_name.c_str());
        return -1;
    }
    ScopeGuard delete_auto_decrease([&extra_fs] () {
        extra_fs.close();
    });
    std::string not_cache_fields_str;
    for (const auto& field_id : faiss_index->need_not_cache_fields) {
        not_cache_fields_str += std::to_string(field_id) + ",";
    }
    if (!not_cache_fields_str.empty()) {
        not_cache_fields_str.pop_back();
    }
    extra_fs << not_cache_fields_str;
    DB_WARNING("success write_not_cache_fields_to_file, file_name: %s, not_cache_fields_str: %s", 
                full_file_name.c_str(), not_cache_fields_str.c_str());
    return 0;
}

bool VectorIndex::need_compact(const int64_t table_lines) {
    int64_t faiss_idx_lines = 0;
    if (_is_separate) {
        auto call = [this, &faiss_idx_lines] (SmartFaissIndex& faiss_index) {
            if (faiss_index == nullptr) {
                return;
            }
            ScopeGuard guard([&faiss_index] () {
                if (faiss_index != nullptr) {
                    faiss_index->dec_ref();
                }
            });
            bthread::RWLockWrGuard lock(faiss_index->rwlock);
            if (faiss_index->index != nullptr) {
                faiss_idx_lines += faiss_index->index->ntotal;
            }
            if (faiss_index->flat_index != nullptr) {
                faiss_idx_lines += faiss_index->flat_index->ntotal;
            }
        };
        _separate_faiss_index_map->traverse(call);
    } else {
        if (_faiss_index == nullptr) {
            return false;
        }
        bthread::RWLockWrGuard lock(_faiss_index->rwlock);
        if (_faiss_index->index != nullptr) {
            faiss_idx_lines += _faiss_index->index->ntotal;
        }
        if (_faiss_index->flat_index != nullptr) {
            faiss_idx_lines += _faiss_index->flat_index->ntotal;
        }
    }
    if (table_lines > 0 && table_lines < faiss_idx_lines * FLAGS_table_lines_compaction_threshold) {
        DB_WARNING("need compact, tbale_lines: %ld, faiss_idx_lines: %ld", table_lines, faiss_idx_lines);
        return true;
    }
    return false;
}

}
