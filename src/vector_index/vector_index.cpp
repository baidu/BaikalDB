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
#include "transaction.h"
#include <faiss/AutoTune.h>
#include <faiss/IndexIDMap.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

namespace baikaldb {
DEFINE_int32(efsearch_retry_pow, 4, "efsearch_retry_pow");
DEFINE_int32(max_efsearch, 16384, "max_efsearch");
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
    } else if (boost::istarts_with(info.vector_description, "IVF")) {
        _vector_description = info.vector_description;
        _need_train = true;
        _is_ivf = true;
    } else if (boost::istarts_with(info.vector_description, "L2norm,IVF")) {
        _vector_description = info.vector_description;
        _need_train = true;
        _is_ivf = true;
        _is_l2norm = true;
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
        std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
        faiss_index->dump_idx = faiss_index->idx;
        faiss::write_index(faiss_index->index, full_path.c_str());
        extra_fs << faiss_index->separate_value << "\t" 
                 << faiss_index->dump_idx << "\t"
                 << faiss_index->del_count.load() << "\n";
    };
    _separate_faiss_index_map->traverse_copy(inc_ref_faiss_index, call);
    if (!is_succ) {
        DB_WARNING("fail to restore_faiss_index");
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
        std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
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
        std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
        faiss_index->dump_idx = dump_idx;
        faiss_index->idx = faiss_index->dump_idx;
        faiss_index->cache_idx = faiss_index->dump_idx;
        faiss_index->del_count = del_count;
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
    int ret = add_to_rocksdb(txn->get_txn(), faiss_index, pk, V_NORMAL, cache_idx);
    if (ret != 0) {
        return ret;
    }
    // TODO:乱序可能有问题
    return add_to_faiss(faiss_index, word, cache_idx);
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
    if (faiss_index != nullptr) {
        faiss_index->del_count++;
    }
    return del_to_rocksdb(txn->get_txn(), pk, V_DELETE);
}

int VectorIndex::search_vector(
            myrocksdb::Transaction* txn,
            const uint64_t separate_value,
            SmartIndex& pk_info,
            SmartTable& table_info,
            const std::string& search_data,
            int64_t topk,
            std::vector<SmartRecord>& records,
            int retry_count,
            bool& eos) {
    SmartFaissIndex faiss_index;
    if (_is_separate) {
        faiss_index = _separate_faiss_index_map->get(separate_value);
        if (faiss_index == nullptr) {
            // 没有数据，直接返回
            eos = true;
            return 0;
        }
    } else {
        faiss_index = _faiss_index;
    }
    return search(txn, faiss_index, pk_info, table_info, search_data, topk, records, retry_count, eos);
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

int VectorIndex::del_to_rocksdb(myrocksdb::Transaction* txn, const std::string& pk, VFlag flag) {
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

    key = _key_prefix;
    level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    key.append(value);
    value.clear();
    value.append(1, (char)V_DELETE);
    value.append(pk);
    auto put_res = txn->Put(data_cf, key, value);
    if (!put_res.ok()) {
        DB_WARNING("rocksdb put error: code=%d, msg=%s",
                    put_res.code(), put_res.ToString().c_str());
        return -1;
    }
    return 0;
}

int VectorIndex::add_to_faiss(SmartFaissIndex faiss_index, 
                              const std::string& word, 
                              int64_t cache_idx) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    std::vector<std::string> split_vec;
    boost::split(split_vec, word,
            boost::is_any_of(" ,"), boost::token_compress_on);
    if ((int)split_vec.size() != _dimension) {
        DB_FATAL("split_vec.size:%lu != _dimension:%d", split_vec.size(), _dimension);
        return -1;
    }
    std::vector<int64_t> cache_idxs;
    std::vector<float> cache_vectors;
    cache_vectors.reserve(_dimension);
    for (auto& str : split_vec) {
        cache_vectors.emplace_back(strtof(str.c_str(), NULL));
    }
    cache_idxs.emplace_back(cache_idx++);
    std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
    if (_is_flat) {
        if (faiss_index->index == nullptr) {
            DB_WARNING("faiss_index->index is nullptr");
            return -1;
        }
        faiss_index->index->add_with_ids(1, 
                                         &cache_vectors[cache_vectors.size() - _dimension], 
                                         &cache_idxs[cache_idxs.size() - 1]);
        faiss_index->idx = cache_idx;
    } else {
        if (faiss_index->index == nullptr || faiss_index->flat_index == nullptr) {
            DB_WARNING("faiss_index->index/faiss_index->flat_index is nullptr");
            return -1;
        }
        if (faiss_index->flat_index->ntotal >= 1000) {
            // reconstruct from faiss_index, 减少缓存，降低内存占用
            faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->flat_index);
            size_t origin_cache_size = cache_vectors.size();
            size_t cache_size = origin_cache_size + idmap2_index->rev_map.size() * _dimension;
            cache_vectors.resize(cache_size);
            int pos_idx = origin_cache_size;
            for (const auto& [id, _] : idmap2_index->rev_map) {
                cache_idxs.emplace_back(id);
                idmap2_index->reconstruct(id, cache_vectors.data() + pos_idx);
                pos_idx += _dimension;
            }
            idmap2_index->rev_map.clear();
            faiss_index->index->add_with_ids(cache_idxs.size(), 
                                             &cache_vectors[0], 
                                             &cache_idxs[0]);
            faiss_index->flat_index->reset();
            faiss_index->idx = cache_idx;
        } else {
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
                   std::vector<SmartRecord>& records,
                   int retry_count,
                   bool& eos) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return -1;
    }
    std::vector<std::string> split_vec;
    boost::split(split_vec, search_data,
            boost::is_any_of(" ,"), boost::token_compress_on);
    if ((int)split_vec.size() != _dimension) {
        DB_FATAL("search_data:%s, split_vec.size:%lu != _dimension:%d", search_data.c_str(), split_vec.size(), _dimension);
        return -1;
    }
    std::vector<float> search_vector;
    search_vector.reserve(_dimension);
    for (auto& str : split_vec) {
        search_vector.emplace_back(strtof(str.c_str(), NULL));
    }
    int64_t k = 2 * topk;
    std::vector<int64_t> flat_idxs;
    std::vector<float> flat_dis;
    std::vector<int64_t> idxs;
    std::vector<float> dis;
    records.clear();
    eos = false;
    int64_t least = 10;
    int64_t search_count = k;
    {
        std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
        if (_is_flat) {
            if (faiss_index->index == nullptr) {                
                DB_WARNING("faiss_index->index is nullptr");
                return -1;
            }
            if (k >= faiss_index->index->ntotal) {
                eos = true;
            }
            int64_t mink = std::max(std::min(k, faiss_index->index->ntotal), least); //0,1都会core
            idxs.resize(mink, -1);
            dis.resize(mink, -1);
            faiss_index->index->search(1, &search_vector[0], mink, &dis[0], &idxs[0]);
            DB_WARNING("search:%ld, flat:", mink);
        } else {
            if (faiss_index->index == nullptr || faiss_index->flat_index == nullptr) {
                DB_WARNING("faiss_index->index/faiss_index->flat_index is nullptr");
                return -1;
            }
            int64_t mink = std::max(std::min(k, faiss_index->flat_index->ntotal), least);
            flat_idxs.resize(mink, -1);
            flat_dis.resize(mink, -1);
            faiss_index->flat_index->search(1, &search_vector[0], mink, &flat_dis[0], &flat_idxs[0]);
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
            // TODO: 扩展efsearch可能会乱序，后续得优化
            int efsearch = std::min(static_cast<int>(_efsearch * std::pow(FLAGS_efsearch_retry_pow, retry_count)), FLAGS_max_efsearch);
            faiss::ParameterSpace().set_index_parameter(faiss_index->index, "efSearch", efsearch);
            faiss_index->index->search(1, &search_vector[0], mink, &dis[0], &idxs[0]);
            int64_t search_cnt = 0;
            for (int64_t i : idxs) {
                if (i == -1) {
                    break;
                } else {
                    ++search_cnt;
                }
            }
            DB_WARNING("separate_value: %lu, search:%ld, faiss.ntotal:%lu,%lu, "
                        "search_cnt: %ld,%ld, retry_count:%d, efsearch:%d", 
                        faiss_index->separate_value, mink, faiss_index->flat_index->ntotal, faiss_index->index->ntotal, 
                        flat_search_cnt, search_cnt, retry_count, efsearch);
            if (k >= faiss_index->index->ntotal + faiss_index->flat_index->ntotal) {
                if (efsearch >= FLAGS_max_efsearch || search_cnt >= faiss_index->index->ntotal) {
                    eos = true;
                }
            }
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
        if (i < flat_idxs.size() && flat_idxs[i] == -1) {
            // 后面会扩展召回，没召回满的需要停止召回
            if (faiss_index->flat_index != nullptr && i < faiss_index->flat_index->ntotal) {
                break;
            }
            i = flat_idxs.size();
        }
        if (j < idxs.size() && idxs[j] == -1) {
            // 后面会扩展召回，没召回满的需要停止召回
            if (j < faiss_index->index->ntotal) {
                break;
            }
            j = idxs.size();
        }
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
            // 后面会扩展召回，没召回满的需要停止召回
            if (j < faiss_index->index->ntotal) {
                break;
            }
            result_idxs.emplace_back(flat_idxs[i]);
            result_dis.emplace_back(flat_dis[i]);
            i++;
        } else if (j < idxs.size()) {
            // 后面会扩展召回，没召回满的需要停止召回
            if (faiss_index->flat_index != nullptr && i < faiss_index->flat_index->ntotal) {
                break;
            }
            result_idxs.emplace_back(idxs[j]);
            result_dis.emplace_back(dis[j]);
            j++;
        }
    }
    int ret = construct_records(txn, faiss_index->separate_value, pk_info, table_info, result_idxs, result_dis, records);
    if (ret < 0) {
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

int VectorIndex::compact(const pb::RegionInfo& region_info, bool is_force) {
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
                DB_WARNING("fail to restore_faiss_index");
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
                std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
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
    if (_is_ivf) {
        faiss::ParameterSpace().set_index_parameter(faiss_index->index, "nprobe", _nprobe);
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
    return 0;
}

int VectorIndex::reset_faiss_index(SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        return -1;
    }
    std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
    if (_is_flat) {
        faiss_index->index->reset();
        faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->index);
        idmap2_index->rev_map.clear();
    } else {
        faiss_index->index->reset();
        faiss_index->flat_index->reset();
        faiss::IndexIDMap2* idmap2_index = static_cast<faiss::IndexIDMap2*>(faiss_index->flat_index);
        idmap2_index->rev_map.clear();   
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
    field_ids[index_info->fields[vector_field_idx].id] = &(index_info->fields[vector_field_idx]);

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
            SmartRecord record = factory->new_record(_table_id);
            if (record == nullptr) {
                DB_FATAL("record is null, table_id:%ld", _table_id);
                return -1;
            }
            int pos = 1;
            record->decode_key(*pk_info, value_slice, pos);
            int ret = txn->get_update_primary(_region_id, *pk_info, record, field_ids, GET_ONLY, true);
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
                ret = add_to_faiss(faiss_index, word, cache_idx);
                if (ret < 0) {
                    DB_WARNING("add_to_faiss fail, index_id: %ld", _index_id);
                    return ret;
                }
            } else if (ret == -3) {
                need_del = true;
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
    }
    txn->commit();
    DB_DEBUG("restore_index success, region_id: %ld, faiss dump_idx: %ld, "
             "faiss cache_idx:%ld, del_cnt:%ld, separate_value: %lu", 
              region_info.region_id(), faiss_index->dump_idx, 
              faiss_index->cache_idx.load(), del_cnt, faiss_index->separate_value);
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
    ScopeGuard auto_decrease([&faiss_index] () {
        faiss_index->disable_write_cond.decrease_signal();
    });
    faiss_index->swap(tmp_faiss_index);
    ret = restore_faiss_index(region_info, faiss_index);
    if (ret < 0) {
        DB_FATAL("Fail to restore_index, region_id: %ld", _region_id);
        return -1;
    }
    return 0;
}

 bool VectorIndex::need_compact_faiss_index(SmartFaissIndex faiss_index) {
    if (faiss_index == nullptr) {
        DB_WARNING("faiss_index is nullptr");
        return false;
    }
    std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
    int64_t ntotal = 0;
    if (faiss_index->index != nullptr) {
        ntotal += faiss_index->index->ntotal;
    }
    if (faiss_index->flat_index != nullptr) {
        ntotal += faiss_index->flat_index->ntotal;
    }
    if (ntotal > 10 && faiss_index->del_count * 100 / ntotal > 30) {
        DB_WARNING(
                "region_id: %ld, ntotal:%ld, _del_count:%ld",
                _region_id,
                ntotal,
                faiss_index->del_count.load());
        return true;
    }
    // 用于向量隔离索引删除空FaissIndex
    if (_is_separate) {
        if (ntotal > 0 && faiss_index->del_count >= ntotal) {
            DB_WARNING(
                    "region_id: %ld, ntotal:%ld, _del_count:%ld",
                    _region_id,
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
    std::lock_guard<bthread::Mutex> lock(faiss_index->mutex);
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

}
