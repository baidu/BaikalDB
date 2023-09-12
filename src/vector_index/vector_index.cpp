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
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

namespace baikaldb {
int VectorIndex::init(const pb::RegionInfo& region_info, int64_t region_id, int64_t index_id, int64_t table_id) {
    _rocksdb = RocksWrapper::get_instance();
    _region_id = region_id;
    _index_id = index_id;
    _table_id = table_id;
    auto factory = SchemaFactory::get_instance();
    SmartIndex index_info = factory->get_index_info_ptr(_index_id);
    IndexInfo& info = *index_info;
    _dimension = info.dimension;
    if (_dimension <= 0) {
        DB_WARNING("dimension fail:%d", _dimension);
        return -1;
    }

    // TODO: 后续可以用正则匹配
    if (info.vector_description.empty()) {
        _vector_description = "IDMap,HNSW16";
    } else if (info.vector_description == "Flat") {
        _vector_description = "IDMap,Flat";
        _is_flat = true;
    } else if (info.vector_description == "L2norm,Flat") {
        _vector_description = "IDMap,L2norm,Flat";
        _is_flat = true;
        _is_l2norm = true;
    } else if (boost::istarts_with(info.vector_description, "HNSW")) {
        _vector_description = "IDMap," + info.vector_description;
    } else if (boost::istarts_with(info.vector_description, "L2norm,HNSW")) {
        _vector_description = "IDMap," + info.vector_description;
        _is_l2norm = true;
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
    _faiss_index = faiss::index_factory(_dimension, _vector_description.c_str(), _metrix_type);
    if (_faiss_index == nullptr) {
        DB_WARNING("_faiss_index init fail");
        return -1;
    }
    if (_is_ivf) {
        faiss::ParameterSpace().set_index_parameter(_faiss_index, "nprobe", 5);
    }
    if (!_is_flat) {
        if (_is_l2norm) {
            _faiss_flat_index = faiss::index_factory(_dimension, "IDMap,L2norm,Flat", _metrix_type);
        } else {
            _faiss_flat_index = faiss::index_factory(_dimension, "IDMap,Flat", _metrix_type);
        }
        if (_faiss_flat_index == nullptr) {
            DB_WARNING("_faiss_flat_index init fail");
            return -1;
        }
    }
    DB_WARNING("vector_description:%s, metrix_type:%d, dimension:%d, nprobe:%d", 
            _vector_description.c_str(), _metrix_type, _dimension, _nprobe);

    uint64_t region_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_region_id));
    _key_prefix.append((char*)&region_encode, sizeof(uint64_t));
    uint64_t table_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_index_id));
    _key_prefix.append((char*)&table_encode, sizeof(uint64_t));
    return 0;
}

int VectorIndex::restore_index(const pb::RegionInfo& region_info) {
    DB_WARNING("restore_index, region_id: %ld, _faiss_dump_idx: %ld, _faiss_cache_idx:%ld", region_info.region_id(), _faiss_dump_idx, _faiss_cache_idx.load());
    auto factory = SchemaFactory::get_instance();
    std::string key = _key_prefix;
    uint8_t level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    // compaction会多次调用restore_index，第一次是seek faiss_dump_idx
    uint64_t idx_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_faiss_cache_idx.load()));
    key.append((char*)&idx_encode, sizeof(uint64_t));
    auto data_cf = _rocksdb->get_data_handle();
    SmartIndex index_info = factory->get_index_info_ptr(_index_id);
    SmartIndex pk_info = factory->get_index_info_ptr(_table_id);
    SmartTable table_info = factory->get_table_info_ptr(_table_id);
    if (index_info == nullptr || table_info == nullptr || pk_info == nullptr) {
        DB_WARNING("table info not found _table_id:%ld", _table_id);
        return -1;
    }
    std::shared_ptr<RegionResource> resource(new RegionResource);
    resource->region_info = region_info;
    std::map<int32_t, FieldInfo*> field_ids;
    field_ids[index_info->fields[0].id] = &(index_info->fields[0]);

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

    for (iter->Seek(key); iter->Valid(); iter->Next()) {
        // 需要check region 范围
        TableKey key_slice(iter->key());
        uint8_t level = key_slice.extract_u8(sizeof(uint64_t) * 2);
        if (level == 1) {
            break;
        }
        _faiss_cache_idx = key_slice.extract_i64(sizeof(uint64_t) * 2 + sizeof(uint8_t));
        rocksdb::Slice value_slice(iter->value());
        // 最后一行_faiss_cache_idx+1
        int64_t cache_idx = _faiss_cache_idx.fetch_add(1);
        bool need_del = false;
        if (value_slice[0] == V_NORMAL) {
            SmartRecord record = factory->new_record(_table_id);
            int pos = 1;
            record->decode_key(*pk_info, value_slice, pos);
            int ret = txn->get_update_primary(_region_id, *pk_info, record, field_ids, GET_ONLY, true);
            if (ret == 0) {
                auto field = record->get_field_by_idx(index_info->fields[0].pb_idx);
                if (record->is_null(field)) {
                    continue;
                }
                std::string word;
                ret = record->get_reverse_word(*index_info, word);
                if (ret < 0) {
                    DB_WARNING("index_info to word fail for index_id: %ld", _index_id);
                    return ret;
                }
                ret = add_to_faiss(word, cache_idx);
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
            int64_t idx = TableKey(value).extract_i64(0);
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
    DB_WARNING("restore_index success, region_id: %ld, _faiss_dump_idx: %ld, _faiss_cache_idx:%ld, del_cnt:%ld",
            region_info.region_id(), _faiss_dump_idx, _faiss_cache_idx.load(), del_cnt);
    return 0;
}

int VectorIndex::insert_vector(
                   SmartTransaction& txn,
                   const std::string& word, 
                   const std::string& pk,
                   SmartRecord record) {
    _disable_write_cond.wait();
    int64_t cache_idx = 0;
    int ret = add_to_rocksdb(txn->get_txn(), pk, V_NORMAL, cache_idx);
    if (ret != 0) {
        return ret;
    }
    // TODO:乱序可能有问题
    return add_to_faiss(word, cache_idx);
}

int VectorIndex::delete_vector(
                   SmartTransaction& txn,
                   const std::string& word, 
                   const std::string& pk,
                   SmartRecord record) {
    ++_del_count;
    return del_to_rocksdb(txn->get_txn(), pk, V_DELETE);;
}

int VectorIndex::add_to_rocksdb(myrocksdb::Transaction* txn, const std::string& pk, VFlag flag, int64_t& cache_idx) {
    // region_id,index_id,0,faiss_idx => flag,pk
    std::string key = _key_prefix;
    uint8_t level = 0;
    key.append((char*)&level, sizeof(uint8_t));
    cache_idx = _faiss_cache_idx.fetch_add(1);
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
    value.assign((char*)&idx_encode, sizeof(uint64_t));
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

int VectorIndex::add_to_faiss(const std::string& word, int64_t cache_idx) {
    std::vector<std::string> split_vec;
    boost::split(split_vec, word,
            boost::is_any_of(" ,"), boost::token_compress_on);
    if ((int)split_vec.size() != _dimension) {
        DB_FATAL("split_vec.size:%lu != _dimension:%d", split_vec.size(), _dimension);
        return -1;
    }
    std::lock_guard<bthread::Mutex> lock(_faiss_mutex);
    for (auto& str : split_vec) {
        _cache_vectors.emplace_back(strtof(str.c_str(), NULL));
    }
    _cache_idxs.emplace_back(cache_idx++);
    
    if (_is_flat) {
        _faiss_index->add_with_ids(1, &_cache_vectors[_cache_vectors.size() - _dimension], 
                &_cache_idxs[_cache_idxs.size() - 1]);
        _cache_vectors.clear();
        _cache_idxs.clear();
        _faiss_idx = cache_idx;
    } else {
        if (_cache_idxs.size() > 1000) {
            _faiss_index->add_with_ids(_cache_idxs.size(), &_cache_vectors[0], &_cache_idxs[0]);
            _faiss_flat_index->reset();
            _cache_idxs.clear();
            _cache_vectors.clear();
            _faiss_idx = cache_idx;
        } else {
            _faiss_flat_index->add_with_ids(1, &_cache_vectors[_cache_vectors.size() - _dimension], 
                    &_cache_idxs[_cache_idxs.size() - 1]);
            //DB_WARNING("insert word:%s", word.c_str());
        }
    }
    return 0;
}

int VectorIndex::search(
                   myrocksdb::Transaction* txn,
                   SmartIndex& pk_info,
                   SmartTable& table_info,
                   const std::string& search_data,
                   int64_t topk,
                   std::vector<SmartRecord>& records) {
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
    {
        std::lock_guard<bthread::Mutex> lock(_faiss_mutex);
        if (_is_flat) {
            flat_idxs.resize(k, -1);
            flat_dis.resize(k, -1);
            _faiss_index->search(1, &search_vector[0], k, &flat_dis[0], &flat_idxs[0]);
            DB_WARNING("search:%ld, flat:", k);
        } else {
            flat_idxs.resize(k, -1);
            flat_dis.resize(k, -1);
            _faiss_flat_index->search(1, &search_vector[0], k, &flat_dis[0], &flat_idxs[0]);
            idxs.resize(k, -1);
            dis.resize(k, -1);
            _faiss_index->search(1, &search_vector[0], k, &dis[0], &idxs[0]);
            DB_WARNING("search:%ld, faiss:", k);
        }
    }
    std::vector<int64_t> result_idxs;
    std::vector<float> result_dis;
    result_idxs.reserve(k);
    result_dis.reserve(k);
    size_t i = 0;
    size_t j = 0;
    while ((i < flat_idxs.size() || j < idxs.size())
            && (int64_t)result_idxs.size() < k) {
        if (i < flat_idxs.size() && flat_idxs[i] == -1) {
            i = flat_idxs.size();
        }
        if (j < idxs.size() && idxs[j] == -1) {
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
            result_idxs.emplace_back(flat_idxs[i]);
            result_dis.emplace_back(flat_dis[i]);
            i++;
        } else if (j < idxs.size()) {
            result_idxs.emplace_back(idxs[j]);
            result_dis.emplace_back(dis[j]);
            j++;
        }
    }
    construct_records(txn, pk_info, table_info, result_idxs, result_dis, records);
    return 0;
}
int VectorIndex::construct_records(myrocksdb::Transaction* txn, SmartIndex& pk_info, SmartTable& table_info, const std::vector<int64_t>& idxs, 
        const std::vector<float>& dis, std::vector<SmartRecord>& records) {
    auto factory = SchemaFactory::get_instance();
    std::vector<std::string> keys;
    keys.reserve(idxs.size());
    std::vector<rocksdb::Slice> rocksdb_keys;
    rocksdb_keys.reserve(idxs.size());
    for (int64_t id : idxs) {
        std::string key = _key_prefix;
        uint8_t level = 0;
        key.append((char*)&level, sizeof(uint8_t));
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
}
