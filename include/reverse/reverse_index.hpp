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

namespace baikaldb {
template <typename Schema>
int ReverseIndex<Schema>::reverse_merge_func(pb::RegionInfo info, bool need_remove_third) {
    _key_range = KeyRange(info.start_key(), info.end_key());
    if (need_remove_third) {
        _reverse_remove_range_for_third_level(2);
        _reverse_remove_range_for_third_level(3);
    }
    if (_write_count <= 0) {
        return 0;
    }
    _write_count = 0;
    int8_t status = 0;
    TimeCost timer;

    _level_1_scan_count = 0;

    //DB_NOTICE("region %ld table %ld merge %d wait time %lu", 
    //                    _region_id, _index_id, _reverse_prefix, timer.get_time());
    uint8_t prefix = _reverse_prefix;
    
    //1. create prefix key (regionid+tableid+_reverse_prefix)
    std::string key;
    //prefix 0，第一层
    _create_reverse_key_prefix(prefix, key);
    //2. scan every term
    rocksdb::ReadOptions roptions;
    const uint64_t max = UINT64_MAX;
    std::string end_key = key;
    end_key.append((char*)&max, sizeof(uint64_t));
    rocksdb::Slice upper_bound_slice = end_key;
    roptions.iterate_upper_bound = &upper_bound_slice;
    roptions.prefix_same_as_start = true;
    roptions.total_order_seek = false;
    roptions.fill_cache = false;
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    std::unique_ptr<myrocksdb::Iterator> iter(new myrocksdb::Iterator(_rocksdb->new_iterator(roptions, data_cf)));
    usleep(200);
    iter->Seek(key);
    bool end_flag = is_prefix_end(iter, prefix);
    if (_is_over_cache) {
        _cache_keys.clear();
    }
    int64_t seek_time = timer.get_time();
    if (end_flag) {
        DB_WARNING("seek end merge dowith time:%ld, seek time:%ld, region_id:%ld, cache:%s, "
                "seg_cache:%s, prefix:%d,level_1_scan_count:%ld", 
                timer.get_time(), seek_time, _region_id, 
                _cache.get_info().c_str(), _seg_cache.get_info().c_str(), prefix, _level_1_scan_count);
        return 0;
    }
    while (true) {
        //第一层数据合并到第二层。
        status = _reverse_merge_to_second_level(iter, prefix);
        if (status == -1) {
            DB_WARNING("error merge dowith time:%ld, seek time:%ld, region_id:%ld, cache:%s, "
                    "seg_cache:%s, prefix:%d,level_1_scan_count:%ld", 
                    timer.get_time(), seek_time, _region_id, 
                    _cache.get_info().c_str(), _seg_cache.get_info().c_str(), prefix, _level_1_scan_count);
            return -1;
        } 
        if (status == 1) {
            //if over
            break;
        }
    }
    //清理旧缓存
    if (_is_over_cache) {
        for (auto& key: _cache_keys) {
            _cache.del(key);
        }
    }
    
    DB_WARNING("merge dowith time:%ld, seek time:%ld, region_id:%ld, index_id:%ld, cache:%s, "
    "seg_cache:%s, prefix:%d,level_1_scan_count:%ld", 
            timer.get_time(), seek_time, _region_id, _index_id, 
            _cache.get_info().c_str(), _seg_cache.get_info().c_str(), prefix, _level_1_scan_count);
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::handle_reverse(
                                    SmartTransaction& txn,
                                    pb::ReverseNodeType flag,
                                    const std::string& word,
                                    const std::string& pk,
                                    SmartRecord record) {
    if (word.empty()) {
        return 0;
    }
    txn->set_reverse_set(this);
    static bvar::LatencyRecorder reverse_time_cost("reverse_time_cost");
    TimeCost cost;
    int8_t status;
    std::shared_ptr<std::map<std::string, ReverseNode>> cache_seg_res;
    std::shared_ptr<std::map<std::string, ReverseNode>> seg_res =
        std::make_shared<std::map<std::string, ReverseNode>>();
    if (_is_seg_cache) {
        if (_seg_cache.find(word, &cache_seg_res) != 0) {
            Schema::segment(word, pk, record, _segment_type, _name_field_id_map, flag, *seg_res, _charset);
            _seg_cache.add(word, seg_res);
        } else {
            *seg_res = *cache_seg_res;
            // 填充pk，flag信息
            Schema::segment(word, pk, record, _segment_type, _name_field_id_map, flag, *seg_res, _charset);
        }
    } else {
        Schema::segment(word, pk, record, _segment_type, _name_field_id_map, flag, *seg_res, _charset);
    }
    auto map_it = seg_res->begin();
    while (map_it != seg_res->end()) {
        status = _insert_one_reverse_node(txn->get_txn(), map_it->first, &map_it->second);
        if (status != 0) {
            return -1;
        }
        ++map_it;
    }
    reverse_time_cost << cost.get_time();
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::insert_reverse(
                                    SmartTransaction& txn,
                                    const std::string& word,
                                    const std::string& pk,
                                    SmartRecord record) {
    return handle_reverse(txn, pb::REVERSE_NODE_NORMAL, word, pk, record);
}

template <typename Schema>
int ReverseIndex<Schema>::delete_reverse(
                                    SmartTransaction& txn,
                                    const std::string& word,
                                    const std::string& pk,
                                    SmartRecord record) {
    return handle_reverse(txn, pb::REVERSE_NODE_DELETE, word, pk, record);
}

template <typename Schema>
int ReverseIndex<Schema>::search(
                       myrocksdb::Transaction* txn,
                       const IndexInfo& index_info,
                       const TableInfo& table_info,
                       const std::string& search_data,
                       pb::MatchMode mode,
                       std::vector<ExprNode*> conjuncts, 
                       bool is_fast) {
    TimeCost time;
    int ret = create_executor(txn, index_info, table_info, search_data, mode, conjuncts, is_fast);
    if (ret < 0) {
        return -1;
    }
    DB_NOTICE("bianli time : %lu", time.get_time());
    print_reverse_statistic_log();
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::get_reverse_list_two(
                                    myrocksdb::Transaction* txn,  
                                    const std::string& term, 
                                    ReverseListSptr& list_new_ptr,
                                    ReverseListSptr& list_old_ptr,
                                    bool is_fast) {
    rocksdb::ReadOptions roptions;
    roptions.prefix_same_as_start = true;
    roptions.fill_cache = false;
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return 0;
    }
    ItemStatistic tmp_statistic;
    ItemStatistic* item_statistic = &tmp_statistic;
    auto schema_info = bthread_local_schema();
    if (schema_info != nullptr) {
        schema_info->schema->statistic().term_times.push_back(ItemStatistic());
        item_statistic = &schema_info->schema->statistic().term_times[schema_info->schema->statistic().term_times.size() - 1];
    }

    item_statistic->term = term;
    TimeCost timer;
    TimeCost timer_tmp;
    if (is_fast) {
        _get_level_reverse_list(txn, 2, term, list_new_ptr, true);

        item_statistic->is_fast = true;
        item_statistic->get_new += timer_tmp.get_time();
        timer_tmp.reset();
    } else {
        std::string key_first_new;
        _create_reverse_key_prefix(_reverse_prefix, key_first_new);
        key_first_new.append(term);
        std::unique_ptr<myrocksdb::Iterator> iter_first_new(new myrocksdb::Iterator(txn->GetIterator(roptions, data_cf)));
        iter_first_new->Seek(key_first_new);
        item_statistic->seek_new += timer_tmp.get_time();
        timer_tmp.reset();
        FirstLevelMSIterator<ReverseNode, ReverseList> iter_first(
                                            iter_first_new, 
                                            _reverse_prefix, 
                                            _key_range, 
                                            term);
        
        ReverseListSptr second_list(new ReverseList()); 
        _get_level_reverse_list(txn, 2, term, second_list, true);
        SecondLevelMSIterator<ReverseNode, ReverseList> iter_second(
                                                            (ReverseList&)*second_list, 
                                                            _key_range);
        item_statistic->get_two += timer_tmp.get_time();
        timer_tmp.reset();
        ReverseListSptr tmp_ptr(new ReverseList());
        level_merge<ReverseNode, ReverseList>(
                            &iter_first, &iter_second, 
                            (ReverseList&)*tmp_ptr, false);
        list_new_ptr = tmp_ptr;
        item_statistic->merge_one_two += timer_tmp.get_time();
        timer_tmp.reset();
    }

    _get_level_reverse_list(txn, 3, term, list_old_ptr, true, true);
    ReverseList* tmp = nullptr;
    tmp = (ReverseList*)list_new_ptr.get();
    if (tmp != nullptr) {
        item_statistic->second_length = tmp->reverse_nodes_size();
    }
    tmp = nullptr;
    tmp = (ReverseList*)list_old_ptr.get();
    if (tmp != nullptr) { 
        item_statistic->third_length = tmp->reverse_nodes_size();
    }
    item_statistic->get_three += timer_tmp.get_time();
    item_statistic->get_list += timer.get_time();
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::create_executor(
                            myrocksdb::Transaction* txn,
                            const IndexInfo& index_info,
                            const TableInfo& table_info,
                            const std::string& search_data, 
                            pb::MatchMode mode,
                            std::vector<ExprNode*> conjuncts, 
                            bool is_fast) {
    TimeCost timer;
    auto schema_info = create_bthread_local_schema_if_null();
    if (schema_info == nullptr) {
        return -1;
    }
    
    schema_info->schema = new (std::nothrow) Schema();
    if (schema_info->schema == nullptr) {
        return -1;
    }
    schema_info->schema_ptrs.emplace_back(schema_info->schema);
    schema_info->schema->init(this, txn, _key_range, conjuncts, is_fast);
    timer.reset();
    schema_info->schema->set_index_info(index_info);
    schema_info->schema->set_table_info(table_info);
    schema_info->schema->set_index_search(this);
    int ret = schema_info->schema->create_executor(search_data, mode, _segment_type, _charset);
    schema_info->schema->statistic().bool_engine_time += timer.get_time();
    if (ret < 0) {
        DB_WARNING("create_executor fail, region:%ld, index:%ld", _region_id, _index_id);
        return -1;
    }
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::_create_reverse_key_prefix(uint8_t level, std::string& key) {
    uint64_t region_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_region_id));
    key.append((char*)&region_encode, sizeof(uint64_t));
    uint64_t table_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(_index_id));
    key.append((char*)&table_encode, sizeof(uint64_t));
    key.append((char*)&level, sizeof(uint8_t));
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::_reverse_remove_range_for_third_level(uint8_t prefix) {
    // 和merge在同一个线程调度，简化处理
    // 如果后续发现性能问题分开的话，需要这里和merge都要调整成GetForUpdate
    //int8_t status = 0;
    //1. create prefix key (regionid+tableid+_reverse_prefix)
    std::string key;
    _create_reverse_key_prefix(prefix, key);
    const uint64_t max = UINT64_MAX;
    std::string end_key = key;
    end_key.append((char*)&max, sizeof(uint64_t));
    rocksdb::Slice upper_bound_slice = end_key;

    //2. scan every term
    rocksdb::ReadOptions roptions;
    roptions.iterate_upper_bound = &upper_bound_slice;
    roptions.prefix_same_as_start = true;
    roptions.fill_cache = false;
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    int64_t scan_rows = 0;
    int64_t remove_rows = 0;
    int64_t scan_node_count = 0;
    int64_t remove_node_count = 0;
    TimeCost cost;
    std::unique_ptr<myrocksdb::Iterator> iter(new myrocksdb::Iterator(_rocksdb->new_iterator(roptions, data_cf)));
    for (iter->Seek(key); !is_prefix_end(iter, prefix); iter->Next()) {
        ++scan_rows;
        // 内部txn，不提交出作用域自动析构
        SmartTransaction txn(new Transaction(0, nullptr));
        rocksdb::TransactionOptions txn_opt;
        txn_opt.lock_timeout = 100;
        txn->begin(txn_opt);
        //get merge term for debug
        const size_t len = 8 + 8 + 1;
        if (iter->key().size() <= len) {
            DB_FATAL("region_id: %ld, index_id: %ld, key size: %lu less len: %lu", 
                    _region_id, _index_id, iter->key().size(), len);
            continue;
        }
        std::string merge_term(iter->key().data() + len, iter->key().size() - len);

        // 搞个空的level2，用来帮助merge
        ReverseListSptr second_level_list(new ReverseList());
        ReverseListSptr third_level_list(new ReverseList());
        //deserialize
        if (!third_level_list->ParseFromArray(iter->value().data(), iter->value().size())) {
            DB_FATAL("parse level %d list from pb failed, region_id: %ld, index_id: %ld",
                    prefix, _region_id, _index_id);
            return -1;
        }
        ReverseList& third_msg = static_cast<ReverseList&>(*third_level_list);
        int old_count = third_msg.reverse_nodes_size();
        scan_node_count += old_count;
        if (old_count > 0) {
            std::string first_key = third_msg.reverse_nodes(0).key();
            std::string last_key = third_msg.reverse_nodes(old_count - 1).key();
            if (first_key >= _key_range.first &&
                    end_key_compare(last_key, _key_range.second) < 0) {
                DB_DEBUG("in range need not remove, region_id: %ld, index_id: %ld, old_count: %d",
                        _region_id, _index_id, old_count);
                continue;
            }
        } else {
            DB_WARNING("old_count is 0; region_id: %ld, index_id: %ld, term: %s",
                    _region_id, _index_id, merge_term.c_str());
        }
        SecondLevelMSIterator<ReverseNode, ReverseList> 
            second_iter((ReverseList&)*second_level_list, _key_range);
        SecondLevelMSIterator<ReverseNode, ReverseList> 
            third_iter((ReverseList&)*third_level_list, _key_range);
        std::unique_ptr<ReverseList> new_third_level_list(new ReverseList());
        // merge里判断了范围(迭代器扫描内判断了范围)
        int result_count = level_merge<ReverseNode, ReverseList>(
                &second_iter, &third_iter, *new_third_level_list, false);
        if (old_count > 0 && result_count == old_count) {
            DB_WARNING("need not remove, region_id: %ld, index_id: %ld, old_count: %d",
                    _region_id, _index_id, old_count);
            continue;
        }
        remove_node_count += old_count - result_count;
        if (result_count == -1) {
            DB_FATAL("remove_range 2 and 3 failed");
            return -1;
        }   
        std::string value;
        if (!new_third_level_list->SerializeToString(&value)) {
            DB_FATAL("remove_range serialize failed, region_id: %ld, index_id: %ld, old_count: %d",
                    _region_id, _index_id, old_count);
            return -1;
        }
        if (result_count > 0) {
            auto put_res = txn->get_txn()->Put(data_cf, iter->key(), value);
            if (!put_res.ok()) {
                DB_FATAL("region_id: %ld, index_id: %ld, old_count: %d, rocksdb put error: code=%d, msg=%s",
                        _region_id, _index_id, old_count, put_res.code(), put_res.ToString().c_str());
                return -1;
            }
        } else {
            ++remove_rows;
            auto del_res = txn->get_txn()->Delete(data_cf, iter->key());
            if (!del_res.ok()) {
                DB_FATAL("region_id: %ld, index_id: %ld, old_count: %d, rocksdb del error: code=%d, msg=%s",
                        _region_id, _index_id, old_count, del_res.code(), del_res.ToString().c_str());
                return -1;
            }
        }
        auto s = txn->commit();
        if (!s.ok()) {
            DB_WARNING("remove_range commit failed: %s", s.ToString().c_str());
            return -1;
        }
    }
    DB_WARNING("prefix:%d, _reverse_remove_range_for_third_level, region_id: %ld, index_id: %ld, "
            "scan rows: %ld, remove rows: %ld, scan_node_count: %ld, remove_node_count:%ld , cost: %ld",
            prefix, _region_id, _index_id, scan_rows, remove_rows, scan_node_count, remove_node_count, cost.get_time());
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::_reverse_merge_to_second_level(
                                std::unique_ptr<myrocksdb::Iterator>& iterator, 
                                uint8_t prefix) {
    int8_t status;
    bool end_flag = is_prefix_end(iterator, prefix);
    if (end_flag) {
        //end 
        return 1;
    }
    // 内部txn，不提交出作用域自动析构
    SmartTransaction txn(new Transaction(0, nullptr));
    rocksdb::TransactionOptions txn_opt;
    txn_opt.lock_timeout = 100;
    txn->begin(txn_opt);
    //get merge term
    std::string merge_term = get_term_from_reverse_key(iterator->key());
    FirstLevelMSIterator<ReverseNode, ReverseList> first_iter(iterator, prefix, 
                                        _key_range, merge_term, true, _rocksdb, txn->get_txn());
    //create second level key
    std::string second_level_key;
    _create_reverse_key_prefix(2, second_level_key);
    second_level_key.append(merge_term);
    //get second level reverse list
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    std::string value;
    ReverseListSptr second_level_list(new ReverseList());
    status = _get_level_reverse_list(txn->get_txn(), 2, merge_term, second_level_list);
    if (status != 0) {
        DB_WARNING("get second level list failed");
        return -1;
    }
    SecondLevelMSIterator<ReverseNode, ReverseList> second_iter(
                                                            (ReverseList&)*second_level_list, 
                                                            _key_range);
    std::unique_ptr<ReverseList> new_second_level_list(new ReverseList());
    int result_count = level_merge<ReverseNode, ReverseList>(
                    &first_iter, &second_iter, *new_second_level_list, false);
    if (result_count == -1) {
        DB_WARNING("merge 1 and 2 failed, term:%s", merge_term.c_str());
        return -1;
    }   
    int old_second_level_size = static_cast<ReverseList&>(*second_level_list).reverse_nodes_size();
    int second_level_size = new_second_level_list->reverse_nodes_size(); 
    _level_1_scan_count += second_level_size - old_second_level_size;
    //if (second_level_size > 0 && second_level_size < _second_level_length) {
    if (!new_second_level_list->SerializeToString(&value)) {
        DB_WARNING("serialize failed");
        return -1;
    }
    auto put_res = txn->get_txn()->Put(data_cf, second_level_key, value);
    if (!put_res.ok()) {
        DB_WARNING("rocksdb put error: code=%d, msg=%s",
                put_res.code(), put_res.ToString().c_str());
        return -1;
    }
    auto s = txn->commit();
    if (!s.ok()) {
        DB_WARNING("merge commit failed: %s", s.ToString().c_str());
        return -1;
    }
    if (second_level_size >= _second_level_length) {
        //DB_WARNING("merge 2 level to 3");
        // 2/3层合并单独开txn处理
        SmartTransaction txn_level2(new Transaction(0, nullptr));
        txn_level2->begin(txn_opt);
        ReverseListSptr third_level_list(new ReverseList());
        status = _get_level_reverse_list(txn_level2->get_txn(), 3, merge_term, third_level_list);
        if (status != 0) {
            return -1;
        }
        SecondLevelMSIterator<ReverseNode, ReverseList> 
                        third_iter((ReverseList&)*third_level_list, _key_range);
        SecondLevelMSIterator<ReverseNode, ReverseList> second_iter(
                                                        *new_second_level_list, 
                                                        _key_range);
        std::unique_ptr<ReverseList> new_third_level_list(new ReverseList());
        int result_count = level_merge<ReverseNode, ReverseList>(
                        &second_iter, &third_iter, *new_third_level_list, true);
        if (result_count == -1) {
            DB_WARNING("merge 2 and 3 failed");
            return -1;
        }   
        if (!new_third_level_list->SerializeToString(&value)) {
            DB_WARNING("serialize failed");
            return -1;
        }
        std::string third_level_key;
        _create_reverse_key_prefix(3, third_level_key);
        third_level_key.append(merge_term);
        if (result_count > 0) {
            auto put_res = txn_level2->get_txn()->Put(data_cf, third_level_key, value);
            if (!put_res.ok()) {
                DB_WARNING("rocksdb put error: code=%d, msg=%s",
                        put_res.code(), put_res.ToString().c_str());
                return -1;
            }
        } else {
            auto del_res = txn_level2->get_txn()->Delete(data_cf, third_level_key);
            if (!del_res.ok()) {
                DB_WARNING("rocksdb del error: code=%d, msg=%s",
                        del_res.code(), del_res.ToString().c_str());
                return -1;
            }
        }
        status = _delete_level_reverse_list(txn_level2->get_txn(), 2, merge_term);
        if (status != 0) {
            DB_WARNING("delete reverse list failed");
            return -1;
        }
        auto s = txn_level2->commit();
        if (!s.ok()) {
            DB_WARNING("merge commit failed: %s", s.ToString().c_str());
            return -1;
        }
        if (_is_over_cache) {
            _cache_keys.push_back(third_level_key);
        }
    }
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::_get_level_reverse_list(
                                    myrocksdb::Transaction* txn, 
                                    uint8_t level, 
                                    const std::string& term, 
                                    ReverseListSptr& list_ptr,
                                    bool is_statistic,
                                    bool is_over_cache) {
    std::string key;
    _create_reverse_key_prefix(level, key);
    key.append(term);
    rocksdb::ReadOptions roptions;
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    ItemStatistic* item_statistic = nullptr;
    if (is_statistic) {
        auto schema_info = bthread_local_schema();
        if (schema_info && schema_info->schema) {
            item_statistic = 
                &schema_info->schema->statistic().term_times[schema_info->schema->statistic().term_times.size() - 1];
        }
    }
    TimeCost time;
    if (_is_over_cache) {
        //cache key is same as db key
        if (is_over_cache) {
            if (_cache.find(key, &list_ptr) == 0) {
                DB_WARNING("cached");
                if (item_statistic) {
                    item_statistic->is_cache = true;
                }
                return 0;
            }
        }
    }
    std::string value;
    auto get_res = txn->Get(roptions, data_cf, key, &value);      
    time.reset();
    if (get_res.ok()) {
        //deserialize
        ReverseListSptr tmp_ptr(new ReverseList());
        if (!tmp_ptr->ParseFromString(value)) {
            DB_FATAL("parse second level list from pb/arrow failed");
            return -1;
        }
        if (item_statistic) {
            item_statistic->parse += time.get_time();
        }
        list_ptr = tmp_ptr;
        if (_is_over_cache) {
            if (is_over_cache) {
                if (((ReverseList*)tmp_ptr.get())->reverse_nodes_size() >= _cached_list_length) {
                    _cache.add(key, tmp_ptr);
                }
            }
        }
    } else if (get_res.IsNotFound()) {
    } else {
        DB_WARNING("rocksdb get error: code=%d, msg=%s", 
            get_res.code(), get_res.ToString().c_str());
        return -1;
    }
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::_delete_level_reverse_list(
                                    myrocksdb::Transaction* txn, 
                                    uint8_t level, 
                                    const std::string& term) {
    std::string key;
    _create_reverse_key_prefix(level, key);
    key.append(term);
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    auto remove_res = txn->Delete(data_cf, key);       
    if (!remove_res.ok()) {
        DB_WARNING("rocksdb delete error: code=%d, msg=%s", 
            remove_res.code(), remove_res.ToString().c_str());
        return -1;
    }
    return 0;
}

template <typename Schema>
int ReverseIndex<Schema>::_insert_one_reverse_node(
                                    myrocksdb::Transaction* txn,
                                    const std::string& term,
                                    const ReverseNode* node) {
    // 1. create the first level key (regionid + tableid + reverse_prefix + term + \0 + pk)
    //DB_WARNING("insert node term[%s]", term.c_str());
    std::string key;
    _create_reverse_key_prefix(_reverse_prefix, key);
    key.append(term);
    key.append(1, '\0');
    key.append(node->key());
    int64_t  time = butil::gettimeofday_us();
    uint64_t time_encode = KeyEncoder::to_endian_u64(KeyEncoder::encode_i64(time));
    key.append(1, '\0');
    key.append((char*)&time_encode, sizeof(uint64_t));

    // 2. create value
    std::string value;
    if (!node->SerializeToString(&value)) {
        DB_WARNING("serialize failed: table =%lu, region=%lu", _index_id, _region_id);
        return -1;
    }
    // 3. put to RocksDB
    auto data_cf = _rocksdb->get_data_handle();
    if (data_cf == nullptr) {
        DB_WARNING("get rocksdb data column family failed");
        return -1;
    }
    auto put_res = txn->Put(data_cf, key, value);
    if (!put_res.ok()) {
        DB_WARNING("rocksdb put error: code=%d, msg=%s",
                    put_res.code(), put_res.ToString().c_str());
        return -1;
    }
    ++g_statistic_insert_key_num;
    return 0;
}

template <typename Schema>
int MutilReverseIndex<Schema>::search(
                       myrocksdb::Transaction* txn,
                       const IndexInfo& index_info,
                       const TableInfo& table_info,
                       const std::vector<ReverseIndex<Schema>*>& reverse_indexes,
                       const std::vector<std::string>& search_datas,
                       const std::vector<pb::MatchMode>& modes,
                       bool is_fast, bool bool_or) {
    uint32_t son_size = reverse_indexes.size();
    if (son_size == 0) {
        _exe = nullptr;
        return 0;
    }
    _reverse_indexes = reverse_indexes;
    _index_info = index_info;
    _table_info = table_info;
    _weight_field = get_field_info_by_name(_table_info.fields, "__weight");
    _query_words_field = get_field_info_by_name(_table_info.fields, "__querywords");
    bool_executor_type type = NODE_COPY;
    _son_exe_vec.resize(son_size);
    bool type_init = false; 
    for (uint32_t i = 0; i < son_size; ++i) {
        reverse_indexes[i]->create_executor(txn, index_info, table_info, search_datas[i], modes[i],
            std::vector<ExprNode*>(), is_fast);
        _query_words += reverse_indexes[i]->get_query_words();
        _query_words += ";";
        _son_exe_vec[i] = reverse_indexes[i]->get_executor();
        if (!type_init && _son_exe_vec[i]) {
            type = ((BooleanExecutor<Schema>*)_son_exe_vec[i])->get_type();
            type_init = true;
        } 
        reverse_indexes[i]->print_reverse_statistic_log();
    } 
    if (bool_or) {
        _exe = new OrBooleanExecutor<Schema>(type, nullptr);
        _exe->set_merge_func(Schema::merge_or);
        for (uint32_t i = 0; i < son_size; ++i) {
            if (_son_exe_vec[i]) {
                _exe->add((BooleanExecutor<Schema>*)_son_exe_vec[i]);
            }
        }
    } else {
        _exe = new AndBooleanExecutor<Schema>(type, nullptr);
        _exe->set_merge_func(Schema::merge_or);
        for (uint32_t i = 0; i < son_size; ++i) {
            if (_son_exe_vec[i]) {
                _exe->add((BooleanExecutor<Schema>*)_son_exe_vec[i]);
            }
        }
    }
    if (_query_words.size() > 0 && _query_words.back() == ';') {
        _query_words.pop_back();
    }
    return 0;
}

template <typename Schema>
int MutilReverseIndex<Schema>::search(
    myrocksdb::Transaction* txn,
    const IndexInfo& index_info,
    const TableInfo& table_info,
    std::map<int64_t, ReverseIndexBase*>& reverse_index_map,
    bool is_fast, const pb::FulltextIndex& fulltext_index_info) {

    _index_info = index_info;
    _table_info = table_info;
    _txn = txn;
    _is_fast = is_fast;
    _weight_field = get_field_info_by_name(_table_info.fields, "__weight");
    _query_words_field = get_field_info_by_name(_table_info.fields, "__querywords");
    _reverse_index_map = reverse_index_map;    
    _reverse_indexes.reserve(5);
    init_operator_executor(fulltext_index_info, _exe);
    if (_query_words.size() > 0 && _query_words.back() == ';') {
        _query_words.pop_back();
    }
    return 0;
}

template <typename Schema>
int MutilReverseIndex<Schema>::init_operator_executor(
    const pb::FulltextIndex& fulltext_index_info, OperatorBooleanExecutor<Schema>*& exe) {

    if (fulltext_index_info.fulltext_node_type() == pb::FNT_AND) {
        exe = new AndBooleanExecutor<Schema>(_type, nullptr);
        exe->set_merge_func(Schema::merge_or);
        for (const auto& child : fulltext_index_info.nested_fulltext_indexes()) {
            if (child.fulltext_node_type() == pb::FNT_AND || child.fulltext_node_type() == pb::FNT_OR) {
                OperatorBooleanExecutor<Schema>* child_exe = nullptr;
                if (init_operator_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            } else {
                BooleanExecutor<Schema>* child_exe = nullptr;
                if (init_term_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            }
        }
    } else if (fulltext_index_info.fulltext_node_type() == pb::FNT_OR) {
        exe = new OrBooleanExecutor<Schema>(_type, nullptr);
        exe->set_merge_func(Schema::merge_or);
        for (const auto& child : fulltext_index_info.nested_fulltext_indexes()) {

            if (child.fulltext_node_type() == pb::FNT_AND || child.fulltext_node_type() == pb::FNT_OR) {
                OperatorBooleanExecutor<Schema>* child_exe = nullptr;
                if (init_operator_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            } else {
                BooleanExecutor<Schema>* child_exe = nullptr;
                if (init_term_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            }
        }
    } else {
        DB_WARNING("unknown node type[%s].", fulltext_index_info.ShortDebugString().c_str());
    }
    return 0;
}

template <typename Schema>
int MutilReverseIndex<Schema>::init_term_executor(
    const pb::FulltextIndex& fulltext_index_info, BooleanExecutor<Schema>*& exe) {

    auto index_id = fulltext_index_info.possible_index().index_id();
    auto reverse_iter = static_cast<ReverseIndex<Schema>*>(_reverse_index_map[index_id]);
    //析构用，可以重复。
    _reverse_indexes.push_back(reverse_iter);
    SmartRecord record = SchemaFactory::get_instance()->new_record(_table_info.id);
    record->decode(fulltext_index_info.possible_index().ranges(0).left_pb_record());
    auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
    if (index_info == nullptr || index_info->id == -1) {
        DB_WARNING("no index_info found for index id: %ld", index_id);
        return -1;
    }
    std::string word;
    int ret = record->get_reverse_word(*index_info, word);
    if (ret < 0) {
        DB_WARNING("index_info to word fail for index_id: %ld", index_id);
        return ret;
    }
    reverse_iter->create_executor(_txn, _index_info, _table_info, word, 
        fulltext_index_info.possible_index().ranges(0).match_mode(),
        std::vector<ExprNode*>(), _is_fast);

    _query_words += reverse_iter->get_query_words();
    _query_words += ";";
    exe = static_cast<BooleanExecutor<Schema>*>(reverse_iter->get_executor());
    reverse_iter->print_reverse_statistic_log();
    return 0;
}
} // end of namespace



/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
