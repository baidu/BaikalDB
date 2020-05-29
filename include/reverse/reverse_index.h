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
#include "reverse_common.h"
#include "rocksdb/utilities/transaction.h"
#include "key_encoder.h"
#include "table_record.h"
#include "rocks_wrapper.h"
#include "transaction.h"
#include "boolean_executor.h"
#include "schema_factory.h"
#include "expr_node.h"
#include <atomic>
#include <map>
#include "proto/store.interface.pb.h"

namespace baikaldb {

class ReverseIndexBase {
public:
    virtual ~ReverseIndexBase() {
    }
    //倒排表的1、2、3级倒排链表merge函数
    virtual int reverse_merge_func(pb::RegionInfo info, bool need_remove_third) = 0;
    //新加正排 创建倒排索引
    virtual int insert_reverse(
                       rocksdb::Transaction* txn,
                       pb::StoreReq* req,
                       const std::string& word, 
                       const std::string& pk,
                       SmartRecord record) = 0;
    //删除正排 删除倒排索引
    virtual int delete_reverse(
                       rocksdb::Transaction* txn,
                       pb::StoreReq* req,
                       const std::string& word, 
                       const std::string& pk,
                       SmartRecord record) = 0;
    //单索引检索接口，fast为true，性能会提高，但会出现ms级别的不一致性
    virtual int search(
                       rocksdb::Transaction* txn,
                       const IndexInfo& index_info,
                       const TableInfo& table_info,
                       const std::string& search_data,
                       pb::MatchMode mode,
                       std::vector<ExprNode*> conjuncts, 
                       bool is_fast = false) = 0;
    virtual bool valid() = 0;
    virtual void clear() = 0;
    virtual int get_next(SmartRecord record) = 0;
    virtual void sync(AtomicManager<std::atomic<long>>& am) = 0;

    //获取1、2level倒排集合和3level倒排，用于Parser获取底层数据
    /*
    virtual int get_reverse_list_two(
                       rocksdb::Transaction* txn,  
                       const std::string& term, 
                       MessageSP& list_new_ptr,
                       MessageSP& list_old_ptr,
                       bool is_fast = false) = 0;
    */
    //返回exe，用来给多个倒排索引字段join
    virtual int create_executor(
                    rocksdb::Transaction* txn,
                    const IndexInfo& index_info,
                    const TableInfo& table_info,
                    const std::string& search_data,
                    pb::MatchMode mode,
                    std::vector<ExprNode*> conjuncts, 
    //                BooleanExecutorBase*& exe,
                    bool is_fast = false) = 0;
    virtual void set_second_level_length(int length) = 0;
    virtual void set_cache_size(int size) = 0;
    virtual void set_cached_list_length(int length) = 0;
    virtual void print_reverse_statistic_log() = 0;
    virtual void add_field(const std::string& name, int32_t field_id) = 0;
};

template<typename ReverseNode, typename ReverseList>
class SchemaBase {
public:
    using PrimaryIdT = typename ReverseTrait<ReverseList>::PrimaryType;
    using PostingNodeT = ReverseNode;
    SchemaBase() {
    }
    void init(ReverseIndexBase *reverse, rocksdb::Transaction *txn, 
            const KeyRange& key_range, std::vector<ExprNode*> conjuncts, bool is_fast) {
        _reverse = reverse;
        _txn = txn;
        _key_range = key_range;
        _conjuncts = conjuncts;
        _is_fast = is_fast;
    }
    static int compare_id_func(const PrimaryIdT& id1, const PrimaryIdT& id2) {
        return id1.compare(id2);
    }
    // term filter，not return the node when true;
    static bool filter(const ReverseNode& node, BoolArg* arg) {
        return false;
    }
    static void init_node(ReverseNode&, const std::string&, BoolArg*) {
    }
    virtual ~SchemaBase() {
        delete _exe;
    }
    
    virtual bool valid() {
        if (_exe != NULL) {
            while (true) {
                _cur_node = (const ReverseNode*)(_exe->next());
                if (_cur_node) {
                    if (_cur_node->flag() == pb::REVERSE_NODE_NORMAL) {
                        return true;
                    } else {
                        continue;
                    }
                } else {
                    return false;
                }
            }
        } else {
            DB_WARNING("exec is nullptr");
            return false;
        }
    }
    KeyRange key_range() {
        return _key_range;
    }
    BooleanExecutorBase<PostingNodeT>*& exe() {
        return _exe;
    }
    ReverseSearchStatistic& statistic() {
        return _statistic;
    }
    void print_statistic_log() {
        _statistic.print_log();
    }
    void set_index_info(const IndexInfo& index_info) {
        _index_info = index_info;
    }
    void set_table_info(const TableInfo& table_info) {
        _table_info = table_info;
    }
    virtual int create_executor(const std::string& search_data, 
            pb::MatchMode mode, pb::SegmentType segment_type) = 0;
    virtual int next(SmartRecord record) = 0;

protected:
    int32_t _idx = 0;
    BooleanExecutorBase<PostingNodeT>* _exe = NULL;
    const ReverseNode* _cur_node = NULL;
    ReverseIndexBase *_reverse;
    rocksdb::Transaction *_txn;//读取时用的transaction，由调用者释放
    KeyRange _key_range;
    bool _is_fast = false;
    IndexInfo _index_info;
    TableInfo _table_info;
    ReverseSearchStatistic _statistic;
    std::vector<ExprNode*> _conjuncts;
};

template <typename Schema> 
class ReverseIndex : public ReverseIndexBase {
public:
    typedef typename Schema::ReverseNode ReverseNode;
    typedef typename Schema::ReverseList ReverseList;
    using ReverseListSptr = typename Schema::ReverseListSptr;
    ReverseIndex(
            int64_t region_id, 
            int64_t index_id, 
            int length, 
            RocksWrapper* rocksdb,
            pb::SegmentType segment_type = pb::S_DEFAULT,
            bool is_over_cache = true,
            bool is_seg_cache = true,
            int cache_size = 300,
            int cached_list_length = 3000) : 
                        _region_id(region_id),
                        _index_id(index_id),
                        _second_level_length(length),
                        _rocksdb(rocksdb),
                        _segment_type(segment_type),
                        _is_over_cache(is_over_cache),
                        _is_seg_cache(is_seg_cache),
                        _cached_list_length(cached_list_length) {
        _sync_prefix_0 = 0;
        _sync_prefix_1 = 0;
        if (is_over_cache) {
            _cache.init(cache_size);
        }
        if (is_seg_cache) {
            _seg_cache.init(cache_size);
        }
    }
    ~ReverseIndex(){}

    virtual int reverse_merge_func(pb::RegionInfo info, bool need_remove_third);
    //0:success    -1:fail
    virtual int insert_reverse(
                        rocksdb::Transaction* txn,
                        pb::StoreReq* req,
                        const std::string& word, 
                        const std::string& pk,
                        SmartRecord record);
    //0:success    -1:fail
    virtual int delete_reverse(
                        rocksdb::Transaction* txn,
                        pb::StoreReq* req,
                        const std::string& word, 
                        const std::string& pk,
                        SmartRecord record);
    virtual int search(
                       rocksdb::Transaction* txn,
                       const IndexInfo& index_info,
                       const TableInfo& table_info,
                       const std::string& search_data,
                       pb::MatchMode mode,
                       std::vector<ExprNode*> conjuncts, 
                       bool is_fast = false); 
    virtual bool valid() {
        return _schema->valid();
    }
    // release immediately
    virtual void clear() {
        TimeCost timer;
        delete _schema;
        DB_NOTICE("reverse delete time:%ld", timer.get_time());
        _schema = nullptr;
    }
    virtual int get_next(SmartRecord record) {
        //DB_WARNING("schema get_next()");
        return _schema->next(record);
    }
    virtual int get_reverse_list_two(
                       rocksdb::Transaction* txn,  
                       const std::string& term, 
                       ReverseListSptr& list_new,
                       ReverseListSptr& list_old,
                       bool is_fast = false);
    virtual int create_executor(
                    rocksdb::Transaction* txn,
                    const IndexInfo& index_info,
                    const TableInfo& table_info,
                    const std::string& search_data,
                    pb::MatchMode mode,
                    std::vector<ExprNode*> conjuncts, 
        //            BooleanExecutorBase*& exe,
                    bool is_fast = false);
    //读写和merge同步
    void sync(AtomicManager<std::atomic<long>>& am) {
        if (_reverse_prefix == 0) {
            am.set(&_sync_prefix_0);
        } else {
            am.set(&_sync_prefix_1);
        }
    }
    void set_second_level_length(int length) {
        _second_level_length = length;
    }
    void set_cache_size(int size) {
        _cache.init(size);
    }
    void set_cached_list_length(int length) {
        _cached_list_length = length;
    }
    void print_reverse_statistic_log() {
        _schema->print_statistic_log();
    }
    virtual void add_field(const std::string& name, int32_t field_id) {
        _name_field_id_map[name] = field_id;
    }
    
    BooleanExecutorBase<typename Schema::PostingNodeT>* get_executor() {
        auto exe_ptr = _schema->exe();
        _schema->exe() = nullptr;
        return exe_ptr;
    }
private:
    //0:success    -1:fail
    int handle_reverse(
                        rocksdb::Transaction* txn,
                        pb::StoreReq* req,
                        pb::ReverseNodeType flag,
                        const std::string& word, 
                        const std::string& pk,
                        SmartRecord record);
    //level取值0、1、2或3, 0和1属于一级 2是2级 3是3级
    //key = tableid_regionid_level
    int _create_reverse_key_prefix(uint8_t level, std::string& key);
    //remove out of range keys when split
    int _reverse_remove_range_for_third_level(uint8_t prefix);
    //first(0/1) level merge to second(2) level
    int _reverse_merge_to_second_level(std::unique_ptr<rocksdb::Iterator>&, uint8_t);
    //get some level list
    int _get_level_reverse_list(
                    rocksdb::Transaction* txn, 
                    uint8_t level, 
                    const std::string& term, 
                    ReverseListSptr& list,
                    bool is_statistic = false,
                    bool is_over_cache = false);
    //delete some level list
    int _delete_level_reverse_list(
                    rocksdb::Transaction* txn, 
                    uint8_t level, 
                    const std::string& term);
    //对一条倒排链增加一个倒排节点
    int _insert_one_reverse_node( 
                    rocksdb::Transaction* txn, 
                    pb::StoreReq* req,
                    const std::string& term, 
                    const ReverseNode* node);

private:
    int64_t             _region_id;
    int64_t             _index_id;
    uint8_t             _merge_prefix = 0;
    uint8_t             _reverse_prefix = 1;
    std::atomic<long>    _sync_prefix_0;
    std::atomic<long>    _sync_prefix_1;
    int                 _second_level_length;
    RocksWrapper*       _rocksdb;
    KeyRange            _key_range;
    bool                _prefix_0_succ = false;
    bool                _merge_success_flag = true;
    int64_t             _level_1_scan_count = 0;
    // todo: replace thread_local because bthread will switch thread
    static thread_local Schema* _schema;
    Cache<std::string, ReverseListSptr> _cache;
    Cache<uint64_t, std::shared_ptr<std::map<std::string, ReverseNode>>> _seg_cache;
    pb::SegmentType _segment_type;
    bool _is_over_cache;
    bool _is_seg_cache;
    int _cached_list_length;//被缓存的链表的最小长度
    std::vector<std::string> _cache_keys;
    // 存储额外字段时需要
    std::map<std::string, int32_t> _name_field_id_map;
};
template<typename Schema>
thread_local Schema* ReverseIndex<Schema>::_schema = nullptr;

//多个倒排索引间做or操作，只读
template<typename Schema>
class MutilReverseIndex {
public:
    typedef typename Schema::ReverseNode ReverseNode;
    typedef typename Schema::ReverseList ReverseList;
    ~MutilReverseIndex() {
        delete _exe;
        for (auto& index : _reverse_indexes) {
            index->clear();
        }
    }
    int search(
            rocksdb::Transaction* txn,
            const IndexInfo& index_info,
            const TableInfo& table_info,
            const std::vector<ReverseIndex<Schema>*>& reverse_indexes,
            const std::vector<std::string>& search_datas,
            const std::vector<pb::MatchMode>& modes,
            bool is_fast, bool bool_or); 
    bool valid() {
        if (_exe != NULL) {
            while (true) {
                _cur_node = (const ReverseNode*)(_exe->next());
                if (_cur_node) {
                    if (_cur_node->flag() == pb::REVERSE_NODE_NORMAL) {
                        return true;
                    } else {
                        continue;
                    }
                } else {
                    return false;
                }
            }
        } else {
            while (true) {
                if (_son_exe_vec_idx >= _son_exe_vec.size()) {
                    return false;
                } 
                _cur_node = (const ReverseNode*)(_son_exe_vec[_son_exe_vec_idx]->next());
                if (_cur_node) {
                    if (_cur_node->flag() == pb::REVERSE_NODE_NORMAL) {
                        return true;
                    } else {
                        continue;
                    }
                } else {
                    ++_son_exe_vec_idx;
                }
            }
        }
    }
    int get_next(SmartRecord record) {
        if (!_cur_node) {
            return -1;
        }
        int ret = record->decode_key(_index_info, _cur_node->key());
        if (ret < 0) {
            return -1;
        }
        if (_weight_field_id > 0) {
            auto field = record->get_field_by_tag(_weight_field_id);
            record->set_float(field, _cur_node->weight());
            // if (ret < 0) {
            //     return -1;
            // }
        }
        return 0;
    }
private:
    OperatorBooleanExecutor<Schema>* _exe = nullptr;
    const ReverseNode* _cur_node = NULL;
    IndexInfo _index_info;
    TableInfo _table_info;
    std::vector<BooleanExecutorBase<typename Schema::PostingNodeT>*> _son_exe_vec;
    std::vector<ReverseIndex<Schema>*> _reverse_indexes;
    size_t _son_exe_vec_idx = 0;
    int32_t _weight_field_id = 0;
};
} // end of namespace

#include "reverse_index.hpp"

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
