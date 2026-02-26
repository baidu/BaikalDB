#pragma once

#include "schema_factory.h"
#include "table_record.h"
#include "mut_table_key.h"
#include "rocks_wrapper.h"
#include "my_raft_log_storage.h"
#include "meta_writer.h"
#include "column_record.h"

namespace baikaldb {
struct Row2ColOptions {
     // 行存相关参数
    int64_t                             region_id;
    int64_t                             table_id;
    int                                 read_batch_size = 1024; // default 1024
    bool                                is_cold_rocksdb = false;
    const rocksdb::Snapshot*            snapshot = nullptr;
    int64_t                             start_index;
    int64_t                             end_index;
    std::shared_ptr<ColumnSchemaInfo>   schema_info;
}; 

class Row2ColumnReader : public arrow::RecordBatchReader {
public:
    Row2ColumnReader(const Row2ColOptions& options) : _options(options), _schema_info(options.schema_info)  { }
    virtual ~Row2ColumnReader() {}
    int init(bool schema_with_order_info) {
        if (schema_with_order_info) {
            _column_record = std::make_shared<ColumnRecord>(_schema_info->schema_with_order_info, _options.read_batch_size);
        } else {
            _column_record = std::make_shared<ColumnRecord>(_schema_info->schema, _options.read_batch_size);
        }
        
        int ret = _column_record->init();
        if (ret < 0) {
            return -1;
        } 
        _column_record->reserve(_options.read_batch_size);
        return 0;
    }
    ExprValue get_default_value(const FieldInfo& field);
    int row2col(rocksdb::Slice key, rocksdb::Slice value, ColumnKeyType key_type, int64_t raft_index, int batch_pos);
    virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        return arrow::Status::Invalid("Not Implemented");;
    }
    virtual std::shared_ptr<::arrow::Schema> schema() const override {
        return nullptr;
    }

protected:
    Row2ColOptions _options;
    std::shared_ptr<ColumnSchemaInfo> _schema_info;
    std::shared_ptr<ColumnRecord> _column_record = nullptr;
    TimeCost _cost;
    int _read_times = 0;
    int _total_row_nums = 0;
};

class RocksdbBaseReader : public Row2ColumnReader {
public:
    RocksdbBaseReader(const Row2ColOptions& options) : Row2ColumnReader(options) {

    }
    virtual ~RocksdbBaseReader() {}
    int init();
    virtual arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;
    virtual std::shared_ptr<::arrow::Schema> schema() const override {
        return nullptr;
    }
private:
    std::string _prefix;
    std::string _end;
    rocksdb::Slice _upper_bound_slice;
    std::shared_ptr<rocksdb::Iterator> _iter;
    bool _is_finish = false;
    bool _init = false;
};

struct RaftLogCacheIter {
    TimeCost begin_time;
    int64_t commit_index = -1;
    std::map<int64_t, pb::StoreReq> log_index_req_map;
};

struct RaftLogCache {
    bool empty() {
        return txn_id_raft_log_map.empty();
    }

    int64_t time() {
        int64_t t = 0;
        for (auto iter : txn_id_raft_log_map) {
            if (t < iter.second->begin_time.get_time()) {
                t = iter.second->begin_time.get_time();
            }
        }

        return t;
    }

    std::map<int64_t, std::shared_ptr<RaftLogCacheIter>> txn_id_raft_log_map;
};

class RaftLogMgr {
public:
    ~RaftLogMgr() {}

    static RaftLogMgr* get_instance() {
        static RaftLogMgr _instance;
        return &_instance;
    }

    std::shared_ptr<RaftLogCache> get_raft_log_cache(int64_t region_id) {
        std::unique_lock<bthread::Mutex> l(_lock);
        auto iter = _region_id_raft_log.find(region_id);
        if (iter == _region_id_raft_log.end()) {
            auto cache = std::make_shared<RaftLogCache>();
            return cache;
        } else {
            auto cache = iter->second;
            _region_id_raft_log.erase(iter);
            return cache;
        }
    }

    void release_raft_log_cache(int64_t region_id, std::shared_ptr<RaftLogCache> cache) {
        if (!cache->empty()) {
            std::unique_lock<bthread::Mutex> l(_lock);
            if (_region_id_raft_log.count(region_id) > 0) {
                DB_COLUMN_FATAL("region_id: %ld, cache not empty", region_id);
            }
            _region_id_raft_log[region_id] = cache;
        }
    }    


private:
    bthread::Mutex _lock;
    std::map<int64_t, std::shared_ptr<RaftLogCache>> _region_id_raft_log;

private:
    RaftLogMgr() {}
    DISALLOW_COPY_AND_ASSIGN(RaftLogMgr);
};

class RaftLogReader : public Row2ColumnReader {
public:
    RaftLogReader(const Row2ColOptions& options) : Row2ColumnReader(options), _region_id(options.region_id) {
        _column_record = std::make_shared<ColumnRecord>(_schema_info->schema_with_order_info, _options.read_batch_size);
        _raft_log_cache = RaftLogMgr::get_instance()->get_raft_log_cache(_region_id);
    }

    virtual ~RaftLogReader() {
        int64_t time_cost = 0;
        if (!_raft_log_cache->txn_id_raft_log_map.empty()) {
            time_cost = _raft_log_cache->time();
            if (time_cost > 15 * 60 * 1000 * 1000ULL) {
                DB_COLUMN_FATAL("region_id: %ld, time_cost: %ld", _region_id, time_cost);
            }
        }
        
        RaftLogMgr::get_instance()->release_raft_log_cache(_region_id, _raft_log_cache);
    }

    int init();

    int get_raft_log(int64_t start_index, int64_t end_index, uint64_t txn_id, std::map<int64_t, pb::StoreReq>& pre_reqs);

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

    std::shared_ptr<::arrow::Schema> schema() const override {
        return _schema_info->schema_with_order_info;
    }

    int delete_column_txn_log_index() {
        return MetaWriter::get_instance()->delete_column_txn_log_index(_region_id, _txn_ids);
    }

    int64_t get_last_raft_index() const {
        return _last_index;
    }

    int64_t row_count() const {
        return _total_row_nums;
    }

    int64_t put_count() const {
        return _put_count;
    }

    int64_t delete_count() const {
        return _delete_count;
    }

    void commit(int64_t txn_id, int64_t raft_index);

    void rollback(int64_t txn_id, int64_t raft_index);

    void insert(int64_t txn_id, int64_t raft_index, pb::StoreReq& request);

    int get(int64_t txn_id, std::map<int64_t, pb::StoreReq>& log_index_req_map);

private:
    int64_t _first_index  = -1;
    int64_t _last_index   = -1;
    int64_t _skip_count   = 0;
    int64_t _put_count    = 0;
    int64_t _delete_count = 0;
    int64_t _merge_count  = 0;
    bool _init = false;
    std::vector<std::shared_ptr<arrow::RecordBatch>> _batchs;
    int64_t _region_id = 0;
    int64_t _commited_txn_id = -1;
    std::shared_ptr<RaftLogCache> _raft_log_cache;
    std::vector<uint64_t> _txn_ids;
};

} // baikaldb