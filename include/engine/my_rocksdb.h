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

#include "rocks_wrapper.h"

namespace baikaldb {

namespace myrocksdb {
#define COLD_DB_CHECK_READ(handle) \
    do {\
        if (_use_cold_db && handle != _cold_data_handle) {\
            DB_FATAL("cold db read not cold handle"); \
            return rocksdb::Status::IOError("not cold handle");\
        }\
    }while (0);
#define COLD_DB_CHECK_WRITE \
    do {\
        if (_use_cold_db) {\
            DB_FATAL("cold db not support write"); \
            return rocksdb::Status::IOError("cold db not support write");\
        }\
    }while (0);

class Iterator {
public:
    explicit Iterator(rocksdb::Iterator* iter) : _iter(iter) { }

    virtual ~Iterator() { delete _iter; }

    bool Valid() { return _iter->Valid(); }

    void Seek(const rocksdb::Slice& target);

    void SeekForPrev(const rocksdb::Slice& target);

    void Next();

    void Prev();

    rocksdb::Slice key()   { return _iter->key(); }

    rocksdb::Slice value() { return _iter->value(); }

private:
    rocksdb::Iterator* _iter = nullptr;
};

class Transaction {
public:
    explicit Transaction(rocksdb::Transaction* txn, bool use_cold_db, rocksdb::ColumnFamilyHandle* handle) : 
            _txn(txn), _use_cold_db(use_cold_db), _cold_data_handle(handle) { }

    virtual ~Transaction() { delete _txn; }

    // rocksdb::Transaction* get_txn() { return _txn; }

    // rocksdb::Status Get(const rocksdb::ReadOptions& options, const rocksdb::Slice& key,
    //                     std::string* value) {
    //     return _txn->Get(options, key, value);
    // }

    // rocksdb::Status Get(const rocksdb::ReadOptions& options, const rocksdb::Slice& key,
    //                     rocksdb::PinnableSlice* pinnable_val) {
    //     return _txn->Get(options, key, pinnable_val);
    // }

    rocksdb::Status Get(const rocksdb::ReadOptions& options,
                     rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key,
                     std::string* value);

    rocksdb::Status Get(const rocksdb::ReadOptions& options,
                     rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key,
                     rocksdb::PinnableSlice* pinnable_val);

    void MultiGet(const rocksdb::ReadOptions& options,
                     rocksdb::ColumnFamilyHandle* column_family,
                     const std::vector<rocksdb::Slice>& keys,
                     std::vector<rocksdb::PinnableSlice>& values,
                     std::vector<rocksdb::Status>& statuses,
                     bool sorted_input);

    rocksdb::Status GetForUpdate(const rocksdb::ReadOptions& options,
                              rocksdb::ColumnFamilyHandle* column_family,
                              const rocksdb::Slice& key, std::string* value);

    rocksdb::Status GetForUpdate(const rocksdb::ReadOptions& options,
                              rocksdb::ColumnFamilyHandle* column_family,
                              const rocksdb::Slice& key, rocksdb::PinnableSlice* pinnable_val);

    // rocksdb::Status Put(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    //     return _txn->Put(key, value);
    // }

    // rocksdb::Status Put(const rocksdb::SliceParts& key, const rocksdb::SliceParts& value) {
    //     return _txn->Put(key, value);
    // }

    rocksdb::Status Put(rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key,
                     const rocksdb::Slice& value);

    rocksdb::Status Put(rocksdb::ColumnFamilyHandle* column_family, const rocksdb::SliceParts& key,
                     const rocksdb::SliceParts& value);

    rocksdb::Status Merge(rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key,
                       const rocksdb::Slice& value,
                       const bool assume_tracked = false);
    rocksdb::Status Delete(rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key) {
        COLD_DB_CHECK_WRITE;
        return _txn->Delete(column_family, key);
    }

    rocksdb::Status Delete(rocksdb::ColumnFamilyHandle* column_family,
                        const rocksdb::SliceParts& key) {
        COLD_DB_CHECK_WRITE;
        return _txn->Delete(column_family, key);
    }

    rocksdb::Status SetName(const rocksdb::TransactionName& name) { return _txn->SetName(name); }

    rocksdb::TransactionName GetName() const { return _txn->GetName(); }

    rocksdb::TransactionID GetID() const { return _txn->GetID(); }

    // rocksdb::Iterator* GetIterator(const rocksdb::ReadOptions& read_options) {
    //     return _txn->GetIterator(read_options);
    // }

    rocksdb::Iterator* GetIterator(const rocksdb::ReadOptions& read_options,
                                rocksdb::ColumnFamilyHandle* column_family) {
        if (_use_cold_db) {
            return _txn->GetIterator(read_options, _cold_data_handle);
        } else {
            return _txn->GetIterator(read_options, column_family);
        }
    }

    rocksdb::Status Prepare()  { return _txn->Prepare(); }

    rocksdb::Status Commit()   { return _txn->Commit(); }

    rocksdb::Status Rollback() { return _txn->Rollback(); }

    void SetSavePoint() { _txn->SetSavePoint(); }

    rocksdb::Status RollbackToSavePoint() { return _txn->RollbackToSavePoint(); }

    std::vector<rocksdb::TransactionID> GetWaitingTxns(uint32_t* column_family_id, 
                                                        std::string* key) const {
          return _txn->GetWaitingTxns(column_family_id, key);
    }

    void DisableIndexing() { _txn->DisableIndexing(); }

    bool use_cold_db() const { return _use_cold_db; }

private:
    rocksdb::Transaction* _txn = nullptr;
    bool _use_cold_db = false;
    rocksdb::ColumnFamilyHandle* _cold_data_handle = nullptr;
};

} // namespace myrocksdb
} // namespace baikaldb