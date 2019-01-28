// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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
#include "mut_table_key.h"
#include "rocks_wrapper.h"
#include "proto/store.interface.pb.h"

namespace baikaldb {
class MetaWriter {
public:
    static const rocksdb::WriteOptions write_options;
    static const std::string META_IDENTIFY;
    static const std::string APPLIED_INDEX_INDENTIFY;
    static const std::string NUM_TABLE_LINE_INDENTIFY;
    static const std::string PREPARED_TXN_LOG_INDEX_IDENTIFY;
    static const std::string PREPARED_TXN_PB_IDENTIYF;
    static const std::string REGION_INFO_IDENTIFY;
    
    virtual ~MetaWriter() {}
   
    static MetaWriter* get_instance() {
        static MetaWriter _instance;
        return &_instance;
    } 
    void init(RocksWrapper* rocksdb, 
            rocksdb::ColumnFamilyHandle* meta_cf) {
        _rocksdb = rocksdb;
        _meta_cf = meta_cf;
    }
    int init_meta_info(const pb::RegionInfo& region_info);
    int update_region_info(const pb::RegionInfo& region_info);
    int update_num_table_lines(int64_t region_id, int64_t num_table_lines);
    int update_apply_index(int64_t region_id, int64_t applied_index);
    int write_batch(rocksdb::WriteBatch* updates, int64_t region_id);
    int ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id);
    
    int clear_meta_info(int64_t drop_region_id);
    int clear_txn_log_index(int64_t region_id);
    int clear_txn_infos(int64_t region_id);

    int parse_region_infos(std::vector<pb::RegionInfo>& region_infos);
    int parse_txn_infos(int64_t region_id, std::map<int64_t, std::string>& prepared_txn_infos);
    int parse_txn_log_indexs(int64_t region_id, std::set<int64_t>& log_indexs);
    int64_t read_applied_index(int64_t region_id);
    int64_t read_num_table_lines(int64_t region_id);
    int read_region_info(int64_t region_id, pb::RegionInfo& region_info);
public:
    std::string region_info_key(int64_t region_id) const;
    std::string region_for_store_key(int64_t region_id) const;
    std::string applied_index_key(int64_t region_id) const;
    std::string num_table_lines_key(int64_t region_id) const;
    std::string transcation_log_index_key(int64_t region_id, uint64_t txn_id) const;
    std::string log_index_key_prefix(int64_t region_id) const;
    std::string transcation_pb_key(int64_t region_id, uint64_t txn_id, int64_t log_index) const;
    std::string transcation_pb_key_prefix(int64_t region_id) const;
    std::string encode_applied_index(int64_t index) const;
    std::string encode_num_table_lines(int64_t line) const;
    std::string encode_region_info(const pb::RegionInfo& region_info) const;
    std::string encode_transcation_pb_value(const pb::StoreReq& txn) const;
    std::string encode_transcation_log_index_value(int64_t log_index) const;
    int64_t decode_log_index_value(const rocksdb::Slice& value);
    uint64_t decode_log_index_key(const rocksdb::Slice& key);

    std::string meta_info_prefix(int64_t region_id);
    rocksdb::ColumnFamilyHandle* get_handle() {
        return _meta_cf;
    }
private:
    MetaWriter() {}
private:
    RocksWrapper*       _rocksdb;
    rocksdb::ColumnFamilyHandle* _meta_cf;    
};

} // end of namespace
