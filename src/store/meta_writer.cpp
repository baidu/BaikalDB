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

#include "meta_writer.h"
#include <cstdint>
#include "table_key.h"

namespace baikaldb {
const rocksdb::WriteOptions MetaWriter::write_options;
//key: META_IDENTIFY + region_id + identify + other: value
const std::string MetaWriter::META_IDENTIFY(1, 0x01);
const std::string MetaWriter::APPLIED_INDEX_INDENTIFY(1, 0x01);
const std::string MetaWriter::NUM_TABLE_LINE_INDENTIFY(1, 0x02);
const std::string MetaWriter::PREPARED_TXN_LOG_INDEX_IDENTIFY(1, 0x03);
const std::string MetaWriter::PREPARED_TXN_PB_IDENTIYF(1, 0x04);
const std::string MetaWriter::REGION_INFO_IDENTIFY(1, 0x05);

int MetaWriter::init_meta_info(const pb::RegionInfo& region_info) {
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::string string_region_info = encode_region_info(region_info);
    if (string_region_info.empty()) {
        return -1;
    }
    int64_t region_id = region_info.region_id();
    keys.push_back(region_info_key(region_id));
    values.push_back(string_region_info);
 
    keys.push_back(applied_index_key(region_id));
    values.push_back(encode_applied_index(0));

    keys.push_back(num_table_lines_key(region_id));
    values.push_back(encode_num_table_lines(0));
    auto status = _rocksdb->write(MetaWriter::write_options, _meta_cf, keys, values);
    if (!status.ok()) {
        DB_FATAL("write init_meta_info fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return 0; 
}

int MetaWriter::update_region_info(const pb::RegionInfo& region_info) {
    int64_t region_id = region_info.region_id();
    std::string string_region_info;
    if (!region_info.SerializeToString(&string_region_info)) {
        DB_FATAL("region_info: %s serialize to string fail, region_id: %ld",
                region_info.ShortDebugString().c_str(), region_id);
    }
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
                rocksdb::Slice(region_info_key(region_id)), rocksdb::Slice(string_region_info));
    if (!status.ok()) {
        DB_FATAL("write update_region_info fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    } else {
        DB_WARNING("write region info success, region_info: %s", region_info.ShortDebugString().c_str());
    }
    return 0; 
}

int MetaWriter::update_num_table_lines(int64_t region_id, int64_t num_table_lines) {
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                    rocksdb::Slice(num_table_lines_key(region_id)), 
                    rocksdb::Slice(encode_num_table_lines(num_table_lines)));
    if (!status.ok()) {
        DB_FATAL("write update_num_table_lines fail, err_msg: %s, region_id: %ld",
                status.ToString().c_str(), region_id);
        return -1;    
    }
    return 0;
}

int MetaWriter::update_apply_index(int64_t region_id, int64_t applied_index) {
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
                rocksdb::Slice(applied_index_key(region_id)), 
                rocksdb::Slice(encode_applied_index(applied_index)));
    if (!status.ok()) {
        DB_FATAL("write apply index fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return 0; 
}

int MetaWriter::write_batch(rocksdb::WriteBatch* updates, int64_t region_id) {
    auto status = _rocksdb->write(MetaWriter::write_options, updates);
    if (!status.ok()) {
        DB_FATAL("write batch fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return 0; 
}

int MetaWriter::ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id) {
    rocksdb::IngestExternalFileOptions ifo;
    auto res = _rocksdb->ingest_external_file(_meta_cf, {meta_sst_file}, ifo);
    if (!res.ok()) {
         DB_WARNING("Error while adding file %s, Error %s, region_id: %ld",
                 meta_sst_file.c_str(), res.ToString().c_str(), region_id);
         return -1;
    }
    return 0;
}

int MetaWriter::clear_meta_info(int64_t drop_region_id) {
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    batch.Delete(_meta_cf, region_info_key(drop_region_id));
    batch.Delete(_meta_cf, applied_index_key(drop_region_id));
    batch.Delete(_meta_cf, num_table_lines_key(drop_region_id));
    auto status = _rocksdb->write(options, &batch);
    if (!status.ok()) {
        DB_FATAL("drop region fail, error: code=%d, msg=%s, region_id: %ld", 
        status.code(), status.ToString().c_str(), drop_region_id);
        return -1;
    }
    auto ret = clear_txn_infos(drop_region_id);
    if (ret < 0) {
        DB_FATAL("clear_txn_infos fail, region_id: %ld", drop_region_id);
        return ret;
    }
    return clear_txn_log_index(drop_region_id);
}

int MetaWriter::clear_txn_log_index(int64_t region_id) {
    std::string start_key = transcation_log_index_key(region_id, 0);
    std::string end_key = transcation_log_index_key(region_id, UINT64_MAX);
    auto status = _rocksdb->remove_range(MetaWriter::write_options, _meta_cf,
            start_key, end_key);
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}
int MetaWriter::clear_txn_infos(int64_t region_id) {
    std::string start_key = transcation_pb_key(region_id, 0);
    std::string end_key = transcation_pb_key(region_id, INT64_MAX);
    auto status = _rocksdb->remove_range(MetaWriter::write_options, _meta_cf,
            start_key, end_key);
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}

int MetaWriter::parse_region_infos(std::vector<pb::RegionInfo>& region_infos) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string region_info_prefix = MetaWriter::META_IDENTIFY;
    for (iter->Seek(region_info_prefix); iter->Valid(); iter->Next()) {
        std::string identify;
        TableKey(iter->key()).extract_char(1 + sizeof(int64_t), 1, identify);
        if (identify != MetaWriter::REGION_INFO_IDENTIFY) {
            continue;
        }
        pb::RegionInfo region_info;
        if (!region_info.ParseFromString(iter->value().ToString())) {
            DB_FATAL("parse from pb fail when load region snapshot, key:%s",
                iter->value().ToString().c_str());
            return -1;
        }
        region_infos.push_back(region_info);
    }
    return 0;
}

int MetaWriter::parse_txn_infos(int64_t region_id, std::map<int64_t, std::string>& prepared_txn_infos) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string prefix = transcation_pb_key_prefix(region_id) ;
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        if (!iter->key().starts_with(prefix)) {
            //DB_WARNING("read wrong info, region_id: %ld", region_id);
            continue;
        }
        int64_t log_index = TableKey(iter->key()).extract_i64(1 + sizeof(int64_t) + 1);
        prepared_txn_infos[log_index] = iter->value().ToString();
    }
    return 0;
}

int MetaWriter::parse_txn_log_indexs(int64_t region_id, std::set<int64_t>& log_indexs) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string prefix = log_index_key_prefix(region_id);
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        if (!iter->key().starts_with(prefix)) {
            //DB_WARNING("read wrong info, region_id: %ld", region_id);
            continue;
        }
        TableKey log_index(iter->value());
        log_indexs.insert(log_index.extract_i64(0));
        DB_WARNING("read prepared transcation log index: %ld", log_index.extract_i64(0));
    }
    return 0; 
}

int64_t MetaWriter::read_applied_index(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(applied_index_key(region_id)), &value);
    if (!status.ok()) {
        DB_WARNING("Error while read applied index, Error %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return TableKey(rocksdb::Slice(value)).extract_i64(0);
}

int64_t MetaWriter::read_num_table_lines(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(num_table_lines_key(region_id)), &value);
    if (!status.ok()) {
        DB_WARNING("Error while read num_table_lines fail, Error %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return TableKey(rocksdb::Slice(value)).extract_i64(0);
}
int MetaWriter::read_region_info(int64_t region_id, pb::RegionInfo& region_info) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(region_info_key(region_id)), &value);
    if (!status.ok()) {
        DB_WARNING("Error while read applied index, Error %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1; 
    }
    if (!region_info.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when read region info, region_id: %ld, value:%s",
            region_id, value.c_str());
        return -1;
    }
    return 0;
}
std::string MetaWriter::region_info_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.data(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::REGION_INFO_IDENTIFY.data(), 1);
    return key.data();
}
std::string MetaWriter::applied_index_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.data(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::APPLIED_INDEX_INDENTIFY.data(), 1);
    return key.data();
}
std::string MetaWriter::num_table_lines_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::NUM_TABLE_LINE_INDENTIFY.c_str(), 1);
    return key.data();
}
std::string MetaWriter::transcation_log_index_key(int64_t region_id, uint64_t txn_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PREPARED_TXN_LOG_INDEX_IDENTIFY.c_str(), 1);
    key.append_u64(txn_id);
    return key.data();
}
std::string MetaWriter::log_index_key_prefix(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PREPARED_TXN_LOG_INDEX_IDENTIFY.c_str(), 1);
    return key.data();
}
std::string MetaWriter::transcation_pb_key(int64_t region_id, int64_t log_index) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PREPARED_TXN_PB_IDENTIYF.c_str(), 1);
    key.append_i64(log_index);
    return key.data();
}
std::string MetaWriter::transcation_pb_key_prefix(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PREPARED_TXN_PB_IDENTIYF.c_str(), 1);
    return key.data(); 
}
std::string MetaWriter::encode_applied_index(int64_t index) const {
    MutTableKey index_value;
    index_value.append_i64(index);
    return index_value.data();
}
std::string MetaWriter::encode_num_table_lines(int64_t line) const {
    MutTableKey line_value;
    line_value.append_i64(line);
    return line_value.data();
}
std::string MetaWriter::encode_region_info(const pb::RegionInfo& region_info) const {
    std::string string_region_info;
    if (!region_info.SerializeToString(&string_region_info)) {
        DB_FATAL("region_info: %s serialize to string fail, region_id: %ld", 
                region_info.ShortDebugString().c_str(), region_info.region_id()); 
        string_region_info.clear();
    }
    return string_region_info;
}
std::string MetaWriter::encode_transcation_pb_value(const pb::StoreReq& txn) const {
    std::string string_txn;
    if (!txn.SerializeToString(&string_txn)) {
        DB_FATAL("txn: %s serialize to string fail", txn.ShortDebugString().c_str());
        string_txn.clear();
    }
    return string_txn;
}

std::string MetaWriter::encode_transcation_log_index_value(int64_t log_index) const {
    MutTableKey index_value;
    index_value.append_i64(log_index);
    return index_value.data();
}

int64_t MetaWriter::decode_log_index_value(const rocksdb::Slice& value) {
    TableKey index_value(value);
    return index_value.extract_i64(0);
}

std::string MetaWriter::meta_info_prefix(int64_t region_id) {
    MutTableKey prefix_key;
    prefix_key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    prefix_key.append_i64(region_id);
    return prefix_key.data();
}
} // end of namespace
