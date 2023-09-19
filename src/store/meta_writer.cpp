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

#include "meta_writer.h"
#include <cstdint>
#include <rocksdb/sst_file_reader.h>
#include "table_key.h"
#include "region_control.h"

namespace baikaldb {
const rocksdb::WriteOptions MetaWriter::write_options;

// LEVEL 1
const std::string MetaWriter::META_IDENTIFY(1, 0x01);
//key: ROCKS_HANG_CHECK_IDENTIFY + -1, 反复写这个key，判断store是否卡住
const std::string MetaWriter::ROCKS_HANG_CHECK_IDENTIFY(1, 0x02);

// LEVEL 2
//key: META_IDENTIFY + region_id + identify: value
const std::string MetaWriter::APPLIED_INDEX_INDENTIFY(1, 0x01);
const std::string MetaWriter::NUM_TABLE_LINE_INDENTIFY(1, 0x02);
//key: META_IDENTIFY + region_id + identify + txn_id : log_index
const std::string MetaWriter::PREPARED_TXN_LOG_INDEX_IDENTIFY(1, 0x03);
//key: META_IDENTIFY + region_id + identify + txn_id + log_indx: transaction_pb
const std::string MetaWriter::PREPARED_TXN_PB_IDENTIYF(1, 0x04);
//key: META_IDENTIFY + region_id + identify:
const std::string MetaWriter::REGION_INFO_IDENTIFY(1, 0x05);
//key: META_IDENIFY + region_id + identify + txn_id : num_table_lines + applied_index
const std::string MetaWriter::PRE_COMMIT_IDENTIFY(1, 0x06);
//key: META_IDENIFY + region_id + identify
const std::string MetaWriter::DOING_SNAPSHOT_IDENTIFY(1, 0x07);
//key: META_IDENIFY + region_id + identify
const std::string MetaWriter::REGION_DDL_INFO_IDENTIFY(1, 0x08);

const std::string MetaWriter::ROLLBACKED_TXN_IDENTIFY(1, 0x09);

const std::string MetaWriter::BINLOG_CHECK_POINT_IDENTIFY(1, 0x0A);

const std::string MetaWriter::BINLOG_OLDEST_IDENTIFY(1, 0x0B);

const std::string MetaWriter::LEARNER_IDENTIFY(1, 0x0C);

const std::string MetaWriter::LOCAL_STORAGE_IDENTIFY(1, 0x0D);
//key: META_IDENIFY + region_id + identify
const std::string MetaWriter::OLAP_REGION_IDENTIFY(1, 0x0E);
//key: META_IDENIFY + region_id + identify
const std::string MetaWriter::REGION_OFFLINE_BINLOG_IDENTIFY(1, 0x0F);

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
    values.push_back(encode_applied_index(0, 0));

    keys.push_back(num_table_lines_key(region_id));
    values.push_back(encode_num_table_lines(0));

    keys.push_back(binlog_check_point_key(region_id));
    values.push_back(encode_binlog_ts(0));

    keys.push_back(binlog_oldest_ts_key(region_id));
    values.push_back(encode_binlog_ts(0));

    //keys.push_back(learner_key(region_id));
    //values.push_back(region_info.is_learner() ? encode_learner_flag(1) : encode_learner_flag(0));
    
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

int MetaWriter::update_region_ddl_info(const pb::StoreRegionDdlInfo& region_ddl_info) {
    int64_t region_id = region_ddl_info.region_id();
    std::string string_region_ddl_info;
    if (!region_ddl_info.SerializeToString(&string_region_ddl_info)) {
        DB_FATAL("region_ddl_info: %s serialize to string fail, region_id: %ld",
                region_ddl_info.ShortDebugString().c_str(), region_id);
    }
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
                rocksdb::Slice(region_ddl_info_key(region_id)), rocksdb::Slice(string_region_ddl_info));
    if (!status.ok()) {
        DB_FATAL("write update_region_ddl_info fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    } else {
        DB_NOTICE("write region info success, region_ddl_info: %s", region_ddl_info.ShortDebugString().c_str());
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
    } else {
        //DB_WARNING("region_id: %ld update num_table_lines: %ld success", region_id, num_table_lines);
    }
    return 0;
}

int MetaWriter::update_apply_index(int64_t region_id, int64_t applied_index, int64_t data_index) {
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf, 
                rocksdb::Slice(applied_index_key(region_id)), 
                rocksdb::Slice(encode_applied_index(applied_index, data_index)));
    if (!status.ok()) {
        DB_FATAL("write apply index fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    } else {
        //DB_WARNING("region_id: %ld update_apply_index : %ld success", 
        //            region_id, applied_index);
    }
    return 0; 
}
int MetaWriter::write_pre_commit(int64_t region_id, uint64_t txn_id, int64_t num_table_lines,
                                 int64_t applied_index) {
    if (applied_index == 0) {
        return 0;
    }
    MutTableKey line_value;
    line_value.append_i64(num_table_lines).append_i64(applied_index);
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                rocksdb::Slice(pre_commit_key(region_id, txn_id)),
                rocksdb::Slice(line_value.data()));
    if (!status.ok()) {
        DB_FATAL("write pre commit fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}
int MetaWriter::write_doing_snapshot(int64_t region_id) {
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
            rocksdb::Slice(doing_snapshot_key(region_id)),
            rocksdb::Slice(""));
    if (!status.ok()) {
        DB_FATAL("write doing snapshot fail, err_msg: %s, region_id: %ld",
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

int MetaWriter::read_transcation_rollbacked_tag(int64_t region_id, uint64_t txn_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(rollbacked_transcation_key(region_id, txn_id)), &value);
    if (!status.ok()) {
        return -1;
    }
    return 0;
}

int MetaWriter::write_meta_after_commit(int64_t region_id, int64_t num_table_lines, 
            int64_t applied_index, int64_t data_index, uint64_t txn_id, bool need_write_rollback) {
    if (applied_index == 0) {
        return 0;
    }
    rocksdb::WriteBatch batch;
    batch.Put(_meta_cf, applied_index_key(region_id), encode_applied_index(applied_index, data_index));
    batch.Put(_meta_cf, num_table_lines_key(region_id), encode_num_table_lines(num_table_lines));
    if (need_write_rollback) {
        // 这条记录会残留，考虑ttl解决
        batch.Put(_meta_cf, rollbacked_transcation_key(region_id, txn_id), rocksdb::Slice(""));
    }
    batch.Delete(_meta_cf, pre_commit_key(region_id, txn_id));
    batch.Delete(_meta_cf, transcation_log_index_key(region_id, txn_id));
    return write_batch(&batch, region_id);
}

int MetaWriter::clear_error_pre_commit(int64_t region_id, uint64_t txn_id) {
    rocksdb::WriteBatch batch;
    batch.Delete(_meta_cf, pre_commit_key(region_id, txn_id));
    batch.Delete(_meta_cf, transcation_log_index_key(region_id, txn_id));
    return write_batch(&batch, region_id);
}

int MetaWriter::write_meta_begin_index(int64_t region_id, int64_t log_index, int64_t data_index, uint64_t txn_id) {
    if (log_index == 0) {
        return 0;
    }
    rocksdb::WriteBatch batch;
    batch.Put(_meta_cf, applied_index_key(region_id), encode_applied_index(log_index, data_index));
    batch.Put(_meta_cf, transcation_log_index_key(region_id, txn_id), encode_transcation_log_index_value(log_index));
    return write_batch(&batch, region_id);
}
int MetaWriter::write_meta_index_and_num_table_lines(int64_t region_id, int64_t log_index, int64_t data_index,
                        int64_t num_table_lines, SmartTransaction txn) {
    if (log_index == 0) {
        return 0;
    }
    txn->put_meta_info(applied_index_key(region_id), encode_applied_index(log_index, data_index));
    txn->put_meta_info(num_table_lines_key(region_id), encode_num_table_lines(num_table_lines));
    return 0;
}
/*
int MetaWriter::ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id) {
    rocksdb::IngestExternalFileOptions ifo;
    auto res = _rocksdb->ingest_external_file(_meta_cf, {meta_sst_file}, ifo);
    if (!res.ok()) {
         DB_WARNING("Error while adding file %s, Error %s, region_id: %ld",
                 meta_sst_file.c_str(), res.ToString().c_str(), region_id);
         return -1;
    }
    return 0;
}*/
// meta数据量很少，直接写入memtable减少ingest 导致的flush
int MetaWriter::ingest_meta_sst(const std::string& meta_sst_file, int64_t region_id) {
    RocksWrapper* db = RocksWrapper::get_instance();
    rocksdb::Options options = db->get_options(db->get_meta_info_handle());
    rocksdb::SstFileReader reader(options);
    auto res = reader.Open(meta_sst_file);
    if (!res.ok()) {
         DB_WARNING("SstFileReader open fail %s, Error %s, region_id: %ld",
                 meta_sst_file.c_str(), res.ToString().c_str(), region_id);
         return -1;
    }
    rocksdb::ReadOptions read_opt;
    std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(read_opt));
    for (iter->Seek(""); iter->Valid(); iter->Next()) {
        auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                iter->key(), iter->value());
        if (!status.ok()) {
            DB_FATAL("put meta fail, err_msg: %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
            return -1;
        }
    }
    return 0;
}

int MetaWriter::clear_meta_info(int64_t drop_region_id) {
    TimeCost cost;
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    //batch.Delete(_meta_cf, region_info_key(drop_region_id));
    batch.Delete(_meta_cf, applied_index_key(drop_region_id));
    batch.Delete(_meta_cf, num_table_lines_key(drop_region_id));
    //batch.Delete(_meta_cf, doing_snapshot_key(drop_region_id));
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
    ret = clear_txn_log_index(drop_region_id);
    if (ret < 0) {
        DB_FATAL("clear txn log index fail, region_id: %ld", drop_region_id);
        return ret;
    }
    ret = clear_pre_commit_infos(drop_region_id);
    if (ret < 0) {
        DB_FATAL("clear pre commit infos fail, region_id: %ld", drop_region_id);
        return ret;
    }
    DB_WARNING("clear meta info success, cost: %ld, region_id: %ld", cost.get_time(), drop_region_id);
    return 0;
}

int MetaWriter::clear_all_meta_info(int64_t drop_region_id) {
    TimeCost cost;
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    batch.Delete(_meta_cf, region_info_key(drop_region_id));
    batch.Delete(_meta_cf, applied_index_key(drop_region_id));
    batch.Delete(_meta_cf, num_table_lines_key(drop_region_id));
    batch.Delete(_meta_cf, doing_snapshot_key(drop_region_id));
    batch.Delete(_meta_cf, learner_key(drop_region_id));
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
    ret = clear_txn_log_index(drop_region_id);
    if (ret < 0) {
        DB_FATAL("clear txn log index fail, region_id: %ld", drop_region_id);
        return ret;
    }
    ret = clear_pre_commit_infos(drop_region_id);
    if (ret < 0) {
        DB_FATAL("clear pre commit infos fail, region_id: %ld", drop_region_id);
        return ret;
    }
    DB_WARNING("clear meta info success, cost: %ld, region_id: %ld", cost.get_time(), drop_region_id);
    return 0;
}

int MetaWriter::clear_txn_log_index(int64_t region_id) {
    std::string start_key = transcation_log_index_key(region_id, 0);
    std::string end_key = transcation_log_index_key(region_id, UINT64_MAX);
    auto status = _rocksdb->remove_range(MetaWriter::write_options, _meta_cf,
            start_key, end_key, false);
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}
int MetaWriter::clear_txn_infos(int64_t region_id) {
    std::string start_key = transcation_pb_key(region_id, 0, 0);
    std::string end_key = transcation_pb_key(region_id, UINT64_MAX, INT64_MAX);
    auto status = _rocksdb->remove_range(MetaWriter::write_options, _meta_cf,
            start_key, end_key, false);
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}

int MetaWriter::clear_pre_commit_infos(int64_t region_id) {
    std::string start_key = pre_commit_key(region_id, 0);
    std::string end_key = pre_commit_key(region_id, UINT64_MAX);
    auto status = _rocksdb->remove_range(MetaWriter::write_options, _meta_cf,
            start_key, end_key, false);
    if (!status.ok()) {
        DB_WARNING("remove_range error: code=%d, msg=%s, region_id: %ld",
            status.code(), status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}

int MetaWriter::clear_doing_snapshot(int64_t region_id) {
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    batch.Delete(_meta_cf, doing_snapshot_key(region_id));
    auto status = _rocksdb->write(options, &batch);
    if (!status.ok()) {
        DB_FATAL("drop region fail, error: code=%d, msg=%s, region_id: %ld", 
        status.code(), status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}

int MetaWriter::clear_region_info(int64_t region_id) {
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    batch.Delete(_meta_cf, region_info_key(region_id));
    auto status = _rocksdb->write(options, &batch);
    if (!status.ok()) {
        DB_FATAL("drop region fail, error: code=%d, msg=%s, region_id: %ld", 
        status.code(), status.ToString().c_str(), region_id);
        return -1;
    }
    return 0;
}
int MetaWriter::parse_region_infos(std::vector<pb::RegionInfo>& region_infos) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
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
            continue;
        }
        region_infos.push_back(region_info);
    }
    return 0;
}

int MetaWriter::parse_txn_infos(int64_t region_id, std::map<int64_t, std::string>& prepared_txn_infos) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string prefix = transcation_pb_key_prefix(region_id) ;
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        if (!iter->key().starts_with(prefix)) {
            DB_WARNING("read end info, region_id: %ld, key:%s", region_id, iter->key().ToString(true).c_str());
            break;
        }
        int64_t log_index = 0;
        //key: identify + region_id + identify + txn_id + log_index
        if (iter->key().size() == (1 + sizeof(int64_t) + 1 + sizeof(uint64_t) + sizeof(int64_t))) {
            log_index = TableKey(iter->key()).extract_i64(1 + sizeof(int64_t) + 1 + sizeof(uint64_t));
            DB_WARNING("parse txn info, log_index: %ld, txn_id: %lu, region_id: %ld", 
                        log_index, TableKey(iter->key()).extract_u64(1 + sizeof(int64_t) + 1), region_id);
        } else {
            log_index = TableKey(iter->key()).extract_i64(1 + sizeof(int64_t) + 1);
            DB_WARNING("parse txn info, log_index: %ld, region_id: %ld",
                        log_index, region_id);
        }
        prepared_txn_infos[log_index] = iter->value().ToString();
    }
    return 0;
}

int MetaWriter::parse_txn_log_indexs(int64_t region_id, std::unordered_map<uint64_t, int64_t>& log_indexs) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    read_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string prefix = log_index_key_prefix(region_id);
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        if (!iter->key().starts_with(prefix)) {
            DB_WARNING("read end info, region_id: %ld, key:%s", region_id, iter->key().ToString(true).c_str());
            break;
        }
        TableKey log_index(iter->value());
        log_indexs[decode_log_index_key(iter->key())] = log_index.extract_i64(0);
        DB_WARNING("region_id: %ld read prepared transcation log index: %ld, transaction_id: %lu", 

                    region_id, log_index.extract_i64(0), decode_log_index_key(iter->key()));
    }
    return 0; 
}

int MetaWriter::parse_doing_snapshot(std::set<int64_t>& region_ids) {
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    read_options.total_order_seek = false;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _meta_cf));
    std::string prefix = MetaWriter::META_IDENTIFY;
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        std::string identify;
        TableKey(iter->key()).extract_char(1 + sizeof(int64_t), 1, identify);
        if (identify != MetaWriter::DOING_SNAPSHOT_IDENTIFY) {
            continue;
        }
        region_ids.insert(TableKey(rocksdb::Slice(iter->key())).extract_i64(1));
    }
    return 0;
}

void MetaWriter::read_applied_index(int64_t region_id, int64_t* applied_index, int64_t* data_index) {
    std::string value;
    rocksdb::ReadOptions options;
    read_applied_index(region_id, options, applied_index, data_index);
}

void MetaWriter::read_applied_index(int64_t region_id, const rocksdb::ReadOptions& options, int64_t* applied_index, int64_t* data_index) {
    std::string value;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(applied_index_key(region_id)), &value);
    if (!status.ok()) {
        DB_WARNING("Error while read applied index, Error %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        *applied_index = -1;
        *data_index = -1;
        return;
    }
    TableKey tk(value);
    *applied_index = tk.extract_i64(0);
    // 兼容处理
    if (value.size() == 16) {
        *data_index = tk.extract_i64(8);
    } else {
        *data_index = *applied_index;
    }
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

int MetaWriter::read_learner_key(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(learner_key(region_id)), &value);
    if (!status.ok()) {
        DB_WARNING("Error while read learner key, Error %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    DB_DEBUG("region_id %ld read learner %s", region_id, str_to_hex(value).c_str());
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

int MetaWriter::read_region_ddl_info(int64_t region_id, pb::StoreRegionDdlInfo& region_ddl_info) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(region_ddl_info_key(region_id)), &value);
    if (!status.ok()) {
        DB_NOTICE("DDL_LOG region_id: %ld have no ddlwork.", region_id);
        return -1; 
    }
    if (!region_ddl_info.ParseFromString(value)) {
        DB_FATAL("parse from pb fail when read region ddl info, region_id: %ld, value:%s",
            region_id, value.c_str());
        return -1;
    }
    return 0;
}
int MetaWriter::read_doing_snapshot(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(doing_snapshot_key(region_id)), &value);
    if (!status.ok()) {
        DB_WARNING("region_id: %ld not doing snapshot", region_id);
        return -1;
    }
    DB_WARNING("region_id: %ld read doing snapshot success", region_id);
    return 0;
}
int MetaWriter::read_pre_commit_key(int64_t region_id, uint64_t txn_id, 
            int64_t& num_table_lines,
            int64_t& applied_index) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(pre_commit_key(region_id, txn_id)), &value);
    if (!status.ok()) {
        return -1;
    }
    num_table_lines = TableKey(rocksdb::Slice(value)).extract_i64(0);
    applied_index = TableKey(rocksdb::Slice(value)).extract_i64(sizeof(int64_t));
    DB_WARNING("region_id: %ld read pre commit value, txn_id: %lu num_table_lines: %ld, applied_index: %ld",
                region_id, txn_id, num_table_lines, applied_index);
    return 0;
}

std::string MetaWriter::region_info_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.data(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::REGION_INFO_IDENTIFY.data(), 1);
    return key.data();
}

std::string MetaWriter::region_ddl_info_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.data(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::REGION_DDL_INFO_IDENTIFY.data(), 1);
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
std::string MetaWriter::transcation_pb_key(int64_t region_id, uint64_t txn_id, int64_t log_index) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PREPARED_TXN_PB_IDENTIYF.c_str(), 1);
    key.append_u64(txn_id);
    key.append_i64(log_index);
    return key.data();
}

std::string MetaWriter::rollbacked_transcation_key(int64_t region_id, uint64_t txn_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::ROLLBACKED_TXN_IDENTIFY.c_str(), 1);
    key.append_u64(txn_id);
    return key.data();
}

std::string MetaWriter::transcation_pb_key_prefix(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PREPARED_TXN_PB_IDENTIYF.c_str(), 1);
    return key.data(); 
}
std::string MetaWriter::pre_commit_key_prefix(int64_t region_id)  const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PRE_COMMIT_IDENTIFY.c_str(), 1);
    return key.data();
}
std::string MetaWriter::pre_commit_key(int64_t region_id, uint64_t txn_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::PRE_COMMIT_IDENTIFY.c_str(), 1);
    key.append_u64(txn_id);
    return key.data();
}

std::string MetaWriter::doing_snapshot_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::DOING_SNAPSHOT_IDENTIFY.c_str(), 1);
    return key.data();
}
std::string MetaWriter::encode_applied_index(int64_t applied_index, int64_t data_index) const {
    MutTableKey index_value;
    index_value.append_i64(applied_index);
    index_value.append_i64(data_index);
    return index_value.data();
}

std::string MetaWriter::encode_removed_ddl_key(int64_t region_id, int64_t index_id)  const {
    MutTableKey ddl_key;
    ddl_key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    ddl_key.append_i64(region_id);
    ddl_key.append_char(MetaWriter::LOCAL_STORAGE_IDENTIFY.c_str(), 1);
    ddl_key.append_i64(index_id);
    return ddl_key.data();
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
uint64_t MetaWriter::decode_log_index_key(const rocksdb::Slice& key) {
    TableKey index_key(key);
    return index_key.extract_u64(1 + sizeof(int64_t) + 1);
}

uint64_t MetaWriter::decode_pre_commit_key(const rocksdb::Slice& key) {
    TableKey index_key(key);
    return index_key.extract_u64(1 + sizeof(int64_t) + 1);
}
std::string MetaWriter::meta_info_prefix(int64_t region_id) {
    MutTableKey prefix_key;
    prefix_key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    prefix_key.append_i64(region_id);
    return prefix_key.data();
}

std::string MetaWriter::encode_binlog_ts(int64_t ts) const {
    MutTableKey ts_value;
    ts_value.append_i64(ts);
    return ts_value.data();
}

std::string MetaWriter::encode_learner_flag(int64_t learner_flag) const {
    MutTableKey value;
    value.append_i64(learner_flag);
    return value.data();
}

std::string MetaWriter::binlog_check_point_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::BINLOG_CHECK_POINT_IDENTIFY.c_str(), 1);
    return key.data();
}

int MetaWriter::write_binlog_check_point(int64_t region_id, int64_t ts) {
    MutTableKey value;
    value.append_i64(ts);
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                rocksdb::Slice(binlog_check_point_key(region_id)),
                rocksdb::Slice(value.data()));
    if (!status.ok()) {
        DB_FATAL("write binlog check point fail, err_msg: %s, region_id: %ld, ts: %ld",
                    status.ToString().c_str(), region_id, ts);
        return -1;
    }
    return 0;
}

int MetaWriter::write_learner_key(int64_t region_id, bool is_learner) {
    DB_DEBUG("write learner key region %ld, is_learner %d write %s", 
        region_id, is_learner, str_to_hex(encode_learner_flag(is_learner)).c_str());
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                rocksdb::Slice(learner_key(region_id)),
                rocksdb::Slice(encode_learner_flag(is_learner)));
    if (!status.ok()) {
        DB_FATAL("write binlog check point fail, err_msg: %s, region_id: %ld, learner: %d",
                    status.ToString().c_str(), region_id, is_learner);
        return -1;
    }
    return 0;
}

int64_t MetaWriter::read_binlog_check_point(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(binlog_check_point_key(region_id)), &value);
    if (!status.ok()) {
        DB_FATAL("Error while read binlog check point, Error %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return TableKey(rocksdb::Slice(value)).extract_i64(0);
}

std::string MetaWriter::binlog_oldest_ts_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::BINLOG_OLDEST_IDENTIFY.c_str(), 1);
    return key.data();
}

std::string MetaWriter::learner_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::LEARNER_IDENTIFY.c_str(), 1);
    return key.data();
}

std::string MetaWriter::olap_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::OLAP_REGION_IDENTIFY.c_str(), 1);
    return key.data();
}

std::string MetaWriter::offline_binlog_key(int64_t region_id) const {
    MutTableKey key;
    key.append_char(MetaWriter::META_IDENTIFY.c_str(), 1);
    key.append_i64(region_id);
    key.append_char(MetaWriter::REGION_OFFLINE_BINLOG_IDENTIFY.c_str(), 1);
    return key.data();
}

int MetaWriter::write_olap_info(int64_t region_id, const pb::OlapRegionInfo& olap_info) {
    std::string string_olap_info;
    if (!olap_info.SerializeToString(&string_olap_info)) {
        DB_FATAL("olap_info: %s serialize to string fail, region_id: %ld", 
                olap_info.ShortDebugString().c_str(), region_id); 
        string_olap_info.clear();
        return -1;
    }

    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                rocksdb::Slice(olap_key(region_id)),
                rocksdb::Slice(string_olap_info));
    if (!status.ok()) {
        DB_FATAL("write olap info fail, err_msg: %s, region_id: %ld, info: %s",
                    status.ToString().c_str(), region_id, olap_info.ShortDebugString().c_str());
        return -1;
    }
    return 0;
}

int MetaWriter::read_olap_info(int64_t region_id, pb::OlapRegionInfo& olap_info) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(olap_key(region_id)), &value);
    if (status.ok()) {
        if (!olap_info.ParseFromString(value)) {
            DB_FATAL("parse from pb fail when read olap info, region_id: %ld, value:%s",
                region_id, value.c_str());
            return -1;
        }
        return 0;
    } else if (status.IsNotFound()) {
        olap_info.set_state(pb::OLAP_ACTIVE);
        return 0;
    } else {
        DB_FATAL("Error while read olap info, Error %s, region_id: %ld",
            status.ToString().c_str(), region_id);
        return -1;
    }
}

int MetaWriter::write_binlog_oldest_ts(int64_t region_id, int64_t ts) {
    MutTableKey value;
    value.append_i64(ts);
    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                rocksdb::Slice(binlog_oldest_ts_key(region_id)),
                rocksdb::Slice(value.data()));
    if (!status.ok()) {
        DB_FATAL("write binlog oldest ts fail, err_msg: %s, region_id: %ld, ts: %ld",
                    status.ToString().c_str(), region_id, ts);
        return -1;
    }
    return 0;
}

int64_t MetaWriter::read_binlog_oldest_ts(int64_t region_id) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(binlog_oldest_ts_key(region_id)), &value);
    if (!status.ok()) {
        DB_FATAL("Error while read binlog oldest ts, Error %s, region_id: %ld",
                    status.ToString().c_str(), region_id);
        return -1;
    }
    return TableKey(rocksdb::Slice(value)).extract_i64(0);
}


int MetaWriter::write_region_offline_binlog_info(int64_t region_id, const pb::RegionOfflineBinlogInfo& offline_binlog_info) {
    std::string string_offline_binlog_info;
    if (!offline_binlog_info.SerializeToString(&string_offline_binlog_info)) {
        DB_FATAL("offline_binlog_info: %s serialize to string fail, region_id: %ld", 
                offline_binlog_info.ShortDebugString().c_str(), region_id); 
        string_offline_binlog_info.clear();
        return -1;
    }

    auto status = _rocksdb->put(MetaWriter::write_options, _meta_cf,
                rocksdb::Slice(offline_binlog_key(region_id)),
                rocksdb::Slice(string_offline_binlog_info));
    if (!status.ok()) {
        DB_FATAL("write offline_binlog_info fail, err_msg: %s, region_id: %ld, info: %s",
                    status.ToString().c_str(), region_id, offline_binlog_info.ShortDebugString().c_str());
        return -1;
    }
    return 0;
}

int MetaWriter::read_region_offline_binlog_info(int64_t region_id, pb::RegionOfflineBinlogInfo& offline_binlog_info) {
    std::string value;
    rocksdb::ReadOptions options;
    auto status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(offline_binlog_key(region_id)), &value);
    if (status.ok()) {
        if (!offline_binlog_info.ParseFromString(value)) {
            DB_FATAL("parse from pb fail when read offline_binlog_info, region_id: %ld, value:%s",
                region_id, value.c_str());
            return -1;
        }
        return 0;
    } else if (status.IsNotFound()) {
        return 0;
    } else {
        DB_FATAL("Error while read offline_binlog_info, Error %s, region_id: %ld",
            status.ToString().c_str(), region_id);
        return -1;
    }
}

int MetaWriter::rocks_hang_check() {
    MutTableKey key;
    key.append_char(MetaWriter::ROCKS_HANG_CHECK_IDENTIFY.c_str(), 1);
    key.append_i64(-1);
    
    // write
    MutTableKey value;
    value.append_i64(butil::gettimeofday_us());
    rocksdb::WriteOptions write_option;
    write_option.sync = true;
    auto status = _rocksdb->put(write_option, 
                                _meta_cf, 
                                rocksdb::Slice(key.data()), 
                                rocksdb::Slice(value.data()));
    if (!status.ok()) {
        DB_FATAL("write rocks_hang_check_key fail, err_msg: %s", status.ToString().c_str());
        return -1;
    }

    // read
    std::string r_value;
    rocksdb::ReadOptions options;
    status = _rocksdb->get(options, _meta_cf, rocksdb::Slice(key.data()), &r_value);
    if (!status.ok()) {
        DB_FATAL("Error while read rocks_hang_check_key, Error %s", status.ToString().c_str());
        return -1;
    }
    return 0;
}

} // end of namespace
