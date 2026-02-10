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

#include "gtest/gtest.h"
#include "meta_rocksdb.h"
#include <gflags/gflags.h>
#include "sst_file_writer.h"
DEFINE_bool(only_ingest_external, false, "enable log");
DEFINE_string(ingest_file_name, "", "ingest file name");
using namespace baikaldb;
class RocksdbTEST : public testing::Test {
public:
    ~RocksdbTEST() {}
protected:
    virtual void SetUp() {
        
    }
    virtual void TearDown() {}
};

void print_metadata_info(baikaldb::RocksWrapper*  rocksdb) {
    std::vector<rocksdb::LiveFileMetaData> metadatas;
    rocksdb->get_db()->GetLiveFilesMetaData(&metadatas);
    for (const auto& metadata : metadatas) {
        DB_WARNING("LiveFileMetaData[cf_name: %s level: %d]; "
            "smallestkey[%s]; largestkey[%s]; "
            "SstFileMetaData[smallest_seqno: %lu largest_seqno: %lu num_reads_sampled: %lu being_compacted: %d "
            "num_entries: %lu num_deletions: %lu file_creation_time: %lu name: %s db_path: %s]; "
            "FileStorageInfo[relative_filename: %s directory: %s file_number: %lu size: %lu epoch_number: %lu file_checksum: %s file_checksum_func_name: %s].", 
            metadata.column_family_name.c_str(), metadata.level,
            metadata.smallestkey.c_str(), metadata.largestkey.c_str(),
            metadata.smallest_seqno, metadata.largest_seqno, metadata.num_reads_sampled, (int)metadata.being_compacted,
            metadata.num_entries, metadata.num_deletions, metadata.file_creation_time, metadata.name.c_str(), metadata.db_path.c_str(), 
            metadata.relative_filename.c_str(), metadata.directory.c_str(), metadata.file_number, metadata.size, metadata.epoch_number, 
            metadata.file_checksum.c_str(), metadata.file_checksum_func_name.c_str());
    }
}

void print_properties_info(baikaldb::RocksWrapper*  rocksdb) {
    rocksdb::TablePropertiesCollection props;
    rocksdb->get_db()->GetPropertiesOfAllTables(rocksdb->get_data_handle(), &props);
    for (const auto& item : props) {
        DB_WARNING("TablePropertiesCollection[cf_name: %s] [props: %s]; ", item.first.c_str(), item.second->ToString("; ", ": ").c_str());
    }
}   

TEST_F(RocksdbTEST, ingest_test7) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db7");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });
    // auto dbimpl = static_cast<rocksdb::DBImpl*>(_rocksdb->get_db()->GetRootDB());
    for (int i = 0; i < 15; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+1)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+1).c_str());
    }
    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    DB_WARNING("begin compact");
    auto s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);


    int i = 1;
    _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+2)));
    DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+2).c_str());
    i = 3;
    _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+2)));
    DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+2).c_str());
    _rocksdb->remove(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("4"));
    DB_WARNING("remove key: %s", "4");
    // _rocksdb->remove_range(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("6"), rocksdb::Slice("8"), false);
    DB_WARNING("remove_range key: [6-8]");
    // _rocksdb->remove_range(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("7"), rocksdb::Slice("9"), false);
    DB_WARNING("remove_range key: [7-9]");
    // _rocksdb->remove_range(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("4"), rocksdb::Slice("5"), false);
    DB_WARNING("remove_range key: [4-5]");

    rocksdb::FlushOptions flush_options;
    auto status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
    if (!status.ok()) {
        DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
    }

    _rocksdb->remove_range(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("1"), rocksdb::Slice("3"), false);

    print_metadata_info(_rocksdb);
    std::string delete_range_str;
    s = _rocksdb->get_db()->RangeTombstoneSummary(_rocksdb->get_data_handle(), 10, &delete_range_str);
    if (!s.ok()) {
        DB_WARNING("RangeTombstoneSummary error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    DB_WARNING("delete_range_str: %s", delete_range_str.c_str());
    rocksdb::ReadOptions read_options;
    read_options.ignore_range_deletions = true;
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    DB_WARNING("ignore_range_deletions: true");
    read_options.ignore_range_deletions = false;
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    bthread_usleep(60*1000*1000);
}

TEST_F(RocksdbTEST, ingest_test6) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db6");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });
    // auto dbimpl = static_cast<rocksdb::DBImpl*>(_rocksdb->get_db()->GetRootDB());
    for (int i = 0; i < 15; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+1)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+1).c_str());
    }
    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    DB_WARNING("begin compact");
    auto s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    FLAGS_enable_remote_compaction = true;
    s = _rocksdb->compact_files(rocksdb::CompactionOptions(), _rocksdb->get_data_handle(), {"./rocks_db6/000020.sst"}, 6);
    if (!s.ok()) {
        DB_WARNING("compact_files error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }

    FLAGS_enable_remote_compaction = false;
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter2(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        rocksdb::Slice key_slice(iter2->key());
        rocksdb::Slice value_slice(iter2->value());
        DB_WARNING("scan1 key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }

    for (int i = 0; i < 10; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+2)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+2).c_str());
    }
    const rocksdb::Snapshot* snapshot = _rocksdb->get_snapshot();
    rocksdb::FlushOptions flush_options;
    auto status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
    if (!status.ok()) {
        DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
    }
    print_metadata_info(_rocksdb);

    iter2.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        rocksdb::Slice key_slice(iter2->key());
        rocksdb::Slice value_slice(iter2->value());
        DB_WARNING("scan2 key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }

    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }

}

TEST_F(RocksdbTEST, ingest_test5) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db5");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });
    // auto dbimpl = static_cast<rocksdb::DBImpl*>(_rocksdb->get_db()->GetRootDB());
    for (int i = 0; i < 15; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i).c_str());
    }
    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    DB_WARNING("begin compact");
    auto s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    for (int i = 0; i < 10; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+1)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+1).c_str());
    }
    const rocksdb::Snapshot* snapshot = _rocksdb->get_snapshot();
    rocksdb::FlushOptions flush_options;
    auto status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
    if (!status.ok()) {
        DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    DB_WARNING("222222222222222");
    for (int i = 3; i < 7; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+2)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+2).c_str());
    }

    status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
    if (!status.ok()) {
        DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
    }

    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter2(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        rocksdb::Slice key_slice(iter2->key());
        rocksdb::Slice value_slice(iter2->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    print_metadata_info(_rocksdb);
    read_options.table_filter = [&](const rocksdb::TableProperties& props) {
        if (props.orig_file_number == 20) {
            // DB_WARNING("RETRUN FALSE file_number: %lu TableProperties: %s", props.orig_file_number, props.ToString("; ", ": ").c_str());
            return false;
        } else {
            // DB_WARNING("RETRUN TRUE file_number: %lu TableProperties: %s", props.orig_file_number, props.ToString("; ", ": ").c_str());
            return true;
        }
    };
    s = _rocksdb->compact_files(rocksdb::CompactionOptions(), _rocksdb->get_data_handle(), {"./rocks_db5/000022.sst","./rocks_db5/000024.sst"}, 1);
    if (!s.ok()) {
        DB_WARNING("compact_files error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    s = _rocksdb->compact_files(rocksdb::CompactionOptions(), _rocksdb->get_data_handle(), {"./rocks_db5/000025.sst"}, 5);
    if (!s.ok()) {
        DB_WARNING("compact_files error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    _rocksdb->release_snapshot(snapshot);
    bthread_usleep(10*1000*1000);
    s = _rocksdb->compact_files(rocksdb::CompactionOptions(), _rocksdb->get_data_handle(), {"./rocks_db5/000026.sst"}, 5);
    if (!s.ok()) {
        DB_WARNING("compact_files error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    bthread_usleep(10*1000*1000);
    s = _rocksdb->compact_files(rocksdb::CompactionOptions(), _rocksdb->get_data_handle(), {"./rocks_db5/000027.sst"}, 5);
    if (!s.ok()) {
        DB_WARNING("compact_files error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    int i = 5;
    _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+3)));
    i = 6;
    _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+3)));
    DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+3).c_str());
    // snapshot = _rocksdb->get_snapshot();
    // DB_WARNING("seq: %lu", snapshot->GetSequenceNumber());
    status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
    if (!status.ok()) {
        DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    bthread_usleep(10*1000*1000);
    s = _rocksdb->compact_files(rocksdb::CompactionOptions(), _rocksdb->get_data_handle(), {"./rocks_db5/000030.sst"}, 5);
    if (!s.ok()) {
        DB_WARNING("compact_files error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    // bthread_usleep(10*1000*1000);
    // s = _rocksdb->compact_files(rocksdb::CompactionOptions(), _rocksdb->get_data_handle(), {"./rocks_db5/000029.sst"}, 5);
    // if (!s.ok()) {
    //     DB_WARNING("compact_files error: code=%d, msg=%s", 
    //             s.code(), s.ToString().c_str());
    // }
    // print_metadata_info(_rocksdb);
    bthread_usleep(60 * 1000 * 1000);
}

TEST_F(RocksdbTEST, ingest_test4) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db4");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });
    // auto dbimpl = static_cast<rocksdb::DBImpl*>(_rocksdb->get_db()->GetRootDB());
    for (int i = 0; i < 15; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i).c_str());
    }
    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    DB_WARNING("begin compact");
    auto s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    DB_WARNING("1111111111111");
    for (int i = 0; i < 10; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+1)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+1).c_str());
    }
    const rocksdb::Snapshot* snapshot = _rocksdb->get_snapshot();
    rocksdb::FlushOptions flush_options;
    auto status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
    if (!status.ok()) {
        DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    DB_WARNING("222222222222222");
    for (int i = 0; i < 7; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+2)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+2).c_str());
    }

    status = _rocksdb->flush(flush_options, _rocksdb->get_data_handle());
    if (!status.ok()) {
        DB_WARNING("flush data to rocksdb fail, err_msg:%s", status.ToString().c_str());
    }

    _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("9"), rocksdb::Slice("12"));
    DB_WARNING("put key: %s, value: %s", "9", "12");
    _rocksdb->remove(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("5"));
    DB_WARNING("remove key: %s", "5");
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter2(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        rocksdb::Slice key_slice(iter2->key());
        rocksdb::Slice value_slice(iter2->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    
    DB_WARNING("InternalTravel begin seq: %lu", snapshot->GetSequenceNumber());
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), snapshot->GetSequenceNumber());
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }

    print_metadata_info(_rocksdb);
    read_options.table_filter = [&](const rocksdb::TableProperties& props) {
        if (props.orig_file_number == 20) {
            DB_WARNING("RETRUN FALSE file_number: %lu TableProperties: %s", props.orig_file_number, props.ToString("; ", ": ").c_str());
            return false;
        } else {
            DB_WARNING("RETRUN TRUE file_number: %lu TableProperties: %s", props.orig_file_number, props.ToString("; ", ": ").c_str());
            return true;
        }
    };
    iter2.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        rocksdb::Slice key_slice(iter2->key());
        rocksdb::Slice value_slice(iter2->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    read_options.snapshot = nullptr; 
    // kMaxSequenceNumber
    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    
    // for (int i = 0 ; i < 5; i++) {
    //     DB_WARNING("BEGIN READ ROUND %d", i);
    //     iter2.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    //     for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
    //         rocksdb::Slice key_slice(iter2->key());
    //         rocksdb::Slice value_slice(iter2->value());
    //         DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    //     }
    //     DB_WARNING("END READ ROUND %d", i);
    // }
    
    // rocksdb::Slice start("0");
    // rocksdb::Slice end("999999");
    // rocksdb::CompactRangeOptions compact_options;
    // compact_options.exclusive_manual_compaction = false;
    // DB_WARNING("begin compact");
    // s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    // if (!s.ok()) {
    //     DB_WARNING("compact_range error: code=%d, msg=%s", 
    //             s.code(), s.ToString().c_str());
    // }

    // print_metadata_info(_rocksdb);
    // print_properties_info(_rocksdb);
    // for (int i = 0 ; i < 3; i++) {
    //     DB_WARNING("BEGIN READ ROUND %d", i);
    //     iter2.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    //     for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
    //         rocksdb::Slice key_slice(iter2->key());
    //         rocksdb::Slice value_slice(iter2->value());
    //         DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    //     }
    //     DB_WARNING("END READ ROUND %d", i);
    // }

    bthread_usleep(60 * 1000 * 1000);
}
TEST_F(RocksdbTEST, ingest_test3) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db3");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });
    // auto dbimpl = static_cast<rocksdb::DBImpl*>(_rocksdb->get_db()->GetRootDB());
    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(_rocksdb->get_options(_rocksdb->get_data_handle())));
    std::unique_ptr<SstFileWriter> writer2(new SstFileWriter(_rocksdb->get_options(_rocksdb->get_data_handle())));
    std::string path1 = "./test_ingest1.sst";
    std::string path2 = "./test_ingest2.sst";
    auto s = writer->open(path1);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s", path1.c_str(), s.ToString().c_str());
        return;
    }
    writer->put(rocksdb::Slice("1"), rocksdb::Slice("1"));
    writer->put(rocksdb::Slice("4"), rocksdb::Slice("4"));
    writer->put(rocksdb::Slice("7"), rocksdb::Slice("7"));
    writer->put(rocksdb::Slice("9"), rocksdb::Slice("9"));
    writer->finish();

    s = writer2->open(path2);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s", path2.c_str(), s.ToString().c_str());
        return;
    }
    writer2->put(rocksdb::Slice("11"), rocksdb::Slice("11"));
    writer2->put(rocksdb::Slice("3"), rocksdb::Slice("3"));
    writer2->put(rocksdb::Slice("6"), rocksdb::Slice("6"));
    writer2->put(rocksdb::Slice("9"), rocksdb::Slice("9_2"));
    writer2->put(rocksdb::Slice("99"), rocksdb::Slice("99"));
    writer2->finish();

    const rocksdb::Snapshot* snapshot = nullptr;
    rocksdb::IngestExternalFileOptions ifo;
    ifo.move_files = true;
    ifo.write_global_seqno = false;
    ifo.allow_blocking_flush = false;
    rocksdb::ReadOptions read_options;
    s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {path1}, ifo);
    if (!s.ok()) {
        DB_WARNING("ingest fail: %s", path1.c_str());
    }

    s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {path2}, ifo);
    if (!s.ok()) {
        DB_WARNING("ingest fail: %s", path1.c_str());
    }
    print_metadata_info(_rocksdb);



    s = _rocksdb->get_db()->InternalTravel(read_options, _rocksdb->get_data_handle(), rocksdb::kMaxSequenceNumber);
    if (!s.ok()) {
        DB_WARNING("InternalTravel error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }

    print_properties_info(_rocksdb);
    std::unique_ptr<rocksdb::Iterator> iter2(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        rocksdb::Slice key_slice(iter2->key());
        rocksdb::Slice value_slice(iter2->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    read_options.table_filter = [&](const rocksdb::TableProperties& props) {
        if (butil::fast_rand() % 2 == 0) {
            DB_WARNING("RETRUN TRUE file_number: %lu TableProperties: %s", props.orig_file_number, props.ToString("; ", ": ").c_str());
            return true;
        } else {
            DB_WARNING("RETRUN FALSE file_number: %lu TableProperties: %s", props.orig_file_number, props.ToString("; ", ": ").c_str());
            return false;
        }
    };
    for (int i = 0 ; i < 5; i++) {
        DB_WARNING("BEGIN READ ROUND %d", i);
        iter2.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
        for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
            rocksdb::Slice key_slice(iter2->key());
            rocksdb::Slice value_slice(iter2->value());
            DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
        }
        DB_WARNING("END READ ROUND %d", i);
    }
    
    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    DB_WARNING("begin compact");
    s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }

    print_metadata_info(_rocksdb);
    print_properties_info(_rocksdb);
    for (int i = 0 ; i < 3; i++) {
        DB_WARNING("BEGIN READ ROUND %d", i);
        iter2.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
        for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
            rocksdb::Slice key_slice(iter2->key());
            rocksdb::Slice value_slice(iter2->value());
            DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
        }
        DB_WARNING("END READ ROUND %d", i);
    }


}

TEST_F(RocksdbTEST, ingest_test2) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db2");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });
    
    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(_rocksdb->get_options(_rocksdb->get_data_handle())));
    std::string path1 = "./test_ingest1.sst";
    auto s = writer->open(path1);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s", path1.c_str(), s.ToString().c_str());
        return;
    }
    for (int i = 0; i < 5; i++) {
        writer->put(rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
    }
    writer->finish();

    const rocksdb::Snapshot* snapshot = nullptr;
    rocksdb::IngestExternalFileOptions ifo;
    ifo.move_files = true;
    ifo.write_global_seqno = false;
    ifo.allow_blocking_flush = false;
    rocksdb::ReadOptions read_options;
    s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {path1}, ifo);
    if (!s.ok()) {
        DB_WARNING("ingest fail: %s", path1.c_str());
    }
    
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rocksdb::Slice key_slice(iter->key());
        rocksdb::Slice value_slice(iter->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    print_metadata_info(_rocksdb);
    FLAGS_enable_remote_compaction = true;
    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    DB_WARNING("begin compact");
    s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    iter.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rocksdb::Slice key_slice(iter->key());
        rocksdb::Slice value_slice(iter->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    DB_WARNING("ingest_test2 ENDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    FLAGS_enable_remote_compaction = false;
}

TEST_F(RocksdbTEST, ingest_test1) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db1");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });

    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;

    for (int i = 0; i < 7; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i).c_str());
    }
    DB_WARNING("begin compact");
    auto s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    print_metadata_info(_rocksdb);
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    DB_WARNING("begin iterator");
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rocksdb::Slice key_slice(iter->key());
        rocksdb::Slice value_slice(iter->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    const rocksdb::Snapshot* snapshot = _rocksdb->get_snapshot();
    for (int i = 0; i < 7; i++) {
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+1)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+1).c_str());
    }
    rocksdb::WriteOptions           write_opt;
    rocksdb::TransactionOptions     txn_opt;
    auto txn = _rocksdb->begin_transaction(write_opt, txn_opt);
    for (int i = 5; i < 10; i++) {
        txn->Put(_rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i+2)));
        DB_WARNING("put key: %s, value: %s", std::to_string(i).c_str(), std::to_string(i+2).c_str());
    }


  

    DB_WARNING("begin compact1");
    s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    bthread_usleep(60 * 1000 * 1000LL);
    iter.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    DB_WARNING("begin iterator1");
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rocksdb::Slice key_slice(iter->key());
        rocksdb::Slice value_slice(iter->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }

    print_metadata_info(_rocksdb);


    txn->Prepare();
    txn->Commit();
    _rocksdb->release_snapshot(snapshot);
    DB_WARNING("begin compact2");
    s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    bthread_usleep(60 * 1000 * 1000LL);
    print_metadata_info(_rocksdb);
    iter.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    DB_WARNING("begin iterator2");
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rocksdb::Slice key_slice(iter->key());
        rocksdb::Slice value_slice(iter->value());
        DB_WARNING("scan key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
}

TEST_F(RocksdbTEST, ingest_test) {
    baikaldb::RocksWrapper*  _rocksdb = RocksWrapper::get_instance();
    if (!_rocksdb) {
        DB_FATAL("create rocksdb handler failed");
        return;
    }
    int ret = _rocksdb->init("./rocks_db");
    if (ret != 0) {
        DB_FATAL("rocksdb init failed: code:%d", ret);
        return;
    }
    ON_SCOPE_EXIT([_rocksdb]() {
        _rocksdb->close();
    });
    
    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(_rocksdb->get_options(_rocksdb->get_data_handle())));
    std::unique_ptr<SstFileWriter> writer2(new SstFileWriter(_rocksdb->get_options(_rocksdb->get_data_handle())));
    std::string path1 = "./test_ingest1.sst";
    std::string path2 = "./test_ingest2.sst";
    auto s = writer->open(path1);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s", path1.c_str(), s.ToString().c_str());
        return;
    }
    writer->put(rocksdb::Slice("1"), rocksdb::Slice("1"));
    writer->put(rocksdb::Slice("4"), rocksdb::Slice("4"));
    writer->put(rocksdb::Slice("7"), rocksdb::Slice("7"));
    writer->put(rocksdb::Slice("9"), rocksdb::Slice("9"));
    writer->finish();

    s = writer2->open(path2);
    if (!s.ok()) {
        DB_FATAL("open sst file path: %s failed, err: %s", path2.c_str(), s.ToString().c_str());
        return;
    }
    writer2->put(rocksdb::Slice("11"), rocksdb::Slice("11"));
    writer2->put(rocksdb::Slice("3"), rocksdb::Slice("3"));
    writer2->put(rocksdb::Slice("6"), rocksdb::Slice("6"));
    writer2->put(rocksdb::Slice("9"), rocksdb::Slice("9_2"));
    writer2->put(rocksdb::Slice("99"), rocksdb::Slice("99"));
    writer2->finish();

    const rocksdb::Snapshot* snapshot = nullptr;
    rocksdb::IngestExternalFileOptions ifo;
    ifo.move_files = true;
    ifo.write_global_seqno = false;
    ifo.allow_blocking_flush = false;
    rocksdb::ReadOptions read_options;
    if (!FLAGS_only_ingest_external) {
        s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {path1}, ifo);
        if (!s.ok()) {
            DB_WARNING("ingest fail: %s", path1.c_str());
        }
        
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
        int idx = 0;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            rocksdb::Slice key_slice(iter->key());
            rocksdb::Slice value_slice(iter->value());
            DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
            idx++;
        }
        // ASSERT_EQ(idx, 4);

        s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {path2}, ifo);
        if (!s.ok()) {
            DB_WARNING("ingest fail: %s", path1.c_str());
        }
        std::unique_ptr<rocksdb::Iterator> iter2(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
        idx = 0;
        for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
            rocksdb::Slice key_slice(iter2->key());
            rocksdb::Slice value_slice(iter2->value());
            DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
            idx++;
        }
        // ASSERT_EQ(idx, 8);
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("5"), rocksdb::Slice("5"));
        _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice("8"), rocksdb::Slice("8"));
        for (int i = 0; i < 10; i++) {
            _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
        }
        snapshot = _rocksdb->get_snapshot();
        for (int i = 90; i < 110; i++) {
            _rocksdb->put(rocksdb::WriteOptions(), _rocksdb->get_data_handle(), rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
        }
        std::unique_ptr<rocksdb::Iterator> iter3(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
        for (iter3->SeekToFirst(); iter3->Valid(); iter3->Next()) {
            rocksdb::Slice key_slice(iter3->key());
            rocksdb::Slice value_slice(iter3->value());
            DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
        }
        // MutTableKey start_key;
        // MutTableKey end_key;
        // start_key.append_u64(0);
        // end_key.append_u64(UINT64_MAX);
        rocksdb::Slice start("0");
        rocksdb::Slice end("999999");
        rocksdb::CompactRangeOptions compact_options;
        compact_options.exclusive_manual_compaction = false;
        s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
        if (!s.ok()) {
            DB_WARNING("compact_range error: code=%d, msg=%s", 
                    s.code(), s.ToString().c_str());
        }
    }

    bthread_usleep(60 * 1000 * 1000LL);


    if (FLAGS_ingest_file_name != "") {
        s = _rocksdb->ingest_external_file(_rocksdb->get_data_handle(), {FLAGS_ingest_file_name}, ifo);
        if (!s.ok()) {
            DB_WARNING("ingest fail: %s error: code=%d, msg=%s", FLAGS_ingest_file_name.c_str(), s.code(), s.ToString().c_str());
        } else {
            DB_WARNING("ingest succ: %s", FLAGS_ingest_file_name.c_str());
        }
    }

    std::unique_ptr<rocksdb::Iterator> iter4(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter4->SeekToFirst(); iter4->Valid(); iter4->Next()) {
        rocksdb::Slice key_slice(iter4->key());
        rocksdb::Slice value_slice(iter4->value());
        DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }
    if (snapshot) {
        _rocksdb->release_snapshot(snapshot);
    }
    rocksdb::Slice start("0");
    rocksdb::Slice end("999999");
    rocksdb::CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    s = _rocksdb->compact_range(compact_options, _rocksdb->get_data_handle(), &start, &end);
    if (!s.ok()) {
        DB_WARNING("compact_range error: code=%d, msg=%s", 
                s.code(), s.ToString().c_str());
    }
    bthread_usleep(60 * 1000 * 1000LL);
    DB_WARNING("after compact range");
    iter4.reset(_rocksdb->new_iterator(read_options, _rocksdb->get_data_handle()));
    for (iter4->SeekToFirst(); iter4->Valid(); iter4->Next()) {
        rocksdb::Slice key_slice(iter4->key());
        rocksdb::Slice value_slice(iter4->value());
        DB_WARNING("key: %s, value: %s", key_slice.ToString(false).c_str(), value_slice.ToString(false).c_str());
    }

} // TEST_F

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
