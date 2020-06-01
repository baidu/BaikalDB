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

#include <rocks_wrapper.h>
#include <my_raft_log_storage.h>
#include <raft_log_compaction_filter.h>
#include <proto/meta.interface.pb.h>

int main(int argc, char** argv) {
    const std::string rocks_path = "rocks_raft_log";
    baikaldb::RocksWrapper* rocksdb_instance = 
        baikaldb::RocksWrapper::get_instance();
    int ret = rocksdb_instance->init(rocks_path);
    if (ret != 0) {
        std::cout << "rocksdb init fail" << std::endl;
        return -1;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    static baikaldb::MyRaftLogStorage my_raft_log_storage;
    std::string uri = "raft_log?id=1";
    raft::LogStorage* raft_log = 
        my_raft_log_storage.new_instance(uri);
    raft::ConfigurationManager* configuration_manager = new raft::ConfigurationManager;
    ret = raft_log->init(configuration_manager);
    if (ret < 0) {
        std::cout << "raft log storage init fail" << std::endl;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    /*{
        rocksdb::Status status = rocksdb_instance->compact_range(rocksdb::CompactRangeOptions(), 
                                              rocksdb_instance->get_raft_log_handle(),
                                              NULL,
                                              NULL);
        if (!status.ok()) {
            std::cout << "raft log compact range fail" << std::endl;
            return -1;
        } else {
            std::cout << "raft log compact range success" << std::endl;
        }
    }*/
    {
        // configure peer 0
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->id = raft::LogId(1, 1); // index(1), term(1);
        std::vector<raft::PeerId> peers;
        raft::PeerId peer_id;
        peer_id.parse("10.101.85.30:8010");
        peers.push_back(peer_id); 
        peer_id.parse("10.101.85.30:8011");
        peers.push_back(peer_id);
        peer_id.parse("10.101.85.30:8012");
        peers.push_back(peer_id); 
        entry->peers = &peers;
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }
        std::cout << "index:1, term:" << raft_log->get_term(1) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(1);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
        for (size_t i = 0; i < read_entry->peers->size(); ++i) {
            std::cout << "log_entry peers id: " << i << " address: " << (*(read_entry->peers))[i].to_string()<< std::endl;
        }
    }
    /*{
        rocksdb::Status status = rocksdb_instance->compact_range(rocksdb::CompactRangeOptions(), 
                                              rocksdb_instance->get_raft_log_handle(),
                                              NULL,
                                              NULL);
        if (!status.ok()) {
            std::cout << "raft log compact range fail" << std::endl;
            return -1;
        } else {
            std::cout << "raft log compact range success" << std::endl;
        }
    }*/
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    {
        // configure peer 1
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->id = raft::LogId(2, 1); // index(2), term(1);
        std::vector<raft::PeerId> peers;
        raft::PeerId peer_id;
        peer_id.parse("20.101.85.30:8010");
        peers.push_back(peer_id); 
        peer_id.parse("20.101.85.30:8011");
        peers.push_back(peer_id);
        peer_id.parse("20.101.85.30:8012");
        peers.push_back(peer_id); 
        entry->peers = &peers;
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }   
        std::cout << "index:2, term:" << raft_log->get_term(2) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(2);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
        for (size_t i = 0; i < read_entry->peers->size(); ++i) {
            std::cout << "log_entry peers id: " << i << " address: " << (*(read_entry->peers))[i].to_string()<< std::endl;
        }
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    // construct NO OP
    {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_NO_OP;
        entry->id = raft::LogId(3, 1); // index(3), term(1);
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }
        std::cout << "index:3, term:" << raft_log->get_term(3) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(3);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    // add data
    {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(4, 1);
        //construct data
        baikaldb::pb::RaftControlResponse response;
        response.set_region_id(10);
        response.set_errcode(baikaldb::pb::SUCCESS);
        response.set_leader("10.0.0.1:8010");
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper_write(&data);
        if (!response.SerializeToZeroCopyStream(&wrapper_write)) {
            std::cout << "SerializeToZeroCopyStream fail" << std::endl; 
            delete entry;
            return -1;
        }
        entry->data = data;
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }
        std::cout << "index:4, term:" << raft_log->get_term(4) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(4);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
        response.Clear();
        butil::IOBufAsZeroCopyInputStream wrapper_read(read_entry->data);
        if (!response.ParseFromZeroCopyStream(&wrapper_read)) {
            std::cout << "rocksdb read entry fail" << std::endl;
        }
        std::cout << "response success: " << response.errcode() << std::endl;
        std::cout << "response leader: " << response.leader() << std::endl;
        std::cout << "response region_id: " << response.region_id() << std::endl;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    // construct NO OP
    {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_NO_OP;
        entry->id = raft::LogId(5, 2); // index(5), term(2);
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }
        std::cout << "index:5, term:" << raft_log->get_term(5) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(5);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
    }
    {
        // configure peer 1
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_CONFIGURATION;
        entry->id = raft::LogId(6, 2); // index(6), term(2);
        std::vector<raft::PeerId> peers;
        raft::PeerId peer_id;
        peer_id.parse("30.101.85.30:8010");
        peers.push_back(peer_id); 
        peer_id.parse("30.101.85.30:8011");
        peers.push_back(peer_id);
        peer_id.parse("30.101.85.30:8012");
        peers.push_back(peer_id); 
        entry->peers = &peers;
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }   
        std::cout << "index:6, term:" << raft_log->get_term(6) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(6);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
        for (size_t i = 0; i < read_entry->peers->size(); ++i) {
            std::cout << "log_entry peers id: " << i << " address: " << (*(read_entry->peers))[i].to_string()<< std::endl;
        }
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    // add data
    {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(7, 2);
        //construct data
        baikaldb::pb::RaftControlResponse response;
        response.set_errcode(baikaldb::pb::SUCCESS);
        response.set_region_id(10);
        response.set_leader("20.0.0.1:8010");
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper_write(&data);
        if (!response.SerializeToZeroCopyStream(&wrapper_write)) {
            std::cout << "SerializeToZeroCopyStream fail" << std::endl; 
            delete entry;
            return -1;
        }
        entry->data = data;
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }
        std::cout << "index:7, term:" << raft_log->get_term(7) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(7);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
        response.Clear();
        butil::IOBufAsZeroCopyInputStream wrapper_read(read_entry->data);
        if (!response.ParseFromZeroCopyStream(&wrapper_read)) {
            std::cout << "rocksdb read entry fail" << std::endl;
        }
        std::cout << "response success: " << response.errcode() << std::endl;
        std::cout << "response leader: " << response.leader() << std::endl;
        std::cout << "response region_id: " << response.region_id() << std::endl;
    }
    
    // add data
    {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(8, 2);
        //construct data
        baikaldb::pb::RaftControlResponse response;
        response.set_errcode(baikaldb::pb::SUCCESS);
        response.set_region_id(10);
        response.set_leader("30.0.0.1:8010");
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper_write(&data);
        if (!response.SerializeToZeroCopyStream(&wrapper_write)) {
            std::cout << "SerializeToZeroCopyStream fail" << std::endl; 
            delete entry;
            return -1;
        }
        entry->data = data;
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }
        std::cout << "index:8, term:" << raft_log->get_term(8) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(8);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
        response.Clear();
        butil::IOBufAsZeroCopyInputStream wrapper_read(read_entry->data);
        if (!response.ParseFromZeroCopyStream(&wrapper_read)) {
            std::cout << "rocksdb read entry fail" << std::endl;
        }
        std::cout << "response success: " << response.errcode() << std::endl;
        std::cout << "response leader: " << response.leader() << std::endl;
        std::cout << "response region_id: " << response.region_id() << std::endl; 
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    // add data
    {
        raft::LogEntry* entry = new raft::LogEntry();
        entry->type = raft::ENTRY_TYPE_DATA;
        entry->id = raft::LogId(9, 3);
        //construct data
        baikaldb::pb::RaftControlResponse response;
        response.set_errcode(baikaldb::pb::SUCCESS);
        response.set_region_id(10);
        response.set_leader("30.0.0.1:8010");
        butil::IOBuf data;
        butil::IOBufAsZeroCopyOutputStream wrapper_write(&data);
        if (!response.SerializeToZeroCopyStream(&wrapper_write)) {
            std::cout << "SerializeToZeroCopyStream fail" << std::endl; 
            delete entry;
            return -1;
        }
        entry->data = data;
        ret = raft_log->append_entry(entry);
        if (ret < 0) {
            std::cout << "rocksdb append entry fail" << std::endl;
        }
        std::cout << "index:8, term:" << raft_log->get_term(9) << std::endl;
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        raft::LogEntry* read_entry = raft_log->get_entry(9);
        std::cout << "log_entry type: " << read_entry->type << std::endl;
        std::cout << "log_entry index:" << read_entry->id.index << std::endl;
        std::cout << "log_entry term:" << read_entry->id.term << std::endl;
        response.Clear();
        butil::IOBufAsZeroCopyInputStream wrapper_read(read_entry->data);
        if (!response.ParseFromZeroCopyStream(&wrapper_read)) {
            std::cout << "rocksdb read entry fail" << std::endl;
        }
        std::cout << "response success: " << response.errcode() << std::endl;
        std::cout << "response leader: " << response.leader() << std::endl;
        std::cout << "response region_id: " << response.region_id() << std::endl; 
    }
    // add entries
    {
        std::vector<raft::LogEntry*> entries;
        for (int i = 0; i < 10; ++i) {
            raft::LogEntry* entry = new raft::LogEntry();
            entry->type = raft::ENTRY_TYPE_DATA;
            entry->id = raft::LogId(10+i, 4);
            baikaldb::pb::RaftControlResponse response;
            response.set_errcode(baikaldb::pb::SUCCESS);
            response.set_region_id(10);
            response.set_leader("30.0.0.1:8010");
            butil::IOBuf data;
            butil::IOBufAsZeroCopyOutputStream wrapper_write(&data);
            if (!response.SerializeToZeroCopyStream(&wrapper_write)) {
                std::cout << "SerializeToZeroCopyStream fail" << std::endl;
                delete entry;
                return -1;
            }
            entry->data = data;
            entries.push_back(entry);
        }
        ret = raft_log->append_entries(entries);
        if (ret < 0) {
            std::cout << "rocksdb append entries fail" << std::endl;
        }
        for (int i = 0; i < 10; ++i) {
            baikaldb::pb::RaftControlResponse response;
            std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
            std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
            raft::LogEntry* read_entry = raft_log->get_entry(10 +i );
            std::cout << "log_entry type: " << read_entry->type << std::endl;
            std::cout << "log_entry index:" << read_entry->id.index << std::endl;
            std::cout << "log_entry term:" << read_entry->id.term << std::endl;
            butil::IOBufAsZeroCopyInputStream wrapper_read(read_entry->data);
            if (!response.ParseFromZeroCopyStream(&wrapper_read)) {
                std::cout << "rocksdb read entry fail" << std::endl;
            }
            std::cout << "response success: " << response.errcode() << std::endl;
            std::cout << "response leader: " << response.leader() << std::endl;
            std::cout << "response region_id: " << response.region_id() << std::endl; 
        }
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    /*{
        rocksdb::Status status = rocksdb_instance->compact_range(rocksdb::CompactRangeOptions(), 
                                              rocksdb_instance->get_raft_log_handle(),
                                              NULL,
                                              NULL);
        if (!status.ok()) {
            std::cout << "raft log compact range fail" << std::endl;
            return -1;
        } else {
            std::cout << "raft log compact range success" << std::endl;
        }
    }*/
    {
        ret = raft_log->truncate_prefix(1);
        if (ret < 0) {
            std::cout << "truncate prefix fail" << std::endl;
            return -1;
        }
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        std::cout << "index:3, term:" << raft_log->get_term(3) << std::endl;
        std::cout << "index:4, term:" << raft_log->get_term(4) << std::endl;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    /*{
        rocksdb::Status status = rocksdb_instance->compact_range(rocksdb::CompactRangeOptions(), 
                                              rocksdb_instance->get_raft_log_handle(),
                                              NULL,
                                              NULL);
        if (!status.ok()) {
            std::cout << "raft log compact range fail" << std::endl;
            return -1;
        } else {
            std::cout << "raft log compact range success" << std::endl;
        }
    }*/
    {
        ret = raft_log->truncate_prefix(3);
        if (ret < 0) {
            std::cout << "truncate prefix fail" << std::endl;
            return -1;
        }
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        std::cout << "index:4, term:" << raft_log->get_term(4) << std::endl;
        std::cout << "index:7, term:" << raft_log->get_term(7) << std::endl;
    }
    {
        ret = raft_log->truncate_prefix(4);
        if (ret < 0) {
            std::cout << "truncate prefix fail" << std::endl;
            return -1;
        }
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        std::cout << "index:6, term:" << raft_log->get_term(6) << std::endl;
        std::cout << "index:9, term:" << raft_log->get_term(9) << std::endl;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    {
        ret = raft_log->truncate_prefix(6);
        if (ret < 0) {
            std::cout << "truncate prefix fail" << std::endl;
            return -1;
        }
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        std::cout << "index:6, term:" << raft_log->get_term(6) << std::endl;
        std::cout << "index:9, term:" << raft_log->get_term(9) << std::endl;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    /*{
        rocksdb::Status status = rocksdb_instance->compact_range(rocksdb::CompactRangeOptions(), 
                                              rocksdb_instance->get_raft_log_handle(),
                                              NULL,
                                              NULL);
        if (!status.ok()) {
            std::cout << "raft log compact range fail" << std::endl;
            return -1;
        } else {
            std::cout << "raft log compact range success" << std::endl;
        }
    }*/

    {
        ret = raft_log->truncate_suffix(8);
        if (ret < 0) {
            std::cout << "truncate prefix fail" << std::endl;
            return -1;
        }
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
        std::cout << "index:6, term:" << raft_log->get_term(6) << std::endl;
        std::cout << "index:8, term:" << raft_log->get_term(8) << std::endl;
    }

    {
        ret = raft_log->reset(8);
        if (ret < 0) {
            std::cout << "reset fail" << std::endl;
            return -1;
        }
        std::cout << "first log index: " << raft_log->first_log_index() << std::endl;
        std::cout << "last log index: " << raft_log->last_log_index() << std::endl;
    }
    baikaldb::RaftLogCompactionFilter::get_instance()->print_map();
    return 0;
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
