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

#include "my_raft_log_storage.h"
#include <boost/lexical_cast.hpp>
#include "raft_log_compaction_filter.h"
#include "can_add_peer_setter.h"
#include "concurrency.h"

namespace baikaldb {

static int parse_my_raft_log_uri(const std::string& uri, std::string& id){
    size_t pos = uri.find("id=");
    if (pos == 0 || pos == std::string::npos) {
        return -1;
    }
    id = uri.substr(pos + 3);
    return 0;
}

//const static size_t LOG_HEAD_SIZE = sizeof(int64_t) + sizeof(int);

MyRaftLogStorage::MyRaftLogStorage(
        int64_t region_id,
        RocksWrapper* db, 
        rocksdb::ColumnFamilyHandle* handle) :
            _first_log_index(0),
            _last_log_index(0),
            _region_id(region_id),
            _db(db),
            _handle(handle) {
    bthread_mutex_init(&_mutex, NULL);        
}

MyRaftLogStorage::~MyRaftLogStorage() {
    bthread_mutex_destroy(&_mutex);
}

braft::LogStorage* MyRaftLogStorage::new_instance(const std::string& uri) const {
    RocksWrapper* rocksdb = RocksWrapper::get_instance();
    if (rocksdb == NULL) {
        DB_FATAL("rocksdb is not set");
        return NULL;
    }
    std::string string_region_id;
    int ret = parse_my_raft_log_uri(uri, string_region_id);
    if (ret != 0) {
        DB_FATAL("parse uri fail, uri:%s", uri.c_str());
        return NULL;
    }
    int64_t region_id = boost::lexical_cast<int64_t>(string_region_id);
    rocksdb::ColumnFamilyHandle* handle = rocksdb->get_raft_log_handle();
    if (handle == NULL) {
        DB_FATAL("get raft log handle from rocksdb fail, region_id: %ld", 
                    uri.c_str(), region_id);
        return NULL;
    }
    braft::LogStorage* instance = new(std::nothrow) MyRaftLogStorage(region_id, rocksdb, handle);
    if (instance == NULL) {
        DB_FATAL("new log_storage instance fail, region_id: %ld",
                  region_id);
    }
    RaftLogCompactionFilter::get_instance()->update_first_index_map(region_id, 0);
    return instance;
}

int MyRaftLogStorage::init(braft::ConfigurationManager* configuration_manager) {
    TimeCost time_cost; 
    //read metaInfo from rocksdb
    char log_meta_key[LOG_META_KEY_SIZE];
    _encode_log_meta_key(log_meta_key, LOG_META_KEY_SIZE);
    if (_handle == NULL) {
        DB_FATAL("raft init state is not right, handle is null, region_id: %ld",
                _region_id);
        return -1;
    }
    int64_t first_log_index = 1;
    int64_t last_log_index = 0;
    std::string string_first_log_index;
    rocksdb::Status status = _db->get(rocksdb::ReadOptions(), 
                                      _handle, 
                                      rocksdb::Slice(log_meta_key, LOG_META_KEY_SIZE),
                                      &string_first_log_index);
    // region is new
    if (!status.ok() && status.IsNotFound()) {
        rocksdb::WriteOptions write_option;
        //write_option.sync = true;
        //write_option.disableWAL = true;
        rocksdb::Status put_res = _db->put(write_option, 
                                           _handle,
                                           rocksdb::Slice(log_meta_key, LOG_META_KEY_SIZE),
                                           rocksdb::Slice((char*)&first_log_index, sizeof(int64_t)));
        if (!put_res.ok()) {
            DB_WARNING("update first log index to rocksdb fail, region_id: %ld, err_mes:%s",
                            _region_id, put_res.ToString().c_str());
            return -1;
        }
    } else if (!status.ok()) { 
        DB_FATAL("read log meta info from rocksdb wrong, region_id: %ld, err_mes:%s",
                        _region_id, status.ToString().c_str());   
        return -1;
    } else {
        first_log_index = *(int64_t*)string_first_log_index.c_str();
    }
    // read log data
    char log_data_key[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(log_data_key, LOG_DATA_KEY_SIZE, first_log_index);
    rocksdb::ReadOptions opt;
    opt.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> iter(_db->new_iterator(opt, _handle));
    iter->Seek(rocksdb::Slice(log_data_key, LOG_DATA_KEY_SIZE));
    
    //construct term_map and last_log_index
    int64_t expected_index = first_log_index;
    while (iter->Valid()) {
        //decode key
        rocksdb::Slice key = iter->key();
        int64_t index = 0;
        int64_t region_id = 0;
        if (_decode_log_data_key(key, region_id, index) != 0) {
            DB_FATAL("value of region_id: %ld is corrupted", _region_id);
            return -1;
        }
        if (region_id != _region_id) {
            DB_FATAL("rock seek paramter may has problem, region_id: %ld:%ld", region_id, _region_id);
            return -1;
        }
        if (expected_index != index) {
            DB_FATAL("Found a hole in region_id: %ld, expected_index:%ld, real_index:%ld",
                    _region_id, expected_index, index);
            return -1;
        }
        //decode value
        rocksdb::Slice value = iter->value();
        if (value.size() < LOG_HEAD_SIZE) {
            DB_FATAL("value of log index:%ld of region_id: %ld is corrupted", index, _region_id);
            return -1;
        }
        LogHead head(value);
        if (_term_map.append(braft::LogId(index, head.term)) != 0) {
            DB_FATAL("fail to append term_map, region_id: %ld, index:%ld, term:%ld",
                            _region_id, index, head.term);
            return -1;
        }
        if (head.type == braft::ENTRY_TYPE_CONFIGURATION){
            value.remove_prefix(LOG_HEAD_SIZE);
            scoped_refptr<braft::LogEntry> entry = new braft::LogEntry();
            entry->id.index = index;
            entry->id.term = head.term;
            if (_parse_meta(entry, value) != 0) {
                DB_FATAL("Fail to parse meta at index:%ld, region_id: %ld",
                            index, _region_id);
                return -1;
            }
            std::string peers_str = "new: ";
            for (size_t i = 0; i < entry->peers->size(); ++i) {
                peers_str += (*(entry->peers))[i].to_string() + ",";
            }
            if (entry->old_peers) {
                peers_str += " old: ";
                for (size_t i = 0; i < entry->old_peers->size(); ++i) {
                    peers_str += (*(entry->old_peers))[i].to_string() + ",";
                }
            }
            DB_WARNING("begin add configuration, index:%ld, term:%ld, peers:%s, region_id: %ld", 
                        index, head.term, peers_str.c_str(), _region_id);
            braft::ConfigurationEntry conf_entry;
            conf_entry.id = entry->id;
            conf_entry.conf = *(entry->peers);
            if (entry->old_peers) {
                conf_entry.old_conf = *(entry->old_peers);
            }
            configuration_manager->add(conf_entry);
        }
        last_log_index = index;
        ++expected_index;
        iter->Next();
    }
    if (!iter->status().ok()) {
        DB_FATAL("Fail to iterate rocksdb, region_id: %ld", _region_id);
        return -1;
    }

    if (last_log_index == 0) {
        last_log_index = first_log_index - 1;
    }
    _first_log_index.store(first_log_index);
    _last_log_index.store(last_log_index);
    RaftLogCompactionFilter::get_instance()->update_first_index_map(_region_id, first_log_index);
    DB_WARNING("region_id: %ld, first_log_index:%ld, last_log_index:%ld, time_cost: %ld",
                    _region_id, _first_log_index.load(), _last_log_index.load(), time_cost.get_time());
    return 0;
}

braft::LogEntry* MyRaftLogStorage::get_entry(const int64_t index) {
    char buf[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(buf, LOG_DATA_KEY_SIZE, index);
    std::string value;
    rocksdb::Status status = _db->get(rocksdb::ReadOptions(), _handle, 
            rocksdb::Slice(buf, LOG_DATA_KEY_SIZE), &value);
    if (!status.ok()) {
        DB_WARNING("get index:%ld from rocksdb fail, region_id: %ld",
                index, _region_id);
        return NULL;
    }
    if (value.size() < LOG_HEAD_SIZE) {
        DB_FATAL("value of log index:%ld of region id:%ld is corrupted",
                index, _region_id);
        return NULL;
    }
    rocksdb::Slice value_slice(value);
    LogHead head(value_slice);
    value_slice.remove_prefix(LOG_HEAD_SIZE);
    braft::LogEntry* entry = new braft::LogEntry;
    entry->AddRef();
    entry->type = (braft::EntryType)head.type;
    entry->id = braft::LogId(index, head.term);
    switch (entry->type) {
        case braft::ENTRY_TYPE_DATA:
            entry->data.append(value_slice.data(), value_slice.size());
            break;
        case braft::ENTRY_TYPE_CONFIGURATION:
            if (_parse_meta(entry, value_slice) != 0) {
                entry->Release();
                entry = NULL;
            }
            //DB_WARNING("log storage enty is configure mata, region_id: %ld, log_index:%ld",
            //        _region_id, index);
            break;
        case braft::ENTRY_TYPE_NO_OP:
            if (value_slice.size() != 0) {
                DB_FATAL("Data of NO_OP must be empty,"
                               "log index:%ld of region id:%ld ",
                                index, _region_id);
                entry->Release();
                entry = NULL;  
            }
            DB_WARNING("log storage enty is no op, region_id: %ld, log_index:%ld",
                            _region_id, index);
            break;
        default:
            DB_FATAL("Unknown entry type, log index:%ld of region id:%ld",
                    index, _region_id);
            entry->Release();
            entry = NULL;
            break; 
    }
    return entry;
}

int64_t MyRaftLogStorage::get_term(const int64_t index) {
    int64_t term = 0;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (index < _first_log_index.load()
                || index > _last_log_index.load()) {
            DB_WARNING("index is greater than last_log_index or less than first_log_index, "
                            "index:%ld, first_log_index:%ld, last_log_index:%ld, region_id: %ld",
                            index, _first_log_index.load(), _last_log_index.load(), _region_id);
            return 0;
        }
        term = _term_map.get_term(index);
    }
    return term;
}

int MyRaftLogStorage::append_entry(const braft::LogEntry* entry) {
    std::vector<braft::LogEntry*> entries;
    entries.push_back(const_cast<braft::LogEntry*>(entry));
    return append_entries(entries, nullptr) == 1 ? 0 : -1;
}

int MyRaftLogStorage::append_entries(const std::vector<braft::LogEntry*>& entries
        , braft::IOMetric* metric) {
    TimeCost time_cost;
    if (entries.empty()) {
        return 0;
    }

    if (_last_log_index.load() + 1 != entries.front()->id.index) {
        DB_FATAL("There's gap betwenn appending entries and _last_log_index,"
                " last_log_index: %ld, entry_log_index: %ld, term:%ld region_id: %ld",
                _last_log_index.load(), entries.front()->id.index, 
                entries.front()->id.term, _region_id);
        return -1;
    }
    Concurrency::get_instance()->raft_write_concurrency.increase_wait();
    ON_SCOPE_EXIT([]() {
        Concurrency::get_instance()->raft_write_concurrency.decrease_broadcast();
    });

    //construct data
    std::vector<std::pair<rocksdb::SliceParts, rocksdb::SliceParts>> kv_vec;
    kv_vec.reserve(entries.size());
    butil::Arena arena;
    for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
        rocksdb::SliceParts key;
        rocksdb::SliceParts value;
        if (_build_key_value(&key, &value, *iter, arena) != 0) {
            DB_FATAL("Fail to build key-value, logid:%ld:%ld, region_id: %ld",
                            (*iter)->id.index, (*iter)->id.term, _region_id);
            return -1;
        }
        kv_vec.emplace_back(key, value);
    }
    
    // write date to rocksdb in batch
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    //options.sync = true;
    //options.disableWAL = true;
    
    for (auto iter = kv_vec.begin(); iter != kv_vec.end(); ++iter) {
        batch.Put(_handle, iter->first, iter->second);
    }
    auto status = _db->write(options, &batch);
    if (!status.ok()) {
        DB_FATAL("Fail to write db, region_id: %ld, err_mes:%s",
                        _region_id, status.ToString().c_str());
        return -1;
    }

    // update _term map and _last_log_index after success
    BAIDU_SCOPED_LOCK(_mutex);
    for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
        if (_term_map.append((*iter)->id) != 0) {
            DB_FATAL("Fail to update _term_map, region_id: %ld", _region_id);
            _term_map.truncate_suffix(_last_log_index.load());
            return -1;
        }
    }
    _last_log_index.fetch_add(entries.size());
    //DB_WARNING("append_entry, entries.size:%ld, time_cost:%ld, region_id: %ld",
    //            entries.size(), time_cost.get_time(), _region_id);
    return (int)entries.size();
}

int MyRaftLogStorage::truncate_prefix(const int64_t first_index_kept) {
    if (first_index_kept <= _first_log_index.load()) {
        return 0;
    }
    DB_WARNING("Truncating region_id: %ld to first index kept:%ld from first log index:%ld",
            _region_id, first_index_kept, _first_log_index.load());
    
    _first_log_index.store(first_index_kept);
    if (first_index_kept > _last_log_index.load()) {
        _last_log_index.store(first_index_kept - 1);
    }
    {
        std::unique_lock<bthread_mutex_t> lck(_mutex);
        _term_map.truncate_prefix(first_index_kept);
    }
    //替换为remove_range
    char start_key[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(start_key, LOG_DATA_KEY_SIZE, 0);
    char end_key[LOG_DATA_KEY_SIZE];
    _encode_log_data_key(end_key, LOG_DATA_KEY_SIZE, first_index_kept);
   
    auto status = _db->remove_range(rocksdb::WriteOptions(), 
                _handle, 
                rocksdb::Slice(start_key, LOG_DATA_KEY_SIZE), 
                rocksdb::Slice(end_key, LOG_DATA_KEY_SIZE));
    if (!status.ok()) {
        DB_WARNING("tuncate log entry fail, region_id: %ld, truncate to first index kept:%ld from first log index:%ld",
                 _region_id, first_index_kept, _first_log_index.load());
        return -1;
    } else {
        DB_WARNING("tuncate log entry success, region_id: %ld, truncate to first index kept:%ld from first log index:%ld",
                    _region_id, first_index_kept, _first_log_index.load());
    }
    //RaftLogCompactionFilter::get_instance()->update_first_index_map(_region_id, first_index_kept);
    
    CanAddPeerSetter::get_instance()->set_can_add_peer(_region_id);
    //write first_log_index to rocksdb, real delete when compaction
    char key_buf[LOG_META_KEY_SIZE]; 
    _encode_log_meta_key(key_buf, LOG_META_KEY_SIZE); 
    
    rocksdb::WriteOptions write_option;
    //write_option.sync = true;
    //write_option.disableWAL = true;
    status = _db->put(write_option, 
                      _handle,
                      rocksdb::Slice(key_buf, LOG_META_KEY_SIZE),
                      rocksdb::Slice((char*)&first_index_kept, sizeof(int64_t)));
    if (!status.ok()) {
        DB_WARNING("update first log index to rocksdb fail, region_id: %ld, err_mes:%s",
                        _region_id, status.ToString().c_str());
    }
    return 0;
}

int MyRaftLogStorage::truncate_suffix(const int64_t last_index_kept) {
    std::unique_lock<bthread_mutex_t> lck(_mutex);
    const int64_t last_log_index = _last_log_index.load();
    if (last_index_kept >= last_log_index) {
        return 0;
    }
    _term_map.truncate_suffix(last_index_kept);
    _last_log_index.store(last_index_kept);
    lck.unlock();
    DB_WARNING("Truncating region_id: %ld to last index kept:%ld from last log index:%ld",
            _region_id, last_index_kept, _last_log_index.load()); 
    // delete from rocksdb
    const size_t truncate_size = last_log_index - last_index_kept;
    std::unique_ptr<char[]> auto_buf(new char[truncate_size * LOG_DATA_KEY_SIZE]);
    char* buf = auto_buf.get();
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions options;
    //options.sync = true;
    //options.disableWAL = true;
    for (int64_t i = last_index_kept + 1; i <= last_log_index; ++i) {
        _encode_log_data_key(buf, LOG_DATA_KEY_SIZE, i);
        batch.Delete(_handle, rocksdb::Slice(buf, LOG_DATA_KEY_SIZE));
        buf += LOG_DATA_KEY_SIZE;
    }
    auto status = _db->write(options, &batch);
    if (!status.ok()) {
        DB_FATAL("Fail to write db, region_id: %ld, err_mes:%s",
                        _region_id, status.ToString().c_str());
    } 
    return 0;
}

int  MyRaftLogStorage::reset(const int64_t next_log_index) {
    DB_WARNING("Reseting region_id: %ld to next log index :%ld", 
                _region_id, next_log_index);
    truncate_prefix(next_log_index);
    truncate_suffix(next_log_index - 1);
    BAIDU_SCOPED_LOCK(_mutex);
    _term_map.reset();
    return 0;
}

int MyRaftLogStorage::_build_key_value(
        rocksdb::SliceParts* key, rocksdb::SliceParts* value,
        const braft::LogEntry* entry, butil::Arena& arena) {

    // construct key
    void* key_buf = arena.allocate(LOG_DATA_KEY_SIZE);
    if (key_buf == NULL) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return -1;
    }
    _encode_log_data_key(key_buf, LOG_DATA_KEY_SIZE, entry->id.index);
    
    void* key_slice_mem = arena.allocate(sizeof(rocksdb::Slice));
    if (key_slice_mem == NULL) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return -1;
    }
    new (key_slice_mem) rocksdb::Slice((char*)key_buf, LOG_DATA_KEY_SIZE);
    key->parts = (rocksdb::Slice*)key_slice_mem;
    key->num_parts = 1;

    // construct value
    LogHead head(entry->id.term, entry->type);
    void* head_buf = arena.allocate(LOG_HEAD_SIZE);
    if (head_buf == NULL) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return -1;
    }
    head.serialize_to(head_buf);
    value->parts = NULL;
    switch (entry->type) {
    case braft::ENTRY_TYPE_DATA:
        value->parts = _construct_slice_array(head_buf, entry->data, arena);
        value->num_parts = entry->data.backing_block_num() + 1;
        break;
    case braft::ENTRY_TYPE_CONFIGURATION:
        value->parts = _construct_slice_array(head_buf, entry->peers, 
                entry->old_peers, arena);
        value->num_parts = 2;
        //DB_WARNING("region_id: %ld, term:%ld, index:%ld",
        //            _region_id, entry->id.term, entry->id.index);
        break;
    case braft::ENTRY_TYPE_NO_OP:
        value->parts = _construct_slice_array(head_buf, nullptr, nullptr, arena);
        value->num_parts = 1;
        break;
    default:
        DB_FATAL("Unknown type:%d, region_id: %ld", entry->type, _region_id);
        return -1; 
    }
    if (value->parts == NULL) {
        DB_FATAL("Fail to construct value, region_id: %ld", _region_id);
        return -1;
    }    
    return 0;
}

rocksdb::Slice* MyRaftLogStorage::_construct_slice_array(
    void* head_buf, const butil::IOBuf& buf, butil::Arena& arena) {
    const size_t block_num = buf.backing_block_num();
    auto slices = (rocksdb::Slice*)arena.allocate(
        sizeof(rocksdb::Slice) * (block_num + 1));
    if (slices == NULL) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return NULL;
    }
    new (slices) rocksdb::Slice((const char*)head_buf, LOG_HEAD_SIZE);
    for (size_t i = 0; i < block_num; ++i) {
        auto block = buf.backing_block(i);
        new (slices + i + 1) rocksdb::Slice(block.data(), block.size());
    }
    return slices;
}

rocksdb::Slice* MyRaftLogStorage::_construct_slice_array(
    void* head_buf, const std::vector<braft::PeerId>* peers, 
            const std::vector<braft::PeerId>* old_peers, butil::Arena& arena) {
    auto slices = (rocksdb::Slice*)arena.allocate(
                    sizeof(rocksdb::Slice) * (!!peers + 1));
    if (slices == NULL) {
        DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
        return NULL;
    }
    new (slices) rocksdb::Slice((const char*)head_buf, LOG_HEAD_SIZE);
    if (peers) {
        braft::ConfigurationPBMeta meta;
        meta.mutable_peers()->Reserve(peers->size());
        for (auto iter = peers->begin(); iter != peers->end(); ++iter) {
            meta.add_peers(iter->to_string());
        }
        if (old_peers) {
            meta.mutable_old_peers()->Reserve(old_peers->size());
            for (size_t i = 0; i < old_peers->size(); ++i) {
                meta.add_old_peers((*old_peers)[i].to_string());
            }
        }
        //DB_WARNING("region_id: %ld, configuration:%s", _region_id, meta.ShortDebugString().c_str());
        const size_t byte_size = meta.ByteSize();
        void *meta_buf = arena.allocate(byte_size);
        if (meta_buf == NULL) {
            DB_FATAL("Fail to allocate mem, region_id: %ld", _region_id);
            return NULL;
        }
        if (!meta.SerializeToArray(meta_buf, byte_size)) {
            DB_FATAL("Fail to serialize mem, region_id: %ld", _region_id);
            return NULL;
        }
        new (slices + 1) rocksdb::Slice((const char*)meta_buf, byte_size);
    }
    return slices;
}

int MyRaftLogStorage::_parse_meta(braft::LogEntry* entry,
                                const rocksdb::Slice& value) {
    braft::ConfigurationPBMeta meta;
    if (!meta.ParseFromArray(value.data(), value.size())) {
        DB_FATAL("Fail to parse ConfigurationPBMeta, region_id: %ld", _region_id);
        return -1;
    }
    //DB_WARNING("raft configuration pb meta from entry:%s", meta.ShortDebugString().c_str());
    entry->peers = new std::vector<braft::PeerId>;
    for (int j = 0; j < meta.peers_size(); ++j) {
        entry->peers->push_back(braft::PeerId(meta.peers(j)));
    }
    if (meta.old_peers_size() > 0) {
        entry->old_peers = new std::vector<braft::PeerId>;
        for (int i = 0; i < meta.old_peers_size(); i++) {
            entry->old_peers->push_back(braft::PeerId(meta.old_peers(i)));
        }
    }
    return 0;
}

int MyRaftLogStorage::_encode_log_data_key(void* key_buf, size_t n, int64_t index) {
    if (n < LOG_DATA_KEY_SIZE) {
        DB_WARNING("key buf is not enough");
        return -1;
    }
    //regionId
    uint64_t region_id_tmp = KeyEncoder::to_endian_u64(
                                KeyEncoder::encode_i64(_region_id));
    memcpy(key_buf, (void*)&region_id_tmp, sizeof(int64_t));
    //0x02
    memcpy((void*)((char*)key_buf + sizeof(int64_t)), &LOG_DATA_IDENTIFY, 1);
    //index
    uint64_t index_tmp = KeyEncoder::to_endian_u64(
                        KeyEncoder::encode_i64(index));
    memcpy((void*)((char*)key_buf + sizeof(int64_t) + 1), 
            (void*)&index_tmp, sizeof(int64_t));
    return 0;
}

int MyRaftLogStorage::_encode_log_meta_key(void* key_buf, size_t n) {
    if (n < LOG_META_KEY_SIZE) {
        DB_WARNING("key buf is not enough, region_id: %ld", _region_id);
        return -1;
    }
    //region_id
     uint64_t region_id_tmp = KeyEncoder::to_endian_u64(
                                KeyEncoder::encode_i64(_region_id));
    memcpy(key_buf, (void*)&region_id_tmp, sizeof(int64_t));
    //0x01
    memcpy((void*)((char*)key_buf + sizeof(int64_t)), &LOG_META_IDENTIFY, 1); 
    return 0;
}

int MyRaftLogStorage::_decode_log_data_key(const rocksdb::Slice& data_key,
                                            int64_t& region_id,
                                            int64_t& index) {
    if (data_key.size() != LOG_DATA_KEY_SIZE) {
        DB_FATAL("key of log data is corrupted, region_id: %ld", _region_id);
        return -1;
    }
    uint64_t region_id_tmp = *(uint64_t*)data_key.data();
    region_id = KeyEncoder::decode_i64(
                        KeyEncoder::to_endian_u64(region_id_tmp));
    
    uint64_t index_tmp = *(uint64_t*)(data_key.data() + sizeof(int64_t) + 1);
    index =  KeyEncoder::decode_i64(
                    KeyEncoder::to_endian_u64(index_tmp)); 
    return 0;
}

}


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
