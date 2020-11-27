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

#include "my_raft_meta_storage.h"
#include <sstream>
#include <boost/lexical_cast.hpp>
#include "mut_table_key.h"

namespace baikaldb {
//DEFINE_string(old_stable_path, "/home/work/shared/data/raft_data/stable", "old stable path");

static int parse_my_raft_meta_uri(const std::string& uri, std::string& id){
    size_t pos = uri.find("id=");
    if (pos == 0 || pos == std::string::npos) {
        return -1;
    }
    id = uri.substr(pos + 3);
    return 0;
}

MyRaftMetaStorage::MyRaftMetaStorage(
        int64_t region_id,
        RocksWrapper* db, 
        rocksdb::ColumnFamilyHandle* handle) :
            _region_id(region_id),
            _db(db),
            _handle(handle) {
}

RaftMetaStorage* MyRaftMetaStorage::new_instance(const std::string& uri) const {
    RocksWrapper* rocksdb = RocksWrapper::get_instance();
    if (rocksdb == NULL) {
        DB_FATAL("rocksdb is not set");
        return NULL;
    }
    std::string string_region_id;
    int ret = parse_my_raft_meta_uri(uri, string_region_id);
    if (ret != 0) {
        DB_FATAL("parse uri fail, uri:%s", uri.c_str());
        return NULL;
    }
    int64_t region_id = boost::lexical_cast<int64_t>(string_region_id);
    rocksdb::ColumnFamilyHandle* handle = rocksdb->get_raft_log_handle();
    if (handle == NULL) {
        DB_FATAL("get raft log handle from rocksdb fail,uri:%s, region_id: %ld", 
                    uri.c_str(), region_id);
        return NULL;
    }
    RaftMetaStorage* instance = new(std::nothrow) MyRaftMetaStorage(region_id, rocksdb, handle);
    if (instance == NULL) {
        DB_FATAL("new raft_meta_storage instance fail, region_id: %ld",
                  region_id);
    }
    DB_WARNING("new my_raft_meta_storage success, region_id: %ld", region_id);
    return instance;
}


int MyRaftMetaStorage::set_term(const int64_t term) {
    if (_is_inited) {
        _term = term;
        return save();
    } else {
        LOG(WARNING) << "MyRaftMetaStorage not init(), region_id: " << _region_id;
        return -1;
    }
}

int64_t MyRaftMetaStorage::get_term() {
    if (_is_inited) {
        return _term;
    } else {
        LOG(WARNING) << "MyRaftMetaStorage not init(), region_id: " << _region_id;
        return -1;
    }
}

int MyRaftMetaStorage::set_votedfor(const braft::PeerId& peer_id) {
    if (_is_inited) {
        _votedfor = peer_id;
        return save();
    } else {
        LOG(WARNING) << "MyRaftMetaStorage not init(), region_id: " << _region_id;
        return -1;
    }
}

int MyRaftMetaStorage::get_votedfor(braft::PeerId* peer_id) {
    if (_is_inited) {
        *peer_id = _votedfor;
        return 0;
    } else {
        LOG(WARNING) << "MyRaftMetaStorage not init(), region_id: " << _region_id;
        return -1;
    }
}

int MyRaftMetaStorage::set_term_and_votedfor(const int64_t term, const braft::PeerId& peer_id) {
    if (_is_inited) {
        _term = term;
        _votedfor = peer_id;
        return save();
    } else {
        LOG(WARNING) << "MyRaftMetaStorage not init(), region_id: " << _region_id;
        return -1;
    }
}

butil::Status MyRaftMetaStorage::init() {
    butil::Status status;
    if (_is_inited) {
        return status;
    }

    int ret = load();
    if (ret == 0) {
        _is_inited = true;
        return status;
    }
    status.set_error(EINVAL, "MyRaftMetaStorage load pb meta error, region_id: %ld", _region_id);
    return status;
}

butil::Status MyRaftMetaStorage::set_term_and_votedfor(const int64_t term, 
        const braft::PeerId& peer_id, const braft::VersionedGroupId& group) {
    butil::Status status;
    int ret = set_term_and_votedfor(term, peer_id);
    if (ret < 0) {
        status.set_error(EINVAL, "MyRaftMetaStorage is error, region_id: %ld", _region_id);
    }
    return status;
}

butil::Status MyRaftMetaStorage::get_term_and_votedfor(int64_t* term, braft::PeerId* peer_id, 
        const braft::VersionedGroupId& group) {
    butil::Status status;
    if (_is_inited) {
        *peer_id = _votedfor;
        *term = _term;
        return status;
    }
    status.set_error(EINVAL, "MyRaftMetaStorage is error, region_id: %ld", _region_id);
    return status;
}

int MyRaftMetaStorage::load() {
    braft::StablePBMeta meta;
    MutTableKey mut_key;
    mut_key.append_i64(_region_id);
    mut_key.append_u8(RAFT_META_IDENTIFY);

    rocksdb::Slice key(mut_key.data());
    std::string value_str;
    rocksdb::ReadOptions options;
    auto status = _db->get(options, _handle, key, &value_str);
    int ret = 0;
    if (status.ok()) {
        meta.ParseFromString(value_str);
        _term = meta.term();
        ret = _votedfor.parse(meta.votedfor());
        LOG(INFO) << "load meta success " << _term << " _votedfor" << _votedfor.to_string();
    } else {
        LOG(WARNING) << "Fail to load meta by rocksdb region_id: " << _region_id;
        //return old_load();
    }

    return ret;
}

int MyRaftMetaStorage::save() {
    butil::Timer timer;
    timer.start();

    braft::StablePBMeta meta;
    meta.set_term(_term);
    meta.set_votedfor(_votedfor.to_string());
    std::string value_str;
    meta.SerializeToString(&value_str);

    MutTableKey mut_key;
    mut_key.append_i64(_region_id);
    mut_key.append_u8(RAFT_META_IDENTIFY);

    rocksdb::Slice key(mut_key.data());
    rocksdb::Slice value(value_str);
    rocksdb::WriteOptions options;
    auto status = _db->put(options, _handle, key, value);
    if (!status.ok()) {
        PLOG(ERROR) << "Fail to save meta to region_id: " << _region_id
            << ", rocksdb error " << status.ToString();
        return -1;
    }

    timer.stop();
    LOG(INFO) << "save stable meta region_id: " << _region_id 
        << " term " << _term << " votedfor " << _votedfor.to_string() << " time: " << timer.u_elapsed();
    return 0;
}

}


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
