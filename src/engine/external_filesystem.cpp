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
// #include <string>
#include "external_filesystem.h"

namespace baikaldb {

#ifdef BAIDU_INTERNAL
AfsFileSystem::~AfsFileSystem() {
    for (auto& info : _ugi_infos) {
        if (info.afs != nullptr) {
            info.afs->DisConnect();
        }
    }
}

std::shared_ptr<afs::AfsFileSystem> AfsFileSystem::init(const std::string& uri, const std::string& user, 
                                            const std::string& password, const std::string& conf_file) {
    TimeCost cost;
    std::shared_ptr<afs::AfsFileSystem> fs(new afs::AfsFileSystem(uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str()));
    if (fs == nullptr) {
        DB_FATAL("new afs filesystem failed, uri: %s, user: %s, password: %s, conf_file: %s", 
            uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str());
        return nullptr;
    }

    int afs_res = fs->Init(true, false);
    if (afs_res != ds::kOk) {
        DB_FATAL("Init afs filesystem failed, uri: %s, user: %s, password: %s, conf_file: %s, error: %s", 
            uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str(), ds::Rc2Str(afs_res));
        return nullptr;
    }

    afs_res = fs->Connect();
    if (afs_res != ds::kOk) {
        DB_FATAL("Connect afs filesystem failed, uri: %s, user: %s, password: %s, conf_file: %s, error: %s", 
            uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str(), ds::Rc2Str(afs_res));
        return nullptr;
    }

    DB_NOTICE("init afs filesystem succ, uri: %s, user: %s, password: %s, conf_file: %s, cost: %ld", 
        uri.c_str(), user.c_str(), password.c_str(), conf_file.c_str(), cost.get_time());  
    return fs;
}

int AfsFileSystem::init() {
    // 初始化如果失败直接跳过，后续使用afs时再次初始化，避免afs影响store重启
    for (AfsUgi& ugi_info : _ugi_infos) {
        // afs://yinglong.afs.baidu.com:9902中提取yinglong
        if (_uri_afs_statics.find(ugi_info.uri) == _uri_afs_statics.end()) {
            _uri_afs_statics[ugi_info.uri] = std::make_shared<AfsStatis>(ugi_info.uri.substr(6, ugi_info.uri.find_first_of(".") - 6));
        }
        auto fs = init(ugi_info.uri, ugi_info.user, ugi_info.password, "./conf/client.conf");
        if (fs == nullptr) {
            continue;
        }
        ugi_info.afs = fs;
    }

    return 0;
}

std::shared_ptr<afs::AfsFileSystem> AfsFileSystem::get_fs_by_full_name(const std::string& full_name, std::string* uri, std::string* absolute_path) {
    std::lock_guard<bthread::Mutex> l(_lock);
    for (const AfsUgi& ugi_info : _ugi_infos) {
        auto uri_pos  = full_name.find(ugi_info.uri);
        auto path_pos = full_name.find(ugi_info.root_path + "/");
        if (uri_pos != std::string::npos && path_pos != std::string::npos) {
            if (uri != nullptr) {
                *uri = ugi_info.uri;
            }
            if (absolute_path != nullptr) {
                *absolute_path = full_name.substr(uri_pos + ugi_info.uri.size());
            }
            return ugi_info.afs;
        }
    }

    return nullptr;
}

// 进程启动时不要求全部afs初始化成功，make_full_name时如果未初始化则再次初始化
std::string AfsFileSystem::make_full_name(const std::string& cluster, bool force, const std::string& user_define_path) {
    if (_ugi_infos.empty()) {
        return "";
    }
    int pos = 0;
    if (!cluster.empty()) {
        bool find = false;
        for (const auto& info : _ugi_infos) {
            if (info.cluster_name == cluster) {
                find = true;
                break;
            }
            pos++;
        }

        if (!find) {
            if (force) {
                DB_FATAL("cant find cluster: %s", cluster.c_str());
                return "";
            } else {
                pos = 0;
            }
        }
    } 

    std::string full_name;
    {
        std::lock_guard<bthread::Mutex> l(_lock);
        if (_ugi_infos[pos].afs != nullptr) {
            full_name = _ugi_infos[pos].uri + _ugi_infos[pos].root_path + "/" + user_define_path;
        } else {
            // 放在锁外初始化？OLAPTODO
            auto fs = init(_ugi_infos[pos].uri, _ugi_infos[pos].user, _ugi_infos[pos].password, "./conf/client.conf");
            if (fs == nullptr) {
                DB_FATAL("init afs: %s failed", _ugi_infos[pos].uri.c_str());
                return "";
            }
            _ugi_infos[pos].afs = fs;
            full_name = _ugi_infos[pos].uri + _ugi_infos[pos].root_path + "/" + user_define_path;
        }
    }
    DB_NOTICE("cluster: %s, force: %d, user_define_path: %s, full_name: %s", cluster.c_str(), force, user_define_path.c_str(), full_name.c_str());
    return full_name;
}

int AfsFileSystem::open_reader(const std::string& path, std::unique_ptr<ExtFileReader>* reader) {
    std::string uri;
    std::string absolute_path;
    reader->reset();
    auto fs = get_fs_by_full_name(path, &uri, &absolute_path);
    if (fs == nullptr) {
        DB_FATAL("get_fs_by_full_name failed, path: %s", path.c_str());
        return -1;
    }

    afs::ReaderOptions options;
    options.buffer_size = 0; // 关闭预读
    afs::Reader* r = fs->OpenReader(absolute_path.c_str(), options);
    if (r == nullptr) {
        DB_FATAL("open reader failed, path: %s, absolute_path: %s, error: %d", path.c_str(), absolute_path.c_str(), errno);
        return -1;
    }

    reader->reset(new AfsFileReader(uri, absolute_path, r, fs, _uri_afs_statics[uri]));
    return 0;
}

int AfsFileSystem::open_writer(const std::string& path, std::unique_ptr<ExtFileWriter>* writer) {
    std::string uri;
    std::string absolute_path;
    writer->reset();
    auto fs = get_fs_by_full_name(path, &uri, &absolute_path);
    if (fs == nullptr) {
        DB_FATAL("get_fs_by_full_name failed, path: %s", path.c_str());
        return -1;
    }

    afs::Writer* w = fs->OpenWriter(absolute_path.c_str(), afs::WriterOptions());
    if (w == nullptr) {
        DB_FATAL("open writer failed, path: %s, absolute_path: %s, error: %d", path.c_str(), absolute_path.c_str(), errno);
        return -1;
    }

    writer->reset(new AfsFileWriter(uri, absolute_path, w, fs, _uri_afs_statics[uri]));
    return 0;
}

int AfsFileSystem::delete_path(const std::string& path, bool recursive) {
    std::string uri;
    std::string absolute_path;
    auto fs = get_fs_by_full_name(path, &uri, &absolute_path);
    if (fs == nullptr) {
        DB_FATAL("get_fs_by_full_name failed, path: %s", path.c_str());
        return -1;
    }

    int ret = fs->Delete(absolute_path.c_str(), recursive);
    if (ret != ds::kOk) {
        DB_FATAL("delete failed, path: %s, error: %s", path.c_str(), ds::Rc2Str(ret));
        return -1;
    }

    return 0;
}

int AfsFileSystem::create(const std::string& path) {
    std::string uri;
    std::string absolute_path;
    auto fs = get_fs_by_full_name(path, &uri, &absolute_path);
    if (fs == nullptr) {
        DB_FATAL("get_fs_by_full_name failed, path: %s", path.c_str());
        return -1;
    }

    afs::CreateOptions create_opts;
    create_opts.block_size_type = afs::BlockSizeType::BST_64M;
    int ret = fs->Create(absolute_path.c_str(), create_opts);
    if (ret != ds::kOk) {
        DB_FATAL("create failed, path: %s, error: %s", path.c_str(), ds::Rc2Str(ret));
        return -1;
    }

    return 0;
}

int AfsFileSystem::path_exists(const std::string& path) {
    std::string uri;
    std::string absolute_path;
    auto fs = get_fs_by_full_name(path, &uri, &absolute_path);
    if (fs == nullptr) {
        DB_FATAL("get_fs_by_full_name failed, path: %s", path.c_str());
        return -1;
    }

    int ret = fs->Exist(absolute_path.c_str());
    if (ret == ds::kOk) {
        return 1;
    } else if (ret == ds::kNoEntry) {
        return 0;
    } else {
        DB_FATAL("exist failed, path: %s, error: %s", path.c_str(), ds::Rc2Str(ret));
        return -1;
    }
}

int AfsFileSystem::list(const std::string& cluster, const std::string& path, std::set<std::string>& files) {
    std::string uri;
    std::string absolute_path;
    std::string full_path = make_full_name(cluster, false, path);
    auto fs = get_fs_by_full_name(full_path, &uri, &absolute_path);
    if (fs == nullptr) {
        DB_FATAL("get_fs_by_full_name failed, path: %s", path.c_str());
        return -1;
    }

    std::vector<afs::DirEntry> dirents;
    int ret = fs->Readdir(absolute_path.c_str(), &dirents);
    if (ret < 0){
        DB_FATAL("afs readdir failed: path: %s, error: %s", absolute_path.c_str(), ds::Rc2Str(ret));
        return -1;
    }
    for (int i = 0; i < dirents.size(); i++) {
        files.insert(full_path + dirents[i].name);
    }
    return 0;
}
#endif
} // namespace baikaldb
