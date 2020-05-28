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

#include <stdint.h>
#include <fstream>
#include <vector>
#include <atomic>
#include <memory>
#include <boost/lexical_cast.hpp>
#ifdef BAIDU_INTERNAL
#include <base/iobuf.h>
#include <base/containers/bounded_queue.h>
#include <base/time.h>
#include <raft/raft.h>
#include <raft/util.h>
#include <raft/storage.h>
#include <baidu/rpc/stream.h>
#else
#include <butil/iobuf.h>
#include <butil/containers/bounded_queue.h>
#include <butil/time.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/storage.h>
#include <brpc/stream.h>
#endif
#include "common.h"
#include "schema_factory.h"
#include "store_interact.hpp"
#include "table_key.h"
#include "mut_table_key.h"
#include "meta_writer.h"
#include "rocks_wrapper.h"
#include "sst_file_writer.h"

namespace baikaldb {
//backup
struct FileInfo {
    std::string path {""};
    int64_t size {0};
};

struct BackupInfo {
    FileInfo meta_info;
    FileInfo data_info;
};

class CommonStreamReceiver : public brpc::StreamInputHandler {
public:
    enum class ReceiverState : int8_t {
        RS_LOG_INDEX,
        RS_FILE_NUM,
        RS_META_FILE_SIZE,
        RS_META_FILE,
        RS_DATA_FILE_SIZE,
        RS_DATA_FILE
    };

    enum class StreamState : int8_t {
        SS_INIT,
        SS_PROCESSING,
        SS_SUCCESS,
        SS_FAIL
    };

    virtual int on_received_messages(brpc::StreamId id, 
        base::IOBuf *const messages[], 
        size_t size) {
        return 0;
    }

    virtual void on_closed(brpc::StreamId id) override {
        DB_NOTICE("id[%lu] closed.", id);
        _cond.decrease_signal();
    }

    virtual void on_idle_timeout(brpc::StreamId id) {
        DB_WARNING("idle timeout %lu", id);
        _status = StreamState::SS_FAIL;
    }

    void wait() {
        _cond.wait();
    }

    StreamState get_status() const {
        return _status;
    }
    
protected:
    void multi_iobuf_action(brpc::StreamId id, base::IOBuf *const messages[], size_t all_size, size_t* index_ptr, 
        std::function<size_t(base::IOBuf *const message, size_t size)> read_action, size_t* action_size_ptr) {
        size_t& index = *index_ptr;
        size_t& action_size = *action_size_ptr;
        DB_DEBUG("stream_%lu to read size %zu", id, action_size);
        for (; index < all_size; ++index) {
            DB_DEBUG("stream_%lu all_size[%zu] index[%zu]", id, all_size, index);
            size_t complete_size = read_action(messages[index], action_size);
            action_size -= complete_size;
            if (action_size == 0) {
                DB_DEBUG("stream_%lu read size %zu", id, complete_size);
                return;
            }
            DB_DEBUG("stream_%lu read size %zu", id, complete_size);
        }
        DB_DEBUG("stream_%lu remain size %zu", id, action_size);
    }
protected:
    BthreadCond _cond {1};
    StreamState _status {StreamState::SS_INIT};
};

class StreamReceiver : public CommonStreamReceiver {
public:
    bool set_info(const BackupInfo& backup_info) {
        _meta_file_streaming.open(backup_info.meta_info.path, 
            std::ios::out | std::ios::binary | std::ios::trunc);
        _data_file_streaming.open(backup_info.data_info.path, 
            std::ios::out | std::ios::binary | std::ios::trunc);
        auto ret = _meta_file_streaming.is_open() && _data_file_streaming.is_open();
        if (!ret) {
            _status = StreamState::SS_FAIL;
        }
        return ret;
    }

    virtual int on_received_messages(brpc::StreamId id, 
        base::IOBuf *const messages[],
        size_t size) override;

private:
    int8_t _file_num {0};
    int64_t _meta_file_size {0};
    int64_t _data_file_size {0};
    std::ofstream _meta_file_streaming {};
    std::ofstream _data_file_streaming {};
    size_t _to_process_size {sizeof(int64_t)};
    ReceiverState _state {ReceiverState::RS_LOG_INDEX};
};

class Region;
class Backup {
public:
    Backup() = default;
    void set_info(std::shared_ptr<Region> region_ptr, int64_t region_id) {
        _region_id = region_id;
        _region = region_ptr;
    }
    void process_download_sst(brpc::Controller* cntl, 
        std::vector<std::string>& request_vec, SstBackupType backup_type);
    void process_upload_sst(brpc::Controller* cntl, bool ingest_store_latest_sst);

    //streaming download
    void process_download_sst_streaming(brpc::Controller* controller, 
        const pb::BackupRequest* request,
        pb::BackupResponse* response);
    
    //streawming upload
    void process_upload_sst_streaming(brpc::Controller* cntl, bool ingest_store_latest_sst, 
        const pb::BackupRequest* request, pb::BackupResponse* response);
private:
    struct ProgressiveAttachmentWritePolicy {
        ProgressiveAttachmentWritePolicy(brpc::ProgressiveAttachment* attach) : attachment(attach) {}
        int Write(const void* data, size_t n) {
            return attachment->Write(data, n);
        }

        int WriteStreamingSize(int64_t) {
            return 0;
        }
        brpc::ProgressiveAttachment* attachment;
    };

    struct StreamingWritePolicy {
        StreamingWritePolicy(brpc::StreamId sid) : sid(sid) {}
        int Write(const void* data, size_t n) {
            base::IOBuf msg;
            msg.append(data, n);
            int err = brpc::StreamWrite(sid, msg);
            while (err == EAGAIN) {
                bthread_usleep(10 * 1000);
                err = brpc::StreamWrite(sid, msg);
            }
            return err;
        }
        int WriteStreamingSize(int64_t streaming_size) {
            return Write(reinterpret_cast<char*>(&streaming_size), sizeof(int64_t));
        }
        brpc::StreamId sid;
    };
private:
    ///download sst
    void backup_datainfo(brpc::Controller* controller, int64_t log_index);
    int dump_sst_file(BackupInfo& backup_info);
    int backup_datainfo_to_file(const std::string& path, int64_t& file_size);
    int backup_metainfo_to_file(const std::string& path, int64_t& file_size);

    ///upload sst
    int upload_sst_info(brpc::Controller* controller, BackupInfo& backup_info);


    int backup_datainfo_streaming(baidu::rpc::StreamId sd, int64_t log_index);

    int upload_sst_info_streaming(
        baidu::rpc::StreamId sd, std::shared_ptr<StreamReceiver> receiver, 
        bool ingest_store_latest_sst, const BackupInfo& backup_info);

    template<typename WritePolicy>
    int send_file(BackupInfo& backup_info, WritePolicy* pa, int64_t log_index) {
        const static int BUF_SIZE {2 * 1024 * 1024};
        std::unique_ptr<char[]> buf(new char[BUF_SIZE]);
        //写入流总数据长度
        int64_t all_streaming_size = sizeof(int64_t) * 2 + sizeof(int8_t) + backup_info.meta_info.size + (
            backup_info.data_info.size == 0 ? 0 : (sizeof(int64_t) + backup_info.data_info.size)
        );
        DB_NOTICE("region_%lld backup size meta[%ld] data[%ld]", 
            _region_id, backup_info.meta_info.size, backup_info.data_info.size);
        auto ret = pa->WriteStreamingSize(all_streaming_size);
        if (ret != 0) {
            char error_buf[21];
            DB_FATAL("region_%lld write streaming size error[%s] ret[%d]",
                _region_id, strerror_r(errno, error_buf, 21), ret);
            return -1;
        }
        //插入log_index
        ret = pa->Write(reinterpret_cast<char*>(&log_index), sizeof(int64_t));
        if (ret != 0) {
            char error_buf[21];
            DB_FATAL("region_%lld write index error[%s] ret[%d]",
                _region_id, strerror_r(errno, error_buf, 21), ret);
            return -1;
        }
        int8_t file_num = backup_info.data_info.size == 0 ? 1 : 2;
        //插入文件个数
        ret = pa->Write(reinterpret_cast<char*>(&file_num), sizeof(int8_t));
        if (ret != 0) {
            char error_buf[21];
            DB_FATAL("region_%lld write file number error[%s] ret[%d]", 
                _region_id, strerror_r(errno, error_buf, 21), ret);
            return -1;
        }

        auto append_file = [&pa, this, &buf](FileInfo& file_info) -> int {
            butil::File f(butil::FilePath{file_info.path}, butil::File::FLAG_OPEN);
            if (!f.IsValid()) {
                DB_WARNING("file[%s] is not valid.", file_info.path.c_str());
                return -1;
            }

            //写入文件大小
            auto ret = pa->Write(reinterpret_cast<char*>(&file_info.size), sizeof(int64_t));
            if (ret != 0) {
                char error_buf[21];
                DB_FATAL("region_%lld write file size error[%s] ret[%d]", 
                    _region_id, strerror_r(errno, error_buf, 21), ret);
                return -1;
            }
            int64_t read_ret = 0;
            int64_t read_size = 0;
            do {
                read_ret = f.Read(read_size, buf.get(), BUF_SIZE);
                if (read_ret == -1) {
                    DB_WARNING("read file[%s] error.", file_info.path.c_str());
                    return -1;
                } 
                if (read_ret != 0) {
                    DB_DEBUG("region_%lld read: %lld", _region_id, read_ret);
                    ret = pa->Write(buf.get(), read_ret);
                    if (ret != 0) {
                        char error_buf[21];
                        DB_FATAL("region_%lld write error[%s] ret[%d]",
                            _region_id, strerror_r(errno, error_buf, 21), ret);
                        return -1;
                    }
                }
                read_size += read_ret;
                DB_DEBUG("region_%lld_all: %lld", _region_id, read_size);
            } while (read_ret == BUF_SIZE);
            return 0;
        };

        if (append_file(backup_info.meta_info) == -1) {
            DB_WARNING("backup region[%lld] send meta file[%s] error.", 
                _region_id, backup_info.meta_info.path.c_str());
            return -1;
        }
        if (backup_info.data_info.size > 0) {
            if (append_file(backup_info.data_info) == -1) {
                DB_WARNING("backup region[%lld] send data file[%s] error.", 
                    _region_id, backup_info.data_info.path.c_str());
                return -1;
            }
        }
        return 0;
    }
private:
    int64_t _region_id;
    std::weak_ptr<Region> _region;
};
}
