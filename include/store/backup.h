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
#include "concurrency.h"
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
#include "backup_stream.h"
#include "schema_factory.h"
#include "store_interact.hpp"
#include "table_key.h"
#include "mut_table_key.h"
#include "meta_writer.h"
#include "rocks_wrapper.h"
#include "sst_file_writer.h"

namespace baikaldb {
class Region;
typedef std::shared_ptr<Region> SmartRegion;
class Backup {
public:
    Backup() = default;
    void set_info(std::shared_ptr<Region> region_ptr, int64_t region_id) {
        _region_id = region_id;
        _region = region_ptr;
    }
    void process_download_sst(brpc::Controller* cntl, 
        std::vector<std::string>& request_vec, SstBackupType backup_type);
    int process_upload_sst(brpc::Controller* cntl, bool ingest_store_latest_sst);

    //streaming download
    void process_download_sst_streaming(brpc::Controller* controller, 
        const pb::BackupRequest* request,
        pb::BackupResponse* response);
    
    //streaming upload
    void process_upload_sst_streaming(brpc::Controller* cntl, bool ingest_store_latest_sst, 
        const pb::BackupRequest* request, pb::BackupResponse* response);
private:
    struct ProgressiveAttachmentWritePolicy {
        ProgressiveAttachmentWritePolicy(butil::intrusive_ptr<brpc::ProgressiveAttachment> attach) : attachment(attach) {}
        int Write(const void* data, size_t n) {
            return attachment->Write(data, n);
        }

        int WriteStreamingSize(int64_t) {
            return 0;
        }
        butil::intrusive_ptr<brpc::ProgressiveAttachment> attachment;
    };

    struct StreamingWritePolicy {
        StreamingWritePolicy(brpc::StreamId sid) : sid(sid) {}
        int Write(const void* data, size_t n) {
            butil::IOBuf msg;
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


    int backup_datainfo_streaming(brpc::StreamId sd, int64_t log_index, SmartRegion region_ptr);

    int upload_sst_info_streaming(
        brpc::StreamId sd, std::shared_ptr<StreamReceiver> receiver, 
        bool ingest_store_latest_sst, int64_t data_sst_to_process_size,
        const BackupInfo& backup_info, SmartRegion region_ptr, brpc::StreamId client_sd = 0);

    template<typename WritePolicy>
    int send_file(BackupInfo& backup_info, WritePolicy* pa, int64_t log_index) {
        const static int BUF_SIZE {2 * 1024 * 1024};
        std::unique_ptr<char[]> buf(new char[BUF_SIZE]);
        //写入流总数据长度
        int64_t all_streaming_size = sizeof(int64_t) * 2 + sizeof(int8_t) + backup_info.meta_info.size + (
            backup_info.data_info.size == 0 ? 0 : (sizeof(int64_t) + backup_info.data_info.size)
        );
        DB_NOTICE("region_%ld backup size meta[%ld] data[%ld]", 
            _region_id, backup_info.meta_info.size, backup_info.data_info.size);
        auto ret = pa->WriteStreamingSize(all_streaming_size);
        if (ret != 0) {
            char error_buf[21];
            DB_FATAL("region_%ld write streaming size error[%s] ret[%d]",
                _region_id, strerror_r(errno, error_buf, 21), ret);
            return -1;
        }
        //插入log_index
        ret = pa->Write(reinterpret_cast<char*>(&log_index), sizeof(int64_t));
        if (ret != 0) {
            char error_buf[21];
            DB_FATAL("region_%ld write index error[%s] ret[%d]",
                _region_id, strerror_r(errno, error_buf, 21), ret);
            return -1;
        }
        int8_t file_num = backup_info.data_info.size == 0 ? 1 : 2;
        //插入文件个数
        ret = pa->Write(reinterpret_cast<char*>(&file_num), sizeof(int8_t));
        if (ret != 0) {
            char error_buf[21];
            DB_FATAL("region_%ld write file number error[%s] ret[%d]", 
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
                DB_FATAL("region_%ld write file size error[%s] ret[%d]", 
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
                    DB_DEBUG("region_%ld read: %ld", _region_id, read_ret);
                    ret = pa->Write(buf.get(), read_ret);
                    if (ret != 0) {
                        char error_buf[21];
                        DB_FATAL("region_%ld write error[%s] ret[%d]",
                            _region_id, strerror_r(errno, error_buf, 21), ret);
                        return -1;
                    }
                }
                read_size += read_ret;
                DB_DEBUG("region_%ld_all: %ld", _region_id, read_size);
            } while (read_ret == BUF_SIZE);
            return 0;
        };

        if (append_file(backup_info.meta_info) == -1) {
            DB_WARNING("backup region[%ld] send meta file[%s] error.", 
                _region_id, backup_info.meta_info.path.c_str());
            return -1;
        }
        if (backup_info.data_info.size > 0) {
            if (append_file(backup_info.data_info) == -1) {
                DB_WARNING("backup region[%ld] send data file[%s] error.", 
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
