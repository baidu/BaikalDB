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
#include "common.h"
#ifdef BAIDU_INTERNAL
#include <base/iobuf.h>
#include <baidu/rpc/stream.h>
#else
#include <butil/iobuf.h>
#include <brpc/stream.h>
#endif

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

    virtual int on_received_messages(brpc::StreamId id, 
        butil::IOBuf *const messages[], 
        size_t size) {
        return 0;
    }

    virtual void on_closed(brpc::StreamId id) override {
        DB_NOTICE("id[%lu] closed.", id);
        _cond.decrease_signal();
    }

    virtual void on_idle_timeout(brpc::StreamId id) {
        DB_WARNING("idle timeout %lu", id);
        _status = pb::StreamState::SS_FAIL;
    }

    void wait() {
        _cond.wait();
    }

    int timed_wait(int64_t timeout) {
        return _cond.timed_wait(timeout);
    }

    pb::StreamState get_status() const {
        return _status;
    }
    
protected:
    void multi_iobuf_action(brpc::StreamId id, butil::IOBuf *const messages[], size_t all_size, size_t* index_ptr, 
        std::function<size_t(butil::IOBuf *const message, size_t size)> read_action, size_t* action_size_ptr) {
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
    pb::StreamState _status {pb::StreamState::SS_INIT};
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
            _status = pb::StreamState::SS_FAIL;
        }
        return ret;
    }

    virtual int on_received_messages(brpc::StreamId id, 
        butil::IOBuf *const messages[],
        size_t size) override;

    void set_only_data_sst(size_t to_process_size) {
        if (to_process_size > 0) {
            _to_process_size = to_process_size;
            _state = ReceiverState::RS_DATA_FILE;
        }
    }
    virtual void on_closed(brpc::StreamId id) override {
        DB_NOTICE("id[%lu] closed.", id);
        if (_to_process_size > 0) {
            _status = pb::StreamState::SS_FAIL;
        }
        _cond.decrease_signal();
    }

private:
    int8_t _file_num {0};
    int64_t _meta_file_size {0};
    int64_t _data_file_size {0};
    std::ofstream _meta_file_streaming {};
    std::ofstream _data_file_streaming {};
    size_t _to_process_size {sizeof(int64_t)};
    ReceiverState _state {ReceiverState::RS_LOG_INDEX};
};
}
