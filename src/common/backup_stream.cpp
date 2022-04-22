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

#include "backup_stream.h"
namespace baikaldb {
int StreamReceiver::on_received_messages(brpc::StreamId id, 
    butil::IOBuf *const messages[],
    size_t size) {
    size_t current_index = 0;
    switch (_state) {
    case ReceiverState::RS_LOG_INDEX: {
        DB_NOTICE("get rs_log_index id[%lu]", id);
        multi_iobuf_action(id, messages, size, &current_index, 
            [](butil::IOBuf *const message, size_t size) -> size_t {
                butil::IOBuf buf;
                return message->cutn(&buf, size);
            }, &_to_process_size);

        if (_to_process_size == 0) {
            _state = ReceiverState::RS_FILE_NUM;
            _to_process_size = sizeof(int8_t);
        } else {
            break;
        }
    }
    case ReceiverState::RS_FILE_NUM: {
        DB_NOTICE("get file num id[%lu]", id);
        multi_iobuf_action(id, messages, size, &current_index,
            [this](butil::IOBuf *const message, size_t size) {
                return message->cutn((char*)(&_file_num) + 
                    (sizeof(int8_t) - _to_process_size), size);
            }, &_to_process_size);

        if (_to_process_size == 0) {
            DB_NOTICE("id[%lu] file number %d", id, _file_num);
            _state = ReceiverState::RS_META_FILE_SIZE;
            _to_process_size = sizeof(int64_t);
        } else {
            break;
        }
    }
    case ReceiverState::RS_META_FILE_SIZE: {
        DB_NOTICE("get meta file size id[%lu]", id);
        multi_iobuf_action(id, messages, size, &current_index, 
            [this](butil::IOBuf *const message, size_t size) -> size_t {
                return message->cutn((char*)(&_meta_file_size) + 
                    (sizeof(int64_t) - _to_process_size), size);
            }, &_to_process_size);

        if (_to_process_size == 0) {
            DB_DEBUG("id[%lu] get meta file size %ld", id, _meta_file_size);
            _state = ReceiverState::RS_META_FILE;
            _to_process_size = _meta_file_size;
        } else {
            break;
        }
    }
    case ReceiverState::RS_META_FILE: {
        DB_NOTICE("get meta file id[%lu]", id);
        multi_iobuf_action(id, messages, size, &current_index, 
            [this](butil::IOBuf *const message, size_t size) -> size_t {
                butil::IOBuf buf;
                auto write_size = message->cutn(&buf, _to_process_size);
                _meta_file_streaming << buf;
                return write_size;
            }, &_to_process_size);

        if (_to_process_size == 0) {
            DB_NOTICE("id[%lu] get meta file size %ld", id, _meta_file_size);
            _state = ReceiverState::RS_DATA_FILE_SIZE;
            _to_process_size = sizeof(int64_t);
            if (_file_num == 1) {
                _meta_file_streaming.flush();
                _data_file_streaming.flush();
                _status = (!_meta_file_streaming.bad() && !_data_file_streaming.bad()) ?
                          pb::StreamState::SS_SUCCESS : pb::StreamState::SS_FAIL;
            }
        } else {
            break;
        }
    }
    case ReceiverState::RS_DATA_FILE_SIZE: {
        DB_NOTICE("get data file size id[%lu]", id);
        multi_iobuf_action(id, messages, size, &current_index, 
            [this](butil::IOBuf *const message, size_t size) -> size_t {
                return message->cutn((char*)(&_data_file_size) + 
                    (sizeof(int64_t) - _to_process_size), size);
            }, &_to_process_size);

        if (_to_process_size == 0) {
            _state = ReceiverState::RS_DATA_FILE;
            DB_NOTICE("id[%lu] get data file size %ld", id, _data_file_size);
            _to_process_size = _data_file_size;
        } else {
            break;
        }
    }
    case ReceiverState::RS_DATA_FILE: {
        DB_DEBUG("stream_%lu get data file, process size_%zu", id, _to_process_size);
        multi_iobuf_action(id, messages, size, &current_index, 
            [this](butil::IOBuf *const message, size_t size) -> size_t {
                butil::IOBuf buf;
                auto write_size = message->cutn(&buf, _to_process_size);
                _data_file_streaming << buf << std::flush;
                return write_size;
            }, &_to_process_size);

        if (_to_process_size == 0) {
            DB_NOTICE("id[%lu] get data_size[%ld] all_size[%ld]", 
                id, _data_file_size, _data_file_size + _meta_file_size + 25);
            _meta_file_streaming.flush();
            _data_file_streaming.flush();
            _status = (!_meta_file_streaming.bad() && !_data_file_streaming.bad()) ?
                      pb::StreamState::SS_SUCCESS : pb::StreamState::SS_FAIL;
        }
        break;
    }
    }
    return 0;
}

}  // baikaldb
