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

#include "backup.h"
#include "region.h"

namespace baikaldb {
DEFINE_int64(streaming_max_buf_size, 60 * 1024 * 1024LL, "streaming max buf size : 60M");
DEFINE_int64(streaming_idle_timeout_ms, 1800 * 1000LL, "streaming idle time : 1800s");

void Backup::process_download_sst(brpc::Controller* cntl, 
    std::vector<std::string>& request_vec, SstBackupType backup_type) {
    
    if (auto region_ptr = _region.lock()) {
        int64_t log_index = 0;
        if (request_vec.size() == 4) {
            auto client_index = request_vec[3];
            log_index = region_ptr->get_log_index();
            if (client_index == std::to_string(log_index)) {
                DB_NOTICE("backup region[%lld] not changed.", _region_id);
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_NO_CONTENT);
                return;
            }
        }
        if (backup_type == SstBackupType::DATA_BACKUP) {
            backup_datainfo(cntl, log_index);
            DB_NOTICE("backup datainfo region[%lld]", _region_id);
        }   
    } else {
        DB_NOTICE("backup region[%lld] is quit.", _region_id);
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
    }
}

void Backup::backup_datainfo(brpc::Controller* cntl, int64_t log_index) {
    BackupInfo backup_info;
    backup_info.data_info.path = 
        std::string{"region_datainfo_backup_"} + std::to_string(_region_id) + ".sst";
    backup_info.meta_info.path = 
        std::string{"region_metainfo_backup_"} + std::to_string(_region_id) + ".sst";

    ON_SCOPE_EXIT(([&backup_info]() {
        std::remove(backup_info.data_info.path.c_str());
        std::remove(backup_info.meta_info.path.c_str());
    }));
    
    if (dump_sst_file(backup_info) != 0) {
        DB_WARNING("dump sst file error region_%lld", _region_id)
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }

    ProgressiveAttachmentWritePolicy pa{cntl->CreateProgressiveAttachment()};
    if (send_file(backup_info, &pa, log_index) != 0) {
        DB_WARNING("send sst file error region_%lld", _region_id)
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
}

int Backup::dump_sst_file(BackupInfo& backup_info) {
    int err = backup_datainfo_to_file(backup_info.data_info.path, backup_info.data_info.size);
    if (err != 0) {
        if (err == -1) {
            // dump sst file error
            DB_WARNING("backup region[%lld] backup to file[%s] error.", 
                _region_id, backup_info.data_info.path.c_str());
            return -1;
        } else if (err == -2) {
            //无数据。
            DB_NOTICE("backup region[%lld] no datainfo.", _region_id);
        }
    }

    err = backup_metainfo_to_file(backup_info.meta_info.path, backup_info.meta_info.size);
    if (err != 0) {
        DB_WARNING("region[%lld] backup file[%s] error.", 
            _region_id, backup_info.meta_info.path.c_str());
        return -1;
    }
    return 0;
}

int Backup::backup_datainfo_to_file(const std::string& path, int64_t& file_size) {
    uint64_t row = 0;
    RocksWrapper* db = RocksWrapper::get_instance();
    rocksdb::Options options = db->get_options(db->get_data_handle()); 

    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(options));
    rocksdb::ExternalSstFileInfo sst_file_info;
    auto ret = writer->open(path);
    if (!ret.ok()) {
        DB_WARNING("open SstFileWrite error, path[%s] error[%s]", path.c_str(), ret.ToString().c_str());
        return -1;
    }

    MutTableKey upper_bound;
    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = false;
    read_options.total_order_seek = true;
    std::string prefix;
    MutTableKey key;

    key.append_i64(_region_id);
    prefix = key.data();
    key.append_u64(UINT64_MAX);
    rocksdb::Slice upper_bound_slice = key.data();
    read_options.iterate_upper_bound = &upper_bound_slice;

    std::unique_ptr<rocksdb::Iterator> iter(RocksWrapper::get_instance()->new_iterator(read_options, db->get_data_handle()));
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
        auto s = writer->put(iter->key(), iter->value());       
        if (!s.ok()) {
            DB_WARNING("put key error[%s]", s.ToString().c_str());
            return -1;
        } else {
            ++row;
        }
    }

    if (row == 0) {
        DB_NOTICE("region[%lld] no data in datainfo.", _region_id);
        return -2;
    }

    ret = writer->finish(&sst_file_info);
    if (!ret.ok()) {
        DB_WARNING("finish error, path[%s] error[%s]", path.c_str(), ret.ToString().c_str());
        return -1;
    }

    file_size = sst_file_info.file_size;
    return 0;
}

int Backup::backup_metainfo_to_file(const std::string& path, int64_t& file_size) {
    uint64_t row = 0;
    RocksWrapper* db = RocksWrapper::get_instance();
    rocksdb::Options options = db->get_options(db->get_meta_info_handle());
    rocksdb::ExternalSstFileInfo sst_file_info;

    std::unique_ptr<SstFileWriter> writer(new SstFileWriter(options));
    auto ret = writer->open(path);
    if (!ret.ok()) {
        DB_WARNING("open SstFileWrite error, path[%s] error[%s]", path.c_str(), ret.ToString().c_str());
        return -1;
    }

    rocksdb::ReadOptions read_options;
    read_options.prefix_same_as_start = false;
    read_options.total_order_seek = true;
    std::string prefix = MetaWriter::get_instance()->meta_info_prefix(_region_id);
    std::unique_ptr<rocksdb::Iterator> iter(RocksWrapper::get_instance()->new_iterator(read_options, db->get_meta_info_handle()));

    for (iter->Seek(prefix); iter->Valid()  && iter->key().starts_with(prefix); iter->Next()) {

        auto s = writer->put(iter->key(), iter->value());       
        if (!s.ok()) {
            DB_WARNING("put key error[%s]", s.ToString().c_str());
            return -1;
        } else {
            ++row;
        }
    }

    if (row == 0) {
        DB_NOTICE("region[%lld] no data in metainfo.", _region_id);
        return -2;
    }

    ret = writer->finish(&sst_file_info);
    if (!ret.ok()) {
        DB_WARNING("finish error, path[%s] error[%s]", path.c_str(), ret.ToString().c_str());
        return -1;
    }
    file_size = sst_file_info.file_size;
    return 0;
}

void Backup::process_upload_sst(brpc::Controller* cntl, bool ingest_store_latest_sst) {

    BackupInfo backup_info;
    backup_info.meta_info.path = std::to_string(_region_id) + ".upload.meta.sst";
    backup_info.data_info.path = std::to_string(_region_id) + ".upload.data.sst";

    ON_SCOPE_EXIT(([&backup_info]() {
        std::remove(backup_info.data_info.path.c_str());
        std::remove(backup_info.meta_info.path.c_str());
    }));

    if (upload_sst_info(cntl, backup_info) != 0) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }

    if (auto region_ptr = _region.lock()) {
        //设置禁写，新数据写入sst.
        region_ptr->set_disable_write();
        ON_SCOPE_EXIT(([this, region_ptr]() {
            region_ptr->reset_allow_write();
        }));
        int ret = region_ptr->_real_writing_cond.timed_wait(FLAGS_disable_write_wait_timeout_us * 10);
        if (ret != 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            DB_FATAL("_real_writing_cond wait timeout, region_id: %ld", _region_id);
            return;
        }

        ret = region_ptr->ingest_sst(backup_info.data_info.path, backup_info.meta_info.path);
        if (ret != 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            DB_NOTICE("upload region[%lld] ingest failed.", _region_id);
            return;
        }

        DB_NOTICE("backup region[%lld] ingest_store_latest_sst [%d]", _region_id, int(ingest_store_latest_sst));
        if (!ingest_store_latest_sst) {
            DB_NOTICE("region[%lld] not ingest lastest sst.", _region_id);
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
            return;
        }

        BackupInfo latest_backup_info;
        latest_backup_info.meta_info.path = std::to_string(_region_id) + ".latest.meta.sst";
        latest_backup_info.data_info.path = std::to_string(_region_id) + ".latest.data.sst";

        ON_SCOPE_EXIT(([&latest_backup_info]() {
            std::remove(latest_backup_info.data_info.path.c_str());
            std::remove(latest_backup_info.meta_info.path.c_str());
        }));

        if (dump_sst_file(latest_backup_info) != 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_PARTIAL_CONTENT);
            DB_NOTICE("upload region[%lld] ingest latest sst failed.", _region_id);
            return;
        }

        DB_NOTICE("region[%lld] ingest latest data.", _region_id)
        ret = region_ptr->ingest_sst(latest_backup_info.data_info.path, latest_backup_info.meta_info.path);
        if (ret == 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
            DB_NOTICE("upload region[%lld] ingest latest sst success.", _region_id);
        } else {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_PARTIAL_CONTENT);
            DB_NOTICE("upload region[%lld] ingest latest sst failed.", _region_id);
        }        
    } else {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        DB_WARNING("region_%ld is quit.", _region_id);
        return;
    }
}

int Backup::upload_sst_info(brpc::Controller* cntl, BackupInfo& backup_info) {
    DB_NOTICE("upload datainfo region_id[%lld]", _region_id);
    auto& request_attachment = cntl->request_attachment();
    int64_t log_index;
    if (request_attachment.cutn(&log_index, sizeof(int64_t)) != sizeof(int64_t)) {
            DB_WARNING("upload region_%lld sst not enough data for log index", _region_id);
            return -1;
    }
    int8_t file_num;
    if (request_attachment.cutn(&file_num, sizeof(int8_t)) != sizeof(int8_t)) {
            DB_WARNING("upload region_%lld sst not enough data.", _region_id);
            return -1;
    }

    auto save_sst_file = [this, &request_attachment](FileInfo& fi) -> int {
        if (request_attachment.cutn(&fi.size, sizeof(int64_t)) != sizeof(int64_t)) {
            DB_WARNING("upload region_%lld sst not enough data.", _region_id);
            return -1;
        }
        if (fi.size <= 0) {
            DB_WARNING("upload region_%lld sst wrong meta_data size [%lld].", 
                _region_id, fi.size);
            return -1;
        }

        butil::IOBuf out_io;
        if (request_attachment.cutn(&out_io, fi.size) != fi.size) {
            DB_WARNING("upload region_%lld sst not enough data.", _region_id);
            return -1;
        }
        std::ofstream os(fi.path, std::ios::out | std::ios::binary);
        os << out_io;
        return 0;
    };

    if (save_sst_file(backup_info.meta_info) != 0) {
        return -1;
    }
    if (file_num == 2 && save_sst_file(backup_info.data_info) != 0) {
        DB_WARNING("region_%lld save data sst file error.", _region_id);
        return -1;
    }
    return 0;
}

void Backup::process_download_sst_streaming(brpc::Controller* cntl, 
        const pb::BackupRequest* request,
        pb::BackupResponse* response) {
    
    if (auto region_ptr = _region.lock()) {
        auto log_index = region_ptr->get_log_index();
        if (request->log_index() == log_index) {
            response->set_errcode(pb::BACKUP_SAME_LOG_INDEX);
            DB_NOTICE("backup region[%lld] not changed.", _region_id);
            return;
        }
        response->set_log_index(log_index);
        //async
        std::shared_ptr<CommonStreamReceiver> receiver(new CommonStreamReceiver);
        brpc::StreamId sd;
        brpc::StreamOptions stream_options;
        stream_options.handler = receiver.get();
        stream_options.max_buf_size = FLAGS_streaming_max_buf_size;
        stream_options.idle_timeout_ms = FLAGS_streaming_idle_timeout_ms;
        if (brpc::StreamAccept(&sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            DB_WARNING("fail to accept stream.");
            return;
        }
        Bthread streaming_work {&BTHREAD_ATTR_NORMAL};
        streaming_work.run([this, sd, receiver, log_index]() {
            backup_datainfo_streaming(sd, log_index);
            receiver->wait();
        });
        response->set_errcode(pb::SUCCESS);
        DB_NOTICE("backup datainfo region[%lld]", _region_id);       
    } else {
        response->set_errcode(pb::BACKUP_ERROR);
        DB_NOTICE("backup datainfo region[%lld] error, region quit.", _region_id);
    }

}

int Backup::backup_datainfo_streaming(brpc::StreamId sd, int64_t log_index) {
    BackupInfo backup_info;
    backup_info.data_info.path = 
        std::string{"region_datainfo_backup_"} + std::to_string(_region_id) + ".sst";
    backup_info.meta_info.path = 
        std::string{"region_metainfo_backup_"} + std::to_string(_region_id) + ".sst";

    ON_SCOPE_EXIT(([&backup_info]() {
        std::remove(backup_info.data_info.path.c_str());
        std::remove(backup_info.meta_info.path.c_str());
    }));
    
    if (dump_sst_file(backup_info) != 0) {
        DB_WARNING("dump sst file error region_%lld", _region_id)
        return -1;
    }

    StreamingWritePolicy sw{sd};
    if (send_file(backup_info, &sw, log_index) != 0) {
        DB_WARNING("send sst file error region_%lld", _region_id)
        return -1;
    }
    return 0;
}
void Backup::process_upload_sst_streaming(brpc::Controller* cntl, bool ingest_store_latest_sst, 
    const pb::BackupRequest* request, pb::BackupResponse* response) {

    BackupInfo backup_info;
    backup_info.meta_info.path = std::to_string(_region_id) + ".upload.meta.sst";
    backup_info.data_info.path = std::to_string(_region_id) + ".upload.data.sst";
    std::shared_ptr<StreamReceiver> receiver(new StreamReceiver);
    if (!receiver->set_info(backup_info)) {
        DB_WARNING("region_%ld set backup info error.", _region_id);
        return;
    }
    brpc::StreamId sd;
    brpc::StreamOptions stream_options;
    stream_options.handler = receiver.get();
    stream_options.max_buf_size = FLAGS_streaming_max_buf_size;
    stream_options.idle_timeout_ms = FLAGS_streaming_idle_timeout_ms;
    if (brpc::StreamAccept(&sd, *cntl, &stream_options) != 0) {
        cntl->SetFailed("Fail to accept stream");
        DB_WARNING("fail to accept stream.");
        return;
    }

    //async
    Bthread streaming_work{&BTHREAD_ATTR_NORMAL};
    streaming_work.run(
        [this, ingest_store_latest_sst, sd, receiver, backup_info]() {
            upload_sst_info_streaming(sd, receiver, ingest_store_latest_sst, backup_info);
        }
    );
}
int Backup::upload_sst_info_streaming(
    brpc::StreamId sd, std::shared_ptr<StreamReceiver> receiver, 
    bool ingest_store_latest_sst, const BackupInfo& backup_info) {

    DB_NOTICE("upload datainfo region_id[%lld]", _region_id);

    ON_SCOPE_EXIT(([&backup_info]() {
        std::remove(backup_info.data_info.path.c_str());
        std::remove(backup_info.meta_info.path.c_str());
    }));

    while (receiver->get_status() == StreamReceiver::StreamState::SS_INIT) {
        bthread_usleep(100 * 1000);
        DB_WARNING("waiting receiver status chanege region_%lu.", _region_id);
    }
    auto streaming_status = receiver->get_status();
    if (streaming_status == StreamReceiver::StreamState::SS_FAIL) {
        DB_WARNING("streaming error.");
        return -1;
    }
    brpc::StreamClose(sd);
    receiver->wait();
    //设置禁写，新数据写入sst.
    
    if (auto region_ptr = _region.lock()) {
        region_ptr->set_disable_write();
        ON_SCOPE_EXIT(([this, region_ptr]() {
            region_ptr->reset_allow_write();
        }));
        int ret = region_ptr->_real_writing_cond.timed_wait(FLAGS_disable_write_wait_timeout_us * 10);
        if (ret != 0) {
            DB_FATAL("upload real_writing_cond wait timeout, region_id: %ld", _region_id);
            return -1;
        }

        ret = region_ptr->ingest_sst(backup_info.data_info.path, backup_info.meta_info.path);
        if (ret != 0) {
            DB_NOTICE("upload region[%lld] ingest failed.", _region_id);
            return -1;
        }

        DB_NOTICE("backup region[%lld] ingest_store_latest_sst [%d]", _region_id, int(ingest_store_latest_sst));
        if (!ingest_store_latest_sst) {
            DB_NOTICE("region[%lld] not ingest lastest sst.", _region_id);
            return 0;
        }

        BackupInfo latest_backup_info;
        latest_backup_info.meta_info.path = std::to_string(_region_id) + ".latest.meta.sst";
        latest_backup_info.data_info.path = std::to_string(_region_id) + ".latest.data.sst";

        ON_SCOPE_EXIT(([&latest_backup_info]() {
            std::remove(latest_backup_info.data_info.path.c_str());
            std::remove(latest_backup_info.meta_info.path.c_str());
        }));

        if (dump_sst_file(latest_backup_info) != 0) {
            DB_NOTICE("upload region[%lld] ingest latest sst failed.", _region_id);
            return -1;
        }

        DB_NOTICE("region[%lld] ingest latest data.", _region_id)
        ret = region_ptr->ingest_sst(latest_backup_info.data_info.path, latest_backup_info.meta_info.path);
        if (ret == 0) {
            DB_NOTICE("upload region[%lld] ingest latest sst success.", _region_id);
        } else {
            DB_NOTICE("upload region[%lld] ingest latest sst failed.", _region_id);
        }
    } else {
        DB_FATAL("region_id: %ld is quit.", _region_id);
        return -1;
    }
    return 0;
}
}
