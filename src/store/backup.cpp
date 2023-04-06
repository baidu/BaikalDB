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
#include <boost/filesystem.hpp>
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
            log_index = region_ptr->get_data_index();
            if (client_index == std::to_string(log_index)) {
                DB_NOTICE("backup region[%ld] not changed.", _region_id);
                cntl->http_response().set_status_code(brpc::HTTP_STATUS_NO_CONTENT);
                return;
            }
        }
        if (backup_type == SstBackupType::DATA_BACKUP) {
            backup_datainfo(cntl, log_index);
            DB_NOTICE("backup datainfo region[%ld]", _region_id);
        }   
    } else {
        DB_NOTICE("backup region[%ld] is quit.", _region_id);
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
        butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false); 
        butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false); 
    }));
    
    if (dump_sst_file(backup_info) != 0) {
        DB_WARNING("dump sst file error region_%ld", _region_id);
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return;
    }

    ProgressiveAttachmentWritePolicy pa{cntl->CreateProgressiveAttachment()};
    if (send_file(backup_info, &pa, log_index) != 0) {
        DB_WARNING("send sst file error region_%ld", _region_id);
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
            DB_WARNING("backup region[%ld] backup to file[%s] error.", 
                _region_id, backup_info.data_info.path.c_str());
            return -1;
        } else if (err == -2) {
            //无数据。
            DB_NOTICE("backup region[%ld] no datainfo.", _region_id);
        }
    }

    err = backup_metainfo_to_file(backup_info.meta_info.path, backup_info.meta_info.size);
    if (err != 0) {
        DB_WARNING("region[%ld] backup file[%s] error.", 
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
    read_options.fill_cache = false;
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
        DB_NOTICE("region[%ld] no data in datainfo.", _region_id);
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
    read_options.fill_cache = false;
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
        DB_NOTICE("region[%ld] no data in metainfo.", _region_id);
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

int Backup::process_upload_sst(brpc::Controller* cntl, bool ingest_store_latest_sst) {

    BackupInfo backup_info;
    backup_info.meta_info.path = std::to_string(_region_id) + ".upload.meta.sst";
    backup_info.data_info.path = std::to_string(_region_id) + ".upload.data.sst";

    ON_SCOPE_EXIT(([&backup_info]() {
        butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false); 
        butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false); 
    }));

    if (upload_sst_info(cntl, backup_info) != 0) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        return -1;
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
            return -1;
        }

        BackupInfo latest_backup_info;
        latest_backup_info.meta_info.path = std::to_string(_region_id) + ".latest.meta.sst";
        latest_backup_info.data_info.path = std::to_string(_region_id) + ".latest.data.sst";

        ON_SCOPE_EXIT(([&latest_backup_info]() {
            butil::DeleteFile(butil::FilePath(latest_backup_info.data_info.path), false); 
            butil::DeleteFile(butil::FilePath(latest_backup_info.meta_info.path), false); 
        }));

        if (dump_sst_file(latest_backup_info) != 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_PARTIAL_CONTENT);
            DB_NOTICE("upload region[%ld] ingest latest sst failed.", _region_id);
            return -1;
        }

        ret = region_ptr->ingest_sst_backup(backup_info.data_info.path, backup_info.meta_info.path);
        if (ret != 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            DB_NOTICE("upload region[%ld] ingest failed.", _region_id);
            return -1;
        }

        DB_NOTICE("backup region[%ld] ingest_store_latest_sst [%d]", _region_id, int(ingest_store_latest_sst));
        if (!ingest_store_latest_sst) {
            DB_NOTICE("region[%ld] not ingest lastest sst.", _region_id);
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
            return -1;
        }

        DB_NOTICE("region[%ld] ingest latest data.", _region_id);
        ret = region_ptr->ingest_sst_backup(latest_backup_info.data_info.path, latest_backup_info.meta_info.path);
        if (ret == 0) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
            DB_NOTICE("upload region[%ld] ingest latest sst success.", _region_id);
            return 0;
        } else {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_PARTIAL_CONTENT);
            DB_NOTICE("upload region[%ld] ingest latest sst failed.", _region_id);
            return -1;
        }        
    } else {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        DB_WARNING("region_%ld is quit.", _region_id);
        return -1;
    }
}

int Backup::upload_sst_info(brpc::Controller* cntl, BackupInfo& backup_info) {
    DB_NOTICE("upload datainfo region_id[%ld]", _region_id);
    auto& request_attachment = cntl->request_attachment();
    int64_t log_index;
    if (request_attachment.cutn(&log_index, sizeof(int64_t)) != sizeof(int64_t)) {
            DB_WARNING("upload region_%ld sst not enough data for log index", _region_id);
            return -1;
    }
    int8_t file_num;
    if (request_attachment.cutn(&file_num, sizeof(int8_t)) != sizeof(int8_t)) {
            DB_WARNING("upload region_%ld sst not enough data.", _region_id);
            return -1;
    }

    auto save_sst_file = [this, &request_attachment](FileInfo& fi) -> int {
        if (request_attachment.cutn(&fi.size, sizeof(int64_t)) != sizeof(int64_t)) {
            DB_WARNING("upload region_%ld sst not enough data.", _region_id);
            return -1;
        }
        if (fi.size <= 0) {
            DB_WARNING("upload region_%ld sst wrong meta_data size [%ld].", 
                _region_id, fi.size);
            return -1;
        }

        butil::IOBuf out_io;
        if (request_attachment.cutn(&out_io, fi.size) != (uint64_t)fi.size) {
            DB_WARNING("upload region_%ld sst not enough data.", _region_id);
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
        DB_WARNING("region_%ld save data sst file error.", _region_id);
        return -1;
    }
    return 0;
}

void Backup::process_download_sst_streaming(brpc::Controller* cntl, 
        const pb::BackupRequest* request,
        pb::BackupResponse* response) {

    bool is_same_log_index = false;
    if (auto region_ptr = _region.lock()) {
        auto log_index = region_ptr->get_data_index();
        if (request->log_index() == log_index) {
            is_same_log_index = true;
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
        streaming_work.run([this, region_ptr, sd, receiver, log_index, is_same_log_index]() {
            if (!is_same_log_index) {
                backup_datainfo_streaming(sd, log_index, region_ptr);
            }
            receiver->wait();
        });
        response->set_errcode(pb::SUCCESS);
        DB_NOTICE("backup datainfo region[%ld]", _region_id);
    } else {
        response->set_errcode(pb::BACKUP_ERROR);
        DB_NOTICE("backup datainfo region[%ld] error, region quit.", _region_id);
    }

}

int Backup::backup_datainfo_streaming(brpc::StreamId sd, int64_t log_index, SmartRegion region_ptr) {
    BackupInfo backup_info;
    backup_info.data_info.path = 
        std::string{"region_datainfo_backup_"} + std::to_string(_region_id) + ".sst";
    backup_info.meta_info.path = 
        std::string{"region_metainfo_backup_"} + std::to_string(_region_id) + ".sst";

    region_ptr->_multi_thread_cond.increase();
    ON_SCOPE_EXIT([region_ptr]() {
        region_ptr->_multi_thread_cond.decrease_signal();
    });

    ON_SCOPE_EXIT(([&backup_info]() {
        butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false); 
        butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false); 
    }));
    
    if (dump_sst_file(backup_info) != 0) {
        DB_WARNING("dump sst file error region_%ld", _region_id);
        return -1;
    }

    StreamingWritePolicy sw{sd};
    if (send_file(backup_info, &sw, log_index) != 0) {
        DB_WARNING("send sst file error region_%ld", _region_id);
        return -1;
    }
    return 0;
}
void Backup::process_upload_sst_streaming(brpc::Controller* cntl, bool ingest_store_latest_sst, 
    const pb::BackupRequest* request, pb::BackupResponse* response) {
        
    // 限速
    int ret = Concurrency::get_instance()->upload_sst_streaming_concurrency.increase_timed_wait(1000 * 1000 * 5); 
    if (ret < 0) {
        response->set_errcode(pb::RETRY_LATER);
        DB_WARNING("upload sst fail, concurrency limit wait timeout, region_id: %lu", _region_id);
        return;
    }
    bool ingest_stall = RocksWrapper::get_instance()->is_ingest_stall();
    if (ingest_stall) {
        response->set_errcode(pb::RETRY_LATER);
        DB_WARNING("upload sst fail, level0 sst num limit, region_id: %lu", _region_id);
        return;
    }

    auto region_ptr = _region.lock();
    if (region_ptr == nullptr) {
        DB_WARNING("upload sst fail, get lock fail, region_id: %lu", _region_id);
        return;
    }
    int64_t data_sst_to_process_size = request->data_sst_to_process_size();
    if (data_sst_to_process_size == 0) {
        // sst备份恢复, 将状态置为doing, 恢复后做一次snapshot,否则add peer异常
        if (region_ptr->make_region_status_doing() < 0) {
            response->set_errcode(pb::RETRY_LATER);
            DB_WARNING("upload sst fail, make region status doing fail, region_id: %lu", _region_id);
            return;
        }
    }
    BackupInfo backup_info;
    // path加个随机数，防多个sst冲突
    int64_t rand = butil::gettimeofday_us() + butil::fast_rand();
    backup_info.meta_info.path = std::to_string(_region_id) + "." + std::to_string(rand) + ".upload.meta.sst";
    backup_info.data_info.path = std::to_string(_region_id) + "." + std::to_string(rand) + ".upload.data.sst";
    std::shared_ptr<StreamReceiver> receiver(new StreamReceiver);
    if (!receiver->set_info(backup_info)) {
        DB_WARNING("region_%ld set backup info error.", _region_id);
        if (data_sst_to_process_size == 0) {
            region_ptr->reset_region_status();
        }
        return;
    }
    brpc::StreamId id = request->streaming_id();
    int64_t row_size = request->row_size(); // 需要调整的num_table_lines
    if (data_sst_to_process_size > 0) {
        receiver->set_only_data_sst(data_sst_to_process_size);
    }

    ScopeGuard auto_decrease([&backup_info, data_sst_to_process_size, region_ptr]() {
        butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false); 
        butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false); 
        Concurrency::get_instance()->upload_sst_streaming_concurrency.decrease_signal();
        if (data_sst_to_process_size == 0) {
            region_ptr->reset_region_status();
        }
    });
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
    response->set_streaming_id(sd);
    DB_WARNING("region_id: %ld, data sst size: %ld, path: %s remote_side: %s, stream_id: %lu",
               _region_id, data_sst_to_process_size, backup_info.data_info.path.c_str(),
               butil::endpoint2str(cntl->remote_side()).c_str(), sd);
    //async
    if (region_ptr != nullptr) {
        auto_decrease.release();
        Bthread streaming_work{&BTHREAD_ATTR_NORMAL};
        streaming_work.run(
            [this, region_ptr, ingest_store_latest_sst, data_sst_to_process_size, sd, receiver, backup_info, row_size, id]() {
                int ret = upload_sst_info_streaming(sd, receiver, ingest_store_latest_sst,
                        data_sst_to_process_size, backup_info, region_ptr, id);
                if (ret == 0 && row_size > 0) {
                    region_ptr->add_num_table_lines(row_size);
                }
                region_ptr->update_streaming_result(sd, ret == 0 ? pb::StreamState::SS_SUCCESS :
                                                        pb::StreamState::SS_FAIL);
            }
        );
    }
}
int Backup::upload_sst_info_streaming(
    brpc::StreamId sd, std::shared_ptr<StreamReceiver> receiver, 
    bool ingest_store_latest_sst, int64_t data_sst_to_process_size, const BackupInfo& backup_info,
    SmartRegion region_ptr, brpc::StreamId client_sd) {
    ScopeGuard auto_decrease([]() {
        Concurrency::get_instance()->upload_sst_streaming_concurrency.decrease_signal();
    });
    TimeCost time_cost;
    region_ptr->_multi_thread_cond.increase();
    DB_NOTICE("upload datainfo region_id[%ld]", _region_id);

    bool upload_success = false;
    ON_SCOPE_EXIT(([&backup_info, data_sst_to_process_size, &upload_success, region_ptr]() {
                butil::DeleteFile(butil::FilePath(backup_info.data_info.path), false); 
                butil::DeleteFile(butil::FilePath(backup_info.meta_info.path), false); 
                region_ptr->_multi_thread_cond.decrease_signal();
                if (data_sst_to_process_size == 0) {
                    if (upload_success) {
                        region_ptr->do_snapshot();
                    }
                    region_ptr->reset_region_status();
                }
                }));

    while (receiver->get_status() == pb::StreamState::SS_INIT) {
        bthread_usleep(100 * 1000);
        DB_WARNING("waiting receiver status change region_%lu, stream_id: %lu", _region_id, sd);
    }
    auto streaming_status = receiver->get_status();
    brpc::StreamClose(sd);
    receiver->wait();
    if (streaming_status == pb::StreamState::SS_FAIL) {
        DB_WARNING("streaming error.");
        return -1;
    }
    //设置禁写，新数据写入sst.
    /*
    region_ptr->set_disable_write();
    ON_SCOPE_EXIT(([this, region_ptr]() {
        region_ptr->reset_allow_write();
    }));

    int ret = region_ptr->_real_writing_cond.timed_wait(FLAGS_disable_write_wait_timeout_us * 10);
    if (ret != 0) {
        DB_FATAL("upload real_writing_cond wait timeout, region_id: %ld", _region_id);
        return -1;
    }
    */
    if (data_sst_to_process_size > 0) {
        if (boost::filesystem::exists(boost::filesystem::path(backup_info.data_info.path))) {
            int64_t data_sst_size = boost::filesystem::file_size(boost::filesystem::path(backup_info.data_info.path));
            if (data_sst_size != data_sst_to_process_size) {
                DB_FATAL("region_id: %ld, local sst data diff with remote, %ld vs %ld", 
                    _region_id, data_sst_size, data_sst_to_process_size);
                return -1;
            }
        } else {
            DB_FATAL("region_id: %ld, has no data sst, path: %s", _region_id, backup_info.data_info.path.c_str());
            return -1;
        }
    }

    BackupInfo latest_backup_info;
    latest_backup_info.meta_info.path = std::to_string(_region_id) + ".latest.meta.sst";
    latest_backup_info.data_info.path = std::to_string(_region_id) + ".latest.data.sst";

    ON_SCOPE_EXIT(([&latest_backup_info, ingest_store_latest_sst]() { 
        if (ingest_store_latest_sst) {
            butil::DeleteFile(butil::FilePath(latest_backup_info.data_info.path), false); 
            butil::DeleteFile(butil::FilePath(latest_backup_info.meta_info.path), false); 
        }
    }));

    // ingest_store_latest_sst流程，先dump sst，在ingest发来的sst，再把dump的sst ingest
    if (ingest_store_latest_sst && dump_sst_file(latest_backup_info) != 0) {
        DB_NOTICE("upload region[%ld] ingest latest sst failed.", _region_id);
        return -1;
    }

    int ret = region_ptr->ingest_sst_backup(backup_info.data_info.path, backup_info.meta_info.path);
    if (ret != 0) {
        DB_NOTICE("upload region[%ld] ingest failed.", _region_id);
        return -1;
    }

    upload_success = true;
    DB_NOTICE("backup region[%ld] ingest_store_latest_sst [%d] data sst size [%ld], time_cost [%ld]", 
        _region_id, int(ingest_store_latest_sst), data_sst_to_process_size, time_cost.get_time());
    if (!ingest_store_latest_sst) {
        DB_NOTICE("region[%ld] not ingest lastest sst.", _region_id);
        return 0;
    }

    DB_NOTICE("region[%ld] ingest latest data.", _region_id);
    ret = region_ptr->ingest_sst_backup(latest_backup_info.data_info.path, latest_backup_info.meta_info.path);
    if (ret == 0) {
        DB_NOTICE("upload region[%ld] ingest latest sst success.", _region_id);
    } else {
        DB_NOTICE("upload region[%ld] ingest latest sst failed.", _region_id);
    }
    return 0;
}
}
