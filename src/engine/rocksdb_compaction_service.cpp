#include "rocksdb_compaction_service.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "rocks_wrapper.h"
#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#else
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#endif
#include "store.h"

namespace baikaldb {
DEFINE_int64(remote_compaction_threshold, 1024 * 1024 * 1024, "remote compaction threshold");
DEFINE_int32(remote_compaction_client_max_num, 3, "remote_compaction_client_max_num");

#ifdef REMOTE_COMPACTION
DECLARE_string(db_path);
#endif

void set_rocksdb_flags(pb::RocksdbGFLAGS* rocksdb_gflags) {
    rocksdb_gflags->set_rocks_use_partitioned_index_filters(FLAGS_rocks_use_partitioned_index_filters);
    rocksdb_gflags->set_rocks_use_ribbon_filter(FLAGS_rocks_use_ribbon_filter);
    rocksdb_gflags->set_olap_table_only(FLAGS_olap_table_only);
    rocksdb_gflags->set_olap_import_mode(FLAGS_olap_import_mode); 
    rocksdb_gflags->set_rocks_use_sst_partitioner_fixed_prefix(FLAGS_rocks_use_sst_partitioner_fixed_prefix);
    rocksdb_gflags->set_key_point_collector_interval(FLAGS_key_point_collector_interval);
    rocksdb_gflags->set_rocks_block_cache_size_mb(FLAGS_rocks_block_cache_size_mb);
    rocksdb_gflags->set_rocks_high_pri_pool_ratio(FLAGS_rocks_high_pri_pool_ratio);
    rocksdb_gflags->set_rocks_block_size(FLAGS_rocks_block_size);
    rocksdb_gflags->set_rocks_use_hyper_clock_cache(FLAGS_rocks_use_hyper_clock_cache);
}

MyCompactionService::OnRPCDone::~OnRPCDone() {
    if (_snapshot != nullptr) {
        RocksWrapper::get_instance()->release_snapshot(_snapshot);
    }
}

rocksdb::CompactionServiceScheduleResponse MyCompactionService::Schedule(const rocksdb::CompactionServiceJobInfo& info,
                                            const std::string& compaction_service_input) {

    if (!FLAGS_enable_remote_compaction) {
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }
    const std::string& remote_compaction_id_str = rocksdb::Env::Default()->GenerateUniqueId();  
#ifdef REMOTE_COMPACTION
    // store端口未启动时，先local compaction
    if (!Store::get_instance()->is_init()) {
        DB_WARNING("store port is not open, use local compaction");
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }

    rocksdb::CompactionServiceInput compaction_input;
    rocksdb::Status s = rocksdb::CompactionServiceInput::Read(compaction_service_input, &compaction_input);
    if (!s.ok()) {
         return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }
    // 统计file大小
    size_t total_file_size = 0;
    for (const auto& file : compaction_input.input_files) {
        butil::FilePath file_path(_db_path + "/" + file);
        butil::File::Info file_info;
        if (!butil::GetFileInfo(file_path, &file_info)) {
            DB_WARNING("fail to get file info: %s, use local compaction", file.c_str());
            return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
        }
        total_file_size += file_info.size;
    }
    if (total_file_size < FLAGS_remote_compaction_threshold) {
        DB_WARNING("input files size: %ld < threshold: %ld", total_file_size, FLAGS_remote_compaction_threshold);
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }
    if (info.is_l0_compaction == true) {
        DB_WARNING("remote_compaction: %s is L0 compaction, use local compaction", remote_compaction_id_str.c_str());
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }
    // 设置请求和回调
    pb::CompactionService_Stub stub(&_channel);
    pb::RemoteCompactionRequest request;
    
    butil::EndPoint addr;
    addr.ip = butil::my_ip();
    addr.port = FLAGS_store_port;
    const std::string& address = endpoint2str(addr).c_str();
    request.set_address(address);
    request.set_remote_compaction_id(remote_compaction_id_str);
    request.set_compaction_input_binary(compaction_service_input);
    request.set_cf_name(compaction_input.cf_name);
    set_rocksdb_flags(request.mutable_rocksdb_gflags());

    std::shared_ptr<OnRPCDone> closure = std::make_shared<OnRPCDone>(remote_compaction_id_str, 
                                                    total_file_size, 
                                                    RocksWrapper::get_instance()->get_snapshot());
    closure->cntl.Reset();
    closure->cntl.set_request_code(make_sign32(address));
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_doing_map.size() > FLAGS_remote_compaction_client_max_num) {
            DB_WARNING("remote_compaction: %s, doing remote compaction num: %ld > max num: %d, use local compaction",
                       remote_compaction_id_str.c_str(), _doing_map.size(), FLAGS_remote_compaction_client_max_num);
            return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
        }
        _time_cost_map[remote_compaction_id_str] = TimeCost();
        _doing_map[remote_compaction_id_str] = closure;
    }
    // 异步调用 RPC，并传入 OnRPCDone 回调
    stub.do_compaction(&closure->cntl, &request, &closure->_response, closure.get());
#endif
    return rocksdb::CompactionServiceScheduleResponse(remote_compaction_id_str, rocksdb::CompactionServiceJobStatus::kSuccess);
}

#ifdef REMOTE_COMPACTION
static int check_file_vaild(const pb::RemoteCompactionResponse& response,
                                        const rocksdb::CompactionServiceResult& compaction_result,
                                        const std::string& remote_compaction_id,
                                        const std::string& file_dir) {
    std::map<std::string, int64_t> file_size_map;
    for (int i = 0; i < response.output_file_info_size(); i++) {
        file_size_map[response.output_file_info(i).file_path()] = response.output_file_info(i).file_size();
    }
    for (int i = 0; i < compaction_result.output_files.size(); ++i) {
        std::string file_path_str = file_dir + "/" + compaction_result.output_files[i].file_name;
        butil::FilePath file_path(file_path_str);
        butil::File::Info info;
        if (!butil::GetFileInfo(file_path, &info)) {
            DB_FATAL("File does not exist: %s, remote_compaction_id: %s", 
                file_path_str.c_str(), remote_compaction_id.c_str());
            return -1;
        }
        if (info.size != file_size_map[compaction_result.output_files[i].file_name]) {
            DB_FATAL("File size not match: %s, remote_compaction_id: %s", 
                file_path_str.c_str(), remote_compaction_id.c_str());
            return -1;
        }
    }
    return 0;
}
#endif

rocksdb::CompactionServiceJobStatus MyCompactionService::Wait(const std::string& remote_compaction_id_str, std::string* result) {
    std::string current_dir = "";
#ifdef REMOTE_COMPACTION
    current_dir = FLAGS_db_path + "/" + FLAGS_secondary_db_path + "/" + remote_compaction_id_str;
    ON_SCOPE_EXIT(([this, remote_compaction_id_str]() {
        BAIDU_SCOPED_LOCK(_mutex);
        _time_cost_map.erase(remote_compaction_id_str);
        _doing_map.erase(remote_compaction_id_str);
    }));
    if (current_dir == "") {
        DB_FATAL("current_dir is empty");
        return rocksdb::CompactionServiceJobStatus::kUseLocal;
    }

    butil::FilePath file_path(current_dir);
    ScopeGuard auto_decrease([file_path, remote_compaction_id_str]() {
        bool success = butil::DeleteFile(file_path, true);
        if (!success) {
            DB_FATAL("Failed to delete file: %s, remote_compaction_id: %s", 
                file_path.BaseName().value().c_str(), remote_compaction_id_str.c_str());
        }
    });

    brpc::CallId call_id;
    pb::RemoteCompactionResponse response;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_doing_map.find(remote_compaction_id_str) != _doing_map.end()) {
            call_id = _doing_map[remote_compaction_id_str]->cntl.call_id();
        }
    }

    brpc::Join(call_id);
    size_t total_file_size = 0;
    int64_t finish_time = 0;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_doing_map.find(remote_compaction_id_str) != _doing_map.end()) {
            if (_doing_map[remote_compaction_id_str]->cntl.Failed()) {
                DB_FATAL("fail to send remote compaction compaction_id: %s, err: %s",
                    remote_compaction_id_str.c_str(),
                    _doing_map[remote_compaction_id_str]->cntl.ErrorText().c_str());
                return rocksdb::CompactionServiceJobStatus::kUseLocal;
            }
            response.Swap(&(_doing_map[remote_compaction_id_str]->_response));
            total_file_size = _doing_map[remote_compaction_id_str]->_total_file_size;
        }
        if (_time_cost_map.find(remote_compaction_id_str) != _time_cost_map.end()) {
            finish_time = _time_cost_map[remote_compaction_id_str].get_time();
        }
    }
    if (response.errcode() != pb::SUCCESS) {
        DB_FATAL("fail to remote compaction compaction_id: %s err: %s",
            remote_compaction_id_str.c_str(),
            pb::ErrCode_Name(response.errcode()).c_str());
        return rocksdb::CompactionServiceJobStatus::kUseLocal;
    }

    rocksdb::CompactionServiceResult compaction_result;
    rocksdb::Status s = rocksdb::CompactionServiceResult::Read(response.compaction_result_binary(), &compaction_result);
    if (!s.ok()) {
        DB_FATAL("remote_compaction_id: %s, compaction_result deserialize fail reason: %s", 
            remote_compaction_id_str.c_str(), s.ToString().c_str());
        return rocksdb::CompactionServiceJobStatus::kUseLocal;
    }
    if (0 != check_file_vaild(response, compaction_result,remote_compaction_id_str, current_dir)) {
        return rocksdb::CompactionServiceJobStatus::kUseLocal;
    }
    // 检查目录是否存在
    if (compaction_result.output_files.size() != 0 && !butil::DirectoryExists(file_path)) {
        DB_FATAL("File does not exist: %s, remote_compaction_id: %s", 
            file_path.BaseName().value().c_str(), remote_compaction_id_str.c_str());
        return rocksdb::CompactionServiceJobStatus::kUseLocal;
    }
    compaction_result.output_path = current_dir;
    compaction_result.stats.cpu_micros = finish_time;

    s = compaction_result.Write(result);
    if (!s.ok()) {
        DB_FATAL("remote_compaction_id: %s, compaction_result serialize fail reason: %s", 
            remote_compaction_id_str.c_str(), s.ToString().c_str());
        return rocksdb::CompactionServiceJobStatus::kUseLocal;
    }

    auto_decrease.release();
    DB_NOTICE("remote_compaction_id=%s, total_cost=%ld, total_size=%ld, do_compaction success", 
        remote_compaction_id_str.c_str(),
        finish_time,
        total_file_size
    );
#endif
    return rocksdb::CompactionServiceJobStatus::kSuccess;
}

void MyCompactionService::OnInstallation(const std::string& remote_compaction_id_str,
                    rocksdb::CompactionServiceJobStatus status) {
    std::string current_dir = "";
#ifdef REMOTE_COMPACTION
    current_dir = FLAGS_db_path + "/" + FLAGS_secondary_db_path + "/" + remote_compaction_id_str;
#endif
    if (current_dir == "") {
        DB_FATAL("current_dir is empty");
        return;
    }
    butil::FilePath file_path(current_dir);
    bool success = butil::DeleteFile(file_path, true);
    if (!success) {
        DB_FATAL("Failed to delete file: %s, remote_compaction_id: %s", 
            file_path.BaseName().value().c_str(), remote_compaction_id_str.c_str());
    }
    _final_updated_status = status;
}


}  // namespace baikaldb
