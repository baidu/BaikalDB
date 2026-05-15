#include <boost/filesystem.hpp>
#include <boost/algorithm/hex.hpp>
#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#else
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#endif
// #ifdef REMOTE_COMPACTION
#include "db/compaction/compaction_job.h"
// #endif
#include "table_key.h"
#include "rocksdb_compaction_ctrl.h"
#include "rocks_wrapper.h"

namespace baikaldb {
DECLARE_string(meta_server_bns);
DEFINE_bool(remote_compaction_uselocal, false, "default: true");
DEFINE_bool(enable_compute_storage_decoupled_mode, false, "default: false");
thread_local std::shared_ptr<RemoteCompactionInfo> g_remote_compaction_info = nullptr;

std::string print_compaction_input(const rocksdb::CompactionServiceJobInfo& info,
                                             const rocksdb::CompactionServiceInput& compaction_input) {
    std::ostringstream oss;
    oss << "CompactionServiceJobInfo: job_id: " << info.job_id;
    oss << ", is_full_compaction: " << info.is_full_compaction;
    oss << ", is_manual_compaction: " << info.is_manual_compaction;
    oss << ", bottommost_level: " << info.bottommost_level;
    oss << ", compaction_reason: " << rocksdb::GetCompactionReasonString(info.compaction_reason);
    oss << ", cf_name: " << compaction_input.cf_name;
    oss << ", output_level: " << compaction_input.output_level;
    oss << ", has_begin: " << compaction_input.has_begin;
    oss << ", has_end: " << compaction_input.has_end;
    oss << " snapshots_size: " << compaction_input.snapshots.size();
    oss << ", input_files[name, level, size, region_id]: ";
    // 00002.sst
    for (int i = 0; i < compaction_input.input_files.size(); ++i) {
        if (i >= info.input_infos.size()) {
            continue;
        }
        int64_t region_id = TableKey(info.input_infos[i].smallest).extract_i64(0);
        oss << "[" << compaction_input.input_files[i] << ", " << info.input_infos[i].level << ", " 
            << info.input_infos[i].file_size << ", " << region_id << "] ";
    }

    return oss.str();
}

rocksdb::CompactionServiceScheduleResponse RemoteCompactionService::Schedule(const rocksdb::CompactionServiceJobInfo& info,
                                             const std::string& compaction_service_input) {
    if (!FLAGS_enable_compute_storage_decoupled_mode) {
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }

    rocksdb::CompactionServiceInput compaction_input;
    rocksdb::Status s = rocksdb::CompactionServiceInput::Read(compaction_service_input, &compaction_input);
    if (!s.ok()) {
        DB_FATAL("CompactionServiceInput Read failed, error: %s", s.ToString().c_str());
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }

    if (compaction_input.cf_name != RocksWrapper::DATA_CF) {
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }

    std::string input_str = print_compaction_input(info, compaction_input);
    DB_WARNING("CompactionServiceSchedule %s", input_str.c_str());
    if (compaction_input.output_level != 6) {
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
    }

    const std::string& remote_compaction_id_str = rocksdb::Env::Default()->GenerateUniqueId();
    if (g_remote_compaction_info != nullptr) {
        // 在自定义compaction线程中执行
        if (g_remote_compaction_info->options.use_local_compaction()) {
            DB_WARNING("use local in user defined thread scheduled_job_id: %s, %s", remote_compaction_id_str.c_str(), input_str.c_str());
            return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
        } else {
            if (g_remote_compaction_info->options.need_check_input_files) {
                // TODO
            }
            if (compaction_input.has_begin || compaction_input.has_end) {
                DB_FATAL("CompactionServiceInput has_begin or has_end not support, %s", input_str.c_str());
                return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
            }
            DB_WARNING("use afs compaction scheduled_job_id: %s, %s", remote_compaction_id_str.c_str(), input_str.c_str());
            return rocksdb::CompactionServiceScheduleResponse(remote_compaction_id_str, rocksdb::CompactionServiceJobStatus::kSuccess);
        }
    } else {
        // 在rocksdb线程中执行
        if (enable_user_defined_compaction(info.input_infos)) {
            // 放入compaction队列中，然后拒掉本次compaction
            DB_WARNING("user definded compaction scheduled_job_id: %s, %s", remote_compaction_id_str.c_str(), input_str.c_str());
            CompactionFiles compaction_files;
            compaction_files.output_level = compaction_input.output_level;
            compaction_files.input_files = compaction_input.input_files;
            push_compaction_files(compaction_files);
            return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUserDefinedCompaction);
        } else {
            return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
        }
    }
}

rocksdb::CompactionServiceJobStatus RemoteCompactionService::Wait(const std::string& scheduled_job_id, std::string* result) {
    if (g_remote_compaction_info == nullptr || 
        g_remote_compaction_info->options.compaction_type != USE_REMOTE_FILES || 
        g_remote_compaction_info->options.output_level != 6) {
        DB_FATAL("scheduled_job_id: %s, compaction_result wait", scheduled_job_id.c_str());
        return rocksdb::CompactionServiceJobStatus::kFailure;
    }
    const BGCompactionOptions& options = g_remote_compaction_info->options;
    rocksdb::CompactionServiceResult compaction_result;
    compaction_result.status = rocksdb::Status::OK();
    compaction_result.output_level = 6;
    compaction_result.output_path = "";

    for (int i = 0; i < options.remote_file_names.size(); ++i) {
        rocksdb::CompactionServiceOutputFile output_file;
        output_file.oldest_ancester_time = options.min_inputfile_oldest_ancester_time;
        output_file.file_creation_time = (uint64_t)time(nullptr);
        output_file.file_name = options.remote_file_names[i];
        output_file.smallest_seqno = 0;
        output_file.largest_seqno = 0;
        output_file.epoch_number = options.min_inputfile_epoch_number;
        output_file.paranoid_hash = 0;
        output_file.marked_for_compaction = false;
        rocksdb::InternalKey smallest_ikey(options.smallest_keys[i], 0, rocksdb::ValueType::kTypeValue);
        rocksdb::InternalKey largest_ikey(options.largest_keys[i], 0, rocksdb::ValueType::kTypeValue);
        output_file.smallest_internal_key = smallest_ikey.Encode().ToString();
        output_file.largest_internal_key = largest_ikey.Encode().ToString();
        compaction_result.output_files.emplace_back(output_file);
    }

    std::ostringstream oss;
    for (const auto& f : options.remote_file_names) {
        oss << f << " ";
    }

    DB_WARNING("region_id: %ld, scheduled_job_id: %s, compaction_result serialize, remote files: %s", 
        options.region_id, scheduled_job_id.c_str(), oss.str().c_str());
    auto s = compaction_result.Write(result);
    if (!s.ok()) {
        DB_FATAL("scheduled_job_id: %s, compaction_result serialize fail reason: %s", 
            scheduled_job_id.c_str(), s.ToString().c_str());
        return rocksdb::CompactionServiceJobStatus::kFailure;
    }
    return rocksdb::CompactionServiceJobStatus::kSuccess;
}

bool RemoteCompactionService::enable_user_defined_compaction(const std::vector<rocksdb::CompactInputFileInfo>& input_infos) {
        if (input_infos.empty()) {
            return false;
        }

        std::set<int64_t> region_ids;
        std::set<int64_t> table_ids;
        bool has_l0_sst = false;
        for (const auto& input_info : input_infos) {
            int64_t smallest_region_id = TableKey(input_info.smallest).extract_i64(0);
            int64_t smallest_table_id  = TableKey(input_info.smallest).extract_i64(sizeof(int64_t));
            int64_t largest_region_id  = TableKey(input_info.largest).extract_i64(0);
            int64_t largest_table_id   = TableKey(input_info.largest).extract_i64(sizeof(int64_t));
            if (smallest_region_id != largest_region_id) {
                if (input_info.level != 0) {
                    // 非L0 sst有多region数据
                    return false;
                } else {
                    has_l0_sst = true;
                    continue;
                }
            }

            region_ids.insert(smallest_region_id);
            table_ids.insert(largest_table_id);
        }

        {
            // 判断是否有table打开存算分离
            bool has_table_open_dsc = false;
            std::unique_lock<std::mutex> l(_mutex);
            for (int64_t table_id : table_ids) {
                if (_dsc_table_ids.count(table_id) > 0) {
                    has_table_open_dsc = true;
                }
            }

            if (!has_table_open_dsc) {
                return false;
            }
        }

        return true;
    }

}  // namespace baikaldb