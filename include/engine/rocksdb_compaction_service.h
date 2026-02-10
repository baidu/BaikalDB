#pragma once
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/channel.h>
#include <baidu/rpc/server.h>
#else
#include <brpc/channel.h>
#include <brpc/server.h>
#endif
#include "rocksdb/options.h"
#include "proto/compaction.interface.pb.h"
#include "common.h"
#ifdef REMOTE_COMPACTION
#include "db/compaction/compaction_job.h"
#endif

namespace baikaldb {
DECLARE_bool(enable_remote_compaction);
extern void set_rocksdb_flags(pb::RocksdbGFLAGS* rocksdb_gflags);

class MyCompactionService : public rocksdb::CompactionService {
public:
    MyCompactionService(const std::string& db_path, const std::string& address) {
        _db_path = db_path;
        _address = address;
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_remote_compaction_request_timeout;
        channel_opt.connect_timeout_ms = FLAGS_remote_compaction_connect_timeout;
        std::string compaction_server_addr = address;
        //bns
        if (address.find(":") == std::string::npos) {
            compaction_server_addr = std::string("bns://") + address;
        } else {
            compaction_server_addr = std::string("list://") + address;
        }
        if (_channel.Init(compaction_server_addr.c_str(), "c_murmurhash", &channel_opt) != 0) {
            DB_FATAL("Failed to initialize channel to %s", _address.c_str());
        }
    }

    static const char* kClassName() { return "MyCompactionService"; }

    const char* Name() const override { return kClassName(); }

    class OnRPCDone : public google::protobuf::Closure {
    public:
        OnRPCDone(const std::string& remote_compaction_id, 
                    size_t total_file_size,
                    const rocksdb::Snapshot* snapshot)
            : _remote_compaction_id(remote_compaction_id),
            _total_file_size(total_file_size),
            _snapshot(snapshot) {}
        
        ~OnRPCDone();
    
        void Run() override {}
        
        brpc::Controller cntl;    
        std::string _remote_compaction_id = "";
        size_t _total_file_size = 0;
        pb::RemoteCompactionResponse _response;
        const rocksdb::Snapshot* _snapshot = nullptr;
    };

    rocksdb::CompactionServiceScheduleResponse Schedule(const rocksdb::CompactionServiceJobInfo& info,
                                             const std::string& compaction_service_input) override;
    rocksdb::CompactionServiceJobStatus Wait(const std::string& scheduled_job_id, std::string* result) override;

    void OnInstallation(const std::string& scheduled_job_id,
                        rocksdb::CompactionServiceJobStatus status) override;
private:
    std::map<std::string, std::shared_ptr<OnRPCDone>> _doing_map;
    std::map<std::string, TimeCost> _time_cost_map;
    brpc::Channel _channel;
    std::string _db_path;
    std::string _address;
    std::atomic<rocksdb::CompactionServiceJobStatus> _final_updated_status{
        rocksdb::CompactionServiceJobStatus::kUseLocal};
    bthread::Mutex _mutex;
};

// struct CompactionServiceJobInfo {
//   std::string db_name;
//   std::string db_id;
//   std::string db_session_id;
//   uint64_t job_id;  // job_id is only unique within the current DB and session,
//                     // restart DB will reset the job_id. `db_id` and
//                     // `db_session_id` could help you build unique id across
//                     // different DBs and sessions.

//   Env::Priority priority;

//   // Additional Compaction Details that can be useful in the CompactionService
//   CompactionReason compaction_reason;
//   bool is_full_compaction;
//   bool is_manual_compaction;
//   bool bottommost_level;
//   bool is_l0_compaction;
//   CompactionServiceJobInfo(std::string db_name_, std::string db_id_,
//                            std::string db_session_id_, uint64_t job_id_,
//                            Env::Priority priority_,
//                            CompactionReason compaction_reason_,
//                            bool is_full_compaction_, bool is_manual_compaction_,
//                            bool bottommost_level_, bool is_l0_compaction_)
//       : db_name(std::move(db_name_)),
//         db_id(std::move(db_id_)),
//         db_session_id(std::move(db_session_id_)),
//         job_id(job_id_),
//         priority(priority_),
//         compaction_reason(compaction_reason_),
//         is_full_compaction(is_full_compaction_),
//         is_manual_compaction(is_manual_compaction_),
//         bottommost_level(bottommost_level_),
//         is_l0_compaction(is_l0_compaction_) {}
// };

// struct CompactionServiceInput {
//   std::string cf_name;

//   std::vector<SequenceNumber> snapshots;

//   // SST files for compaction, it should already be expended to include all the
//   // files needed for this compaction, for both input level files and output
//   // level files.
//   std::vector<std::string> input_files;
//   int output_level;

//   // db_id is used to generate unique id of sst on the remote compactor
//   std::string db_id;

//   // information for subcompaction
//   bool has_begin = false;
//   std::string begin;
//   bool has_end = false;
//   std::string end;

//   uint64_t options_file_number;

//   // serialization interface to read and write the object
//   static Status Read(const std::string& data_str, CompactionServiceInput* obj);
//   Status Write(std::string* output);

//   bool is_l0_compaction = false;
// };
#ifdef REMOTE_COMPACTION

class TESTCompactionService : public rocksdb::CompactionService {
public:
    TESTCompactionService(const std::string& db_path, const std::string& address) {
        _db_path = db_path;
        _address = address;
    }

    static const char* kClassName() { return "TESTCompactionService"; }

    const char* Name() const override { return kClassName(); }

    rocksdb::CompactionServiceScheduleResponse Schedule(const rocksdb::CompactionServiceJobInfo& info,
                                             const std::string& compaction_service_input) override {
        rocksdb::CompactionServiceInput compaction_input;
        rocksdb::Status s = rocksdb::CompactionServiceInput::Read(compaction_service_input, &compaction_input);
        if (!s.ok()) {
            DB_WARNING("CompactionServiceInput Read failed, error: %s", s.ToString().c_str());
            return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
        }

        std::ostringstream oss;
        oss << "CompactionServiceJobInfo: db_name: " << info.db_name;
        oss << ", db_id: " << info.db_id;
        oss << ", db_session_id: " << info.db_session_id;
        oss << ", job_id: " << info.job_id;
        oss << ", priority: " << info.priority;
        // oss << ", compaction_reason: " << info.compaction_reason;
        oss << ", is_full_compaction: " << info.is_full_compaction;
        oss << ", is_manual_compaction: " << info.is_manual_compaction;
        oss << ", bottommost_level: " << info.bottommost_level;
        oss << ", is_l0_compaction: " << info.is_l0_compaction;
        oss << ", cf_name: " << compaction_input.cf_name;
        oss << ", output_level: " << compaction_input.output_level;
        oss << ", db_id: " << compaction_input.db_id;
        oss << ", has_begin: " << compaction_input.has_begin;
        oss << ", begin: " << compaction_input.begin;
        oss << ", has_end: " << compaction_input.has_end;
        oss << ", end: " << compaction_input.end;
        oss << ", options_file_number: " << compaction_input.options_file_number;
        oss << ", is_l0_compaction: " << compaction_input.is_l0_compaction;
        oss << " snapshots: ";
        for (const auto s : compaction_input.snapshots) {
            oss << s << " ";
        }
        oss << ", input_files: ";
        for (const auto f : compaction_input.input_files) {
            oss << f << " ";
        }

        DB_WARNING("%s", oss.str().c_str());
        if (!FLAGS_enable_remote_compaction) {
            DB_WARNING("remote compaction is not enabled");
            return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kUseLocal);
        }
        return rocksdb::CompactionServiceScheduleResponse("", rocksdb::CompactionServiceJobStatus::kSuccess);
    }


// struct CompactionServiceOutputFile {
//   std::string file_name;
//   SequenceNumber smallest_seqno;
//   SequenceNumber largest_seqno;
//   std::string smallest_internal_key;
//   std::string largest_internal_key;
//   uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;
//   uint64_t file_creation_time = kUnknownFileCreationTime;
//   uint64_t epoch_number = kUnknownEpochNumber;
//   std::string file_checksum = kUnknownFileChecksum;
//   std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
//   uint64_t paranoid_hash;
//   bool marked_for_compaction;
//   UniqueId64x2 unique_id{};
// };

// struct CompactionServiceResult {
//   Status status;
//   std::vector<CompactionServiceOutputFile> output_files;
//   int output_level;

//   // location of the output files
//   std::string output_path;

//   uint64_t bytes_read = 0;
//   uint64_t bytes_written = 0;
//   CompactionJobStats stats;

//   // serialization interface to read and write the object
//   static Status Read(const std::string& data_str, CompactionServiceResult* obj);
//   Status Write(std::string* output);
// };
    rocksdb::CompactionServiceJobStatus Wait(const std::string& scheduled_job_id, std::string* result) override {
        rocksdb::CompactionServiceResult compaction_result;
        compaction_result.status = rocksdb::Status::OK();
        compaction_result.output_level = 6;
        compaction_result.output_path = "./";
        rocksdb::CompactionServiceOutputFile output_file;
        output_file.file_name = "abc.sst";
        output_file.smallest_seqno = 0;
        output_file.largest_seqno = 0;
        output_file.epoch_number = 1;
        output_file.paranoid_hash = 0;
        output_file.marked_for_compaction = false;
        rocksdb::InternalKey smallest_ikey("0", 0, rocksdb::ValueType::kTypeValue);
        rocksdb::InternalKey largest_ikey("9", 0, rocksdb::ValueType::kTypeValue);
        output_file.smallest_internal_key = smallest_ikey.Encode().ToString();
        output_file.largest_internal_key = largest_ikey.Encode().ToString();
        compaction_result.output_files.emplace_back(output_file);

        DB_WARNING("scheduled_job_id: %s, compaction_result serialize", scheduled_job_id.c_str());
        auto s = compaction_result.Write(result);
        if (!s.ok()) {
            DB_FATAL("scheduled_job_id: %s, compaction_result serialize fail reason: %s", 
                scheduled_job_id.c_str(), s.ToString().c_str());
            return rocksdb::CompactionServiceJobStatus::kFailure;
        }
        return rocksdb::CompactionServiceJobStatus::kSuccess;
    }

    void OnInstallation(const std::string& scheduled_job_id,
                        rocksdb::CompactionServiceJobStatus status) override {
        DB_WARNING("scheduled_job_id: %s, compaction_result install", 
                scheduled_job_id.c_str());
    }
private:
    std::string _db_path;
    std::string _address;
};
#endif

}  // namespace baikaldb
