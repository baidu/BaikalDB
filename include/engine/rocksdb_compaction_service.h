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

}  // namespace baikaldb
