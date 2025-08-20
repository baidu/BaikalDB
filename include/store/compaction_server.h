#pragma once
#include "proto/compaction.interface.pb.h"
#ifdef BAIDU_INTERNAL
#include <baidu/rpc/server.h>
#else
#include <brpc/server.h>
#endif
#include "rocks_wrapper.h"
#include "rocksdb_filesystem.h"

namespace baikaldb {

class CompactionServer : public pb::CompactionService {
public:
    virtual ~CompactionServer() {}
    
    static CompactionServer* get_instance() {
        static CompactionServer _instance;
        return &_instance;
    }

    virtual void do_compaction(google::protobuf::RpcController* controller,
                   const pb::RemoteCompactionRequest* request,
                   pb::RemoteCompactionResponse* response,
                   google::protobuf::Closure* done);
    void set_options_override(const std::string& cf_name, 
                    const pb::RocksdbGFLAGS& rocksdb_gflags,
                    rocksdb::CompactionServiceOptionsOverride& options_override);
private:
    CompactionServer() : 
            remote_compaction_total_latency("remote_compaction_total_latency"),
            remote_compaction_file_read_latency("remote_compaction_file_read_latency"),
            remote_compaction_copy_file_latency("remote_compaction_copy_file_latency"),
            remote_compaction_cache_rate("remote_compaction_cache_rate"){}

    std::atomic_bool canceled_{false};
    bvar::LatencyRecorder remote_compaction_total_latency;
    bvar::LatencyRecorder remote_compaction_file_read_latency;
    bvar::LatencyRecorder remote_compaction_copy_file_latency;
    bvar::IntRecorder remote_compaction_cache_rate;
};
}  // namespace baikaldb