#include "compaction_server.h"
#include "split_compaction_filter.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/statistics.h"
#include "rocksdb_merge_operator.h"
#ifdef BAIDU_INTERNAL
#include <base/file_util.h>
#else
#include <butil/file_util.h>
#endif
#include "my_listener.h"
#ifdef REMOTE_COMPACTION
#include "db/compaction/compaction_job.h"
#endif
#include "concurrency.h"

namespace baikaldb {

DEFINE_int32(compact_max_open_files, -1, "compact_max_open_files");
DEFINE_int32(remote_copy_thread_num, 5, "remote_copy_thread_num");
DEFINE_int32(remote_copy_buf_size_mb, 4, "remote_copy_buf_size_mb");
int remote_copy_file(const std::string& local_file, 
                    std::shared_ptr<CompactionExtFileSystem> fs, 
                    const std::string& external_file,
                    int64_t& length) {
    TimeCost cost;
    std::unique_ptr<ExtFileWriter> writer;
    int ret = fs->open_writer(external_file, &writer);
    if (ret < 0) {
        DB_WARNING("open writer: %s filed", external_file.c_str());
        return -1;
    }

    butil::File f(butil::FilePath{local_file}, butil::File::FLAG_OPEN | butil::File::FLAG_READ);
    if (!f.IsValid()) {
        DB_WARNING("file[%s] is not valid.", local_file.c_str());
        return -1;
    }
    length = f.GetLength();
    if (length < 0) {
        DB_FATAL("sst: %s get length failed, len: %ld", local_file.c_str(), length);
        return -1;
    }
    int64_t read_ret = 0;
    int64_t write_ret = 0;
    int64_t read_offset = 0;
    const static int BUF_SIZE {FLAGS_remote_copy_buf_size_mb * 1024 * 1024LL};
    std::unique_ptr<char[]> buf(new char[BUF_SIZE]);
    do {
        read_ret = f.Read(read_offset, buf.get(), BUF_SIZE);
        if (read_ret < 0) {
            DB_WARNING("read file[%s] error.", local_file.c_str());
            return -1;
        } 
        if (read_ret != 0) {
            write_ret = writer->append(buf.get(), read_ret);
            if (write_ret != read_ret) {
                DB_FATAL("append external_file: %s failed, offset: %ld, read_ret: %ld, write_ret: %ld", 
                    external_file.c_str(), read_offset, read_ret, write_ret);
                return -1;
            }
        }
        read_offset += read_ret;
    } while (read_ret == BUF_SIZE);

    int64_t write_size = writer->tell();
    if (write_size != length) {
        DB_FATAL("sst_file: %s external_file: %s, write_size: %ld, length: %ld", local_file.c_str(), external_file.c_str(), write_size, length);
        return -1;
    }

    if (!writer->sync()) {
        DB_FATAL("external file: %s sync failed", external_file.c_str());
        return -1;
    }

    if (!writer->close()) {
        DB_FATAL("external file: %s close failed", external_file.c_str());
        return -1;
    }
    // DB_WARNING("local_file: %s, size: %ld, cost: %ld", local_file.c_str(), length, cost.get_time());
    return 0;
}

int delete_dir(const std::string& dir_path_str, const std::string& remote_compaction_id) {
    butil::FilePath dir_path(dir_path_str);
    if (!butil::PathExists(dir_path)) {
        return -1;
    }

    bool success = butil::DeleteFile(dir_path, true);
    if (!success) {
        DB_FATAL("Failed to delete file: %s, remote_compaction_id: %s", 
            dir_path_str.c_str(), remote_compaction_id.c_str());
        return -1;
    }
    return 0;
}

void CompactionServer::set_options_override(const std::string& cf_name, 
                            const pb::RocksdbGFLAGS& rocksdb_gflags, 
                            rocksdb::CompactionServiceOptionsOverride& options_override) {
    rocksdb::BlockBasedTableOptions table_options;
    RocksWrapper::get_instance()->set_table_options(table_options, rocksdb_gflags);
    table_options.block_cache = nullptr;
    table_options.cache_index_and_filter_blocks = true;
    options_override.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    if (cf_name == RocksWrapper::RAFT_LOG_CF) {
        options_override.prefix_extractor.reset(
                rocksdb::NewFixedPrefixTransform(sizeof(int64_t) + 1));
    } else if (cf_name == RocksWrapper::BIN_LOG_CF || cf_name == "bin_log") {
        options_override.prefix_extractor.reset(
                rocksdb::NewFixedPrefixTransform(sizeof(int64_t)));        
    } else if (cf_name == RocksWrapper::DATA_CF) {
        options_override.prefix_extractor.reset(
                rocksdb::NewFixedPrefixTransform(sizeof(int64_t) * 2));
        options_override.merge_operator.reset(new OLAPMergeOperator());
        options_override.compaction_filter = SplitCompactionFilter::get_instance();
        options_override.sst_partitioner_factory = rocksdb::NewSstPartitionerFixedPrefixFactory(sizeof(int64_t));
        if (rocksdb_gflags.rocks_use_sst_partitioner_fixed_prefix()) {
            // 按region_id拆分
    #if ROCKSDB_MAJOR >= 7 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR > 22)
            options_override.sst_partitioner_factory = rocksdb::NewSstPartitionerFixedPrefixFactory(sizeof(int64_t));
    #endif
        }
        if (rocksdb_gflags.key_point_collector_interval() > 0) { 
            options_override.table_properties_collector_factories.emplace_back(
                            new KeyPointsTblPropCollectorFactory());
        }
    } else if (cf_name == RocksWrapper::METAINFO_CF) {
        options_override.prefix_extractor.reset(
                rocksdb::NewFixedPrefixTransform(1));
    } else if (cf_name == RocksWrapper::COLD_DATA_CF || cf_name == RocksWrapper::COLD_BINLOG_CF) {
        options_override.prefix_extractor.reset(
                rocksdb::NewFixedPrefixTransform(sizeof(int64_t) * 2));
        options_override.merge_operator.reset(new OLAPMergeOperator());
        options_override.table_properties_collector_factories.emplace_back(
                new KeyPointsTblPropCollectorFactory());
    }
    std::shared_ptr<rocksdb::EventListener> my_listener = std::make_shared<MyListener>();
    options_override.listeners.emplace_back(my_listener);
    options_override.statistics = rocksdb::CreateDBStatistics();
    options_override.max_open_files = FLAGS_compact_max_open_files;
}

void CompactionServer::do_compaction(google::protobuf::RpcController* controller,
                   const pb::RemoteCompactionRequest* request,
                   pb::RemoteCompactionResponse* response,
                   google::protobuf::Closure* done) {
#ifdef REMOTE_COMPACTION
    brpc::ClosureGuard done_guard(done);
    ScopeGuard auto_decrease([&response]() {
        response->set_errcode(pb::REMOTE_COMPACTION_ERROR);
    });
    const std::string& remote_compaction_id_str = request->remote_compaction_id();
    const std::string& current_job_dir = FLAGS_compaction_db_path + "/" + remote_compaction_id_str;
    ON_SCOPE_EXIT(([this, current_job_dir, remote_compaction_id_str]() {
        // DB_WARNING("remote_compaction_id: %s, concurrency: %d", 
        //     remote_compaction_id_str.c_str(), Concurrency::get_instance()->remote_compaction_server_concurrency.count());
        Concurrency::get_instance()->remote_compaction_server_concurrency.decrease_signal();
        delete_dir(current_job_dir, remote_compaction_id_str);
        CompactionSstExtLinker::get_instance()->finish_job(remote_compaction_id_str);
    }));
    // DB_WARNING("begin remote_compaction_id: %s, concurrency: %d", 
    //         remote_compaction_id_str.c_str(), Concurrency::get_instance()->remote_compaction_server_concurrency.count());
    int ret = Concurrency::get_instance()->remote_compaction_server_concurrency.increase_timed_wait(10 * 1000 * 1000LL);
    if (ret != 0) {
        DB_WARNING("remote_compaction_id: %s, remote_address: %s, compaction concurrency: %d too high", 
                                remote_compaction_id_str.c_str(), request->address().c_str(), 
                                Concurrency::get_instance()->remote_compaction_server_concurrency.count());
        return;
    }

    TimeCost total_cost;
    TimeCost phase_cost;
    // 初始化文件系统
    rocksdb::CompactionServiceInput compaction_input;
    rocksdb::Status s = rocksdb::CompactionServiceInput::Read(request->compaction_input_binary(), &compaction_input);
    if (!s.ok()) {
        DB_FATAL("remote_compaction_id: %s, remote_address: %s, compaction_input deserialize fail reason: %s", 
            remote_compaction_id_str.c_str(), request->address().c_str(), s.ToString().c_str());
        return;
    }

    std::shared_ptr<CompactionExtFileSystem> ext_fs(new CompactionExtFileSystem(request->address(), 
                                                    remote_compaction_id_str));
    ret = ext_fs->init();
    if (ret < 0) {
        DB_FATAL("init external filesystem failed");
        return;
    }
    if (CompactionSstExtLinker::get_instance()->register_job(remote_compaction_id_str, ext_fs, compaction_input.input_files) != 0) {
        DB_FATAL("remote_compaction_id: %s, remote_address: %s, register job failed", 
            remote_compaction_id_str.c_str(), request->address().c_str());
        return;
    }

    rocksdb::OpenAndCompactOptions options;
    options.canceled = &canceled_;
    rocksdb::CompactionServiceOptionsOverride options_override;
    set_options_override(request->cf_name(), request->rocksdb_gflags(), options_override);
    std::unique_ptr<rocksdb::Env> compaction_env = rocksdb::NewCompositeEnv(std::make_shared<CompactionFileSystemWrapper>(request->remote_compaction_id()));
    options_override.env = compaction_env.get();
    
    // do compaction
    std::string compaction_result_binary;
    rocksdb::CompactionServiceResult compaction_result;
    s = rocksdb::DB::OpenAndCompact(options, "", 
                                current_job_dir,
                                request->compaction_input_binary(), 
                                response->mutable_compaction_result_binary(), 
                                options_override);

    if (!s.ok()) {
        // 有可能文件刚刚被删了，文件不存在, 这里返回去做local compaction
        DB_FATAL("remote_compaction_id: %s, remote_address: %s, do_compaction fail reason: %s", 
            remote_compaction_id_str.c_str(), request->address().c_str(), s.ToString().c_str());
        return;
    }
    int64_t open_and_compaction_cost = phase_cost.get_time();
    phase_cost.reset();

    if (open_and_compaction_cost > FLAGS_remote_compaction_request_timeout * 1000LL) {
        DB_WARNING("remote_compaction_id: %s, remote_address: %s, time_cost: %ld open and do compaction cost too long, more than %d", 
            remote_compaction_id_str.c_str(), request->address().c_str(), 
            open_and_compaction_cost, FLAGS_remote_compaction_request_timeout);
        return;
    }

    // 远程拷贝文件
    s = rocksdb::CompactionServiceResult::Read(response->compaction_result_binary(), &compaction_result);
    if (!s.ok()) {
        DB_FATAL("remote_compaction_id: %s, remote_address: %s, compaction_result deserialize fail reason: %s", 
            remote_compaction_id_str.c_str(), request->address().c_str(), s.ToString().c_str());
        return;
    }
    ConcurrencyBthread copy_bth(FLAGS_remote_copy_thread_num);
    int copy_error = 0;

    std::mutex copy_file_mutex;
    for (int i = 0; i < compaction_result.output_files.size(); i++) {
        if (copy_error != 0) {
            break;
        }

        const std::string& output_file_name = compaction_result.output_files[i].file_name;
        const std::string& file_name = current_job_dir + "/" + output_file_name;
        auto copy_file_func = [&, file_name, output_file_name]() {
            int64_t file_size = 0;
            if (0 != remote_copy_file(file_name, ext_fs, output_file_name, file_size)) {
                DB_FATAL("remote_compaction_id: %s, remote_address: %s, time_cost: %ld, %ld copy_file fail", 
                        remote_compaction_id_str.c_str(), request->address().c_str(), 
                        open_and_compaction_cost, phase_cost.get_time());
                copy_error = -1;
                return;
            }

            pb::CompactionFileInfo file_info;
            file_info.set_file_path(output_file_name);
            file_info.set_file_size(file_size);
            BAIDU_SCOPED_LOCK(copy_file_mutex);
            response->add_output_file_info()->Swap(&file_info);
        };

        copy_bth.run(copy_file_func);
    }

    copy_bth.join();

    if (copy_error != 0) {
        return;
    }
    int64_t remote_copy_file_cost = phase_cost.get_time();
    int64_t total_cost_tm = total_cost.get_time();

    if (total_cost_tm > (FLAGS_remote_compaction_request_timeout - 10) * 1000LL) {
        DB_WARNING("remote_compaction_id: %s, remote_address: %s, time_cost: %ld open and do compaction cost too long, more than %d", 
            remote_compaction_id_str.c_str(), request->address().c_str(), 
            total_cost_tm, FLAGS_remote_compaction_request_timeout);
        ext_fs->delete_remote_copy_file_path();
        return;
    }

    std::shared_ptr<CompactionSstExtLinkerData> linker_data;
    if (CompactionSstExtLinker::get_instance()->get_linker_data(remote_compaction_id_str, linker_data) != 0 || linker_data == nullptr) {
        DB_FATAL("remote compaction id: %s not found", remote_compaction_id_str.c_str());
        return;
    }
    // 新增监控
    remote_compaction_total_latency << total_cost_tm;
    remote_compaction_file_read_latency << linker_data->read_file_time;
    remote_compaction_copy_file_latency << remote_copy_file_cost;
    if ((linker_data->cache_size + linker_data->not_cache_size) != 0) {
        remote_compaction_cache_rate << (10000 * linker_data->cache_size / (linker_data->cache_size + linker_data->not_cache_size));
    }
    // 任务结束, 删除文件
    auto_decrease.release();
    response->set_errcode(pb::SUCCESS);
    DB_NOTICE("remote_compaction_id=%s, remote_address=%s, total_cost=%ld|%ld|%ld, file_num=%ld|%ld, cache=%ld|%ld|%ld, read_file_time=%ld", 
        remote_compaction_id_str.c_str(),
        request->address().c_str(),
        open_and_compaction_cost,
        remote_copy_file_cost,
        total_cost_tm,
        compaction_input.input_files.size(),
        compaction_result.output_files.size(),
        linker_data->cache_size,
        linker_data->not_cache_size,
        CompactionSstCache::get_instance()->size(),
        linker_data->read_file_time
    );
#endif
    return;
}

}  // namespace baikaldb
