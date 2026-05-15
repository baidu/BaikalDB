#include <boost/filesystem.hpp>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string.hpp>
#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#else
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#endif
#include "rocks_wrapper.h"
#include "region_compaction.h"
#include "store.h"
#include "rocksdb_filesystem.h"
#include "compaction_server.h"

namespace baikaldb {
DECLARE_string(resource_tag);
DECLARE_string(meta_server_bns);
DEFINE_int32(background_compaction_thread_count, 2, "background_compaction_thread_count, default(2)");
DEFINE_int64(region_remote_compaction_wait_s, 600LL, "default 10 min");

static const rocksdb::SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);
std::string print_compaction_job(const rocksdb::CompactionJobInfo& compaction_job_info) {
    std::ostringstream os;
    os << "CompactionJobInfo[job_id: " << compaction_job_info.job_id << " cf_name: " 
        << compaction_job_info.cf_name << 
        " status: " << compaction_job_info.status.ToString().c_str() << 
        " thread_id: " << compaction_job_info.thread_id << 
        " base_input_level: " << compaction_job_info.base_input_level << 
        " output_level: " << compaction_job_info.output_level;
    //    " input_files: {";
    // // ./rocks_db/001058.sst 全路径
    // for (const auto& file : compaction_job_info.input_files) {
    //     os << file << ",";
    // }
    // os << "}";
    os << " input_file_infos: {";
    for (const auto& file_info : compaction_job_info.input_file_infos) {
        os << "[file_number: " << file_info.file_number << ",";
        os << "level: " << file_info.level << "]";
    }
    os << " }";
    os << " output_files: {";
    // ./rocks_db/001058.sst 全路径
    for (const auto& file : compaction_job_info.output_files) {
        os << file << ",";
    }
    os << "}";
    os << " output_file_infos: {";
    for (const auto& file_info : compaction_job_info.output_file_infos) {
        os << "[file_number: " << file_info.file_number << ",";
        os << "level: " << file_info.level << "]";
    }
    os << " }";
    return os.str();
}

RegionCompactFiles::RegionCompactFiles() {
    _thread_pool = rocksdb::NewThreadPool(FLAGS_background_compaction_thread_count);
}

int RegionCompactFiles::finish_compaction_files(const rocksdb::CompactionJobInfo& job, const BGCompactionOptions& options) {
    if (job.output_level == 6 && options.compaction_type == USE_LOCAL_AND_COPY_TO_REMOTE) {
        std::shared_ptr<SstExtLinker> linker = get_sst_ext_linker();
        if (linker == nullptr) {
            return -1;
        }

        if (job.output_files.empty()) {
            return 0;
        }

        auto rocksdb = RocksWrapper::get_instance();
        uint32_t now = (uint32_t)time(nullptr);
        int64_t raft_index = -1;
        for (const std::string& file_name : job.output_files) {
            raft_index = rocksdb->get_file_raft_index(file_name, options.region_id);
            if (raft_index > 0) {
                break;
            }
        }

        if (raft_index <= 0) {
            DB_FATAL("raft_index is invalid, region_id: %ld", options.region_id);
            return -1;
        }

        std::string address = get_store_ip_port();
        for (const std::string& file_name : job.output_files) {
            TimeCost copy_cost;
            butil::File f(butil::FilePath{file_name}, butil::File::FLAG_OPEN | butil::File::FLAG_READ);
            if (!f.IsValid()) {
                DB_FATAL("file[%s] is not valid.", file_name.c_str());
                continue;
            }
            int64_t length = f.GetLength();
            if (length < 0) {
                DB_FATAL("file[%s] get length failed, len: %ld", file_name.c_str(), length);
                continue;
            }
            std::vector<std::string> items;
            boost::split(items, file_name, boost::is_any_of("./"));
            if (items.size() < 2) {
                DB_FATAL("file[%s] is not valid.", file_name.c_str());
                continue;
            }
            std::string t_short_name = items[items.size() - 2] + ".sst";
            std::string cloud_file_name = make_cloud_file_name("", options.region_id, raft_index, 
                boost::lexical_cast<int64_t>(items[items.size() - 2]), length, now);
            std::string user_define_path = "baikal_cloud/" + FLAGS_meta_server_bns + "/" + FLAGS_resource_tag + "/" + address + "/" 
                + cloud_file_name;
            std::string external_file;
            int ret = copy_file(file_name, user_define_path, external_file, length);
            if (ret != 0) {
                DB_FATAL("copy_file FAIL, file_name: %s, user_define_path: %s", 
                    file_name.c_str(), user_define_path.c_str());
                continue;
            }
            
            ret = linker->sst_link(external_file, t_short_name, length);
            if (ret < 0) {
                DB_FATAL("sst_link FAIL, external_file: %s, t_short_name: %s, length: %ld", 
                    external_file.c_str(), t_short_name.c_str(), length);
                continue;
            }
            DB_WARNING("copy_file SUCCESS, file_name: %s, external_file: %s, time_cost: %ld", 
                file_name.c_str(), external_file.c_str(), copy_cost.get_time());
        }
    }

    return 0;
}

// virtual Status CompactFiles(
//   const CompactionOptions& compact_options,
//   const std::vector<std::string>& input_file_names, const int output_level,
//   const int output_path_id = -1,
//   std::vector<std::string>* const output_file_names = nullptr,
//   CompactionJobInfo* compaction_job_info = nullptr) 
void RegionCompactFiles::bg_compaction_files(const BGCompactionOptions& options, const std::vector<std::string>& input_files) {
    _doing_cnt++;
    auto fn = [this, options, input_files]() {
        auto rocksdb = RocksWrapper::get_instance();
        auto txn_db  = rocksdb->get_db();
        TimeCost time_cost;
        std::shared_ptr<RemoteCompactionInfo> remote_compaction_info = std::make_shared<RemoteCompactionInfo>();
        remote_compaction_info->options = options;
        remote_compaction_info->input_files = input_files;
        g_remote_compaction_info = remote_compaction_info;
        rocksdb::CompactionJobInfo job;
        std::vector<std::string> output_file_names;
        rocksdb::CompactionOptions compaction_options;
        compaction_options.max_subcompactions = 1; // 设置为1，保证单线程执行，不拆分子任务
        auto s = txn_db->CompactFiles(compaction_options, rocksdb->get_data_handle(), input_files, options.output_level, -1, &output_file_names, &job);
        if (!s.ok()) {
            _doing_cnt--;
            if (options.compaction_type == USE_REMOTE_FILES) {
                auto ext_fs = rocksdb->get_exteranl_filesystem();
                for (const std::string& file_name : options.remote_file_names) {
                    if (ext_fs != nullptr) {
                        int ret = ext_fs->delete_path(file_name, true);
                        DB_WARNING("delete_path, file_name: %s, ret: %d", file_name.c_str(), ret);
                    }
                }
            }
            std::ostringstream os;
            for (const auto& f : input_files) {
                os << f << " ";
            }
            DB_FATAL("region_id: %ld, output_level: %d, CompactFiles FAIL: %s, input_files: [%s]", 
                options.region_id, options.output_level, s.ToString().c_str(), os.str().c_str());
            return;
        }

        std::string job_str = print_compaction_job(job);
        DB_WARNING("CompactionJobInfo: [%s] remote compaction info: [%s] time_cost: %ld", 
            job_str.c_str(), remote_compaction_info->to_string().c_str(), time_cost.get_time());
        finish_compaction_files(job, options);
        g_remote_compaction_info = nullptr;
        _doing_cnt--;
    };
    _thread_pool->SubmitJob(fn);
}

int RegionCompactFiles::check_compaction_files(int64_t region_id, std::vector<rocksdb::LiveFileMetaData*> input_file_metadatas, 
        CompactionType& compaction_type, std::vector<std::string>& remote_file_names, 
        std::vector<std::string>& smallest_keys, std::vector<std::string>& largest_keys) {
    rocksdb::SequenceNumber smallest_seqno = kMaxSequenceNumber;
    std::map<int, std::string> level_file;
    int min_level = INT_MAX;
    for (auto meta : input_file_metadatas) {
        if (meta == nullptr) {
            return -1;
        }
        min_level = std::min(meta->level, min_level);
        TableKey smallestkey(meta->smallestkey);
        int64_t smallest_region_id = smallestkey.extract_i64(0);
        int64_t smallest_table_id  = smallestkey.extract_i64(sizeof(int64_t));
        if (smallest_table_id == 0) {
            level_file[meta->level] = meta->directory + "/" + meta->relative_filename;
        }
        if (meta->smallest_seqno != 0) {
            smallest_seqno = std::min(meta->smallest_seqno, smallest_seqno);
        }
    }

    if (smallest_seqno == kMaxSequenceNumber) {
        smallest_seqno = 0;
    }

    if (level_file.find(min_level) == level_file.end()) {
        DB_FATAL("region_id: %ld, level_file not find min_level: %d", region_id, min_level);
        return -1;
    }

    auto rocksdb = RocksWrapper::get_instance();
    auto txn_db  = rocksdb->get_db();
    uint64_t oldest_snapshot_sequence = 0;
    bool ret = txn_db->GetIntProperty(rocksdb->get_data_handle(), "rocksdb.oldest-snapshot-sequence", &oldest_snapshot_sequence);
    if (!ret) {
        DB_FATAL("region_id: %ld GetIntProperty FAIL", region_id);
        return -1;
    }

    if (oldest_snapshot_sequence != 0) {
        // 确保compaction的文件不会在snapshot中
        if (smallest_seqno > oldest_snapshot_sequence) {
            DB_WARNING("region_id: %ld, smallest_seqno: %lu, oldest_snapshot_sequence: %lu", region_id, smallest_seqno, oldest_snapshot_sequence);
            return -1;
        }
    }

    auto region = Store::get_instance()->get_region(region_id);
    if (region == nullptr) {
        compaction_type = ONLY_USE_LOCAL;
        // 该region可能已经迁移了
        DB_FATAL("region_id: %ld, region is nullptr", region_id);
        return 0;
    }

    auto factory = SchemaFactory::get_instance();
    auto table = factory->get_table_info_ptr(region->get_table_id());
    if (table == nullptr) {
        compaction_type = ONLY_USE_LOCAL;
        return 0;
    }

    if (!table->schema_conf.enable_compute_storage_decoupled()) {
        compaction_type = ONLY_USE_LOCAL;
        return 0;
    }

    bool has = false;
    for (int64_t idx : table->indices) {
        auto index = factory->get_index_info_ptr(idx);
        if (index == nullptr) {
            DB_FATAL("index null table region_id: %ld, table_id: %ld, index_id: %ld", 
                region_id, region->get_table_id(), idx);
            continue;
        }

        if (index->type == pb::I_FULLTEXT || index->type == pb::I_VECTOR || index->type == pb::I_ROLLUP) {
            has = true;
            break;
        }
    }

    if (has) {
        DB_WARNING("region_id: %ld, has fulltext or vector index", region_id);
        compaction_type = ONLY_USE_LOCAL;
        return 0;
    }

    if (region->is_leader()) {
        compaction_type = USE_LOCAL_AND_COPY_TO_REMOTE;
        return 0;
    }

    int64_t max_raft_index = rocksdb->get_file_raft_index(level_file[min_level], region_id);
    if (max_raft_index <= 0) {
        compaction_type = USE_LOCAL_AND_COPY_TO_REMOTE;
        DB_WARNING("region_id: %ld, raft_index: %ld", region_id, max_raft_index);
        return 0;
    }

    if (max_raft_index == INT64_MAX) {
        DB_FATAL("INT64_MAX region_id: %ld, raft_index: %ld", region_id, max_raft_index);
        return -1;
    }

    DB_WARNING("region_id: %ld, smallest_seqno: %lu, oldest_snapshot_sequence: %lu, raft_index: %ld", 
        region_id, smallest_seqno, oldest_snapshot_sequence, max_raft_index);
    uint64_t log_id = butil::fast_rand();
    std::string leader = butil::endpoint2str(region->get_leader()).c_str();
    brpc::Channel channel;
    brpc::Controller cntl;
    cntl.set_log_id(log_id);
    brpc::ChannelOptions option;
    option.max_retry = 1;
    option.connect_timeout_ms = 30000;
    option.timeout_ms = 30000;
    channel.Init(leader.c_str(), &option);
    pb::CompactionService_Stub stub(&channel);
    pb::GetRemoteFilesRequest request;
    pb::GetRemoteFilesResponse response;
    request.set_region_id(region_id);
    request.set_raft_index(max_raft_index);
    request.set_dst_address(get_store_ip_port());

    stub.get_remote_files(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        DB_FATAL("log_id: %lu region_id: %ld, connect leader: %s FAIL: %s", log_id, region_id, leader.c_str(), cntl.ErrorText().c_str());
        return -1;
    }

    if (response.errcode() == pb::SUCCESS) { 
        if (response.remote_files_size() > 0) {
            for (auto file : response.remote_files()) {
                remote_file_names.emplace_back(file.file_path());
                smallest_keys.emplace_back(file.smallest_key());
                largest_keys.emplace_back(file.largest_key());
            }
            compaction_type = USE_REMOTE_FILES;
            DB_WARNING("log_id: %lu region_id: %ld, leader:%s response: %s", log_id, region_id, leader.c_str(), response.ShortDebugString().c_str());
            return 0;
        } else {
            DB_WARNING("log_id: %lu no remote files use local region_id: %ld, leader: %s, response: %s", 
                log_id, region_id, leader.c_str(), response.ShortDebugString().c_str());
            compaction_type = USE_LOCAL_AND_COPY_TO_REMOTE;
            return 0;
        }
    } else {
        DB_WARNING("log_id: %lu region_id: %ld, connect leader: %s FAIL: %s", log_id, region_id, leader.c_str(), response.ShortDebugString().c_str());
        if (!need_wait(region_id, max_raft_index)) {
            DB_WARNING("log_id: %lu region_id: %ld, do compaction", log_id, region_id);
            compaction_type = USE_LOCAL_AND_COPY_TO_REMOTE;
            return 0;
        }
        return -1;
    }
}

void RegionCompactFiles::do_compaction_files(const CompactionFiles& compaction_files) {
    std::vector<rocksdb::LiveFileMetaData> metadatas;
    metadatas.reserve(10240);
    auto rocksdb = RocksWrapper::get_instance();
    auto txn_db  = rocksdb->get_db();
    txn_db->GetLiveFilesMetaData(&metadatas);
    std::map<std::string, int> total_file_name_index_map;
    std::map<int64_t, std::set<int>> total_region_id_index_map;
    bool need_compaction = false;
    for (int i = 0; i < metadatas.size(); i++) {
        if (metadatas[i].column_family_name != RocksWrapper::DATA_CF) {
            continue;
        }
        
        // print_metadata_info(metadatas[i]);
        total_file_name_index_map[metadatas[i].relative_filename] = i;
        int64_t smallest_region_id = TableKey(metadatas[i].smallestkey).extract_i64(0);
        int64_t largest_region_id  = TableKey(metadatas[i].largestkey).extract_i64(0);
        if (smallest_region_id != largest_region_id && metadatas[i].level != 0) {
            // 没有完成sst partition
            need_compaction = true;
            break;
        }

        total_region_id_index_map[smallest_region_id].insert(i);
    }

    if (need_compaction) {
        BGCompactionOptions options;
        options.compaction_type = ONLY_USE_LOCAL;
        options.need_check_input_files = false;
        options.output_level = compaction_files.output_level;
        bg_compaction_files(options, compaction_files.input_files);
        return;
    }

    std::map<int64_t, std::set<int>> region_id_index_map;
    std::map<int64_t, std::set<int>> region_id_level_map;
    std::set<int> l0_index_set;
    for (const std::string& f : compaction_files.input_files) {
        auto it = total_file_name_index_map.find(f);
        if (it == total_file_name_index_map.end()) {
            DB_FATAL("cant find file: %s", f.c_str());
            return;
        }

        int index = it->second;
        if (index < 0 || index >= metadatas.size()) {
            DB_FATAL("index: %d out of range", index);
            return;
        }
        int64_t region_id = TableKey(metadatas[index].smallestkey).extract_i64(0);
        region_id_index_map[region_id].insert(index);
        region_id_level_map[region_id].insert(metadatas[index].level);
        if (metadatas[index].level == 0) {
            l0_index_set.insert(index);
        }
        print_metadata_info(metadatas[index]);
        // DB_WARNING("file: %s region_id: %ld level: %d", metadatas[index].relative_filename.c_str(), region_id, metadatas[index].level);
    }

    if (!l0_index_set.empty()) {
        // l0文件可能混合多个region_id，需要先到l5进行sst前缀拆分
        BGCompactionOptions options;
        options.compaction_type = ONLY_USE_LOCAL;
        options.need_check_input_files = false;
        options.output_level = 5;
        std::vector<std::string> input_file_names;
        input_file_names.reserve(l0_index_set.size());
        for (int index : l0_index_set) {
            if (index < 0 || index >= metadatas.size()) {
                continue;
            }
            input_file_names.emplace_back(metadatas[index].relative_filename);
        }
        DB_WARNING("l0_index_set.size(): %lu compaction to L5", l0_index_set.size());
        bg_compaction_files(options, input_file_names);
        return;
    }

    for (const auto& region_id_index : region_id_index_map) {
        int64_t region_id = region_id_index.first;
        const std::set<int>& index_set = region_id_index.second;
        const std::set<int>& level_set = region_id_level_map[region_id];
        if (level_set.size() > 2) {
            // compaction不可能大于2个level
            DB_FATAL("region_id: %ld level_set.size(): %lu", region_id, level_set.size());
            return;
        }

        auto it = total_region_id_index_map.find(region_id);
        if (it == total_region_id_index_map.end()) {
            DB_FATAL("cant find region_id: %ld in total_region_id_index_map", region_id);
            return;
        }

        std::set<int> total_index_set;
        for (int i : it->second) {
            if (i < 0 || i >= metadatas.size()) {
                continue;
            }
            if (metadatas[i].level < *level_set.begin() || metadatas[i].level > std::max(*level_set.rbegin(), compaction_files.output_level)) {
                // 剔除不需要的level文件
                DB_WARNING("remove file %s region_id: %ld level: %d", metadatas[i].relative_filename.c_str(), region_id, metadatas[i].level);
                continue;
            }

            DB_WARNING("keep file %s region_id: %ld level: %d", metadatas[i].relative_filename.c_str(), region_id, metadatas[i].level);
            total_index_set.insert(i);
            // print_metadata_info(metadatas[i]);
        }

        for (int i : index_set) {
            if (total_index_set.count(i) == 0) {
                DB_FATAL("cant find file: %s region_id: %ld", metadatas[i].relative_filename.c_str(), region_id);
                return;
            }
        }

        if (total_index_set.size() != index_set.size()) {
            DB_WARNING("region_id: %ld total_index_set: %lu index_set: %lu", region_id, total_index_set.size(), index_set.size());
        }

        std::vector<rocksdb::LiveFileMetaData*> input_file_metadatas;
        std::vector<std::string> input_file_names;
        input_file_metadatas.reserve(total_index_set.size());
        input_file_names.reserve(total_index_set.size());
        for (int i : total_index_set) {
            if (i < 0 || i >= metadatas.size()) {
                continue;
            }
            input_file_metadatas.emplace_back(&metadatas[i]);
            input_file_names.emplace_back(metadatas[i].relative_filename);
        }

        CompactionType compaction_type;
        std::vector<std::string> remote_file_names;
        std::vector<std::string> smallest_keys;
        std::vector<std::string> largest_keys;
        int ret = check_compaction_files(region_id, input_file_metadatas, compaction_type, remote_file_names, smallest_keys, largest_keys);
        if (ret < 0) {
            DB_WARNING("region_id: %ld check_compaction_files failed", region_id);
            continue;
        }

        BGCompactionOptions options;
        options.compaction_type = compaction_type;
        options.region_id = region_id;
        if (compaction_type == USE_REMOTE_FILES) {
            options.need_check_input_files = true;
            options.min_inputfile_oldest_ancester_time = MinInputFileOldestAncesterTime(input_file_metadatas);
            options.min_inputfile_epoch_number = MinInputFileEpochNumber(input_file_metadatas);
            options.remote_file_names = remote_file_names;
            options.smallest_keys = smallest_keys;
            options.largest_keys = largest_keys;
        }
        options.output_level = compaction_files.output_level;
        bg_compaction_files(options, input_file_names);
    }
}

void RegionCompactFiles::compaction_files() {
    while (_doing_cnt < FLAGS_background_compaction_thread_count) {
        CompactionFiles compaction_files;
        if (RocksWrapper::get_instance()->get_region_compaction_files(compaction_files) == -1) {
            return;
        }

        do_compaction_files(compaction_files);
    }
}

void CompactionServer::get_remote_files(google::protobuf::RpcController* controller,
                   const pb::GetRemoteFilesRequest* request,
                   pb::GetRemoteFilesResponse* response,
                   google::protobuf::Closure* done) {
    uint64_t log_id = 0;
    brpc::Controller* cntl = (brpc::Controller*)controller;
    if (cntl->has_log_id()) {
        log_id = cntl->log_id();
    }
    int64_t region_id = request->region_id();
    int64_t raft_index = request->raft_index();
    std::string dst_address = request->dst_address();
    brpc::ClosureGuard done_guard(done);
    auto region = Store::get_instance()->get_region(region_id);
    if (region == nullptr) {
        DB_WARNING("log_id: %lu region_id: %ld not exist", log_id, region_id);
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("region not exist");
        return;
    }

    if (!region->is_leader()) {
        DB_WARNING("log_id: %lu region_id: %ld not leader", log_id, region_id);
        response->set_errcode(pb::NOT_LEADER);
        response->set_errmsg("region not leader");
        return;
    }

    auto rocksdb = RocksWrapper::get_instance();

    std::vector<rocksdb::LiveFileMetaData> metadatas;
    metadatas.reserve(10240);
    rocksdb->get_db()->GetLiveFilesMetaData(&metadatas);
    std::vector<rocksdb::LiveFileMetaData> region_l6_metadatas;
    region_l6_metadatas.reserve(3);
    std::string file_has_raft_index;
    std::vector<std::string> input_files;
    input_files.reserve(5);
    bool l6_seqno_not_zero = false;
    for (const auto& meta : metadatas) {
        if (meta.column_family_name != RocksWrapper::DATA_CF) {
            continue;
        }

        if (meta.level == 0) {
            continue;
        }

        TableKey smallestkey(meta.smallestkey);
        TableKey largestkey(meta.largestkey);
        int64_t smallest_region_id = smallestkey.extract_i64(0);
        int64_t smallest_table_id  = smallestkey.extract_i64(sizeof(int64_t));
        int64_t largest_region_id  = largestkey.extract_i64(0);
        int64_t largest_table_id   = largestkey.extract_i64(sizeof(int64_t));
        if (smallest_region_id != largest_region_id) {
            // 没有完成region partition
            DB_WARNING("log_id: %lu smallest_region_id: %ld largest_region_id: %ld not partition", log_id, smallest_region_id, largest_region_id);
            response->set_errcode(pb::SUCCESS);
            response->set_errmsg("The partitioning has not been completed");
            response->clear_remote_files(); // 返回空文件列表，对端会进行local compaction
            return;
        }

        if (smallest_region_id == region_id) {
            print_metadata_info(meta);
            if (meta.level == 6) {
                if (meta.smallest_seqno != 0 || meta.largest_seqno != 0) {
                    l6_seqno_not_zero = true;
                }
                region_l6_metadatas.emplace_back(meta);
                if (smallest_table_id == 0) {
                    file_has_raft_index = meta.directory + "/" + meta.relative_filename;
                }
                input_files.emplace_back(meta.relative_filename);
            } else if (meta.level == 5) {
                input_files.emplace_back(meta.relative_filename);
            }
        }
    }

    if (region_l6_metadatas.empty() || file_has_raft_index.empty()) {
        DB_WARNING("log_id: %lu region_id: %ld not exist l6 files", log_id, region_id);
        response->set_errcode(pb::SUCCESS);
        response->set_errmsg("region not exist l6 files");
        response->clear_remote_files(); // 返回空文件列表，对端会进行local compaction
        return;
    }

    auto ext_fs = RocksWrapper::get_instance()->get_exteranl_filesystem();
    if (ext_fs == nullptr) {
        DB_WARNING("log_id: %lu ext fs is nullptr", log_id);
        response->set_errcode(pb::SUCCESS);
        response->set_errmsg("ext fs is nullptr");
        response->clear_remote_files();
        return;
    }

    std::shared_ptr<SstExtLinker> linker = get_sst_ext_linker();
    if (linker == nullptr) {
        DB_WARNING("log_id: %lu linker is nullptr", log_id);
        response->set_errcode(pb::SUCCESS);
        response->set_errmsg("linker is nullptr");
        response->clear_remote_files();
        return;
    }

    std::vector<std::string> external_files;
    external_files.reserve(region_l6_metadatas.size());
    ScopeGuard auto_decrease([&ext_fs, &external_files, log_id]() {
        for (const auto& external_file : external_files) {
            DB_WARNING("log_id: %lu delete external_file: %s", log_id, external_file.c_str());
            ext_fs->delete_path(external_file, false);
        }
    });

    int64_t min_raft_index = rocksdb->get_file_raft_index(file_has_raft_index, region_id);
    if (min_raft_index <= 0) {
        DB_WARNING("log_id: %lu local raft index: %ld <= 0, region_id: %ld, remote_side: %s", 
            log_id, min_raft_index, region_id, dst_address.c_str());
        response->set_errcode(pb::SUCCESS);
        response->set_errmsg("no raft index");
        response->clear_remote_files();
        return;
    }

    if (min_raft_index < raft_index || raft_index <= 0 || l6_seqno_not_zero) {
        DB_WARNING("log_id: %lu region_id: %ld min_raft_index: %ld remote_raft_index: %ld, remote_side: %s, l6_seqno_not_zero: %d", 
            log_id, region_id, min_raft_index, raft_index, dst_address.c_str(), l6_seqno_not_zero);
        response->set_errcode(pb::REMOTE_COMPACTION_ERROR);
        std::ostringstream os;
        os << "remote raft index: " << raft_index << " is bigger than local raft index: " << min_raft_index;
        response->set_errmsg(os.str());
        // 触发leader compaction
        if (input_files.size() > region_l6_metadatas.size() || l6_seqno_not_zero) {
            CompactionFiles compaction_files;
            compaction_files.output_level = 6;
            compaction_files.input_files = input_files;
            
            // 打印input_files和region_id信息
            std::ostringstream files_os;
            for (size_t i = 0; i < input_files.size(); ++i) {
                if (i > 0) {
                    files_os << ",";
                }
                files_os << input_files[i];
            }
            DB_WARNING("log_id: %lu trigger leader compaction, region_id: %ld, input_files: [%s], size: %lu, l6_seqno_not_zero: %d", 
                log_id, region_id, files_os.str().c_str(), input_files.size(), l6_seqno_not_zero);
            
            rocksdb->push_region_compaction_files(compaction_files);
        }
        return;
    }
    uint32_t now = (uint32_t)time(nullptr);
    for (const auto& meta : region_l6_metadatas) {
        SstExtLinker::ExtFileInfo ext_file_info;
        int ret = linker->get_ext_file_info(meta.relative_filename, ext_file_info);
        if (ret < 0) {
            // 没有外部文件，直接返回空文件列表，对端会进行local compaction
            DB_WARNING("log_id: %lu get ext file info failed, region_id: %ld file: %s", log_id, region_id, meta.relative_filename.c_str());
            response->set_errcode(pb::SUCCESS);
            response->set_errmsg("no external file info");
            response->clear_remote_files();
            return;
        }

        std::string full_src_path = ext_file_info.full_name;
        std::string file_name = make_cloud_file_name(get_store_ip_port(), region_id, raft_index, meta.file_number, meta.size, now);
        std::string dst_path = "baikal_cloud/" + FLAGS_meta_server_bns + "/" + FLAGS_resource_tag + "/" + dst_address + "/" + file_name;
        std::string full_dst_path = ext_fs->make_full_name("", false, dst_path);
        if (full_dst_path.empty()) {
            response->set_errcode(pb::SUCCESS);
            response->set_errmsg("make full dst path failed");
            response->clear_remote_files();
            return;
        }

        ret = ext_fs->link(full_src_path, full_dst_path);
        if (ret < 0) {
            DB_FATAL("log_id: %lu link file failed, region_id: %ld file: %s full_src_path: %s full_dst_path: %s", 
                log_id, region_id, meta.relative_filename.c_str(), full_src_path.c_str(), full_dst_path.c_str());
            response->set_errcode(pb::REMOTE_COMPACTION_ERROR);
            response->set_errmsg("link file failed");
            response->clear_remote_files();
            return;
        }

        external_files.emplace_back(full_dst_path);
        DB_WARNING("log_id: %lu link file success, region_id: %ld file: %s full_src_path: %s full_dst_path: %s", 
            log_id, region_id, meta.relative_filename.c_str(), full_src_path.c_str(), full_dst_path.c_str());
        auto remote_file = response->add_remote_files();
        remote_file->set_file_path(full_dst_path);
        remote_file->set_smallest_key(meta.smallestkey);
        remote_file->set_largest_key(meta.largestkey);
    }

    auto_decrease.release();
    response->set_errcode(pb::SUCCESS);
    return;
}

bool RegionCompactFiles::need_wait(int64_t region_id, int64_t raft_index) {
    std::unique_lock<std::mutex> l(_mutex);
    auto iter = _region_wait_time.find(region_id);
    if (iter != _region_wait_time.end()) {
        if (iter->second.raft_index == raft_index) {
            return butil::gettimeofday_us() - iter->second.begin_time < FLAGS_region_remote_compaction_wait_s * 1000000LL;
        } else {
            iter->second.raft_index = raft_index;
            iter->second.begin_time = butil::gettimeofday_us();
            return true;
        }
    } else {
        WaitInfo wait_info;
        wait_info.raft_index = raft_index;
        wait_info.begin_time = butil::gettimeofday_us();
        _region_wait_time[region_id] = wait_info;
        return true;
    }
}

} // namespace baikaldb