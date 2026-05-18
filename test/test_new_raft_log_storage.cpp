// Copyright (c) 2022 Baidu, Inc. All Rights Reserved.
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

#include <gtest/gtest.h>
#include <filesystem>

#include "log.h"
#include "my_raft_log_storage.h"
#include "rocks_wrapper.h"
#include <thread>
#include <time.h>
#include "key_encoder.h"
#include "log_entry_reader.h"
#include "table_key.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
DECLARE_int32(write_buffer_size);
DECLARE_int32(new_log_write_buffer_size);
DECLARE_int32(rocks_new_raft_log_size_mb);
}

void sleep_s(int64_t sleep_time_s) {
    struct timespec ts;
    ts.tv_sec = sleep_time_s;
    nanosleep(&ts, nullptr);
}

class TestManagerTest : public testing::Test {
public:
    ~TestManagerTest() override = default;
protected:
    void SetUp() override {
        baikaldb::ScopeGuard guard([this]() {
            ASSERT_TRUE(_is_init);
        });

        std::filesystem::path dir = DB_PATH;
        std::filesystem::remove_all(dir.parent_path());
        std::filesystem::create_directories(dir);
        baikaldb::FLAGS_rocks_new_raft_log_size_mb = 10;
        baikaldb::FLAGS_write_buffer_size = 1000 * 1000;
        baikaldb::FLAGS_new_log_write_buffer_size = baikaldb::FLAGS_write_buffer_size;
        baikaldb::FLAGS_enable_new_raft_log_storage = true;
        _rocksdb = baikaldb::RocksWrapper::get_instance();
        int ret = _rocksdb->init(DB_PATH);
        baikaldb::NewMyRaftLogStorage::set_first_log_idx_map_inited(false);
        if (ret != 0) {
            DB_FATAL("rocksdb init failed: code:%d", ret);
            return;
        }
        baikaldb::NewMyRaftLogStorage new_storage_factory;
        baikaldb::MyRaftLogStorage raft_storage_factory;
        _raft_storeage0 = dynamic_cast<baikaldb::NewMyRaftLogStorage*>(new_storage_factory.new_instance("raftlog://id=0"));
        _raft_storeage1 = dynamic_cast<baikaldb::NewMyRaftLogStorage*>(new_storage_factory.new_instance("raftlog://id=1"));
        _raft_storeage2 = dynamic_cast<baikaldb::NewMyRaftLogStorage*>(new_storage_factory.new_instance("raftlog://id=2"));

        _old_log_stprage0 = dynamic_cast<baikaldb::MyRaftLogStorage*>(raft_storage_factory.new_instance("raftlog://id=0"));
        _old_log_stprage1 = dynamic_cast<baikaldb::MyRaftLogStorage*>(raft_storage_factory.new_instance("raftlog://id=1"));
        _old_log_stprage2 = dynamic_cast<baikaldb::MyRaftLogStorage*>(raft_storage_factory.new_instance("raftlog://id=2"));
        if (_raft_storeage0 == nullptr || _raft_storeage1 == nullptr || _raft_storeage2 == nullptr) {
            DB_FATAL("new my_raft_log_storage failed");
            return;
        }
        if (_old_log_stprage0 == nullptr || _old_log_stprage1 == nullptr || _old_log_stprage2 == nullptr) {
            DB_FATAL("old my_raft_log_storage failed");
            return;
        }

        // 没有 ENTRY_TYPE_CONFIGURATION，因此传入nullptr不会有问题
        _raft_storeage0->init(nullptr);
        _raft_storeage1->init(nullptr);
        _raft_storeage2->init(nullptr);

        _old_log_stprage0->init(nullptr);
        _old_log_stprage1->init(nullptr);
        _old_log_stprage2->init(nullptr);
        _is_init = true;
    }

    template<typename T>
    static void delete_if_necessary(T*& ptr) {
        if (ptr != nullptr) {
            delete ptr;
        }
        ptr = nullptr;
    }

    void TearDown() override {
        delete_if_necessary(_raft_storeage0);
        delete_if_necessary(_raft_storeage1);
        delete_if_necessary(_raft_storeage2);

        delete_if_necessary(_old_log_stprage0);
        delete_if_necessary(_old_log_stprage1);
        delete_if_necessary(_old_log_stprage2);

        baikaldb::NewMyRaftLogStorage::remove_region_first_log_idx(0);
        baikaldb::NewMyRaftLogStorage::remove_region_first_log_idx(1);
        baikaldb::NewMyRaftLogStorage::remove_region_first_log_idx(2);
        _rocksdb->close();
        std::filesystem::path dir = DB_PATH;
        std::filesystem::remove_all(dir.parent_path());
    }

    void write_new_log(int64_t region_id, int from, int to, std::function<void(int64_t region_id, int64_t row_id)> check_and_do) {
        write_log(true, region_id, from, to, check_and_do);
    }

    void write_old_log(int64_t region_id, int from, int to, std::function<void(int64_t region_id, int64_t row_id)> check_and_do) {
        write_log(false, region_id, from, to, check_and_do);
    }

    // 不包含 to
    void write_log(bool is_new, int64_t region_id, int from, int to, std::function<void(int64_t region_id, int64_t row_id)> check_and_do) {
        baikaldb::MyRaftLogStorage* storage;
        if (is_new) {
            storage = get_new_storage(region_id);
        } else {
            storage = get_old_storage(region_id);
        }
        for (int i = from; i < to; i++) {
            auto log_entry = new braft::LogEntry;
            log_entry->id = {i, 0};
            log_entry->type =  braft::ENTRY_TYPE_DATA;
            log_entry->peers = new std::vector{
                braft::PeerId{base::EndPoint(base::IP_ANY, 8001)},
                braft::PeerId{base::EndPoint(base::IP_ANY, 8002)},
                braft::PeerId{base::EndPoint(base::IP_ANY, 8003)},
            };
            log_entry->data = "hello, this is regionid: " + std::to_string(region_id) + ", log_idx: " + std::to_string(i);
            storage->append_entry(log_entry);
            if ((i - from) % 10000 == 0) {
                DB_NOTICE("append logs, region_id: %ld, log_idx: %d", region_id, i);
            }
            if (check_and_do != nullptr) {
                check_and_do(region_id, i);
            }
        }
    }

    // check_func default just print meta.DebugString
    void read_properties(std::function<bool(const baikaldb::pb::RaftLogMeta& meta)> check_func) {
        rocksdb::TablePropertiesCollection table_props_collection;
        rocksdb::Status s = _rocksdb->get_new_raft_db()
                                ->GetPropertiesOfAllTables(_rocksdb->get_new_raft_log_handle(), &table_props_collection);
        if (!s.ok()) {
            GTEST_FAIL() << "GetPropertiesOfAllTables failed: " << s.ToString();
        }

        for (auto& kv : table_props_collection) {
            std::string file_number = kv.first;
            auto table_props = kv.second;

            std::cout << "\n--- SST #" << file_number << " ---" << std::endl;
            auto& user_props = table_props->user_collected_properties;
            auto it = user_props.find(baikaldb::NewRaftLogCollector::RAFT_LOG_PROPERTIES);
            if (it == user_props.end()) {
                GTEST_FAIL() << "No raft_log_properties found.";
            }

            const std::string& pb_string = it->second;

            baikaldb::pb::RaftLogMeta meta;
            if (!meta.ParseFromString(pb_string)) {
                GTEST_FAIL() << "Failed to parse pb!";
            }
            if (check_func != nullptr) {
                ASSERT_TRUE(check_func(meta));
            }
            std::cout << meta.DebugString() << std::endl;
        }
        std::cout << std::endl;
    }

    baikaldb::NewMyRaftLogStorage* get_new_storage(int i) {
        baikaldb::NewMyRaftLogStorage* storage = nullptr;
        switch (i) {
            case 0:
                storage = _raft_storeage0;
                break;
            case 1:
                storage = _raft_storeage1;
                break;
            case 2:
                storage = _raft_storeage2;
                break;
            default:
                DB_FATAL("unknown region id");
                assert(false);
        }
        return storage;
    }

    baikaldb::MyRaftLogStorage* get_old_storage(int i) {
        baikaldb::MyRaftLogStorage* storage = nullptr;
        switch (i) {
            case 0:
                storage = _old_log_stprage0;
                break;
            case 1:
                storage = _old_log_stprage1;
                break;
            case 2:
                storage = _old_log_stprage2;
                break;
            default:
                DB_FATAL("unknown region id");
                assert(false);
        }
        return storage;
    }

    std::string encode_log_data_key(int64_t region_id, int64_t logid) {
        char buf[baikaldb::MyRaftLogStorage::LOG_DATA_KEY_SIZE];
        //regionId
        uint64_t region_id_tmp = baikaldb::KeyEncoder::to_endian_u64(
            baikaldb::KeyEncoder::encode_i64(region_id));
        memcpy(buf, (void*)&region_id_tmp, sizeof(int64_t));
        //0x02
        memcpy((void*)((char*)buf + sizeof(int64_t)), &baikaldb::MyRaftLogStorage::LOG_DATA_IDENTIFY, 1);
        //index
        uint64_t index_tmp = baikaldb::KeyEncoder::to_endian_u64(
                            baikaldb::KeyEncoder::encode_i64(logid));
        memcpy((void*)((char*)buf + sizeof(int64_t) + 1),
                (void*)&index_tmp, sizeof(int64_t));
        return std::string(buf, baikaldb::MyRaftLogStorage::LOG_DATA_KEY_SIZE);
    }

    rocksdb::Status real_get_old(int64_t region_id, int64_t logid, std::string& value) {
        std::string key = encode_log_data_key(region_id, logid);
        rocksdb::Status status = _rocksdb->get(rocksdb::ReadOptions(), _rocksdb->get_raft_log_handle(),
            key, &value);
        return status;
    }

    rocksdb::Status real_get_new(int64_t region_id, int64_t logid, std::string& value) {
        std::string key = encode_log_data_key(region_id, logid);
        rocksdb::Status status = _rocksdb->new_log_get(rocksdb::ReadOptions(), _rocksdb->get_new_raft_log_handle(),
            key, &value);
        return status;
    }

    // 生成一个包含新旧数据的db
    // 0 - 99999 老数据， 100000-200000 新数据
    // 只操作region_id = 0
    void product_old_new_raft_log() {
        // 先按照baikaldb::FLAGS_enable_new_raft_log_storage = false重启db
        TearDown();
        std::filesystem::path dir = DB_PATH;
        std::filesystem::remove_all(dir.parent_path());
        std::filesystem::create_directories(dir);
        baikaldb::FLAGS_rocks_new_raft_log_size_mb = 10;
        baikaldb::FLAGS_write_buffer_size = 1000 * 1000;
        baikaldb::FLAGS_enable_new_raft_log_storage = false;
        _rocksdb = baikaldb::RocksWrapper::get_instance();
        int ret = _rocksdb->init(DB_PATH);
        ASSERT_EQ(0, ret);

        baikaldb::MyRaftLogStorage raft_storage_factory;
        _old_log_stprage0 = dynamic_cast<baikaldb::MyRaftLogStorage*>(raft_storage_factory.new_instance("raftlog://id=0"));
        _old_log_stprage0->init(nullptr);

        // 先写老的
        write_old_log(0, 1, 100000, nullptr);
        delete_if_necessary(_old_log_stprage0);

        // 关闭db后, FLAGS_enable_new_raft_log_storage = true 重新打开
        _rocksdb->close();
        baikaldb::FLAGS_enable_new_raft_log_storage = true;
        ret = _rocksdb->init(DB_PATH);
        ASSERT_EQ(0, ret);

        baikaldb::NewMyRaftLogStorage new_storage_factory;
        _raft_storeage0 = dynamic_cast<baikaldb::NewMyRaftLogStorage*>(new_storage_factory.new_instance("raftlog://id=0"));
        _raft_storeage0->init(nullptr);

        baikaldb::NewMyRaftLogStorage::set_first_log_idx_map_inited(true);
        // 写新的
        write_new_log(0, 100000, 200000, nullptr);
    }

    static constexpr const char* DB_PATH = "./tmp/raft_log_test_db/";
    baikaldb::RocksWrapper* _rocksdb = nullptr;
    baikaldb::NewMyRaftLogStorage* _raft_storeage0 = nullptr;
    baikaldb::NewMyRaftLogStorage* _raft_storeage1 = nullptr;
    baikaldb::NewMyRaftLogStorage* _raft_storeage2 = nullptr;

    baikaldb::MyRaftLogStorage* _old_log_stprage0 = nullptr;
    baikaldb::MyRaftLogStorage* _old_log_stprage1 = nullptr;
    baikaldb::MyRaftLogStorage* _old_log_stprage2 = nullptr;
    bool _is_init = false;
};

// 测试写入meta信息
TEST_F(TestManagerTest, test_write_meta) {
    std::vector<std::thread> write_threads;
    for (int i = 0; i < 3; ++i) {
        write_threads.emplace_back([this, i] {write_new_log(i, 1, 100000, nullptr);});
    }
    for (auto& thread : write_threads) {
        thread.join();
    }
    sleep_s(1);
    read_properties(nullptr);
}

TEST_F(TestManagerTest, test_fifo_compaction) {
    std::unordered_map<int64_t, int64_t> region_max_log_id_map {
        {0, 30000},
        {1, 30000},
        {2, 30000}
    };
    auto truncate = [this, &region_max_log_id_map] (int64_t region_id, int64_t row_id) {
        auto truncate_rows = region_max_log_id_map[region_id];
        if (row_id == truncate_rows + 1000) {
            DB_FATAL("truncate called, region_id: %ld", region_id);
            auto storage = get_new_storage(region_id);
            storage->truncate_prefix(truncate_rows);
        }
    };

    std::vector<std::thread> write_threads;
    for (int i = 0; i < 3; ++i) {
        write_threads.emplace_back([this, i, truncate] {write_new_log(i, 1, 100000, truncate);});
    }
    for (auto& thread : write_threads) {
        thread.join();
    }
    auto check_delete = [&region_max_log_id_map] (const baikaldb::pb::RaftLogMeta& meta) -> bool {
        bool need_delete = true;
        for (auto& kv : meta.region_max_log_id_map()) {
            int64_t region_id = kv.region_id();
            int64_t max_log_idx = kv.max_logid();
            if (region_max_log_id_map[region_id] <= max_log_idx) {
                need_delete = false;
            }
        }
        if (need_delete) {
            DB_WARNING("sst should be deleted, but not. meta: %s", meta.DebugString().c_str());
        }
        return !need_delete;
    };
    sleep_s(1);
    read_properties(check_delete);
}

// init 设为true前，FIRST_LOG_IDX_MAP 内不存在的region不可以删除
TEST_F(TestManagerTest, test_compaction_before_init) {
    baikaldb::NewMyRaftLogStorage::set_first_log_idx_map_inited(false);
    std::unordered_map<int64_t, int64_t> region_max_log_id_map {
            {0, 50000},
            {1, 50000}
    };
    baikaldb::NewMyRaftLogStorage::remove_region_first_log_idx(2);

    auto truncate = [this, &region_max_log_id_map] (int64_t region_id, int64_t row_id) {
        if (region_max_log_id_map.find(region_id) == region_max_log_id_map.end()) {
            return;
        }
        auto truncate_rows = region_max_log_id_map[region_id];
        if (row_id == truncate_rows + 1000) {
            DB_FATAL("truncate called, region_id: %ld", region_id);
            auto storage = get_new_storage(region_id);
            storage->truncate_prefix(truncate_rows);
        }
    };
    std::vector<std::thread> write_threads;
    for (int i = 0; i < 3; ++i) {
        write_threads.emplace_back([this, i, truncate] {write_new_log(i, 1, 100000, truncate);});
    }
    for (auto& thread : write_threads) {
        thread.join();
    }
    // 此时不该删除任何sst文件，存在region_id = 0/1 小于map内值的sst
    bool exist_sst_should_be_deleted = false;
    auto check_delete = [&region_max_log_id_map, &exist_sst_should_be_deleted] (const baikaldb::pb::RaftLogMeta& meta) -> bool {
        bool need_delete = true;
        for (auto& kv : meta.region_max_log_id_map()) {
            int64_t region_id = kv.region_id();
            int64_t max_log_idx = kv.max_logid();
            if (region_max_log_id_map.find(region_id) == region_max_log_id_map.end()) {
                continue;
            }
            if (region_max_log_id_map[region_id] <= max_log_idx) {
                need_delete = false;
            }
        }
        if (need_delete) {
            exist_sst_should_be_deleted = true;
        }
        return true;
    };
    sleep_s(1);
    read_properties(check_delete);
    ASSERT_TRUE(exist_sst_should_be_deleted);

    baikaldb::NewMyRaftLogStorage::set_first_log_idx_map_inited(true);
    write_threads.clear();
    for (int i = 0; i < 2; ++i) {
        write_threads.emplace_back([this, i, truncate] {write_new_log(i, 100000, 200000, truncate);});
    }
    for (auto& thread : write_threads) {
        thread.join();
    }

    // 此时应该无视meta内的region_id = 2删除sst文件
    auto check_delete2 = [&region_max_log_id_map] (const baikaldb::pb::RaftLogMeta& meta) -> bool {
        bool need_delete = true;
        for (auto& kv : meta.region_max_log_id_map()) {
            int64_t region_id = kv.region_id();
            int64_t max_log_idx = kv.max_logid();
            if (region_max_log_id_map.find(region_id) == region_max_log_id_map.end()) {
                continue;
            }
            if (region_max_log_id_map[region_id] <= max_log_idx) {
                need_delete = false;
            }
        }
        if (need_delete) {
            DB_WARNING("sst should be deleted, but not. meta: %s", meta.DebugString().c_str());
        }
        return !need_delete;
    };
    sleep_s(1);
    read_properties(check_delete2);
}

// 向后兼容测试
// 先写老的，再写新的，再读取/truncate
TEST_F(TestManagerTest, test_truncate_perfix) {
    product_old_new_raft_log();
    std::vector<int64_t> log_ids = {1, 101, 1001, 10001, 100001, 177777, 199999};
    for (auto& log_id : log_ids) {
        braft::LogEntry* log_entry = _raft_storeage0->get_entry(log_id);
        ASSERT_EQ(log_entry->id.index, log_id);
    }

    //----------------------------- truncate perfix -------------------------------
    _raft_storeage0->truncate_prefix(10000);
    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(5000));
    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(9999));
    ASSERT_NE(nullptr, _raft_storeage0->get_entry(10000));
    std::string value;
    rocksdb::Status s = real_get_old(0, 5000, value);
    ASSERT_TRUE(s.IsNotFound());

    _raft_storeage0->truncate_prefix(150000);
    // old应当立刻删除
    s = real_get_old(0, 70000, value);
    ASSERT_TRUE(s.IsNotFound());
    s = real_get_old(0, 99999, value);
    ASSERT_TRUE(s.IsNotFound());

    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(110000));
    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(149999));
    ASSERT_NE(nullptr, _raft_storeage0->get_entry(150000));
    // new应当还在
    s = real_get_new(0, 110000, value);
    ASSERT_EQ(rocksdb::Status::OK(), s);
    s = real_get_new(0, 149999, value);
    ASSERT_EQ(rocksdb::Status::OK(), s);

    // 再插入更多数据触发compaction
    write_new_log(0, 200000, 400000, nullptr);
    // sleep 1s 保证compaction任务被执行
    sleep_s(3);
    // 此时新的应该也被删除
    s = real_get_new(0, 100001, value);
    ASSERT_TRUE(s.IsNotFound());
}

TEST_F(TestManagerTest, test_truncate_suffix) {
    product_old_new_raft_log();
    auto ASSERT_REAL_NOT_EXIST = [this](bool is_new, int64_t region_id, int64_t logid) {
        std::string value;
        if (is_new) {
            rocksdb::Status s = real_get_new(region_id, logid, value);
            ASSERT_TRUE(s.IsNotFound());
        } else {
            rocksdb::Status s = real_get_old(region_id, logid, value);
            ASSERT_TRUE(s.IsNotFound());
        }
    };

    _raft_storeage0->truncate_suffix(180000);
    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(190000));
    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(180001));
    ASSERT_NE(nullptr, _raft_storeage0->get_entry(180000));
    ASSERT_REAL_NOT_EXIST(true, 0, 190000);
    ASSERT_REAL_NOT_EXIST(true, 0, 180001);

    _raft_storeage0->truncate_suffix(90000);

    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(130000));
    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(99999));
    ASSERT_EQ(nullptr, _raft_storeage0->get_entry(90001));
    ASSERT_NE(nullptr, _raft_storeage0->get_entry(90000));
    ASSERT_REAL_NOT_EXIST(false, 0, 90001);
    ASSERT_REAL_NOT_EXIST(false, 0, 99999);
}

TEST_F(TestManagerTest, test_log_entry_reader) {
    product_old_new_raft_log();
    sleep_s(1);

    auto reader = baikaldb::LogEntryReader::get_instance();
    reader->init(_rocksdb, _rocksdb->get_raft_log_handle(), _rocksdb->get_new_raft_log_handle());

    _raft_storeage0->truncate_prefix(5000);
    std::string log_string;
    ASSERT_NE(0, reader->read_log_entry(0, 2000, log_string));
    ASSERT_EQ(0, reader->read_log_entry(0, 40000, log_string));
    ASSERT_EQ(0, reader->read_log_entry(0, 180000, log_string));

    int64_t start_idx, end_idx, break_at, current_idx;

    auto set_paramater = [&] (int64_t start, int64_t end, int64_t break_num) {
        start_idx = start;
        end_idx = end;
        break_at = break_num;
        current_idx = start;
    };
    // typedef std::function<int(const rocksdb::Slice&, const rocksdb::Slice&, bool& need_break)> KV_PROCESS_FUNC;
    auto process_single_log = [&current_idx, &break_at] (const rocksdb::Slice& key, const rocksdb::Slice& value, bool& need_break) {
        int64_t log_index = baikaldb::TableKey(key).extract_i64(sizeof(int64_t) + 1);
        if (break_at == log_index) {
            need_break = true;
            DB_WARNING("break! log_index: %ld", log_index);
            return 0;
        }
        if (current_idx != log_index) {
            DB_FATAL("current_idx[%ld] doesn't eq to log_index[%ld]", current_idx, log_index);
            return -1;
        }
        ++ current_idx;
        return 0;
    };

    int ret = 0;

    // all old
    set_paramater(10000, 30000, 200000);
    ret = reader->process_logs(0, start_idx, end_idx, process_single_log);
    ASSERT_EQ(0, ret);

    // all new
    set_paramater(150000, 160000, 200000);
    ret = reader->process_logs(0, start_idx, end_idx, process_single_log);
    ASSERT_EQ(0, ret);

    // half half
    set_paramater(90000, 120000, 200000);
    ret = reader->process_logs(0, start_idx, end_idx, process_single_log);
    ASSERT_EQ(0, ret);

    // break
    set_paramater(90000, 120000, 110000);
    ret = reader->process_logs(0, start_idx, end_idx, process_single_log);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(110000, current_idx);

    // no end
    set_paramater(190000, -1, 300000);
    ret = reader->process_logs(0, start_idx, end_idx, process_single_log);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(200000, current_idx);
}