// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include "gtest/gtest.h"
#include "schema_factory.h"
#include "fetcher_store.h"
#include "proto/meta.interface.pb.h"
#include "proto/store.interface.pb.h"
#include <gflags/gflags.h>
namespace baikaldb {
DECLARE_bool(fetcher_follower_read);
DECLARE_bool(fetcher_learner_read);
DECLARE_string(insulate_fetcher_resource_tag);
DECLARE_string(fetcher_resource_tag);
}

using namespace baikaldb;
class FetcherStoreTest : public testing::Test {
public:
    ~FetcherStoreTest() {}
protected:
    virtual void SetUp() {
        auto schema_factory = baikaldb::SchemaFactory::get_instance();
        _instance_info["127.0.0.1:pap-bd1"] = "pap";
        _instance_info["127.0.0.1:pap-bd2"] = "pap";
        _instance_info["127.0.0.1:pap-bd3"] = "pap";
        _instance_info["127.0.0.1:pap-bj1"] = "pap";
        _instance_info["127.0.0.1:pap-bj2"] = "pap";
        _instance_info["127.0.0.1:pap-bj3"] = "pap";
        _instance_info["127.0.0.1:watt-bj1"] = "watt";
        _instance_info["127.0.0.1:watt-bj2"] = "watt";
        _instance_info["127.0.0.1:watt-bj3"] = "watt";
        _instance_info["127.0.0.1:offline-bd1"] = "offline";
        _instance_info["127.0.0.1:offline-bd2"] = "offline";
        _instance_info["127.0.0.1:offline-bd3"] = "offline";

        _instance_logical_map["127.0.0.1:pap-bd1"] = "bd";
        _instance_logical_map["127.0.0.1:pap-bd2"] = "bd";
        _instance_logical_map["127.0.0.1:pap-bd3"] = "bd";
        _instance_logical_map["127.0.0.1:pap-bj1"] = "bj";
        _instance_logical_map["127.0.0.1:pap-bj2"] = "bj";
        _instance_logical_map["127.0.0.1:pap-bj3"] = "bj";
        _instance_logical_map["127.0.0.1:watt-bj1"] = "bj";
        _instance_logical_map["127.0.0.1:watt-bj2"] = "bj";
        _instance_logical_map["127.0.0.1:watt-bj3"] = "bj";
        _instance_logical_map["127.0.0.1:offline-bd1"] = "bd";
        _instance_logical_map["127.0.0.1:offline-bd2"] = "bd";
        _instance_logical_map["127.0.0.1:offline-bd3"] = "bd";
        
        pb::IdcInfo idc;
        auto m_bj = idc.add_logical_physical_map();
        m_bj->set_logical_room("bj");
        m_bj->add_physical_rooms("bj");
        auto m_bd = idc.add_logical_physical_map();
        m_bd->set_logical_room("bd");
        m_bd->add_physical_rooms("bd");
        for (auto& pair : _instance_logical_map) {
            auto instance = idc.add_instance_infos();
            instance->set_address(pair.first);
            instance->set_logical_room(pair.second);
            instance->set_physical_room(pair.second);
            auto iter = _instance_info.find(pair.first);
            if (iter != _instance_info.end()) {
                instance->set_resource_tag(iter->second);
            }           
        }
        schema_factory->update_idc(idc);
        schema_factory->_logical_room = "bj";
    }
    void set_all_instance_normal() {
        for (const auto addr : _instance_info) {
            for (auto i = 0; i < 20; ++i) {
                baikaldb::SchemaFactory::get_instance()->update_instance(addr.first, pb::NORMAL, false, false);
            }
        }
    }
    std::map<std::string, std::string> _instance_info;
    std::map<std::string, std::string> _instance_logical_map;
};

// 非select一定选leader
TEST_F(FetcherStoreTest, test_not_select) {
    set_all_instance_normal();
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    region.add_peers("127.0.0.1:watt-bj1");
    region.add_peers("127.0.0.1:offline-bd1");
    FetcherStore fetcher_store;
    RuntimeState state;
    ExecNode store_request;
    OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_INSERT, false);
    {
        FLAGS_fetcher_follower_read = false;
        FLAGS_fetcher_learner_read = false;
        FLAGS_insulate_fetcher_resource_tag = "";
        done._retry_times = 0;
        for (int i = 0; i < 5; ++i) {
            done.select_addr();
            done.retry_times_inc();
            ASSERT_TRUE("127.0.0.1:pap-bd1" == done._addr);
        }
    }
    {
        FLAGS_fetcher_follower_read = true;
        FLAGS_fetcher_learner_read = true;
        FLAGS_insulate_fetcher_resource_tag = "";
        done._retry_times = 0;
        for (int i = 0; i < 5; ++i) {
            done.select_addr();
            done.retry_times_inc();
            ASSERT_TRUE("127.0.0.1:pap-bd1" == done._addr);
        }
    }
    {
        FLAGS_fetcher_follower_read = true;
        FLAGS_fetcher_learner_read = true;
        FLAGS_insulate_fetcher_resource_tag = "watt";
        done._retry_times = 0;
        for (int i = 0; i < 5; ++i) {
            done.select_addr();
            done.retry_times_inc();
            ASSERT_TRUE("127.0.0.1:pap-bd1" == done._addr);
        }
    }
    {
        FLAGS_fetcher_follower_read = true;
        FLAGS_fetcher_learner_read = true;
        FLAGS_insulate_fetcher_resource_tag = "watt";
        // leader faulty, 选leader
        done._retry_times = 0;
        schema_factory->update_instance("127.0.0.1:pap-bd1", pb::FAULTY, false, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bd1" == done._addr);

        // 返回not leader和新leader, 选新leader
        done._response_ptr->set_errcode(pb::NOT_LEADER);
        done._response_ptr->set_leader("127.0.0.1:pap-bd2");
        done.handle_response(done._addr);
        done.retry_times_inc();
        schema_factory->update_instance("127.0.0.1:pap-bd1", pb::NORMAL, false, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bd2" == done._addr);
    }
} 

TEST_F(FetcherStoreTest, test_normal_follower_select) {
    set_all_instance_normal();
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    region.add_peers("127.0.0.1:watt-bj1");
    region.add_peers("127.0.0.1:offline-bd1");
    FLAGS_fetcher_follower_read = true;
    FLAGS_fetcher_learner_read = true;
    FLAGS_insulate_fetcher_resource_tag = "";
    {
        std::string baikaldb_logical_room = SchemaFactory::get_instance()->get_logical_room();
        DB_WARNING("baikaldb_logical_room: %s", baikaldb_logical_room.c_str());

        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr || "127.0.0.1:watt-bj1" == done._addr);
    }
    region.add_learners("127.0.0.1:watt-bj2"); // 加入learner
    {
        // 有learner选learner
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj2" == done._addr); 
    }
    {
        // learner not ready 重试peer
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj2" == done._addr); 
        done._response_ptr->set_errcode(pb::LEARNER_NOT_READY);
        done._response_ptr->set_leader("127.0.0.1:pap-bd2");
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr || "127.0.0.1:watt-bj1" == done._addr);
    }
    region.add_learners("127.0.0.1:offline-bd2"); // 加入learner
    {
        // learner resource tag中间可以相互降级
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj2" == done._addr); 
        done._response_ptr->set_errcode(pb::LEARNER_NOT_READY);
        done._response_ptr->set_leader("127.0.0.1:pap-bd2");
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:offline-bd2" == done._addr);
    }
} 

TEST_F(FetcherStoreTest, test_resource_insulate_read_without_learner) {
    set_all_instance_normal();
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    region.add_peers("127.0.0.1:watt-bj1");
    region.add_peers("127.0.0.1:offline-bd1");
    FLAGS_fetcher_follower_read = true;
    FLAGS_fetcher_learner_read = true;
    FLAGS_insulate_fetcher_resource_tag = "watt";
    {
        // 选watt peer
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        for (int i = 0; i < 5; ++i) {
            done.select_addr();
            done.retry_times_inc();
            ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
        }
    }
    {
        // watt peer返回not_leader读失败，选其他peer
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
        schema_factory->update_instance("127.0.0.1:watt-bj1", pb::FAULTY, false, false);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
        done._response_ptr->set_errcode(pb::NOT_LEADER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" != done._addr);
        schema_factory->update_instance("127.0.0.1:watt-bj1", pb::NORMAL, false, false);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" != done._addr);
    }
    {
        // watt peer can not access，选其他peer
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
        // watt-bj1 异常
        fetcher_store.peer_status.set_cannot_access(region.region_id(), done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr);
    }
} 


TEST_F(FetcherStoreTest, test_resource_insulate_read_with_learner) {
    set_all_instance_normal();
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    region.add_peers("127.0.0.1:watt-bj1");
    region.add_peers("127.0.0.1:offline-bd1");
    region.add_learners("127.0.0.1:watt-bj2");
    FLAGS_fetcher_follower_read = true;
    FLAGS_fetcher_learner_read = true;
    FLAGS_insulate_fetcher_resource_tag = "watt";
    {
        // resource tag又有learner，又有peer，fetcher_learner_read = false下选peer
        FLAGS_fetcher_learner_read = false;
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        for (int i = 0; i < 5; ++i) {
            done.select_addr();
            done.retry_times_inc();
            ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr); // 访问watt learner
        }
    }
    FLAGS_fetcher_learner_read = true;
    {
        // resource tag又有learner，又有peer，fetcher_learner_read = true下选learner
        // learner方式失败选peer
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj2" == done._addr);
        schema_factory->update_instance("127.0.0.1:watt-bj2", pb::FAULTY, false, false);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj2" == done._addr);
        done._response_ptr->set_errcode(pb::LEARNER_NOT_READY); // learner访问失败
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr); // 访问watt peer
        schema_factory->update_instance("127.0.0.1:watt-bj2", pb::NORMAL, false, false);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
    }
    {
        // resource tag又有learner，又有peer
        // learner not access选peer, peer失败选其他peer
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj2" == done._addr);
        // watt-bj2 异常
        fetcher_store.peer_status.set_cannot_access(region.region_id(), done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
        fetcher_store.peer_status.set_cannot_access(region.region_id(), done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" != done._addr && "127.0.0.1:watt-bj2" != done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" != done._addr && "127.0.0.1:watt-bj2" != done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" != done._addr && "127.0.0.1:watt-bj2" != done._addr);
    }
} 

TEST_F(FetcherStoreTest, test_resource_perfer_read) {
    set_all_instance_normal();
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    region.add_peers("127.0.0.1:watt-bj1");
    region.add_peers("127.0.0.1:offline-bd1");
    FLAGS_fetcher_follower_read = true;
    FLAGS_fetcher_learner_read = true;
    FLAGS_insulate_fetcher_resource_tag = "";
    FLAGS_fetcher_resource_tag = "pap";
    {
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr);
        done._response_ptr->set_errcode(pb::NOT_LEADER);
        done._response_ptr->set_leader("127.0.0.1:pap-bd2");
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bd2" == done._addr);
    }
    {
        // 有learner，选learner，learner不可用选leader
        region.add_learners("127.0.0.1:offline-bj2");
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:offline-bj2" == done._addr);
        done._response_ptr->set_errcode(pb::LEARNER_NOT_READY);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr || "127.0.0.1:watt-bj1" == done._addr);
    }
}

TEST_F(FetcherStoreTest, test_GBT_LEARNER_read) {
    set_all_instance_normal();
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    region.add_peers("127.0.0.1:watt-bj1");
    region.add_peers("127.0.0.1:offline-bd1");
    region.add_learners("127.0.0.1:offline-bd2");
    FLAGS_fetcher_follower_read = true;
    FLAGS_fetcher_learner_read = true;
    FLAGS_insulate_fetcher_resource_tag = "";
    {
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        fetcher_store.global_backup_type = GBT_LEARNER;
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:offline-bd2" == done._addr);
        done._response_ptr->set_errcode(pb::LEARNER_NOT_READY);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:offline-bd2" != done._addr);
    }
    {
        FLAGS_insulate_fetcher_resource_tag = "watt";
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        fetcher_store.global_backup_type = GBT_LEARNER;
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:offline-bd2" == done._addr);
        done._response_ptr->set_errcode(pb::LEARNER_NOT_READY);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:offline-bd2" != done._addr);
    }
}

TEST_F(FetcherStoreTest, test_retry_later_choose_other_peer_read1) {
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    {
        FLAGS_insulate_fetcher_resource_tag = "";
        FLAGS_fetcher_learner_read = false;
        FLAGS_fetcher_follower_read = false;
        FLAGS_fetcher_resource_tag = "";
        set_all_instance_normal();
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        // 不支持读从, 访问leader, retry_later重试也访问leader
        ASSERT_TRUE("127.0.0.1:pap-bd1" == done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        auto peer1 = done._addr;
        ASSERT_TRUE("127.0.0.1:pap-bd1" == done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:pap-bd1" == done._addr);
    }
    {
        FLAGS_insulate_fetcher_resource_tag = "";
        FLAGS_fetcher_learner_read = false;
        FLAGS_fetcher_follower_read = true;
        FLAGS_fetcher_resource_tag = "";
        set_all_instance_normal();
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        // 支持读从, 默认访问同机房, retry_later能访问其他peer
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        // RETRY_LATER访问其他peer
        auto peer1 = done._addr;
        ASSERT_TRUE("127.0.0.1:pap-bj1" != done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        // 再次retry later访问最后一个peer
        ASSERT_TRUE("127.0.0.1:pap-bj1" != done._addr && peer1 != done._addr);
    }
}

TEST_F(FetcherStoreTest, test_retry_later_choose_other_peer_read2) {
    auto schema_factory = baikaldb::SchemaFactory::get_instance();
    pb::RegionInfo region;
    region.set_region_id(1);
    region.set_leader("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd1");
    region.add_peers("127.0.0.1:pap-bd2");
    region.add_peers("127.0.0.1:pap-bj1");
    region.add_peers("127.0.0.1:watt-bj1");
    region.add_peers("127.0.0.1:offline-bd1");
    {
        // 资源优先读, retry_later会重试其他peer
        set_all_instance_normal();
        FLAGS_fetcher_follower_read = true;
        FLAGS_fetcher_learner_read = true;
        FLAGS_fetcher_resource_tag = "pap";
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        // 默认访问pap-bj同机房
        ASSERT_TRUE("127.0.0.1:pap-bj1" == done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        // RETRY_LATER访问其他peer
        auto peer1 = done._addr;
        ASSERT_TRUE("127.0.0.1:pap-bj1" != done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        // RETRY_LATER访问其他peer
        ASSERT_TRUE("127.0.0.1:pap-bj1" != done._addr && peer1 != done._addr);
    }
    {
        // 资源隔离读, retry_later不会重试其他peer
        set_all_instance_normal();
        FLAGS_fetcher_follower_read = true;
        FLAGS_fetcher_learner_read = true;
        FLAGS_insulate_fetcher_resource_tag = "watt";
        FLAGS_fetcher_resource_tag = "";
        FetcherStore fetcher_store;
        RuntimeState state;
        ExecNode store_request;
        OnSingleRPCDone done(&fetcher_store, &state, &store_request, &region, 1, 1, 0, 0, pb::OP_SELECT, false);
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
        done._response_ptr->set_errcode(pb::RETRY_LATER);
        done.handle_response(done._addr);
        done.retry_times_inc();
        done.select_addr();
        ASSERT_TRUE("127.0.0.1:watt-bj1" == done._addr);
    }
}
 // TEST_F
