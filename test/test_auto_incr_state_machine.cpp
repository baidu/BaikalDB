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
#include "auto_incr_state_machine.h"

class AutoIncrStateMachineTest : public testing::Test {
public:
    ~AutoIncrStateMachineTest() {}
protected:
    virtual void SetUp() {
        butil::EndPoint addr;
        addr.ip = butil::my_ip();
        addr.port = 8110;
        braft::PeerId peer_id(addr, 0);
        _auto_incr = new baikaldb::AutoIncrStateMachine(peer_id);
    }
    virtual void TearDown() {}
    baikaldb::AutoIncrStateMachine* _auto_incr;
};
// add_logic add_physical add_instance
TEST_F(AutoIncrStateMachineTest, test_create_drop_modify) {
    //test_point: add_table
    baikaldb::pb::MetaManagerRequest add_table_id_request;
    add_table_id_request.set_op_type(baikaldb::pb::OP_ADD_ID_FOR_AUTO_INCREMENT);
    add_table_id_request.mutable_auto_increment()->set_table_id(1);
    add_table_id_request.mutable_auto_increment()->set_start_id(10);
    _auto_incr->add_table_id(add_table_id_request, NULL);
    ASSERT_EQ(1, _auto_incr->_auto_increment_map.size());
    ASSERT_EQ(10, _auto_incr->_auto_increment_map[1]);
    
    //test_point: add_table
    add_table_id_request.mutable_auto_increment()->set_table_id(2);
    add_table_id_request.mutable_auto_increment()->set_start_id(1);
    _auto_incr->add_table_id(add_table_id_request, NULL);
    ASSERT_EQ(2, _auto_incr->_auto_increment_map.size());
    ASSERT_EQ(10, _auto_incr->_auto_increment_map[1]);
    ASSERT_EQ(1, _auto_incr->_auto_increment_map[2]);
    //test_point: gen_id
    baikaldb::pb::MetaManagerRequest gen_id_request;
    gen_id_request.set_op_type(baikaldb::pb::OP_GEN_ID_FOR_AUTO_INCREMENT);
    gen_id_request.mutable_auto_increment()->set_table_id(1);
    gen_id_request.mutable_auto_increment()->set_count(13);
    _auto_incr->gen_id(gen_id_request, NULL);
    ASSERT_EQ(2, _auto_incr->_auto_increment_map.size());
    ASSERT_EQ(23, _auto_incr->_auto_increment_map[1]);

    //test_point: gen_id
    gen_id_request.mutable_auto_increment()->set_start_id(25);
    gen_id_request.mutable_auto_increment()->set_count(10);
    _auto_incr->gen_id(gen_id_request, NULL);
    ASSERT_EQ(2, _auto_incr->_auto_increment_map.size());
    ASSERT_EQ(36, _auto_incr->_auto_increment_map[1]);

    //test_point: update
    baikaldb::pb::MetaManagerRequest update_id_request;
    update_id_request.set_op_type(baikaldb::pb::OP_UPDATE_FOR_AUTO_INCREMENT);
    update_id_request.mutable_auto_increment()->set_table_id(1);
    update_id_request.mutable_auto_increment()->set_start_id(38);
    _auto_incr->gen_id(update_id_request, NULL);
    ASSERT_EQ(2, _auto_incr->_auto_increment_map.size());
    ASSERT_EQ(39, _auto_incr->_auto_increment_map[1]);

    //test_point: drop_table_id
    baikaldb::pb::MetaManagerRequest drop_id_request;
    drop_id_request.set_op_type(baikaldb::pb::OP_DROP_ID_FOR_AUTO_INCREMENT);
    drop_id_request.mutable_auto_increment()->set_table_id(2);
    _auto_incr->drop_table_id(drop_id_request, NULL);
    ASSERT_EQ(1, _auto_incr->_auto_increment_map.size());
    ASSERT_EQ(39, _auto_incr->_auto_increment_map[1]);

    std::string max_id_string;
    _auto_incr->save_auto_increment(max_id_string);
    _auto_incr->parse_json_string(max_id_string);
    ASSERT_EQ(1, _auto_incr->_auto_increment_map.size());
    ASSERT_EQ(39, _auto_incr->_auto_increment_map[1]);
} // TEST_F
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
