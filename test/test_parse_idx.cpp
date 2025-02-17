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
#include "olap_common.h"
#include <gflags/gflags.h>

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
DEFINE_string(idx_file_path, "./data/DailyFeedsURLUnitStats_PRIMARY_561933736_0_3099766_4319962719730142784_0.idx", "default idx_file_path");
DEFINE_string(hdr_file_path, "./data/DailyFeedsURLUnitStats_PRIMARY_561933736.hdr", "default idx_file_path");
DEFINE_string(baikaldb_resource, "baikaldb_olap_conf", "default baikaldb_input_resource");

TEST(test_parse_idx, case_all) {
    const std::string idx_file_path = FLAGS_idx_file_path;
    const std::string baikaldb_resource = FLAGS_baikaldb_resource; 
    baikal::client::Manager baikal_client_manager;
    int ret = baikal_client_manager.init("conf", "baikal_client.conf");
    ASSERT_TRUE(ret == 0); 

    baikal::client::Service* baikaldb_service = baikal_client_manager.get_service(baikaldb_resource);
    ASSERT_TRUE(baikaldb_service != nullptr);

    baikaldb::ParseHdrFile parse_hdr_handler(FLAGS_hdr_file_path);
    baikaldb::OlapStatus status = parse_hdr_handler.parse_hdr_file();
    ASSERT_TRUE(status == baikaldb::OLAP_SUCCESS);

    const pb::OLAPHeaderMessage& olap_header_message = parse_hdr_handler.get_pb_header_referrerence();
    baikaldb::ParseIdxFile parse_idx_handler(idx_file_path, olap_header_message.num_short_key_fields(), baikaldb_service);
    status = parse_idx_handler.parse_idx_header();
    ASSERT_TRUE(status == baikaldb::OLAP_SUCCESS);

    ret = parse_idx_handler.init_short_key();
    ASSERT_TRUE(ret == 0);

    status = parse_idx_handler.parse_idx_item();
    ASSERT_TRUE(status == baikaldb::OLAP_SUCCESS);
}
}