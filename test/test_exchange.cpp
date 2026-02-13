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

#include "arrow/array/concatenate.h"
#include "arrow_exec_node.h"
#include "data_stream_manager.h"
#include "exchange_sender_node.h"
#include "exchange_receiver_node.h"
#include "db_service.h"
#include "region.h"
#include <arrow/compute/cast.h>
#include <gflags/gflags.h>

namespace baikaldb {

using namespace arrow;

//// 测试数据构造
pb::TransmitDataParam make_invalid_transmit_data_param() {
    pb::TransmitDataParam transmit_data_param;
    transmit_data_param.set_exchange_state(pb::ES_DOING);
    transmit_data_param.set_log_id(-1);
    transmit_data_param.set_sender_fragment_instance_id(-1);
    transmit_data_param.set_receiver_fragment_instance_id(-1);
    transmit_data_param.set_receiver_node_id(-1);
    return transmit_data_param;
}

std::shared_ptr<arrow::Array> make_test_array(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    builder.AppendValues(values);
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    return array;
}
std::shared_ptr<arrow::Array> make_test_double_array(const std::vector<double>& values) {
    arrow::DoubleBuilder builder;
    builder.AppendValues(values);
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    return array;
}
std::shared_ptr<arrow::Array> make_test_string_array(const std::vector<std::string>& values) {
    arrow::BinaryBuilder builder;
    builder.AppendValues(values);
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    return array;
}
std::shared_ptr<arrow::Array> make_test_null_array() {
    arrow::NullBuilder builder;
    builder.AppendNull();
    builder.Append(nullptr);
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    return array;
}

std::shared_ptr<arrow::Array> make_test_array_tmp(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    // builder.AppendValues(values);
    builder.AppendNull();
    builder.AppendNull();
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);
    return array;
}
std::shared_ptr<arrow::Schema> make_test_schema() {
    return arrow::schema({arrow::field("col1", arrow::int64()), 
                          arrow::field("col2", arrow::int64()), 
                          arrow::field("col3", arrow::int64())});
}
std::shared_ptr<arrow::Schema> make_test_schema2() {
    return arrow::schema({arrow::field("col1", arrow::int64()), 
                          arrow::field("col2", arrow::int64()), 
                          arrow::field("col3", arrow::utf8())});
}
std::shared_ptr<arrow::Schema> make_test_schema3() {
    return arrow::schema({arrow::field("col3", arrow::int64()), 
                          arrow::field("col1", arrow::float64()), 
                          arrow::field("col2", arrow::utf8())});
}
std::shared_ptr<arrow::Schema> make_test_null_schema() {
    return arrow::schema({arrow::field("col1", arrow::int64()), 
                          arrow::field("col2", arrow::int64()),
                          arrow::field("col3", arrow::int64())});
}
std::shared_ptr<arrow::RecordBatch> make_test_empty_record_batch_schema1() {
    auto out = arrow::RecordBatch::MakeEmpty(make_test_schema());
    return *out;
}
std::shared_ptr<arrow::RecordBatch> make_test_record_batch1_schema1() {
    std::shared_ptr<arrow::Array> array1 = make_test_array({1,2,3,4,5,6,7,8,9,10});
    std::shared_ptr<arrow::Array> array2 = make_test_array({11,12,13,14,15,16,17,18,19,20});
    std::shared_ptr<arrow::Array> array3 = make_test_array({21,22,23,24,25,26,27,28,29,30});
    std::shared_ptr<arrow::RecordBatch> out = 
        arrow::RecordBatch::Make(make_test_schema(), 10, {array1, array2, array3});
    return out;
}
std::shared_ptr<arrow::RecordBatch> make_test_record_batch2_schema1() {
    std::shared_ptr<arrow::Array> array1 = make_test_array({31,32,33,34,35,36,37,38});
    std::shared_ptr<arrow::Array> array2 = make_test_array({41,42,43,44,45,46,47,48});
    std::shared_ptr<arrow::Array> array3 = make_test_array({51,52,53,54,55,56,57,58});
    std::shared_ptr<arrow::RecordBatch> out = 
        arrow::RecordBatch::Make(make_test_schema(), 8, {array1, array2, array3});
    return out;
}
std::shared_ptr<arrow::RecordBatch> make_test_record_batch3_schema1() {
    std::shared_ptr<arrow::Array> array1 = make_test_array({61,62,63,64,65,66,67,68,69});
    std::shared_ptr<arrow::Array> array2 = make_test_array({71,72,73,74,75,76,77,78,79});
    std::shared_ptr<arrow::Array> array3 = make_test_array({81,82,83,84,85,86,87,88,89});
    std::shared_ptr<arrow::RecordBatch> out = 
        arrow::RecordBatch::Make(make_test_schema(), 9, {array1, array2, array3});
    return out;
}
std::shared_ptr<arrow::RecordBatch> make_test_record_batch1_schema2() {
    std::shared_ptr<arrow::Array> array1 = make_test_array({1,2,3,4,5,6,7,8,9,10});
    std::shared_ptr<arrow::Array> array2 = make_test_array({11,12,13,14,15,16,17,18,19,20});
    std::shared_ptr<arrow::Array> array3 = make_test_string_array({"21","22","23","24","25","26","27","28","29","20"});
    std::shared_ptr<arrow::RecordBatch> out = 
        arrow::RecordBatch::Make(make_test_schema2(), 10, {array1, array2, array3});
    return out;
}
std::shared_ptr<arrow::RecordBatch> make_test_record_batch_null_schema() {
    std::shared_ptr<arrow::Array> array1 = make_test_null_array();
    std::shared_ptr<arrow::Array> array2 = make_test_null_array();
    std::shared_ptr<arrow::Array> array3 = make_test_null_array();
    std::shared_ptr<arrow::RecordBatch> out = 
        arrow::RecordBatch::Make(make_test_null_schema(), 2, {array1, array2, array3});
    return out;
}
pb::PlanNode make_exchange_sender_node() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_SENDER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeSenderNode* exchange_sender_node = derive_node->mutable_exchange_sender_node();
    exchange_sender_node->set_log_id(100);
    exchange_sender_node->set_fragment_id(7);
    exchange_sender_node->set_receiver_fragment_id(8);
    exchange_sender_node->set_fragment_instance_id(77);
    exchange_sender_node->mutable_partition_property()->set_type(pb::HashPartitionType);
    exchange_sender_node->mutable_partition_property()->add_hash_cols("col1");
    exchange_sender_node->mutable_partition_property()->add_hash_cols("col3");
    pb::ExchangeDestination* receiver_destination = exchange_sender_node->add_receiver_destinations();
    receiver_destination->set_fragment_instance_id(88);
    receiver_destination->set_node_id(99);
    receiver_destination->set_address("10.12.187.120:8787");
    return plan_node;
}
pb::PlanNode make_exchange_sender_node2() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_SENDER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeSenderNode* exchange_sender_node = derive_node->mutable_exchange_sender_node();
    exchange_sender_node->set_log_id(100);
    exchange_sender_node->set_fragment_id(7);
    exchange_sender_node->set_receiver_fragment_id(8);
    exchange_sender_node->set_fragment_instance_id(78);
    exchange_sender_node->mutable_partition_property()->set_type(pb::HashPartitionType);
    exchange_sender_node->mutable_partition_property()->add_hash_cols("col1");
    exchange_sender_node->mutable_partition_property()->add_hash_cols("col3");
    pb::ExchangeDestination* receiver_destination = exchange_sender_node->add_receiver_destinations();
    receiver_destination->set_fragment_instance_id(88);
    receiver_destination->set_node_id(99);
    receiver_destination->set_address("10.12.187.120:8787");
    return plan_node;
}
pb::PlanNode make_exchange_sender_node3() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_SENDER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeSenderNode* exchange_sender_node = derive_node->mutable_exchange_sender_node();
    exchange_sender_node->set_log_id(100);
    exchange_sender_node->set_fragment_id(7);
    exchange_sender_node->set_receiver_fragment_id(8);
    exchange_sender_node->set_fragment_instance_id(79);
    exchange_sender_node->mutable_partition_property()->set_type(pb::HashPartitionType);
    exchange_sender_node->mutable_partition_property()->add_hash_cols("col1");
    exchange_sender_node->mutable_partition_property()->add_hash_cols("col3");
    pb::ExchangeDestination* receiver_destination = exchange_sender_node->add_receiver_destinations();
    receiver_destination->set_fragment_instance_id(88);
    receiver_destination->set_node_id(99);
    receiver_destination->set_address("10.12.187.120:8787");
    return plan_node;
}
pb::PlanNode make_exchange_receiver_node() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(77);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8788");

    std::shared_ptr<arrow::Schema> arrow_schema = make_test_schema();
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = 
        arrow::ipc::SerializeSchema(*arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return plan_node;
    }
    exchange_receiver_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());

    return plan_node;
}

pb::PlanNode make_invalid_exchange_receiver_node() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(0);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(-1);
    exchange_receiver_node->set_fragment_id(-1);
    exchange_receiver_node->set_sender_fragment_id(-1);
    exchange_receiver_node->set_fragment_instance_id(-1);
    exchange_receiver_node->set_node_id(-1);
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(-1);
    sender_destination->set_node_id(-1);
    sender_destination->set_address("10.12.187.120:8788");

    std::shared_ptr<arrow::Schema> arrow_schema = make_test_schema();
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = 
        arrow::ipc::SerializeSchema(*arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return plan_node;
    }
    exchange_receiver_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());

    return plan_node;
}

pb::PlanNode make_exchange_receiver_node2() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(77);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8788");
    }
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(78);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8789");
    }
    std::shared_ptr<arrow::Schema> arrow_schema = make_test_schema();
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = 
        arrow::ipc::SerializeSchema(*arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return plan_node;
    }
    exchange_receiver_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());
    return plan_node;
}
pb::PlanNode make_exchange_receiver_node3() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(77);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8788");
    }
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(78);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8789");
    }
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(79);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8790");
    }
    std::shared_ptr<arrow::Schema> arrow_schema = make_test_schema();
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = 
        arrow::ipc::SerializeSchema(*arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return plan_node;
    }
    exchange_receiver_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());
    return plan_node;
}

// 接收db_77/db_78/db_79的数据
pb::PlanNode make_exchange_receiver_node_db_schema3() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(77);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8788");
    }
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(78);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8789");
    }
    {
    pb::ExchangeDestination* sender_destination = exchange_receiver_node->add_sender_destinations();
    sender_destination->set_fragment_instance_id(79);
    sender_destination->set_node_id(99);
    sender_destination->set_address("10.12.187.120:8790");
    }
    std::shared_ptr<arrow::Schema> arrow_schema = make_test_schema3();
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = 
        arrow::ipc::SerializeSchema(*arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return plan_node;
    }
    exchange_receiver_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());
    return plan_node;
}
// 接收store region1/region2/region3的数据
pb::PlanNode make_exchange_receiver_node_region_schema3() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    {
    pb::RegionInfo* region = exchange_receiver_node->add_regions();
    region->set_region_id(1);
    region->set_version(1);
    region->set_table_id(1);
    region->set_partition_id(1);
    region->set_replica_num(3);
    region->set_conf_version(1);
    }
    {
    pb::RegionInfo* region = exchange_receiver_node->add_regions();
    region->set_region_id(2);
    region->set_version(1);
    region->set_table_id(1);
    region->set_partition_id(1);
    region->set_replica_num(3);
    region->set_conf_version(1);
    }
    {
    pb::RegionInfo* region = exchange_receiver_node->add_regions();
    region->set_region_id(3);
    region->set_version(1);
    region->set_table_id(1);
    region->set_partition_id(1);
    region->set_replica_num(3);
    region->set_conf_version(1);
    }
    std::shared_ptr<arrow::Schema> arrow_schema = make_test_schema3();
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = 
        arrow::ipc::SerializeSchema(*arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return plan_node;
    }
    exchange_receiver_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());
    return plan_node;
}

// 接收store region1/region3/region4的数据
pb::PlanNode make_exchange_receiver_node_region_new_schema3() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    {
    pb::RegionInfo* region = exchange_receiver_node->add_regions();
    region->set_region_id(1);
    region->set_version(1);
    region->set_table_id(1);
    region->set_partition_id(1);
    region->set_replica_num(3);
    region->set_conf_version(1);
    }
    {
    pb::RegionInfo* region = exchange_receiver_node->add_regions();
    region->set_region_id(3);
    region->set_version(1);
    region->set_table_id(1);
    region->set_partition_id(1);
    region->set_replica_num(3);
    region->set_conf_version(1);
    }
    {
    pb::RegionInfo* region = exchange_receiver_node->add_regions();
    region->set_region_id(4);
    region->set_version(1);
    region->set_table_id(1);
    region->set_partition_id(1);
    region->set_replica_num(3);
    region->set_conf_version(1);
    }
    std::shared_ptr<arrow::Schema> arrow_schema = make_test_schema3();
    arrow::Result<std::shared_ptr<arrow::Buffer>> schema_ret = 
        arrow::ipc::SerializeSchema(*arrow_schema, arrow::default_memory_pool());
    if (!schema_ret.ok()) {
        DB_FATAL("arrow serialize schema fail, status: %s", schema_ret.status().ToString().c_str());
        return plan_node;
    }
    exchange_receiver_node->set_schema((*schema_ret)->data(), (*schema_ret)->size());
    return plan_node;
}

/// 分裂合并场景测试数据构造
pb::RegionInfo make_region_info(const int64_t region_id, const int64_t version) {
    pb::RegionInfo region_info;
    region_info.set_region_id(region_id);
    region_info.set_partition_id(0);
    region_info.set_replica_num(3);
    region_info.set_conf_version(0);
    region_info.set_version(version);
    return region_info;
}
// region1分裂成region1/region2
pb::RegionInfo make_region1_old() {
    return make_region_info(1,1);
}
pb::RegionInfo make_region1_new() {
    return make_region_info(1,2);
}
pb::RegionInfo make_region2() {
    return make_region_info(2,1);
}
pb::TransmitDataParam make_transmit_data_param_split() {
    pb::TransmitDataParam transmit_data_param;
    transmit_data_param.set_exchange_state(pb::ES_VERSION_OLD);
    transmit_data_param.set_log_id(100);
    transmit_data_param.set_sender_fragment_instance_id(77);
    transmit_data_param.set_receiver_fragment_instance_id(88);
    transmit_data_param.set_receiver_node_id(99);
    transmit_data_param.add_region_infos()->CopyFrom(make_region1_new());
    transmit_data_param.add_region_infos()->CopyFrom(make_region2());
    return transmit_data_param;
}
pb::PlanNode make_exchange_receiver_node_split() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    exchange_receiver_node->add_regions()->CopyFrom(make_region1_old());
    return plan_node;
}
// region3/region4合并成region3
pb::RegionInfo make_region3_old() {
    return make_region_info(3,1);
}
pb::RegionInfo make_region3_new() {
    return make_region_info(3,2);
}
pb::RegionInfo make_region4_old() {
    return make_region_info(4,1);
}
pb::RegionInfo make_region4_new() {
    return make_region_info(4,2);
}
pb::TransmitDataParam make_transmit_data_param_merge3() {
    pb::TransmitDataParam transmit_data_param;
    transmit_data_param.set_exchange_state(pb::ES_VERSION_OLD);
    transmit_data_param.set_log_id(100);
    transmit_data_param.set_sender_fragment_instance_id(77);
    transmit_data_param.set_receiver_fragment_instance_id(88);
    transmit_data_param.set_receiver_node_id(99);
    transmit_data_param.set_is_merge(true);
    transmit_data_param.add_region_infos()->CopyFrom(make_region3_new());
    return transmit_data_param;
}
pb::TransmitDataParam make_transmit_data_param_merge4() {
    pb::TransmitDataParam transmit_data_param;
    transmit_data_param.set_exchange_state(pb::ES_VERSION_OLD);
    transmit_data_param.set_log_id(100);
    transmit_data_param.set_sender_fragment_instance_id(77);
    transmit_data_param.set_receiver_fragment_instance_id(88);
    transmit_data_param.set_receiver_node_id(99);
    transmit_data_param.set_is_merge(true);
    transmit_data_param.add_region_infos()->CopyFrom(make_region4_new());
    transmit_data_param.add_region_infos()->CopyFrom(make_region3_new());
    return transmit_data_param;
}
pb::PlanNode make_exchange_receiver_node_merge3() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    exchange_receiver_node->add_regions()->CopyFrom(make_region3_old());
    return plan_node;
}
pb::PlanNode make_exchange_receiver_node_merge4() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    exchange_receiver_node->add_regions()->CopyFrom(make_region4_old());
    return plan_node;
}
pb::PlanNode make_exchange_receiver_node_merge34() {
    pb::PlanNode plan_node;
    plan_node.set_node_type(pb::EXCHANGE_RECEIVER_NODE);
    plan_node.set_num_children(1);
    pb::DerivePlanNode* derive_node = plan_node.mutable_derive_node();
    pb::ExchangeReceiverNode* exchange_receiver_node = derive_node->mutable_exchange_receiver_node();
    exchange_receiver_node->set_log_id(100);
    exchange_receiver_node->set_fragment_id(8);
    exchange_receiver_node->set_sender_fragment_id(7);
    exchange_receiver_node->set_fragment_instance_id(88);
    exchange_receiver_node->set_node_id(99);
    exchange_receiver_node->add_regions()->CopyFrom(make_region3_old());
    exchange_receiver_node->add_regions()->CopyFrom(make_region4_old());
    return plan_node;
}

//// ER/ER交互
TEST(test_rpc_store, case_all) {
    DB_WARNING("-------------------------------test_rpc_store-------------------------------");
    // RecordBatch
    std::shared_ptr<arrow::RecordBatch> batch = make_test_record_batch1_schema1();
    std::shared_ptr<arrow::RecordBatch> batch2 = make_test_record_batch2_schema1();
    std::shared_ptr<arrow::RecordBatch> batch3 = make_test_record_batch3_schema1();
    // Receiver
    RuntimeState recv_state;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_region_schema3();
    ExchangeReceiverNode exchange_receiver;
    exchange_receiver.init(pb_exchange_receiver_node);
    // exchange_receiver.set_limit(24);
    exchange_receiver.open(&recv_state);
    exchange_receiver.build_arrow_declaration(&recv_state);
    Bthread bth;
    bth.run([&recv_state] () {
        arrow::Result<std::shared_ptr<arrow::Table>> final_table = arrow::acero::DeclarationToTable(
            arrow::acero::Declaration::Sequence(std::move(recv_state.acero_declarations)), false);
        if (!final_table.ok()) {
            DB_WARNING("Fail to DeclarationToTable");
            return;
        }
        std::shared_ptr<arrow::Table> table = *final_table;
        DB_WARNING("breakpoint table: %s", table->ToString().c_str());
    });

    // Sender
    RuntimeState send_state;
    send_state._region_id = 1;
    send_state._region_version = 1;
    pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node();    
    ExchangeSenderNode exchange_sender;
    exchange_sender.init(pb_exchange_sender_node);
    exchange_sender.open(&send_state);
    exchange_sender.send_curr_record_batch(&send_state, batch);
    exchange_sender.send_eof_record_batch(&send_state, make_test_empty_record_batch_schema1());

    RuntimeState send_state2;
    send_state2._region_id = 2;
    send_state2._region_version = 1;
    pb::PlanNode pb_exchange_sender_node2 = make_exchange_sender_node2();
    ExchangeSenderNode exchange_sender2;
    exchange_sender2.init(pb_exchange_sender_node2);
    exchange_sender2.open(&send_state2);
    exchange_sender2.send_curr_record_batch(&send_state2, batch2);
    exchange_sender2.send_eof_record_batch(&send_state2, make_test_empty_record_batch_schema1());

    RuntimeState send_state3;
    send_state3._region_id = 3;
    send_state3._region_version = 1;
    pb::PlanNode pb_exchange_sender_node3 = make_exchange_sender_node3();
    ExchangeSenderNode exchange_sender3;
    exchange_sender3.init(pb_exchange_sender_node3);
    exchange_sender3.open(&send_state3);
    exchange_sender3.send_curr_record_batch(&send_state3, batch3);
    exchange_sender3.send_eof_record_batch(&send_state3, make_test_empty_record_batch_schema1());

    bth.join();
    exchange_receiver.close(&recv_state);
    exchange_sender.close(&send_state);
    exchange_sender2.close(&send_state2);
    exchange_sender3.close(&send_state3);
}

TEST(test_rpc_db, case_all) {
    DB_WARNING("-------------------------------test_rpc_db-------------------------------");
    // RecordBatch
    std::shared_ptr<arrow::RecordBatch> batch = make_test_record_batch1_schema1();
    std::shared_ptr<arrow::RecordBatch> batch2 = make_test_record_batch2_schema1();
    std::shared_ptr<arrow::RecordBatch> batch3 = make_test_record_batch3_schema1();

    // Receiver
    RuntimeState recv_state;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_db_schema3();
    ExchangeReceiverNode exchange_receiver;
    exchange_receiver.init(pb_exchange_receiver_node);
    // exchange_receiver.set_limit(24);
    exchange_receiver.open(&recv_state);
    exchange_receiver.build_arrow_declaration(&recv_state);
    Bthread bth;
    bth.run([&recv_state] () {
        arrow::Result<std::shared_ptr<arrow::Table>> final_table = arrow::acero::DeclarationToTable(
            arrow::acero::Declaration::Sequence(std::move(recv_state.acero_declarations)), false);
        if (!final_table.ok()) {
            DB_WARNING("Fail to DeclarationToTable");
            return;
        }
        std::shared_ptr<arrow::Table> table = *final_table;
        DB_WARNING("breakpoint table: %s", table->ToString().c_str());
    });

    // Sender
    {
        pb::PlanNode pb_exchange_receiver_node1 = make_invalid_exchange_receiver_node();
        ExchangeReceiverNode exchange_receiver1;
        exchange_receiver1.init(pb_exchange_receiver_node1);
        ExchangeSenderNode exchange_sender;
        pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node();  
        exchange_sender.init(pb_exchange_sender_node);
        exchange_sender.add_child(&exchange_receiver1);

        RuntimeState send_state;
        exchange_sender.open(&send_state);
        exchange_sender.build_arrow_declaration(&send_state);

        pb::ExtraRes extra_res;
        auto param = make_invalid_transmit_data_param();
        param.set_record_batch_seq(0);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch, extra_res);
        param.set_record_batch_seq(0);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch, extra_res);
        param.set_record_batch_seq(1);
        param.set_exchange_state(pb::ES_EOF);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch, extra_res);

        arrow::Result<std::shared_ptr<arrow::Table>> final_table = arrow::acero::DeclarationToTable(
                arrow::acero::Declaration::Sequence(std::move(send_state.acero_declarations)), false);
        if (!final_table.ok()) {
            DB_WARNING("Fail to DeclarationToTable, %s", final_table.status().ToString().c_str());
            exchange_sender.clear_children();
            return;
        }
        std::shared_ptr<arrow::Table> table = *final_table;
        DB_WARNING("breakpoint table: %s", table->ToString().c_str());
        exchange_sender.close(&send_state);
        exchange_sender.clear_children();
    }
    {
        pb::PlanNode pb_exchange_receiver_node1 = make_invalid_exchange_receiver_node();
        ExchangeReceiverNode exchange_receiver1;
        exchange_receiver1.init(pb_exchange_receiver_node1);
        ExchangeSenderNode exchange_sender;
        pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node2();  
        exchange_sender.init(pb_exchange_sender_node);
        exchange_sender.add_child(&exchange_receiver1);

        RuntimeState send_state;
        exchange_sender.open(&send_state);
        exchange_sender.build_arrow_declaration(&send_state);

        pb::ExtraRes extra_res;
        auto param = make_invalid_transmit_data_param();
        param.set_record_batch_seq(0);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch2, extra_res);
        param.set_record_batch_seq(0);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch2, extra_res);
        param.set_record_batch_seq(1);
        param.set_exchange_state(pb::ES_EOF);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch2, extra_res);

        arrow::Result<std::shared_ptr<arrow::Table>> final_table = arrow::acero::DeclarationToTable(
                arrow::acero::Declaration::Sequence(std::move(send_state.acero_declarations)), false);
        if (!final_table.ok()) {
            DB_WARNING("Fail to DeclarationToTable, %s", final_table.status().ToString().c_str());
            exchange_sender.clear_children();
            return;
        }
        std::shared_ptr<arrow::Table> table = *final_table;
        DB_WARNING("breakpoint table: %s", table->ToString().c_str());
        exchange_sender.close(&send_state);
        exchange_sender.clear_children();
    }
    {
        pb::PlanNode pb_exchange_receiver_node1 = make_invalid_exchange_receiver_node();
        ExchangeReceiverNode exchange_receiver1;
        exchange_receiver1.init(pb_exchange_receiver_node1);
        ExchangeSenderNode exchange_sender;
        pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node3();  
        exchange_sender.init(pb_exchange_sender_node);
        exchange_sender.add_child(&exchange_receiver1);

        RuntimeState send_state;
        exchange_sender.open(&send_state);
        exchange_sender.build_arrow_declaration(&send_state);

        pb::ExtraRes extra_res;
        auto param = make_invalid_transmit_data_param();
        param.set_record_batch_seq(0);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch, extra_res);
        param.set_record_batch_seq(1);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch2, extra_res);
        param.set_record_batch_seq(2);
        param.set_exchange_state(pb::ES_EOF);
        exchange_receiver1.get_data_stream_receiver()->add_record_batch(param, batch3, extra_res);

        arrow::Result<std::shared_ptr<arrow::Table>> final_table = arrow::acero::DeclarationToTable(
                arrow::acero::Declaration::Sequence(std::move(send_state.acero_declarations)), false);
        if (!final_table.ok()) {
            DB_WARNING("Fail to DeclarationToTable, %s", final_table.status().ToString().c_str());
            exchange_sender.clear_children();
            return;
        }
        std::shared_ptr<arrow::Table> table = *final_table;
        DB_WARNING("breakpoint table: %s", table->ToString().c_str());
        exchange_sender.close(&send_state);
        exchange_sender.clear_children();
    }

    bth.join();
    exchange_receiver.close(&recv_state);
}

TEST(test_rpc_error_new, case_all) {
    DB_WARNING("-------------------------------test_rpc_error_new-------------------------------");
    // RecordBatch
    std::shared_ptr<arrow::RecordBatch> batch = make_test_record_batch1_schema1();
    std::shared_ptr<arrow::RecordBatch> batch2 = make_test_record_batch2_schema1();
    std::shared_ptr<arrow::RecordBatch> batch3 = make_test_record_batch3_schema1();

    // Receiver
    RuntimeState recv_state;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_region_schema3();
    ExchangeReceiverNode exchange_receiver;
    exchange_receiver.init(pb_exchange_receiver_node);
    // exchange_receiver.set_limit(24);
    exchange_receiver.open(&recv_state);
    exchange_receiver.build_arrow_declaration(&recv_state);
    Bthread bth;
    bth.run([&recv_state] () {
        arrow::Result<std::shared_ptr<arrow::Table>> final_table = arrow::acero::DeclarationToTable(
            arrow::acero::Declaration::Sequence(std::move(recv_state.acero_declarations)), false);
        if (!final_table.ok()) {
            DB_WARNING("Fail to DeclarationToTable");
            return;
        }
        std::shared_ptr<arrow::Table> table = *final_table;
        DB_WARNING("breakpoint table: %s", table->ToString().c_str());
    });

    // Sender
    RuntimeState send_state;
    send_state._region_id = 1;
    send_state._region_version = 1;
    pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node();    
    ExchangeSenderNode exchange_sender;
    exchange_sender.init(pb_exchange_sender_node);
    exchange_sender.open(&send_state);
    exchange_sender.send_exec_fail();

    RuntimeState send_state2;
    send_state2._region_id = 2;
    send_state2._region_version = 1;
    pb::PlanNode pb_exchange_sender_node2 = make_exchange_sender_node2();
    ExchangeSenderNode exchange_sender2;
    exchange_sender2.init(pb_exchange_sender_node2);
    exchange_sender2.open(&send_state2);
    exchange_sender2.send_curr_record_batch(&send_state2, batch2);
    exchange_sender2.send_eof_record_batch(&send_state2, make_test_empty_record_batch_schema1());

    RuntimeState send_state3;
    send_state3._region_id = 3;
    send_state3._region_version = 1;
    pb::PlanNode pb_exchange_sender_node3 = make_exchange_sender_node3();
    ExchangeSenderNode exchange_sender3;
    exchange_sender3.init(pb_exchange_sender_node3);
    exchange_sender3.open(&send_state3);
    exchange_sender3.send_curr_record_batch(&send_state3, batch3);
    exchange_sender3.send_eof_record_batch(&send_state3, make_test_empty_record_batch_schema1());

    bth.join();
    exchange_receiver.close(&recv_state);
    exchange_sender.close(&send_state);
    exchange_sender2.close(&send_state2);
    exchange_sender3.close(&send_state3);
}

// //// ES测试
// repartition
TEST(test_sender, case_all) {
    DB_WARNING("-------------------------------test_sender-------------------------------");
    {
        // RecordBatch
        std::shared_ptr<arrow::RecordBatch> record_batch = make_test_record_batch1_schema1();
        std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> hash_batch_map;
        ExchangeSenderNode exchange_sender_node;
        auto status = exchange_sender_node.repartition(record_batch, {"col1", "col3"}, {}, 1, hash_batch_map);
        if (!status.ok()) {
            DB_WARNING("Fail to repartition");
        }
        ASSERT_EQ(status.ok(), true);
        for (auto& [bucket, batch] : hash_batch_map) {
            DB_WARNING("bucket: %d, batch: %s", bucket, batch->ToString().c_str());
        }
    }
    {
        // RecordBatch
        std::shared_ptr<arrow::RecordBatch> record_batch = make_test_record_batch1_schema1();
        std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> hash_batch_map;
        ExchangeSenderNode exchange_sender_node;
        auto status = exchange_sender_node.repartition(record_batch, {"col1", "col3"}, {}, 3, hash_batch_map);
        if (!status.ok()) {
            DB_WARNING("Fail to repartition");
        }
        ASSERT_EQ(status.ok(), true);
        for (auto& [bucket, batch] : hash_batch_map) {
            DB_WARNING("bucket: %d, batch: %s", bucket, batch->ToString().c_str());
        }
    }
    {
        // RecordBatch
        std::shared_ptr<arrow::RecordBatch> record_batch = make_test_record_batch1_schema1();
        std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> hash_batch_map;
        ExchangeSenderNode exchange_sender_node;
        auto status = exchange_sender_node.repartition(record_batch, {"col1", "col3"}, {"col3"}, 3, hash_batch_map);
        if (!status.ok()) {
            DB_WARNING("Fail to repartition");
        }
        ASSERT_EQ(status.ok(), true);
        for (auto& [bucket, batch] : hash_batch_map) {
            DB_WARNING("bucket: %d, batch: %s", bucket, batch->ToString().c_str());
        }
    }
    {
        // RecordBatch
        std::shared_ptr<arrow::RecordBatch> record_batch = make_test_record_batch1_schema2();
        DB_WARNING("schema: %s, record_batch: %s", record_batch->schema()->ToString().c_str(), record_batch->ToString().c_str());
        std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> hash_batch_map;
        ExchangeSenderNode exchange_sender_node;
        auto status = exchange_sender_node.repartition(record_batch, {"col1", "col3"}, {"col3"}, 3, hash_batch_map);
        if (!status.ok()) {
            DB_WARNING("Fail to repartition");
        }
        ASSERT_EQ(status.ok(), true);
        for (auto& [bucket, batch] : hash_batch_map) {
            DB_WARNING("bucket: %d, batch: %s", bucket, batch->ToString().c_str());
        }
    }
    {
        // RecordBatch
        std::shared_ptr<arrow::RecordBatch> record_batch = make_test_record_batch1_schema1();
        std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> hash_batch_map;
        ExchangeSenderNode exchange_sender_node;
        auto status = exchange_sender_node.repartition(record_batch, {"col1", "col5"}, {}, 3, hash_batch_map);
        if (!status.ok()) {
            DB_WARNING("Fail to repartition");
            return;
        }
        ASSERT_EQ(status.ok(), true);
        for (auto& [bucket, batch] : hash_batch_map) {
            DB_WARNING("bucket: %d, batch: %s", bucket, batch->ToString().c_str());
        }
    }
}

//// ER测试
/// handle_version_old()
// 分裂场景，region1分裂成region1/region2
// ExchangeReceiverNode原本是region1_old，现在收到region1_new
TEST(test_split, case_all) {
    DB_WARNING("-------------------------------test_split-------------------------------");
    RuntimeState state;
    ExchangeReceiverNode exchange_receiver;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_split();
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&state);
    pb::TransmitDataParam transmit_data_param = make_transmit_data_param_split();
    std::shared_ptr<DataStreamReceiver> receiver = 
        DataStreamManager::get_instance()->get_receiver(transmit_data_param.log_id(), 
                                                        transmit_data_param.receiver_fragment_instance_id(), 
                                                        transmit_data_param.receiver_node_id());
    if (receiver == nullptr) {
        DB_WARNING("receiver is nullptr");
        return;
    }
    receiver->to_string();
    receiver->handle_version_old(transmit_data_param);
    receiver->to_string();
}
// 合并场景，region3/region4合并成region3
// ExchangeReceiverNode原本是region3_old，现在收到region3_new
TEST(test_merge, case_all) {
    DB_WARNING("-------------------------------test_merge-------------------------------");
    RuntimeState state;
    ExchangeReceiverNode exchange_receiver;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_merge3();
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&state);
    pb::TransmitDataParam transmit_data_param = make_transmit_data_param_merge3();
    std::shared_ptr<DataStreamReceiver> receiver = 
        DataStreamManager::get_instance()->get_receiver(transmit_data_param.log_id(), 
                                                        transmit_data_param.receiver_fragment_instance_id(), 
                                                        transmit_data_param.receiver_node_id());
    if (receiver == nullptr) {
        DB_WARNING("receiver is nullptr");
        return;
    }
    receiver->to_string();
    receiver->handle_version_old(transmit_data_param);
    receiver->to_string();
}
// ExchangeReceiverNode原本是region4
TEST(test_merge4, case_all) {
    DB_WARNING("-------------------------------test_merge4-------------------------------");
    RuntimeState state;
    ExchangeReceiverNode exchange_receiver;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_merge4();
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&state);
    pb::TransmitDataParam transmit_data_param = make_transmit_data_param_merge4();
    std::shared_ptr<DataStreamReceiver> receiver = 
        DataStreamManager::get_instance()->get_receiver(transmit_data_param.log_id(), 
                                                        transmit_data_param.receiver_fragment_instance_id(), 
                                                        transmit_data_param.receiver_node_id());
    if (receiver == nullptr) {
        DB_WARNING("receiver is nullptr");
        return;
    }
    receiver->to_string();
    receiver->handle_version_old(transmit_data_param);
    receiver->to_string();
}
// ExchangeReceiverNode原本是region3_old/region4，现在收到region3_new
TEST(test_merge34, case_all) {
    DB_WARNING("-------------------------------test_merge34-------------------------------");
    RuntimeState state;
    ExchangeReceiverNode exchange_receiver;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_merge34();
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&state);
    {
        pb::TransmitDataParam transmit_data_param = make_transmit_data_param_merge4();
        std::shared_ptr<DataStreamReceiver> receiver = 
            DataStreamManager::get_instance()->get_receiver(transmit_data_param.log_id(), 
                                                            transmit_data_param.receiver_fragment_instance_id(), 
                                                            transmit_data_param.receiver_node_id());
        if (receiver == nullptr) {
            DB_WARNING("receiver is nullptr");
            return;
        }
        receiver->to_string();
        receiver->handle_version_old(transmit_data_param);
        receiver->to_string();
    }
    {
        pb::TransmitDataParam transmit_data_param = make_transmit_data_param_merge3();
        std::shared_ptr<DataStreamReceiver> receiver = 
            DataStreamManager::get_instance()->get_receiver(transmit_data_param.log_id(), 
                                                            transmit_data_param.receiver_fragment_instance_id(), 
                                                            transmit_data_param.receiver_node_id());
        if (receiver == nullptr) {
            DB_WARNING("receiver is nullptr");
            return;
        }
        receiver->to_string();
        receiver->handle_version_old(transmit_data_param);
        receiver->to_string();
    }
}

TEST(test_merge34_new, case_all) {
    DB_WARNING("-------------------------------test_merge34_new-------------------------------");
    RuntimeState state;
    ExchangeReceiverNode exchange_receiver;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_merge34();
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&state);
    {
        pb::TransmitDataParam transmit_data_param = make_transmit_data_param_merge3();
        std::shared_ptr<DataStreamReceiver> receiver = 
            DataStreamManager::get_instance()->get_receiver(transmit_data_param.log_id(), 
                                                            transmit_data_param.receiver_fragment_instance_id(), 
                                                            transmit_data_param.receiver_node_id());
        if (receiver == nullptr) {
            DB_WARNING("receiver is nullptr");
            return;
        }
        receiver->to_string();
        receiver->handle_version_old(transmit_data_param);
        receiver->to_string();
    }
    {
        pb::TransmitDataParam transmit_data_param = make_transmit_data_param_merge4();
        std::shared_ptr<DataStreamReceiver> receiver = 
            DataStreamManager::get_instance()->get_receiver(transmit_data_param.log_id(), 
                                                            transmit_data_param.receiver_fragment_instance_id(), 
                                                            transmit_data_param.receiver_node_id());
        if (receiver == nullptr) {
            DB_WARNING("receiver is nullptr");
            return;
        }
        receiver->to_string();
        receiver->handle_version_old(transmit_data_param);
        receiver->to_string();
    }
}

// CAST
TEST(test_cast, case_all) {
    DB_WARNING("-------------------------------test_cast-------------------------------");
    std::shared_ptr<arrow::RecordBatch> record_batch = make_test_record_batch1_schema1();
    auto array = record_batch->column(0);
    auto array_utf8_ret = arrow::compute::Cast(*array, arrow::utf8());
    auto array_utf8 = *array_utf8_ret;
    DB_WARNING("array: %s", array->ToString().c_str());
    DB_WARNING("array_utf8: %s", array_utf8->ToString().c_str());

    auto array_lb1_ret = arrow::compute::Cast(*array, arrow::large_binary());
    auto array_lb1 = *array_lb1_ret;
    auto array_lb2_ret = arrow::compute::Cast(*array_utf8, arrow::large_binary());
    auto array_lb2 = *array_lb2_ret;
    DB_WARNING("array_lb1: %s", array_lb1->ToString().c_str());
    DB_WARNING("array_lb2: %s", array_lb2->ToString().c_str());
}

// mpp_send_version_old
TEST(test_mpp_send_version_old_split, case_all) {
    DB_WARNING("-------------------------------test_mpp_send_version_old_split-------------------------------");
    RuntimeState recv_state;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_region_new_schema3();
    ExchangeReceiverNode exchange_receiver;
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&recv_state);
    exchange_receiver.get_data_stream_receiver()->to_string();
    pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node();
    ::google::protobuf::RepeatedPtrField<pb::RegionInfo> region_infos;
    {
        auto region_info = region_infos.Add();
        region_info->set_region_id(1);
        region_info->set_version(2);
        region_info->set_table_id(1);
        region_info->set_partition_id(1);
        region_info->set_replica_num(3);
        region_info->set_conf_version(1);
    }
    {
        auto region_info = region_infos.Add();
        region_info->set_region_id(2);
        region_info->set_version(1);
        region_info->set_table_id(1);
        region_info->set_partition_id(1);
        region_info->set_replica_num(3);
        region_info->set_conf_version(1);
    }
    Region::mpp_send_version_old(pb_exchange_sender_node.derive_node().exchange_sender_node(), false, region_infos);
    exchange_receiver.get_data_stream_receiver()->to_string();
    exchange_receiver.close(&recv_state);
}

TEST(test_mpp_send_version_old_merge1, case_all) {
    DB_WARNING("-------------------------------test_mpp_send_version_old_merge1-------------------------------");
    RuntimeState recv_state;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_region_new_schema3();
    ExchangeReceiverNode exchange_receiver;
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&recv_state);
    exchange_receiver.get_data_stream_receiver()->to_string();
    pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node();
    ::google::protobuf::RepeatedPtrField<pb::RegionInfo> region_infos;
    {
        auto region_info = region_infos.Add();
        region_info->set_region_id(4);
        region_info->set_version(2);
        region_info->set_table_id(1);
        region_info->set_partition_id(1);
        region_info->set_replica_num(3);
        region_info->set_conf_version(1);
    }
    {
        auto region_info = region_infos.Add();
        region_info->set_region_id(3);
        region_info->set_version(2);
        region_info->set_table_id(1);
        region_info->set_partition_id(1);
        region_info->set_replica_num(3);
        region_info->set_conf_version(1);
    }
    Region::mpp_send_version_old(pb_exchange_sender_node.derive_node().exchange_sender_node(), true, region_infos);
    exchange_receiver.get_data_stream_receiver()->to_string();
    exchange_receiver.close(&recv_state);
}

TEST(test_mpp_send_version_old_merge2, case_all) {
    DB_WARNING("-------------------------------test_mpp_send_version_old_merge2-------------------------------");
    RuntimeState recv_state;
    pb::PlanNode pb_exchange_receiver_node = make_exchange_receiver_node_region_new_schema3();
    ExchangeReceiverNode exchange_receiver;
    exchange_receiver.init(pb_exchange_receiver_node);
    exchange_receiver.open(&recv_state);
    exchange_receiver.get_data_stream_receiver()->to_string();
    pb::PlanNode pb_exchange_sender_node = make_exchange_sender_node();
    ::google::protobuf::RepeatedPtrField<pb::RegionInfo> region_infos;
    {
        auto region_info = region_infos.Add();
        region_info->set_region_id(3);
        region_info->set_version(2);
        region_info->set_table_id(1);
        region_info->set_partition_id(1);
        region_info->set_replica_num(3);
        region_info->set_conf_version(1);
    }
    Region::mpp_send_version_old(pb_exchange_sender_node.derive_node().exchange_sender_node(), false, region_infos);
    exchange_receiver.get_data_stream_receiver()->to_string();
    exchange_receiver.close(&recv_state);
}

TEST(test_null_cast, case_all) {
    DB_WARNING("-------------------------------test_null_cast-------------------------------");
    std::shared_ptr<arrow::RecordBatch> record_batch = make_test_record_batch_null_schema();
    {
        auto array = record_batch->column(0);
        auto array_utf8_ret = arrow::compute::Cast(*array, arrow::utf8());
        auto array_utf8 = *array_utf8_ret;
        DB_WARNING("array: %s", array->ToString().c_str());
        DB_WARNING("array_utf8: %s", array_utf8->ToString().c_str());

        auto array_lb1_ret = arrow::compute::Cast(*array, arrow::large_binary());
        auto array_lb1 = *array_lb1_ret;
        auto array_lb2_ret = arrow::compute::Cast(*array_utf8, arrow::large_binary());
        auto array_lb2 = *array_lb2_ret;
        DB_WARNING("array_lb1: %s", array_lb1->ToString().c_str());
        DB_WARNING("array_lb2: %s", array_lb2->ToString().c_str());
    }
    {
        auto array = record_batch->column(1);
        auto array_utf8_ret = arrow::compute::Cast(*array, arrow::utf8());
        auto array_utf8 = *array_utf8_ret;
        DB_WARNING("array: %s", array->ToString().c_str());
        DB_WARNING("array_utf8: %s", array_utf8->ToString().c_str());

        auto array_lb1_ret = arrow::compute::Cast(*array, arrow::large_binary());
        auto array_lb1 = *array_lb1_ret;
        auto array_lb2_ret = arrow::compute::Cast(*array_utf8, arrow::large_binary());
        auto array_lb2 = *array_lb2_ret;
        DB_WARNING("array_lb1: %s", array_lb1->ToString().c_str());
        DB_WARNING("array_lb2: %s", array_lb2->ToString().c_str());
    }
}

TEST(test_null_hash, case_all) {
    DB_WARNING("-------------------------------test_null_hash-------------------------------");
    std::shared_ptr<arrow::Array> array1 = make_test_array_tmp({1,2,3,4,5,6,7,8,9,10});
    std::shared_ptr<arrow::Array> array2 = make_test_array_tmp({11,12,13,14,15,16,17,18,19,20});
    std::shared_ptr<arrow::Array> array3 = make_test_array_tmp({21,22,23,24,25,26,27,28,29,30});
    DB_WARNING("array length: %ld", array1->length());
    std::shared_ptr<arrow::RecordBatch> record_batch = 
        arrow::RecordBatch::Make(make_test_schema(), 2, {array1, array2, array3});
    DB_WARNING("record_batch: %s", record_batch->ToString().c_str());
    std::unordered_map<int, std::shared_ptr<arrow::RecordBatch>> hash_batch_map;
    ExchangeSenderNode exchange_sender_node;
    auto status = exchange_sender_node.repartition(record_batch, {"col1", "col2"}, {"col1"}, 3, hash_batch_map);
    if (!status.ok()) {
        DB_WARNING("Fail to repartition");
    }
    ASSERT_EQ(status.ok(), true);
    for (auto& [bucket, batch] : hash_batch_map) {
        DB_WARNING("bucket: %d, batch: %s", bucket, batch->ToString().c_str());
    }
}

} // namespace baikaldb

using namespace baikaldb;

int main(int argc, char* argv[]) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
    google::ParseCommandLineFlags(&argc, &argv, true);

    // 初始化日志
    if (baikaldb::init_log(argv[0]) != 0) {
        fprintf(stderr, "log init failed.");
        return -1;
    }
    DB_WARNING("breakpoint main begin");

    if (ArrowExecNodeManager::RegisterAllArrowExecNode() != 0) {
        DB_FATAL("RegisterAllArrowExecNode failed");
        return -1;
    }

    // 启动Server
    brpc::Server server;
    baikaldb::DbService* db_service = baikaldb::DbService::get_instance();
    if (0 != server.AddService(db_service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        DB_FATAL("Fail to Add idonlyeService");
        return -1;
    }
    if (server.Start("10.12.187.120:8787", NULL) != 0) {
        DB_FATAL("Fail to start server");
        return -1;
    }
    // UT
    testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    DB_WARNING("breakpoint UT end");
    // 停止Server
    server.Stop(0);
    server.Join();
    DB_WARNING("breakpoint main end");
    return 0;
}