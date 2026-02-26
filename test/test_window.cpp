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

#include "logical_planner.h"
#include "parser.h"
#include "scan_node.h"
#include "expr_optimizer.h"
#include "window_node.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

void update_table() {
    auto schema_factory = SchemaFactory::get_instance();
    pb::SchemaInfo info;
    info.set_namespace_name("test_namespace");
    info.set_database("test_database");
    info.set_table_name("test_window");
    info.set_partition_num(1);
    info.set_namespace_id(111);
    info.set_database_id(222);
    for (int idx = 1; idx < 5; idx++) {
        baikaldb::pb::FieldInfo *field_string = info.add_fields();
        field_string->set_field_name("col" + std::to_string(idx));
        field_string->set_field_id(idx);
        field_string->set_mysql_type(baikaldb::pb::INT32);
    }
    baikaldb::pb::IndexInfo *index_pk = info.add_indexs();
    index_pk->set_index_type(baikaldb::pb::I_PRIMARY);
    index_pk->set_index_name("pk_index");
    index_pk->add_field_ids(1);
    index_pk->set_index_id(1);
    info.set_table_id(1);
    info.set_version(2);
    schema_factory->init();
    schema_factory->update_table(info);
}

void construct_row_batch(RuntimeState* state, RowBatch& row_batch) {
    for (int i = 0; i <= 5; ++i) {
        for (int j = 0; j <= i; ++j) {
            int loop_cnt = 1;
            if (i == 4) {
                loop_cnt = 2;
            }
            for (int k = 0; k < loop_cnt; ++k) {
                std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
                {
                    ExprValue tmp(pb::INT32);
                    tmp._u.int32_val = i;
                    row->set_value(0, 1, tmp);
                }
                {
                    ExprValue tmp(pb::INT32);
                    tmp._u.int32_val = i;
                    row->set_value(0, 2, tmp);
                }
                {
                    ExprValue tmp(pb::INT32);
                    tmp._u.int32_val = i*10 + j;
                    row->set_value(0, 3, tmp);
                }
                row_batch.move_row(std::move(row));
            }
        }
    }
}

void construct_row_batch(std::shared_ptr<MemRowDescriptor> mem_row_desc, RowBatch& row_batch, int start) {
    for (int i = start; i <= start + 5; ++i) {
        for (int j = 0; j <= i; ++j) {
            int loop_cnt = 1;
            if (i == 4) {
                loop_cnt = 2;
            }
            for (int k = 0; k < loop_cnt; ++k) {
                std::unique_ptr<MemRow> row = mem_row_desc->fetch_mem_row();
                {
                    ExprValue tmp(pb::INT32);
                    tmp._u.int32_val = 1;
                    row->set_value(0, 1, tmp);
                }
                {
                    ExprValue tmp(pb::INT32);
                    tmp._u.int32_val = i;
                    row->set_value(0, 2, tmp);
                }
                {
                    ExprValue tmp(pb::INT32);
                    tmp._u.int32_val = i*10 + j;
                    row->set_value(0, 3, tmp);
                }
                row_batch.move_row(std::move(row));
            }
        }
    }
}


class MockScanNode : public ExecNode {
public:
    virtual int get_next(RuntimeState* state, RowBatch* batch, bool* eos) override {
        construct_row_batch(state, *batch);
        *eos = true;
        std::cout << "MockScanNode batch size: " << batch->size() << std::endl;
        return 0;
    }
};

TEST(test_window, case_sql_parser) {
    parser::SqlParser parser;
    const std::string& sql = 
        "SELECT " 
            "id, "
            "COUNT(*) OVER (PARTITION BY id ORDER BY id ROWS 1 PRECEDING), "
            "COUNT(col1) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), "
            "SUM(col1) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING), "
            "AVG(col1) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "MIN(col1) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING), "
            "MAX(col1) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), "
            "ROW_NUMBER() OVER (PARTITION BY id ORDER BY id), "
            "RANK() OVER (PARTITION BY id ORDER BY id), "
            "DENSE_RANK() OVER (PARTITION BY id ORDER BY id), "
            "PERCENT_RANK() OVER (PARTITION BY id ORDER BY id), "
            "CUME_DIST() OVER (PARTITION BY id ORDER BY id), "
            "NTILE(5) OVER (PARTITION BY id ORDER BY id), "
            "LEAD(id) OVER (PARTITION BY id ORDER BY id), "
            "LEAD(id, 2) OVER (PARTITION BY id ORDER BY id), "
            "LEAD(id, 2, id + col1 + 1) OVER (PARTITION BY id ORDER BY id), "
            "LEAD(id, 2, id + col1 + 1) IGNORE NULLS OVER (PARTITION BY id ORDER BY id), "
            "LEAD(id, 2, id + col1 + 1) RESPECT NULLS OVER (PARTITION BY id ORDER BY id), "
            "LAG(id) OVER (PARTITION BY id ORDER BY id), "
            "LAG(id, 2) OVER (PARTITION BY id ORDER BY id), "
            "LAG(id, 2, id + col1 + 1) OVER (PARTITION BY id ORDER BY id), "
            "FIRST_VALUE(id + 2) OVER (PARTITION BY id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "LAST_VALUE(id) RESPECT NULLS OVER (PARTITION BY id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "NTH_VALUE(id * 3, 2) IGNORE NULLS OVER (PARTITION BY id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "NTH_VALUE(id * 3, 2) IGNORE NULLS OVER (PARTITION BY id ORDER BY id), "
            "NTH_VALUE(id * 3, 2) FROM LAST OVER ()"
        "FROM " 
            "test_window;";
    parser.parse(sql);
    ASSERT_EQ(parser.result.size(), 1);
    ASSERT_EQ(parser.result[0]->node_type, parser::NT_SELECT);
    parser::SelectStmt* stmt = (parser::SelectStmt*)parser.result[0];
    ASSERT_NE(stmt, nullptr);

    const std::string& stmt_str = stmt->to_string();
    const std::string& expect_stmt_str = 
        "SELECT "
            "id,  "
            "(COUNT(*)) OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS 1 PRECEDING),  "
            "(COUNT(col1)) OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),  "
            "(SUM(col1)) OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),  "
            "(AVG(col1)) OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),  "
            "(MIN(col1)) OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING),  "
            "(MAX(col1)) OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),  "
            "(ROW_NUMBER()) OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(RANK()) OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(DENSE_RANK()) OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(PERCENT_RANK()) OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(CUME_DIST()) OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(NTILE(5)) OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LEAD(id)) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LEAD(id,2)) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LEAD(id,2,((id + col1) + 1))) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LEAD(id,2,((id + col1) + 1))) IGNORE NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LEAD(id,2,((id + col1) + 1))) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LAG(id)) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LAG(id,2)) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(LAG(id,2,((id + col1) + 1))) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(FIRST_VALUE((id + 2))) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),  "
            "(LAST_VALUE(id)) RESPECT NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),  "
            "(NTH_VALUE((id * 3),2)) FROM FIRST  IGNORE NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),  "
            "(NTH_VALUE((id * 3),2)) FROM FIRST  IGNORE NULLS  OVER ( PARTITION BY id ASC ORDER BY id ASC),  "
            "(NTH_VALUE((id * 3),2)) FROM LAST  RESPECT NULLS  OVER () "
        "FROM "
            "test_window";
    EXPECT_EQ(stmt_str, expect_stmt_str);
}

TEST(test_window, case_plan) {
    // 更新schema信息
    update_table();
    // logical plan测试
    std::shared_ptr<UserInfo> user_info(new (std::nothrow)UserInfo);
    user_info->namespace_ = "test_namespace";
    user_info->is_super = true;
    SmartSocket client = std::make_shared<NetworkSocket>();
    client->user_info = user_info;
    client->send_buf = new (std::nothrow) DataBuffer;
    client->reset_query_ctx(new (std::nothrow) QueryContext(client->user_info, "test_database"));
    client->query_ctx->client_conn = client.get();
    const std::string& sql = 
        "SELECT "
            "col1, "
            "COUNT(*) OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "AVG(col2) OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "SUM(col3) OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING), "
            "MIN(col3) OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "MAX(col3) OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col3), "
            "RANK() OVER (PARTITION BY col1 ORDER BY col3), "
            "DENSE_RANK() OVER (PARTITION BY col1 ORDER BY col3), "
            "PERCENT_RANK() OVER (PARTITION BY col1 ORDER BY col3), "
            "CUME_DIST() OVER (PARTITION BY col1 ORDER BY col1), "
            "NTILE(5) OVER (PARTITION BY col1 ORDER BY col1), "
            "LEAD(col1) OVER (PARTITION BY col1 ORDER BY col1), "
            "LEAD(col1, 2) OVER (PARTITION BY col1 ORDER BY col1), "
            "LEAD(col1, 2, col1 + col1 + 1) OVER (PARTITION BY col1 ORDER BY col1), "
            "LEAD(col1, 2, col1 + col1 + 1) OVER (PARTITION BY col1 ORDER BY col1), "
            "LEAD(col1, 2, col1 + col1 + 1) RESPECT NULLS OVER (PARTITION BY col1 ORDER BY col1), "
            "LAG(col1) OVER (PARTITION BY col1 ORDER BY col1), "
            "LAG(col1, 2) OVER (PARTITION BY col1 ORDER BY col1), "
            "LAG(col1, 2, col1 + col1 + 1) OVER (PARTITION BY col1 ORDER BY col1), "
            "FIRST_VALUE(col1 + 2) OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "LAST_VALUE(col1) RESPECT NULLS OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "NTH_VALUE(col1 * 3, 2) OVER (PARTITION BY col1 ORDER BY col1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
            "NTH_VALUE(col1 * 3, 2) OVER (PARTITION BY col1 ORDER BY col1), "
            "NTH_VALUE(col1 * 3, 2) OVER () "
        "FROM "
            "test_window";
    client->query_ctx->sql = sql;
    int ret = LogicalPlanner::analyze(client->query_ctx.get());
    ASSERT_EQ(ret, 0);
    // 生成逻辑计划树测试
    ret = client->query_ctx->create_plan_tree();
    ASSERT_EQ(ret, 0);
    // 物理计划优化
    // 执行ExprOptimizer analyze就行
    ret = ExprOptimize().analyze(client->query_ctx.get());
    ASSERT_EQ(ret, 0);
    pb::Plan plan;
    ExecNode::create_pb_plan(0, &plan, client->query_ctx->root);
    std::cout << plan.ShortDebugString() << std::endl;
    // 执行物理计划
    RuntimeState& state = *client->query_ctx->get_runtime_state();
    ret = state.init(client->query_ctx.get(), client->send_buf);
    ASSERT_EQ(ret, 0);
    ScanNode* scan_node = static_cast<ScanNode*>(client->query_ctx->root->get_node(pb::SCAN_NODE));
    ASSERT_NE(scan_node, nullptr);
    ExecNode* parent = scan_node->get_parent();
    ASSERT_NE(scan_node, nullptr);
    std::unique_ptr<MockScanNode> mock_scan_node(new (std::nothrow) MockScanNode);
    parent->replace_child(scan_node, mock_scan_node.release());
    state.set_from_subquery(true);
    ret = client->query_ctx->root->open(&state);
    ASSERT_EQ(ret, 0);
    bool eos = false;
    while (!eos) {
        RowBatch row_batch;
        ret = client->query_ctx->root->get_next(&state, &row_batch, &eos);
        ASSERT_EQ(ret, 0);
        for (int i = 0; i < row_batch.size(); ++i) {
            std::cout << "tuple 0: " << row_batch.get_row(i)->debug_string(0) << std::endl;
            std::cout << "tuple 1: " << row_batch.get_row(i)->debug_string(1) << std::endl;
        }
    }
}

TEST(test_window, case_split_into_partitions) {
    // 构造Tuple
    pb::TupleDescriptor tuple_desc;
    tuple_desc.set_tuple_id(0);
    pb::SlotDescriptor* slot1 = tuple_desc.add_slots();
    slot1->set_tuple_id(0);
    slot1->set_slot_id(1);
    slot1->set_slot_type(pb::INT32);
    pb::SlotDescriptor* slot2 = tuple_desc.add_slots();
    slot2->set_tuple_id(0);
    slot2->set_slot_id(2);
    slot2->set_slot_type(pb::INT32);
    pb::SlotDescriptor* slot3 = tuple_desc.add_slots();
    slot3->set_tuple_id(0);
    slot3->set_slot_id(3);
    slot3->set_slot_type(pb::INT32);
    std::vector<pb::TupleDescriptor> tuple_descs {tuple_desc};

    std::shared_ptr<MemRowDescriptor> mem_row_desc = std::make_shared<MemRowDescriptor>();
    mem_row_desc->init(tuple_descs);

    // 构造WindowNode
    {
        // OVER (PARTITION BY col1, col2)
        pb::PlanNode pb_plan_node;
        pb::DerivePlanNode* derive = pb_plan_node.mutable_derive_node();
        pb::WindowNode* window = derive->mutable_window_node();
        pb::WindowSpec* window_spec = window->mutable_window_spec();
        pb::Expr pb_expr1;
        pb::ExprNode* pb_expr_node1 = pb_expr1.add_nodes();
        pb_expr_node1->set_node_type(pb::SLOT_REF);
        pb_expr_node1->set_col_type(pb::INT32);
        pb_expr_node1->mutable_derive_node()->set_tuple_id(0);
        pb_expr_node1->mutable_derive_node()->set_slot_id(1);
        window_spec->add_partition_exprs()->CopyFrom(pb_expr1);
        pb::Expr pb_expr2;
        pb::ExprNode* pb_expr_node2 = pb_expr2.add_nodes();
        pb_expr_node2->set_node_type(pb::SLOT_REF);
        pb_expr_node2->set_col_type(pb::INT32);
        pb_expr_node2->mutable_derive_node()->set_tuple_id(0);
        pb_expr_node2->mutable_derive_node()->set_slot_id(2);
        window_spec->add_partition_exprs()->CopyFrom(pb_expr2);

        WindowNode window_node;
        window_node.init(pb_plan_node);

        RowBatch row_batch1;
        construct_row_batch(mem_row_desc, row_batch1, 0);
        bool is_first_partition_belong_to_prev = false;
        window_node.split_into_partitions(&row_batch1, is_first_partition_belong_to_prev);
        EXPECT_EQ(is_first_partition_belong_to_prev, false);
        std::string expected_str = "[0,1),[1,3),[3,6),[6,10),[10,20),[20,26)";
        std::string str;
        while (window_node.has_cache_partition()) {
            int32_t start = -1;
            int32_t end = -1;
            window_node.get_next_partition(start, end);
            str += "[" + std::to_string(start) + "," + std::to_string(end) + "),";
        }
        str.pop_back();
        EXPECT_EQ(str, expected_str);

        RowBatch row_batch2;
        construct_row_batch(mem_row_desc, row_batch2, 5);
        window_node.split_into_partitions(&row_batch2, is_first_partition_belong_to_prev);
        EXPECT_EQ(is_first_partition_belong_to_prev, true);
        expected_str = "[0,6),[6,13),[13,21),[21,30),[30,40),[40,51)";
        str.clear();
        while (window_node.has_cache_partition()) {
            int32_t start = -1;
            int32_t end = -1;
            window_node.get_next_partition(start, end);
            str += "[" + std::to_string(start) + "," + std::to_string(end) + "),";
        }
        str.pop_back();
        EXPECT_EQ(str, expected_str);
    }
    {
        // OVER (PARTITION BY col1)
        pb::PlanNode pb_plan_node;
        pb::DerivePlanNode* derive = pb_plan_node.mutable_derive_node();
        pb::WindowNode* window = derive->mutable_window_node();
        pb::WindowSpec* window_spec = window->mutable_window_spec();
        pb::Expr pb_expr1;
        pb::ExprNode* pb_expr_node1 = pb_expr1.add_nodes();
        pb_expr_node1->set_node_type(pb::SLOT_REF);
        pb_expr_node1->set_col_type(pb::INT32);
        pb_expr_node1->mutable_derive_node()->set_tuple_id(0);
        pb_expr_node1->mutable_derive_node()->set_slot_id(1);
        window_spec->add_partition_exprs()->CopyFrom(pb_expr1);
        WindowNode window_node;
        window_node.init(pb_plan_node);

        RowBatch row_batch1;
        construct_row_batch(mem_row_desc, row_batch1, 0);
        bool is_first_partition_belong_to_prev = false;
        window_node.split_into_partitions(&row_batch1, is_first_partition_belong_to_prev);
        EXPECT_EQ(is_first_partition_belong_to_prev, false);
        std::string expected_str = "[0,26)";
        std::string str;
        while (window_node.has_cache_partition()) {
            int32_t start = -1;
            int32_t end = -1;
            window_node.get_next_partition(start, end);
            str += "[" + std::to_string(start) + "," + std::to_string(end) + "),";
        }
        str.pop_back();
        EXPECT_EQ(str, expected_str);

        RowBatch row_batch2;
        construct_row_batch(mem_row_desc, row_batch2, 5);
        window_node.split_into_partitions(&row_batch2, is_first_partition_belong_to_prev);
        expected_str = "[0,51)";
        str.clear();
        EXPECT_EQ(is_first_partition_belong_to_prev, true);
        while (window_node.has_cache_partition()) {
            int32_t start = -1;
            int32_t end = -1;
            window_node.get_next_partition(start, end);
            str += "[" + std::to_string(start) + "," + std::to_string(end) + "),";
        }
        str.pop_back();
        EXPECT_EQ(str, expected_str);
    }
    {
        // OVER ()
        pb::PlanNode pb_plan_node;
        WindowNode window_node;
        window_node.init(pb_plan_node);

        RowBatch row_batch1;
        construct_row_batch(mem_row_desc, row_batch1, 0);
        bool is_first_partition_belong_to_prev = false;
        window_node.split_into_partitions(&row_batch1, is_first_partition_belong_to_prev);
        EXPECT_EQ(is_first_partition_belong_to_prev, true);
        std::string expected_str = "[0,26)";
        std::string str;
        while (window_node.has_cache_partition()) {
            int32_t start = -1;
            int32_t end = -1;
            window_node.get_next_partition(start, end);
            str += "[" + std::to_string(start) + "," + std::to_string(end) + "),";
        }
        str.pop_back();
        EXPECT_EQ(str, expected_str);

        RowBatch row_batch2;
        construct_row_batch(mem_row_desc, row_batch2, 5);
        window_node.split_into_partitions(&row_batch2, is_first_partition_belong_to_prev);
        EXPECT_EQ(is_first_partition_belong_to_prev, true);
        expected_str = "[0,51)";
        str.clear();
        while (window_node.has_cache_partition()) {
            int32_t start = -1;
            int32_t end = -1;
            window_node.get_next_partition(start, end);
            str += "[" + std::to_string(start) + "," + std::to_string(end) + "),";
        }
        str.pop_back();
        EXPECT_EQ(str, expected_str);
    }
}

} // namespace baikaldb