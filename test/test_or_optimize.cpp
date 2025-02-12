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

#include "filter_node.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {

TEST(test_or_node_optimize, test_all_and) {
    // (a = 3 and b = 3) or  (a = 2 and b = 2) or (a = 1 and b = 1)
    pb::Expr expr;
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::OR_PREDICATE);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(3);
        pb::Function func;
        func.set_name("logic_or");
        func.set_fn_op(parser::FT_LOGIC_OR);
        pb_expr_node->mutable_fn()->Swap(&func);
    }
    // and 1
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::AND_PREDICATE);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("logic_and");
        func.set_fn_op(parser::FT_LOGIC_AND);
        pb_expr_node->mutable_fn()->Swap(&func);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(4);
        slot_ref_derive_expr_node.set_field_id(4);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(3);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(3);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    // and 2
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::AND_PREDICATE);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("logic_and");
        func.set_fn_op(parser::FT_LOGIC_AND);
        pb_expr_node->mutable_fn()->Swap(&func);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(4);
        slot_ref_derive_expr_node.set_field_id(4);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(2);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(2);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    // and 3
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::AND_PREDICATE);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("logic_and");
        func.set_fn_op(parser::FT_LOGIC_AND);
        pb_expr_node->mutable_fn()->Swap(&func);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(4);
        slot_ref_derive_expr_node.set_field_id(4);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(1);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(1);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    ExprNode* conjunct = nullptr;
    ExprNode::create_tree(expr, &conjunct); 
    ExprNode::or_node_optimize(&conjunct);
    conjunct->print_expr_info();
    EXPECT_EQ(conjunct->node_type(), pb::IN_PREDICATE);
    EXPECT_EQ(conjunct->children_size(), 4);
    EXPECT_EQ(conjunct->children(0)->node_type(), pb::ROW_EXPR);
    EXPECT_EQ(conjunct->children(0)->children(0)->node_type(), pb::SLOT_REF);
    EXPECT_EQ(conjunct->children(0)->children(1)->node_type(), pb::SLOT_REF);
    EXPECT_EQ(static_cast<SlotRef*>(conjunct->children(0)->children(0))->slot_id(), 4);
    EXPECT_EQ(static_cast<SlotRef*>(conjunct->children(0)->children(1))->slot_id(), 5);
    EXPECT_EQ(conjunct->children(1)->node_type(), pb::ROW_EXPR);
    EXPECT_EQ(conjunct->children(2)->node_type(), pb::ROW_EXPR);
    EXPECT_EQ(conjunct->children(3)->node_type(), pb::ROW_EXPR);
    EXPECT_EQ(conjunct->children(1)->children(0)->node_type(), pb::INT_LITERAL);
    EXPECT_EQ(conjunct->children(1)->children(1)->node_type(), pb::INT_LITERAL);
    EXPECT_EQ(conjunct->children(2)->children(0)->node_type(), pb::INT_LITERAL);
    EXPECT_EQ(conjunct->children(2)->children(1)->node_type(), pb::INT_LITERAL);
    EXPECT_EQ(conjunct->children(3)->children(0)->node_type(), pb::INT_LITERAL);
    EXPECT_EQ(conjunct->children(3)->children(1)->node_type(), pb::INT_LITERAL);
}

TEST(test_or_node_optimize, test_all_eq) {
    pb::Expr expr;
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::OR_PREDICATE);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(3);
        pb::Function func;
        func.set_name("logic_or");
        func.set_fn_op(parser::FT_LOGIC_OR);
        pb_expr_node->mutable_fn()->Swap(&func);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(3);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(2);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(1);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }

    ExprNode* conjunct = nullptr;
    ExprNode::create_tree(expr, &conjunct); 
    ExprNode::or_node_optimize(&conjunct);
    conjunct->print_expr_info();
    EXPECT_EQ(conjunct->node_type(), pb::IN_PREDICATE);
    EXPECT_EQ(conjunct->children_size(), 4);
    EXPECT_EQ(conjunct->children(0)->node_type(), pb::SLOT_REF);
    EXPECT_EQ(static_cast<SlotRef*>(conjunct->children(0))->slot_id(), 5);
    EXPECT_EQ(conjunct->children(1)->node_type(), pb::INT_LITERAL);
    EXPECT_EQ(conjunct->children(2)->node_type(), pb::INT_LITERAL);
    EXPECT_EQ(conjunct->children(3)->node_type(), pb::INT_LITERAL);
}

TEST(test_or_node_optimize, test_not_same_children) {
    pb::Expr expr;
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::OR_PREDICATE);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(3);
        pb::Function func;
        func.set_name("logic_or");
        func.set_fn_op(parser::FT_LOGIC_OR);
        pb_expr_node->mutable_fn()->Swap(&func);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(3);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(2);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    // and 3
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::AND_PREDICATE);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("logic_and");
        func.set_fn_op(parser::FT_LOGIC_AND);
        pb_expr_node->mutable_fn()->Swap(&func);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(4);
        slot_ref_derive_expr_node.set_field_id(4);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(1);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }
    {
        pb::ExprNode* pb_expr_node = expr.add_nodes();
        pb_expr_node->set_node_type(pb::FUNCTION_CALL);
        pb_expr_node->set_col_type(pb::BOOL);
        pb_expr_node->set_num_children(2);
        pb::Function func;
        func.set_name("eq");
        func.set_fn_op(parser::FT_EQ);
        pb_expr_node->mutable_fn()->Swap(&func);

        // slot_ref
        pb::ExprNode* slot_ref_pb_expr_node = expr.add_nodes();
        slot_ref_pb_expr_node->set_node_type(pb::SLOT_REF);
        slot_ref_pb_expr_node->set_col_type(pb::INT32);
        slot_ref_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode slot_ref_derive_expr_node;
        slot_ref_derive_expr_node.set_tuple_id(0);
        slot_ref_derive_expr_node.set_slot_id(5);
        slot_ref_derive_expr_node.set_field_id(5);
        slot_ref_pb_expr_node->mutable_derive_node()->Swap(&slot_ref_derive_expr_node);
        slot_ref_pb_expr_node->set_col_flag(0);

        // literal
        pb::ExprNode* literal_pb_expr_node = expr.add_nodes();
        literal_pb_expr_node->set_node_type(pb::INT_LITERAL);
        literal_pb_expr_node->set_col_type(pb::INT64);
        literal_pb_expr_node->set_num_children(0);
        pb::DeriveExprNode literal_derive_expr_node;
        literal_derive_expr_node.set_int_val(1);
        literal_pb_expr_node->mutable_derive_node()->Swap(&literal_derive_expr_node);
    }

    ExprNode* conjunct = nullptr;
    ExprNode::create_tree(expr, &conjunct); 
    conjunct->print_expr_info();
    EXPECT_EQ(conjunct->has_same_children(), false);
}

} // namespace baikaldb