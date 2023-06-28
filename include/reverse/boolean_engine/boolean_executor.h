// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#pragma once
#include <vector>
#include <google/protobuf/message.h>
namespace baikaldb {

// 传递给bool引擎的参数基类
class BoolArg {
public:
    virtual ~BoolArg() {
    }
};

// 获取并遍历倒排链表的接口
template <typename Schema>
class RindexNodeParser {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;

    RindexNodeParser(Schema* schema) : _schema(schema) {
    } 
    virtual ~RindexNodeParser() {
    }
    virtual int init(const std::string& term) = 0;
    //第一次调用返回第一个元素
    virtual const PostingNodeT* current_node() = 0;
    virtual const PrimaryIdT* current_id() = 0;
    //第一次调用返回第二个元素
    virtual const PostingNodeT* next() = 0;
    //如果倒排链表是有序数组，用二分查找优化
    //大于等于target_id的第一个元素（包括当前元素）
    virtual const PostingNodeT* advance(const PrimaryIdT& target_id) = 0; 
protected:
    Schema* _schema;
};

enum bool_executor_type {
    NODE_COPY = 1,
    NODE_NOT_COPY
};

// 基类，接口
template<typename PostingNodeType>
class BooleanExecutorBase {
public:
    virtual const PostingNodeType* next() = 0;
    virtual ~BooleanExecutorBase() {}
};

// 布尔引擎的节点
// 布尔引擎是一个树，遍历根节点获取最终结果
template <typename Schema>
class BooleanExecutor : public BooleanExecutorBase<typename Schema::PostingNodeT>{
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;

    virtual const PostingNodeT* current_node() = 0;
    virtual const PrimaryIdT* current_id() = 0;
    //第一次调用返回第一个元素
    virtual const PostingNodeT* next() = 0;
    //大于等于target_id的第一个元素（包括当前元素）
    virtual const PostingNodeT* advance(const PrimaryIdT& target_id) = 0;

    bool_executor_type get_type() {
        return _type;
    }

protected:
    bool _init_flag = true;
    bool_executor_type _type;   
};

// Function object
template <typename Schema>
class CompareAsc {
public:
    bool operator()(
            BooleanExecutor<Schema>* exe1,
            BooleanExecutor<Schema>* exe2) {
        if (exe1->current_id() == NULL) {
            return false;
        } else if (exe2->current_id() == NULL) {
            return true;
        } else {
            int res = Schema::compare_id_func(*exe1->current_id(), *exe2->current_id());
            if (res < 0) {
                return true;
            } else {
                return false;
            }
        }
    }
};

// 布尔引擎的叶子节点
// 获取term的倒排链表
template <typename Schema>
class TermBooleanExecutor : public BooleanExecutor<Schema> {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;

    TermBooleanExecutor(
                RindexNodeParser<Schema>* list, 
                const std::string& term, 
                bool_executor_type type = NODE_NOT_COPY, 
                BoolArg* arg = nullptr);
    virtual ~TermBooleanExecutor();
    virtual const PostingNodeT* current_node();
    virtual const PrimaryIdT* current_id();
    virtual const PostingNodeT* next();
    virtual const PostingNodeT* advance(const PrimaryIdT& target_id);
private:
    RindexNodeParser<Schema>* _posting_list;     // 倒排拉链
    std::string _term;
    PostingNodeT _curr_node;
    PostingNodeT* _curr_node_ptr = nullptr;
    BoolArg* _arg = nullptr;    //需要改变拉链节点内容，而原拉链又只读
                                //则需要在布尔节点的_curr_node改变，_arg是用户传过来的参数   
};

// 布尔引擎的非叶子节点
// 包括and or weight三种布尔逻辑节点，后续有新的布尔逻辑，可以扩展
template <typename Schema>
class OperatorBooleanExecutor : public BooleanExecutor<Schema> {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;
    typedef int (*MergeFuncT)(PostingNodeT&, const PostingNodeT&, BoolArg*);

    explicit OperatorBooleanExecutor();
    virtual ~OperatorBooleanExecutor();

    void add(BooleanExecutor<Schema>* executor);
    void set_merge_func(MergeFuncT merge_func);

protected:
    //子节点，针对or做堆
    std::vector<BooleanExecutor<Schema>*> _sub_clauses;
    //布尔操作的函数（and or weight）
    MergeFuncT _merge_func;
    PostingNodeT *_curr_node_ptr;
    const PrimaryIdT *_curr_id_ptr;
    PostingNodeT _curr_node;
    PrimaryIdT _curr_id;
    bool _is_null_flag;
    BoolArg* _arg = nullptr; //需要改变拉链节点内容，而原拉链又只读
                             //则需要在布尔节点的_curr_node改变，_arg是用户传过来的参数
};

// and节点
// 多个倒排链表做and操作
template <typename Schema>
class AndBooleanExecutor : public OperatorBooleanExecutor<Schema> {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;
    typedef int (*MergeFuncT)(PostingNodeT&, const PostingNodeT&, BoolArg*);

    explicit AndBooleanExecutor(bool_executor_type type = NODE_NOT_COPY, BoolArg* arg = nullptr);
    virtual ~AndBooleanExecutor();

    virtual const PostingNodeT* current_node();
    virtual const PrimaryIdT* current_id();
    virtual const PostingNodeT* next();
    virtual const PostingNodeT* advance(const PrimaryIdT& target_id);
private:
    const PostingNodeT* find_next();
};

template <typename Schema>
class OrBooleanExecutor : public OperatorBooleanExecutor<Schema> {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;
    typedef int (*MergeFuncT)(PostingNodeT&, const PostingNodeT&, BoolArg*);

    explicit OrBooleanExecutor(bool_executor_type type = NODE_NOT_COPY, BoolArg* arg = nullptr); 
    virtual ~OrBooleanExecutor();

    virtual const PostingNodeT* current_node();
    virtual const PrimaryIdT* current_id();
    virtual const PostingNodeT* next();
    virtual const PostingNodeT* advance(const PrimaryIdT& target_id);

private:

    const PostingNodeT* find_next();
    void make_heap();
    void shiftdown(size_t index);
};

template <typename Schema>
class WeightedBooleanExecutor : public OperatorBooleanExecutor<Schema> {
public:
    typedef typename Schema::PostingNodeT PostingNodeT;
    typedef typename Schema::PrimaryIdT PrimaryIdT;
    typedef int (*MergeFuncT)(PostingNodeT&, const PostingNodeT&, BoolArg*);

    explicit WeightedBooleanExecutor(
                    bool_executor_type type = NODE_NOT_COPY, 
                    BoolArg* arg = nullptr);
    virtual ~WeightedBooleanExecutor();

    virtual const PostingNodeT* current_node();
    virtual const PrimaryIdT* current_id();
    virtual const PostingNodeT* next();
    virtual const PostingNodeT* advance(const PrimaryIdT& target_id);

    void add_not_must(BooleanExecutor<Schema>* executor);
    void add_must(BooleanExecutor<Schema>* executor);

private:
    void add_weight();
private:
    BooleanExecutor<Schema>* _op_executor;
};

}  // namespace boolean_engine

#include "boolean_executor.hpp"

