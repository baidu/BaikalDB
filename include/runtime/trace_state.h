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

#pragma once

#include <stdint.h>
#include "common.h"
#include "mem_row_descriptor.h"
#include "data_buffer.h"
#include "proto/store.interface.pb.h"
#include "arrow/acero/options.h"
#include "arrow/acero/exec_plan.h"
using google::protobuf::RepeatedPtrField;

namespace baikaldb {
    
enum {
    OPEN_TRACE = 0,
    GET_NEXT_TRACE = 1
    // TXN_GET = 2,
    // TXN_PUT = 3,
    // TXN_DELETE = 4,
    // TXN_COMMIT = 5
};

struct TraceTimeCost {
    int64_t start;
    int64_t other_cost;
};

class TraceLocalNode {
public:
    TraceLocalNode(const char* desc, 
                   pb::TraceNode* trace_node, std::vector<TraceTimeCost>* trace_cost_vec, int trace_type,
                   std::function<void(TraceLocalNode&)> exit_func) : 
                   _description(desc),
                   _trace_node(trace_node),
                   _trace_cost_vec(trace_cost_vec),
                   _trace_type(trace_type),
                   _exit_func(exit_func) {
        if (_trace_node != nullptr) {
            local_trace_cost_constructor();
            if (_trace_type == OPEN_TRACE) {
                _local_node = _trace_node->mutable_open_trace();
            } else if (_trace_type == GET_NEXT_TRACE) {
                _local_node = _trace_node->mutable_get_next_trace();
            }
        }
    }
    
    ~TraceLocalNode() {
        if (_local_node != nullptr) {
            if (_exit_func != nullptr) {
                _exit_func(*this);
            }
            _local_node->set_time_cost_us(_local_node->time_cost_us() 
                                          + local_trace_cost_destructor());
            std::string desc(_description);
            desc += " " + _append_description.str();
            _local_node->set_description(desc.c_str());
        }
    }
    
    void set_successful() {
        _is_successful = 0;
    }
    
    void set_failed() {
        _is_successful = -1;
    }
    
    std::ostringstream& append_description() {
        return _append_description;
    }

    void set_index_name(const std::string& idx_name) {
        if (_trace_node != nullptr) {
            _local_node->set_index_name(idx_name);
        }
    }

    void set_affect_rows(int64_t affect_rows) {
        if (_trace_node != nullptr) {
            _local_node->set_affect_rows(affect_rows);
        }
    }

    void set_scan_rows(int64_t scan_rows) {
        if (_trace_node != nullptr) {
            _local_node->set_scan_rows(scan_rows);
        }
    }
    
    void add_index_filter_rows(int64_t rows) {
        if (_trace_node != nullptr) {
            _local_node->set_index_filter_rows(
                _local_node->index_filter_rows() + rows);
        }
    }

    void add_where_filter_rows(int64_t rows) {
        if (_trace_node != nullptr) {
            _local_node->set_where_filter_rows(
                _local_node->where_filter_rows() + rows);
        }
    }
    
    void add_get_primary_rows(int64_t rows) {
        if (_trace_node != nullptr) {
            _local_node->set_get_primary_rows(
                _local_node->get_primary_rows() + rows);
        }
    }
    
    void add_sort_time(int64_t time_cost) {
        if (_trace_node != nullptr) {
            _local_node->set_sort_time(_local_node->sort_time() + time_cost);
        }
    }

    static int64_t get_scan_rows(pb::TraceNode* trace_node) {
        int64_t rows = 0;
        if (trace_node == nullptr) {
            return 0;
        }
        if (trace_node->has_node_type() && trace_node->node_type() == pb::SCAN_NODE) {
            rows += trace_node->mutable_get_next_trace()->affect_rows();
            return rows;
        }
        if (trace_node->child_nodes().size() > 0) {
            for (auto node : trace_node->child_nodes()) {
                rows += get_scan_rows(&node);
            }
        }
        return rows;
    }

    pb::TraceNode* get_trace() {
        return _trace_node;
    }

    static std::string print_declaration(const arrow::acero::Declaration& dec, std::shared_ptr<arrow::Schema> source_schema = nullptr, bool* info = nullptr) {
        const std::string& factory_name = dec.factory_name;
        std::string desc = "{" + factory_name + ": ";
        if (dec.options == nullptr) {
            desc += "NULL option}";
        } else {
            if (factory_name == "record_batch_source") {
                if (source_schema != nullptr) {
                    desc += "source schema: " + source_schema->ToString() + "}";
                } else {
                    desc += "NULL schema}";
                }
                if (info != nullptr) {
                    desc += " [delay fetcher store: " + std::to_string(*info) + "]";
                }
            } else if (factory_name == "order_by") {
                auto option = static_cast<arrow::acero::OrderByNodeOptions*>(dec.options.get());
                desc += option->ordering.ToString() + "}";
            } else if (factory_name == "filter") {
                auto option = static_cast<arrow::acero::FilterNodeOptions*>(dec.options.get());
                desc += option->filter_expression.ToString() + "}";
            } else if (factory_name == "fetch") {
                auto option = static_cast<arrow::acero::FetchNodeOptions*>(dec.options.get());
                desc += "offset: " + std::to_string(option->offset) + ", count: " + std::to_string(option->count) + "}";
            } else if (factory_name == "aggregate") {
                auto option = static_cast<arrow::acero::AggregateNodeOptions*>(dec.options.get());
                std::string agg_func;
                std::string group_by_keys;
                for (auto& k : option->keys) {
                    group_by_keys += k.ToString() + " ";
                }
                for (auto& agg : option->aggregates) {
                    agg_func += "[" + agg.function + "(";
                    for (auto f : agg.target) {
                        agg_func += f.ToString() + ",";
                    }
                    agg_func.pop_back();
                    agg_func += ") -> " + agg.name + "] ";
                }
                desc += agg_func + " group by [" + group_by_keys + "]}";
            } else if (factory_name == "project") {
                auto option = static_cast<arrow::acero::ProjectNodeOptions*>(dec.options.get());
                desc += " [";
                for (auto& f : option->expressions) {
                    desc += f.ToString() + ",";
                }
                desc.pop_back();
                desc += "] -> [ ";
                for (auto& f : option->names) {
                    desc += f + ",";
                }
                desc.pop_back();
                desc += "] }";
            } else if (factory_name == "hashjoin") {
                auto option = static_cast<arrow::acero::HashJoinNodeOptions*>(dec.options.get());
                desc += "join type: " + arrow::acero::ToString(option->join_type) + ", on left keys: ";
                for (auto field : option->left_keys) {
                    desc += field.ToString() + ",";
                }
                desc.pop_back();
                desc += "; on right keys: ";
                for (auto field : option->right_keys) {
                    desc += field.ToString() + ",";
                }
                desc.pop_back();
                desc += "; filter: " + option->filter.ToString() + "; }";
                if (info != nullptr) {
                    desc += " [use_index_join: " + std::to_string(*info) + "]";
                }
            } else {
                desc += "NOT SUPPORT TYPE}";
            }
        }
        return desc;
    }

    void add_arrow_plan(const arrow::acero::Declaration& dec, std::shared_ptr<arrow::Schema> source_schema = nullptr, bool* info = nullptr) {
        if (_trace_node != nullptr) {
            _local_node->add_arrow_plan(print_declaration(dec, source_schema, info));
        }
    }

    void add_arrow_filter(const arrow::compute::Expression* filter, int64_t limit) {
        if (_trace_node != nullptr) {
            std::string filter_str = (filter == nullptr  ? "null" : filter->ToString());
            std::string desc = "{local filter: " + filter_str + ", " + "limit: " + std::to_string(limit) + "}";
            _local_node->add_arrow_plan(desc);
        }
    }
private:
    void local_trace_cost_constructor() {
        if (_trace_cost_vec != nullptr) {
            TraceTimeCost cost;
            cost.start = butil::gettimeofday_us();
            cost.other_cost = 0;
            _trace_cost_vec->push_back(cost);
        }
        _start_time = butil::gettimeofday_us();
    }

    int64_t local_trace_cost_destructor () {
        int64_t now = butil::gettimeofday_us();
        if (_trace_cost_vec == nullptr) {
            return now - _start_time;
        }
        if (_trace_cost_vec->size() == 0) {
            return -1;
        }
        int64_t real_cost = now - _trace_cost_vec->back().start - _trace_cost_vec->back().other_cost;
        int64_t total_cost = now - _trace_cost_vec->back().start;
        _trace_cost_vec->pop_back();
        if (_trace_cost_vec->size() != 0) {
            _trace_cost_vec->back().other_cost += total_cost;
        }

        return real_cost;
    }
private:
    const char* _description;
    std::ostringstream _append_description;
    pb::TraceNode* _trace_node = nullptr;
    std::vector<TraceTimeCost>* _trace_cost_vec = nullptr;
    int    _trace_type      = OPEN_TRACE;
    int    _is_successful   = 1;
    pb::LocalTraceNode* _local_node = nullptr;
    std::function<void(TraceLocalNode&)> _exit_func = nullptr;
    int64_t _start_time = 0;
};

class TraceDescVoidify {
        public: 
            TraceDescVoidify() { }
            // This has to be an operator with a precedence lower than << but
            // higher than ?:
            void operator&(std::ostream&) { }
};

#define TRACE_LOCAL_NODE_NAME trace_local_node_statistics
#define START_LOCAL_TRACE(trace, vec, type, callback) \
TraceLocalNode TRACE_LOCAL_NODE_NAME (__FUNCTION__, trace, vec, type, callback)
#define LOCAL_TRACE(condition) !(condition) ? void(0) : TraceDescVoidify() & TRACE_LOCAL_NODE_NAME.append_description()
#define LOCAL_TRACE_DESC LOCAL_TRACE(TRACE_LOCAL_NODE_NAME.get_trace())
#define LOCAL_TRACE_ARROW_FILTER(filter, limit) TRACE_LOCAL_NODE_NAME.add_arrow_filter(filter, limit)
#define LOCAL_TRACE_ARROW_PLAN(plan) TRACE_LOCAL_NODE_NAME.add_arrow_plan(plan)
#define LOCAL_TRACE_ARROW_PLAN_WITH_INFO(plan, info) TRACE_LOCAL_NODE_NAME.add_arrow_plan(plan, nullptr, info)
#define LOCAL_TRACE_ARROW_PLAN_WITH_SCHEMA(plan, schema, info) TRACE_LOCAL_NODE_NAME.add_arrow_plan(plan, schema, info)
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
