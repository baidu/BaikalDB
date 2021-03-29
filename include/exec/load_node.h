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

// Brief:  truncate table exec node
#pragma once

#include "exec_node.h"
#include "insert_manager_node.h"

#include <iostream>
#include <istream>
#include <streambuf>

#ifdef BAIDU_INTERNAL
#include <base/files/file.h>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#else
#include <butil/files/file.h>
#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <butil/files/file_enumerator.h>
#endif
#ifdef BAIDU_INTERNAL
namespace butil = base;
#endif

namespace baikaldb {
class LoadNode : public ExecNode {
public:
    LoadNode() {
    }
    virtual ~LoadNode() {
    }
    virtual int init(const pb::PlanNode& node);
    virtual int open(RuntimeState* state);

private:
    int ignore_specified_lines(butil::File& file, char* data_buffer, int64_t buf_size);
    int handle_lines(RuntimeState* state, std::vector<std::string>& row_lines);
    ExprValue create_field_value(FieldInfo& field_info, std::string& str_val, bool& is_legal);
    int fill_field_value(SmartRecord record, FieldInfo& field, ExprValue& value);

private:
    const int64_t    BUFFER_SIZE = 8 * 1024 * 1024ULL; // 8M
    struct MemBuf : std::streambuf{
        MemBuf(char* begin, char* end) {
            this->setg(begin, begin, end);
        }
    };
    int64_t _table_id = -1;
    int32_t _ignore_lines = 0;

    pb::Charset  _char_set;

    bool _opt_enclosed = false;
    std::string _data_path;
    int64_t     _file_size;
    std::string _terminated;
    std::string _enclosed;
    std::string _escaped;
    std::string _line_starting;
    std::string _line_terminated;
    bool       _read_eof = false;
    bool       _has_get_line = false;
    int64_t    _file_cur_pos = 0;
    int64_t    _buf_cur_pos = 0;

    std::vector<pb::SlotDescriptor> _set_slots;
    std::vector<ExprNode*> _set_exprs;
    std::vector<int32_t> _field_ids;
    std::set<int32_t> _default_field_ids;
    std::set<int32_t> _ingore_field_indexes;

    SchemaFactory* _factory = nullptr;
    SmartTable       _table_info;
    SmartIndex       _pri_info;

    InsertManagerNode* _insert_manager = nullptr;
    int    _affected_rows = 0;
};

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
