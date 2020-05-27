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
#include "common.h"
#include "proto/meta.interface.pb.h"

namespace baikaldb {

class CMsketchColumn {
public:
    CMsketchColumn(int depth, int width, int field_id) :  _depth(depth), _width(width),  _field_id(field_id) {
        _array = new int* [_depth];
        for (int i = 0; i < _depth; i++) {
            _array[i] = new int[_width];
        }
        for (int i = 0; i < _depth; i++) {
            for (int j = 0; j < _width; j++) {
                _array[i][j] = 0;
            }
        }
    }

    ~CMsketchColumn() {
        for (int i = 0; i < _depth; i++) {
            delete []_array[i];
        }
        delete []_array;
        _array = nullptr;
    }

    void set_value(uint64_t hash, uint64_t count) {
        for (int i = 0; i < _depth; i++) {
            int j = (hash * i) % _width;
            _array[i][j] += count; 
        }
    }

    int get_value(uint64_t hash) {
        int min_count = INT32_MAX;
        for (int i = 0; i < _depth; i++) {
            int j = (hash * i) % _width;
            if (_array[i][j] < min_count) {
                min_count = _array[i][j];
            }
        }  
        return min_count;  
    }

    CMsketchColumn& operator+=(const CMsketchColumn& other) {
        if (_depth != other._depth || _width != other._width) {
            return *this;
        }
        for (int i = 0; i < _depth; i++) {
            for (int j = 0; j < _width; j++) {
                _array[i][j] += other._array[i][j];
            }
        }
        return *this;
    }

    void to_proto(pb::CMsketchColumn* cmsketch_column) {
        cmsketch_column->set_field_id(_field_id);
        for (int i = 0; i < _depth; i++) {
            for (int j = 0; j < _width; j++) {
                if (_array[i][j] > 0) {
                    pb::CMsketchItem* item = cmsketch_column->add_cmitems();
                    item->set_depth(i);
                    item->set_width(j);
                    item->set_value(_array[i][j]);
                }
            }
        }
    }

    void add_proto(const pb::CMsketchColumn& cmsketch_column) {
        if (_field_id != cmsketch_column.field_id()) {
            return;
        }
        for (auto& item : cmsketch_column.cmitems()) {
            _array[item.depth()][item.width()] += item.value();
        }
    }

    int get_depth() {
        return _depth;
    }

    int get_width() {
        return _width;
    }

    void to_string(std::vector<std::vector<std::string>>& rows) {
        for (int i = 0; i < _depth; i++) {
            std::vector<std::string> row;
            row.push_back(std::to_string(i + 1));
            for (int j = 0; j < _width; j++) {
                row.push_back(std::to_string(_array[i][j]));
            }
            rows.push_back(row);
        }
    }

public:
    int _depth;
    int _width;
    int _field_id;
    int** _array = nullptr;
};

struct CMsketch {
public:
    CMsketch(int depth, int width) : _depth(depth), _width(width) {
        bthread_mutex_init(&_mutex, NULL);
    }

    ~CMsketch() {
        bthread_mutex_destroy(&_mutex);
    }

    void set_value(int field_id, uint64_t hash) {
        BAIDU_SCOPED_LOCK(_mutex);
        auto iter = _column_cmsketch.find(field_id);
        if (iter != _column_cmsketch.end()) {
            iter->second->set_value(hash, 1);
        } else {
            auto ptr = std::make_shared<CMsketchColumn>(_depth, _width, field_id);
            ptr->set_value(hash, 1);
            _column_cmsketch[field_id] = ptr;
        }
    }

    void to_proto(pb::CMsketch* cmsketch) {
        BAIDU_SCOPED_LOCK(_mutex);
        cmsketch->set_depth(_depth);
        cmsketch->set_width(_width);
        for (auto iter = _column_cmsketch.begin(); iter != _column_cmsketch.end(); iter++) {
            pb::CMsketchColumn* cmcloumn = cmsketch->add_cmcolumns();
            iter->second->to_proto(cmcloumn);
        }
    }

    void add_proto(const pb::CMsketch& cmsketch) {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_depth != cmsketch.depth() || _width != cmsketch.width()) {
            return;
        }
        for (auto& cmcolumn : cmsketch.cmcolumns()) {
            auto iter = _column_cmsketch.find(cmcolumn.field_id());
            if (iter != _column_cmsketch.end()) {
                iter->second->add_proto(cmcolumn);
            } else {
                auto ptr = std::make_shared<CMsketchColumn>(_depth, _width, cmcolumn.field_id());
                ptr->add_proto(cmcolumn);
                _column_cmsketch[cmcolumn.field_id()] = ptr;
            }
        }
    }

    int get_depth() {
        return _depth;
    }

    int get_width() {
        return _width;
    }

    int get_sample_rows() {
        return _sample_rows;
    }

    void set_sample_rows(int sample_rows) {
        _sample_rows = sample_rows;
    }

    int64_t get_table_rows() {
        return _table_rows;
    }

    void set_table_rows(int64_t table_rows) {
        _table_rows = table_rows;
    }

public:
    int _depth;
    int _width;
    int _sample_rows;
    int64_t _table_rows;
    bthread_mutex_t _mutex;
    std::map<int, std::shared_ptr<CMsketchColumn>> _column_cmsketch;
};
}
