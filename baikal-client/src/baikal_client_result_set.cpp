// Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
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
 
/**
 * @file baikal_client_result_set.cpp
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/11 16:22:59
 * @brief 
 *  
 **/
#include "baikal_client_result_set.h"
#ifdef BAIDU_INTERNAL
#include "com_log.h"
#endif

using std::string;

namespace baikal {
namespace client {
ResultSet::ResultSet():
        _type(NONE),
        _res(NULL),
        _is_select(false),
        _store (false),
        _pos(0),
        _fetch_row(NULL),
        _rows_count(0),
        _fields_count(0),
        _affected_rows(0),
        _auto_insert_id(0) {
        }

ResultSet::~ResultSet() {
    this->_free_result();
}

bool ResultSet::next() {
    if (_type != MYSQL_CONN) {
        CLIENT_WARNING("connection type is not mysql");
        return false;
    }
    if (!_is_select) {
        CLIENT_WARNING("sql statement is not query");
        return false;
    }
    if (_pos >= _rows_count) {
        return false;
    }
    MYSQL_ROW row = mysql_fetch_row(_res);
    if (row != NULL && _fetch_row != NULL) {
        _fetch_row->set_row(row);
        ++_pos;
        return true;
    }
    return false;
}

bool ResultSet::seek_row(uint64_t row_index) {
    if (_type != MYSQL_CONN) {
        CLIENT_WARNING("connection type is not mysql");
        return false;
    }
    if (!_is_select) {
        CLIENT_WARNING("sql statement is not query");
        return false;
    }
    if (row_index >= _rows_count) {
        return false;
    }
    if (row_index == _pos) {
        return true;
    }
    mysql_data_seek(_res, row_index);
    _pos = row_index;
    return true;
}

uint64_t ResultSet::tell() const {
    return _pos;
}

ValueType ResultSet::get_type(uint32_t column_index) {
    if (!_is_valid(column_index)) {
        return ERROR_TYPE;
    }
    return _fetch_row->get_type(column_index);
}
int ResultSet::get_string(uint32_t column_index, string* value) {
    if (!_is_valid(column_index)) {
        return GET_VALUE_ERROR;
    }
    return _fetch_row->get_string(column_index, value);
}

int ResultSet::get_int32(uint32_t column_index, int32_t* value) {
    if (!_is_valid(column_index)) {
        return GET_VALUE_ERROR;
    }
    return _fetch_row->get_int32(column_index, value);
}

int ResultSet::get_uint32(uint32_t column_index, uint32_t* value) {
    if (!_is_valid(column_index)) {
        return GET_VALUE_ERROR;
    }
    return _fetch_row->get_uint32(column_index, value);
}

int ResultSet::get_int64(uint32_t column_index, int64_t* value) {
    if (!_is_valid(column_index)) {
        return GET_VALUE_ERROR;
    }
    return _fetch_row->get_int64(column_index, value);
}

int ResultSet::get_uint64(uint32_t column_index, uint64_t* value) {
    if (!_is_valid(column_index)) {
        return GET_VALUE_ERROR;
    }
    return _fetch_row->get_uint64(column_index, value);
}

int ResultSet::get_float(uint32_t column_index, float* value) {
    if (!_is_valid(column_index)) {
        return GET_VALUE_ERROR;
    }
    return _fetch_row->get_float(column_index, value);
}

int ResultSet::get_double(uint32_t column_index, double* value) {
    if (!_is_valid(column_index)) {
        return GET_VALUE_ERROR;
    }
    return _fetch_row->get_double(column_index, value);
}

int ResultSet::get_string(const string& column_name, string* value) {
    if (_name_id_map.count(column_name) > 0) {
        unsigned int column_index = _name_id_map[column_name];
        return get_string(column_index, value);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return GET_VALUE_ERROR;
}

int ResultSet::get_int32(const string& column_name, int32_t* value) {
    if (_name_id_map.count(column_name) > 0) { 
        unsigned int column_index = _name_id_map[column_name];
        return get_int32(column_index, value);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return GET_VALUE_ERROR;
}

int ResultSet::get_uint32(const string& column_name, uint32_t* value) {
    if (_name_id_map.count(column_name) > 0) { 
        unsigned int column_index = _name_id_map[column_name];
        return get_uint32(column_index, value);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return GET_VALUE_ERROR;
}

int ResultSet::get_int64(const string& column_name, int64_t* value) {
    if (_name_id_map.count(column_name) > 0) { 
        unsigned int column_index = _name_id_map[column_name];
        return get_int64(column_index, value);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return GET_VALUE_ERROR;
}

int ResultSet::get_uint64(const string& column_name, uint64_t* value) {
    if (_name_id_map.count(column_name) > 0) { 
        unsigned int column_index = _name_id_map[column_name];
        return get_uint64(column_index, value);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return GET_VALUE_ERROR;
}

int ResultSet::get_float(const string& column_name, float* value) {
    if (_name_id_map.count(column_name) > 0) { 
        unsigned int column_index = _name_id_map[column_name];
        return get_float(column_index, value);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return GET_VALUE_ERROR;
}

int ResultSet::get_double(const string& column_name, double* value) {
    if (_name_id_map.count(column_name) > 0) { 
        unsigned int column_index = _name_id_map[column_name];
        return get_double(column_index, value);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return GET_VALUE_ERROR;
}

const char* ResultSet::get_value(uint32_t column_index) {
    if (!_is_valid(column_index)) {
        return NULL;
    }
    return _fetch_row->get_value(column_index);
}

const char* ResultSet::get_value(const string& column_name) {
    if (_name_id_map.count(column_name) > 0) {
        unsigned int column_index = _name_id_map[column_name];
        return get_value(column_index);
    }
    CLIENT_WARNING("column_name is not exist, [%s]", column_name.c_str());
    return NULL;
}

MYSQL_RES* ResultSet::get_mysql_res() const {
    if (_type != MYSQL_CONN) {
        CLIENT_WARNING("connection type is not mysql, _type:%d", _type);
        return NULL;
    }
    return _res;
}

bool ResultSet::is_seekable() const {
    return _store;
}

bool ResultSet::is_select() const {
    return _is_select;
}

uint32_t ResultSet::get_field_count() const {
    return _fields_count;
}

uint64_t ResultSet::get_row_count() const {
    return _rows_count;
}

uint64_t ResultSet::get_affected_rows() const {
    return _affected_rows;
}

uint64_t ResultSet::get_auto_insert_id() const {
    return _auto_insert_id;
}

void ResultSet::set_conn_type(ConnectionType type) {
    _type = type;
}

ConnectionType ResultSet::get_conn_type() {
    return _type;
}

void ResultSet::_free_result() {
    if (_res != NULL) {
        mysql_free_result(_res);
        _res = NULL;
    }
    delete _fetch_row;
    _fetch_row = NULL;
    _is_select = false;
    _store = false;
    _pos = 0;
    _rows_count = 0;
    _fields_count = 0;
    _affected_rows = 0;
    _auto_insert_id = 0;
}

bool ResultSet::_is_valid(uint32_t column_index) {
    if (NULL == _fetch_row) {
        CLIENT_WARNING("Mysql Row is NULL");
        return false;
    }
    if (column_index >= _fields_count) {
        CLIENT_WARNING("column index:%d is greater than fileds count:%d", 
                    column_index, _fields_count);
        return false;
    }
    return true;
}

void ResultSet::_set_affected_rows(uint64_t count) {
    _affected_rows = count;
}

void ResultSet::_set_auto_insert_id(uint64_t id) {
    _auto_insert_id = id;
}

void ResultSet::_set_is_select(bool is_select) {
    _is_select = is_select;
}

void ResultSet::_wrap(MYSQL* mysql, MYSQL_RES* res, bool store) {
    _mysql = mysql;
    this->_free_result();
    _res = res;
    _store = store;
    _is_select = true;
    if (store) {
        _rows_count = mysql_num_rows(_res);
    }
    else{
        _rows_count = 0;
    }
    _fields_count = mysql_num_fields(_res);
    MYSQL_FIELD* fields = mysql_fetch_fields(_res);
    for (unsigned int i = 0; i < _fields_count; ++i) {
        _name_id_map[string(fields[i].name)] = i;
    }
    _pos = 0;
    _fetch_row = new MysqlRow(_res);
    _fetch_row->init_type();
}
} // namespace client
} // namespace baikal

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
