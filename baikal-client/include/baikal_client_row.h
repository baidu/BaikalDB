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
 * @file baikal_client_row.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/12/17 20:17:34
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_ROW_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_ROW_H

#include <mysql.h>
#include <string>
#include "baikal_client_define.h"

namespace baikal {
namespace client {
class Row {
public:
    virtual ~Row() {}
    virtual void set_row(MYSQL_ROW /*fetch_row*/) {}
    virtual void init_type() = 0;
    virtual int get_string(uint32_t column_index, std::string* value) = 0;   
    virtual int get_int32(uint32_t column_index, int32_t* value) = 0; 
    virtual int get_uint32(uint32_t column_index, uint32_t* value) = 0;
    virtual int get_int64(uint32_t column_index, int64_t* value) = 0;
    virtual int get_uint64(uint32_t column_index, uint64_t* value) = 0;
    virtual int get_float(uint32_t column_index, float* value) = 0;
    virtual int get_double(uint32_t column_index, double* value) = 0;
    virtual const char* get_value(uint32_t column_index) = 0;
    virtual ValueType get_type(uint32_t column_index) = 0;
};

class MysqlRow : public Row {

public:
    explicit MysqlRow(MYSQL_RES* res);
    ~MysqlRow();
    void set_row(MYSQL_ROW fetch_row);
    void init_type();
    int get_string(uint32_t column_index, std::string* value);
    int get_int32(uint32_t column_index, int32_t* value);
    int get_uint32(uint32_t column_index, uint32_t* value);
    int get_int64(uint32_t column_index, int64_t* value);
    int get_uint64(uint32_t column_index, uint64_t* value);
    int get_float(uint32_t column_index, float* value);
    int get_double(uint32_t column_index, double* value);
    const char* get_value(uint32_t column_index); 
    ValueType get_type(uint32_t column_index);
private:
    // @breif 判断是否是给定的类型
    bool _is_int32_type(enum_field_types source_type);
    bool _is_int64_type(enum_field_types source_type);
    bool _is_float_type(enum_field_types source_type);
    bool _is_double_type(enum_field_types source_type);
    bool _is_string_type(enum_field_types source_type); 
private:
    MYSQL_RES* _res;
    std::vector<ValueType> _field_type;
    MYSQL_ROW _fetch_row;
};
}
} //namespace

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_ROW_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
