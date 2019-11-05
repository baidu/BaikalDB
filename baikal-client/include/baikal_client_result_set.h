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
 * @file baikal_client_result_set.h
 * @author liuhuicong(com@baidu.com)
 * @date 2015/11/03 13:11:36
 * @brief 
 *  
 **/

#ifndef  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_RESULT_SET_H
#define  FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_RESULT_SET_H 

#include <mysql.h>
#include <map>
#include <string>
#include "baikal_client_define.h"
#include "baikal_client_row.h"
#include "baikal_client_mysql_async.h"

namespace baikal {
namespace client {
/**
 * @brief MySQcL查询结果集封装
 * 该类不支持多线程安全
 **/
class ResultSet {
public:
    friend class Connection;
    friend class MysqlConnection;
    friend class RedisConnection;
    ResultSet();
    ~ResultSet();
    
    // @brief 读取当前结果行，并将指针_pos后移
    // @return bool
    // @note 1.next()和seek_row()配合使用,都会改变光标的位置
    //         下标访问或getAt()方法单独使用
    //         两种方式独立
    //       2.本函数可用于未store的res
    bool next(void);
    
    // @brief 设置结果集指针位置
    // @param [in] pos : unsigned
    //                   设置的位置，应小于行数
    // @return bool
    //         调用是否成功
    // @retval
    //         - true : 成功     
    //         - false : 失败
    // @note 仅能在store的res上调用本函数，否则总是失败
    bool seek_row(uint64_t row_index);
    
    // @brief 获取结果集指针的位置
    // @return unsigned long long 
    //         指针位置
    // @note 本函数可用于未store的res
    uint64_t tell() const;

    // @breif 返回列的类型
    ValueType get_type(uint32_t column_index);
    // @brief 获取_pos指针所在行的指定列的相应列类型值
    // @return int 0表示正确
    // @note 用户只能调用该列对应的方法获取值
    // 若调用不恰当方法会返回错误 
    int get_string(const std::string& column_name, std::string* value);
    int get_int32(const std::string& column_name, int32_t* value);
    int get_uint32(const std::string& column_name, uint32_t* value);
    int get_int64(const std::string& column_name, int64_t* value);
    int get_uint64(const std::string& column_name, uint64_t* value);
    int get_float(const std::string& column_name, float* value);
    int get_double(const std::string& column_name, double* value);
    
    // @brief 获取_pos指针所在行的指定列的相应列类型值
    // @return int 0表示正确
    // @note 用户只能调用该列对应的方法获取值
    // 若调用不恰当方法会返回错误 
    int get_string(uint32_t column_index, std::string* value);
    int get_int32(uint32_t column_index, int32_t* value);
    int get_uint32(uint32_t column_index, uint32_t* value);
    int get_int64(uint32_t column_index, int64_t* value);
    int get_uint64(uint32_t column_index, uint64_t* value);
    int get_float(uint32_t column_index, float* value);
    int get_double(uint32_t column_index, double* value);
    // @brief 获取_pos指针所在行的指定列的值
    //  @param [in] column_index : unsigned
    //                             指定列
    //  @return const char*
    //           列内容
    //  @retval
    //           - 非NULL ： 指定列内容
    //           - NULL   ： 指定列不存在
    //  @note 本函数可用于未stroe的res
    const char* get_value(uint32_t column_index);
     
    // @brief 获取_pos指针所在行的指定列的值
    //
    // @param [in] column_name : std::string
    //                           指定列名称
    // @return const char*
    //          列内容
    //
    // @retval
    //          - 非NULL ： 指定列内容
    //          - NULL   ： 指定列不存在
    // @note 本函数可用于未stroe的res
    //const char* get_value(const char* column_name);
    const char* get_value(const std::string& column_name);

    // @brief 获取封装在内部的MYSQL_RES对象
    // @return MYSQL_RES
    //         内部的MYSQL_RES对象
    // @retval
    //               - 非NULL : 内置的MYSQL_RES对象
    //               - NULL   : res未初始化
    // @note 1.该函数一般用于判断res对象是否初始化
    //       2.避免直接操作MYSQL_RES对象，否则res的一些函数可能返回错误结果
    //       3.不能free返回的MYSQL_RES对象，否则res将错误的重复释放
    MYSQL_RES* get_mysql_res() const;
   
    // @brief 检查结果集是否支持seek
    // @return bool
    //         是否支持seek
    // @note 1.对于未初始化的res，该函数行为未定义
    //       2.本函数返回值等价于是否store
    bool is_seekable() const;
   
    // @brief 检查sql是否是select等返回结果集的查询，还是update、delete等
    // @return bool
    bool is_select() const;

    // @brief 获取结果集列数
    // @return unsigned
    //        结果集列数
    // @note 对于未初始化的res，该函数将导致内存非法访问
    uint32_t get_field_count(void) const;

    // @brief 获取结果集行数
    // @return unsigned
    //          结果集行数
    // @note 对于未初始化或者是非store的res，总是返回0
    uint64_t get_row_count(void) const;
   
    // @brief get affected rows
    //
    // @return  BAIKAL_CLIENT_ULONGLONG
    //           影响的行数 
    //
    // @note 只有sql语句是insert、delete、update等语句时返回值才用意义 
    uint64_t get_affected_rows(void) const;

    // @brief get auto insert id
    // @return  BAIKAL_CLIENT_ULONGLONG
    //          自动插入的id
    // @note 只有sql语句是insert、delete、update等语句时返回值才用意义 
    uint64_t get_auto_insert_id(void) const;
    
    void set_conn_type(ConnectionType type); 

    ConnectionType get_conn_type();

private:
    // @brief 释放结果集
    void _free_result(void);
    
    bool _is_valid(uint32_t column_index);
    // @brief set affected rows
    // @param  [in] count : const BAIKAL_CLIENT_ULONGLONG
    //               影响的行数
    // @return  void 
    // @note 该方法只供友元类Connection使用 
    void _set_affected_rows(uint64_t count);

    // @brief set auto insert id
    // @param [in] id : const BAIKAL_CLIENT_ULONGLONG
    //             自动插入的id 
    // @return  void 
    // @note 该方法只供友元类Connection使用 
    void _set_auto_insert_id(uint64_t id);
   
    // @brief set _is_select
    // @param [in] is_select : bool
    //              是否是select等会返回结果集的sql语句
    // @return  void 
    // @note 该方法只供友元类Connection使用 
    void _set_is_select(bool is_select);

    void _wrap(MYSQL* mysql, MYSQL_RES *res, bool store);

private:

    ConnectionType _type;
    // sql语句是select等语句时，用来保存返回结果集
    // 若sql语句是insert、delete、update等语句时为NULL
    MYSQL* _mysql;

    MYSQL_RES* _res;
    // 标示查询是否返回结果集，若不返回（insert、update or delete语句），则值为false
    // 若为true，则_res示返回的结果集
    // 若为false， 则_res
    // 为NULL，_affected_rows、_auto_insert_id分别代表影响的行数和自动插入的id
    bool _is_select; 
    bool _store;
    
    uint64_t _pos;
    Row*  _fetch_row;

    uint64_t _rows_count;
    uint32_t _fields_count;

    uint64_t _affected_rows; 
    uint64_t _auto_insert_id;
    
    std::map<std::string, uint32_t> _name_id_map; //列名与下标对应关系
    
    ResultSet(const ResultSet&);
    ResultSet& operator=(const ResultSet&); 
};
}    
}

#endif  //FC_DBRD_BAIKAL_CLIENT_INCLUDE_BAIKAL_CLIENT_RESULT_SET_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
