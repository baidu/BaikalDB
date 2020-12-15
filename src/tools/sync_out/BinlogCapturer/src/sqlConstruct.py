#!usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import pymysql

class SqlConstruct(object):
    def __init__(self):
        pass

    def constructInsertSql(self, sqlPrev, vstrList):
        sql = sqlPrev + ','.join(vstrList) + ';'
        return sql

    def constructDeleteSql(self, sqlPrev, vstrList):
        sql = sqlPrev + '(' + ','.join(vstrList) + ');'
        return sql
    
    def constructSqlPrev(self, binlog):
        sqlPrev = ''
        if binlog['type'] == "INSERT":
            keylist = ','.join([ '`%s`' % key for key in binlog['value'].keys()])
            sqlPrev = "replace into `%s` (%s) values " % (binlog['table'], keylist) 
        elif binlog['type'].lower() == "delete":
            pklist = ['`%s`' % key for key in binlog['primary_key']]
            sqlPrev = "delete from `%s` where (%s) in " % (binlog['table'], ','.join(pklist))
        return sqlPrev


    def constructSql(self, binlogType, sqlPrev, vstrList):
        sql = ''
        if binlogType == 'INSERT':
            sql = self.constructInsertSql(sqlPrev, vstrList)
        elif binlogType == 'DELETE':
            sql = self.constructDeleteSql(sqlPrev, vstrList)
        return sql

    def escape_value(self, v):
        res = str(v)
        if type(v) == str:
            res = pymysql.escape_string(v)
        return res
        

    def sqlConstruct(self, binlog_list):
        lastSqlPrev = ""        
        lastCommitTs = 0
        vstrList = []
        sqlList = []
        lastType = ''
        for binlog in binlog_list:
            # 1. sql为空，lastSqlType为空，则开始构造SQL
            # 2. sql不为空，lastSqlType不为空，且与binlog的type一致，则继续拼SQL
            # 3. sql不为空，lastSqlType不为空，但与binlog的type不一致，则发送之前的sql，重新构造SQL
            
            sqlPrev = self.constructSqlPrev(binlog)
            binlogType = binlog['type']
            commitTs = binlog['commitTs']
            
            if binlogType == 'UPDATE' or sqlPrev != lastSqlPrev:
                if len(vstrList) != 0:
                    sql = self.constructSql(lastType, lastSqlPrev, vstrList)
                    if sql != '':
                        sqlList.append(sql)
                vstrList = []
                
    
            # 构造并发送sql
            if binlog['type'] == "INSERT":
                # 累加
                #vstr = tuple(binlog['value'].values()).__str__()
                vstr = '(' + ','.join(["'%s'" % self.escape_value(value) for value in binlog['value'].values()]) + ')'
                vstrList.append(vstr)
            elif binlog['type'] == "DELETE":
                vstr = '(' + ','.join(["'%s'" % self.escape_value(binlog['value'][key]) for key in binlog['primary_key']]) + ')'
                vstrList.append(vstr)
            elif binlog['type'] == "UPDATE":
                # sql不为空就发送sql，否则发送update语句
                sql = self.constructUpdate(binlog)
                if sql != "":
                    sqlList.append((binlog['commitTs'], sql))
            lastSqlPrev = sqlPrev
            lastType = binlogType
            lastCommitTs = commitTs
        if len(vstrList) != 0:
            sql = self.constructSql(lastType, lastSqlPrev, vstrList)
            if sql != '':
                sqlList.append((lastCommitTs, sql))
        return sqlList

    def constructUpdate(self,binlog):
        sql = ""
        update_list = []
        for key in binlog['value']:
            if binlog['value'][key] != binlog['old_value'][key]:
                update_list.append(key)
        sql = "update " + binlog["table"] + " set "
        if len(update_list) == 0:
            return ""
        setValueStr = ",".join(["`%s` = '%s'" % (key, self.escape_value(binlog['value'][key])) for  key in update_list])
        keyList = " and ".join(["`%s` = '%s'" % (key, self.escape_value(binlog["old_value"][key])) for key in binlog['primary_key']])
        sql += setValueStr + ' where ' + keyList + ';'
        return sql 
