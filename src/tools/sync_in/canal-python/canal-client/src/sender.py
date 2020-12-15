#!/usr/local/bin/python3
#-*- coding:utf8 -*-
import os
import sys
import time
import json
import random
import pymysql
import logging
import logging.handlers
import traceback 
import threading
import datetime
import ConfigParser
from client import Client
from connector import Connector
from protocol import CanalProtocol_pb2
from protocol import EntryProtocol_pb2
from baikaldb_client import BaikalDBClient


def initPrintLog(config):
    logger = logging.getLogger('test')
    logger.setLevel(logging.INFO)
    info_handler = logging.handlers.TimedRotatingFileHandler(config.get('global','sql_log'), encoding = 'utf8', when = 'midnight', interval = 1, backupCount = 7)
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d]  %(message)s"))
    info_logging_filter = logging.Filter()
    info_logging_filter.filter = lambda record: record.levelno == logging.INFO
    info_handler.addFilter(info_logging_filter)
    logger.addHandler(info_handler)
    error_handler = logging.FileHandler(filename = config.get('global','err_log'), mode = 'a')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d]  %(message)s"))
    error_logging_filter = logging.Filter()
    error_logging_filter.filter = lambda record: record.levelno == logging.ERROR
    error_handler.addFilter(error_logging_filter)
    logger.addHandler(error_handler)
    

class Sender:
    def __init__(self, config):
	self.conf = config
        self.logger = logging.getLogger('test')
        self.baikaldbClient = BaikalDBClient(
            host    =  self.conf.get("destination_info", "host"),
            user    =  self.conf.get("destination_info", "user"),
            passwd  =  self.conf.get("destination_info", "passwd"),
            db      =  self.conf.get("destination_info", "db"),
            port    =  self.conf.getint("destination_info", "port"),
            charset =  'utf8')
        

    def insert2sql(self, table, afterColumns):
        sql = "replace into %s values(" % (table)
        for column in afterColumns:
            if column.value:
                sql = "%s %s ," % (sql, self.baikaldbClient.conn.escape(column.value))
            else:
                sql = "%s null ," % (sql)
        sql =  "%s );" % (sql[:-1])
        return sql


    def delete2sql(self, table, afterColumns):
        sql = "delete from %s where " % (table)
        for column in afterColumns:
            if (column.isKey):
                sql = "%s %s = %s and " % (sql, column.name, column.value)
        sql = "%s ;" % (sql[:-4])
        return sql


    def update2sql(self, table, afterColumns):
        sql = "replace into %s values(" % (table)
        for column in afterColumns:
            if column.value:
                sql = "%s %s," % (sql, self.baikaldbClient.conn.escape(column.value))
            else:
                sql = "%s null ," % (sql)
        sql = "%s );" % (sql[:-1])
        return sql


    def initCanalClient(self):
        self.client = Client()
        self.client.connect(
            host = self.conf.get("server_info", "host"), 
            port = self.conf.getint("server_info", "port"))
        self.client.check_valid(
            username = self.conf.get("syn_user_auth_info", "username").encode(encoding = 'utf-8'), 
            password = self.conf.get("syn_user_auth_info", "password").encode(encoding = 'utf-8'))
        self.client.subscribe(
            client_id   = self.conf.get("subscribe_instance","client_id").encode(encoding = 'utf-8'), 
            destination = self.conf.get("subscribe_instance","destination").encode(encoding = 'utf-8'), 
            filter      = self.conf.get("subscribe_instance","filter").encode(encoding = 'utf-8'))


    def constructSql(self, row, table, event_type):
        sql = None
        if event_type == EntryProtocol_pb2.EventType.DELETE:
            sql = self.delete2sql(table, row.beforeColumns)
        elif event_type == EntryProtocol_pb2.EventType.INSERT:
            sql = self.insert2sql(table, row.afterColumns)
        elif event_type == EntryProtocol_pb2.EventType.UPDATE:
            sql = self.update2sql(table, row.afterColumns)
        return sql


    def processRow(self, row, table, event_type):
        sql = self.constructSql(row, table, event_type)
        result = self.baikaldbClient.query(exesql = sql)
        if result == 1:
            self.logger.info("sql = [ %s ]" % (sql))
        elif result == 0:
            self.logger.error("执行失败，sql = [  %s  ]" % (sql))
        return result


    def processEntry(self, entry):
        entry_type =  entry.entryType
        if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
            return 2
        row_change = EntryProtocol_pb2.RowChange()
        row_change.MergeFromString(entry.storeValue)
        event_type = row_change.eventType
        header = entry.header
        database = header.schemaName
        table = header.tableName
        event_type = header.eventType
        for row in row_change.rowDatas:
            if self.processRow(row, table, event_type) == 0:
                return 0
        return 1


    def start(self):
        self.initCanalClient()
        while True:
            message = self.client.get_without_ack(100)
            entries = message['entries']
            if len(entries) == 0:
                time.sleep(1)
                continue
            for entry in entries:
                if self.processEntry(entry) == 0:
                    return
            time.sleep(1)
            self.client.ack(message['id'])


if __name__ == "__main__":
    if len(sys.argv) != 2:
	print "usage python src/sender.py configpath"
	exit(1)
    configPath = sys.argv[1]
    config = ConfigParser.ConfigParser()
    config.read(configPath)
    initPrintLog(config)
    sender = Sender(config)
    sender.start()
