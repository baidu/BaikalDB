#!usr/bin/env python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import re
import random
import pymysql
import commands
import io
import time
import json
import signal
import ConfigParser
import threading
import Queue
#from DBUtils.PooledDB import PooledDB
from datetime import datetime
import socket
import multiprocessing
from mysqlSender import MysqlSender
from localSender import LocalSender
from kafkaSender import KafkaSender
from sqlConstruct import SqlConstruct

class SenderManager(multiprocessing.Process):
    def __init__(self,configPath, queue, ackQueue):
        multiprocessing.Process.__init__(self)
        self.datasource = queue
        self.ackQueue = ackQueue
        self.configPath = configPath
        self.daemon = True
    def init(self):
        self.commitTs = 0
        print os.getppid()
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.configPath)
        self.updateCheckpointThread = threading.Thread(target=self.updateCheckpointThreadFunc)
        self.updateCheckpointThread.setDaemon(True)
        self.updateCheckpointThread.start()
        self.type_list = self.config.get('global','type').split(',')
        self.check_list = []
        self.sqlConstruct = SqlConstruct()
        self.initSenders()

    def isAlive(self):
        return self.is_alive()

    def initSenders(self):
        self.senderList = []
        for ty in self.type_list:
            if ty == 'mysql':
                self.senderList.append(MysqlSender(self.config))
            elif ty == 'local':
                self.senderList.append(LocalSender(self.config))
            elif ty == 'kafka':
                self.senderList.append(KafkaSender(self.config))

        for sender in self.senderList:
            sender.start()

    def run(self):
        self.init()
        beg = time.time()
        cnt = 0
        while 1:
            binlog_list = []
            for i in range(1000):
                # 1.binlog队列为空 2.检测到冲突 3.获取到1000条binlog
                # 跳出循环，发送不为空的binlog_list
                #if self.datasource.empty():
                #    break
                binlog = self.datasource.get()

                cnt += 1
                if cnt % 10000 == 0:
                    print cnt, "%.2f" % (time.time() - beg)
                    beg = time.time()
                # 检测冲突
                hash_flag = ""
                hash_flag_old = ""
                hash_flag = '_'.join(['%s' % str(binlog['value'][key]) for key in binlog['primary_key']])
                if binlog['type'] == 'UPDATE':
                    hash_flag_old = '_'.join(['%s' % str(binlog['old_value'][key]) for key in binlog['primary_key']])
                if self.check_list.count(hash_flag):
                    self.flushjob()
                self.check_list.append(hash_flag)
                if binlog['type'] == "UPDATE":
                    self.check_list.append(hash_flag_old)
                    hash_flag = hash_flag_old
                binlog_list.append(binlog)

            # check_list清零
            if len(self.check_list) >= 1000:
                #self.flushjob()
                self.check_list = []
            
            if len(binlog_list) != 0:
                sql_list = self.sqlConstruct.sqlConstruct(binlog_list)
                self.sendSql(sql_list)


    def sendSql(self,sql_list):
        for i in range(len(sql_list)):
            for sender in self.senderList:
                sender.send(sql_list[i])
    
    def updateCheckpointThreadFunc(self):
        while True:
            minTs = 1 << 64
            time.sleep(3)
            for sender in self.senderList:
                minTs = min(minTs, sender.getCheckpoint())
            self.commitTs = minTs
            self.ackQueue.put(self.commitTs)
            print "commitTs",self.commitTs

    def flushjob(self):
        for sender in self.senderList:
            sender.flush()

