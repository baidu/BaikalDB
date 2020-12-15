#!usr/bin/env python
# -*- coding: utf-8 -*-

import threading
import Queue
import pymysql
import time
import traceback
from ackQueue import AckQueue

class MysqlSender(object):
    def __init__(self, config):
        # config.thread_num为线程数
        self.clientCount = int(config.get('mysql','threadnum'))
        self.ackQueue = AckQueue(4000)
        self.sqlQueue = Queue.Queue(4000)
        self.condition = threading.Condition()
        self.maxTs = 0
        self.checkpoint = 0

        self.clientList = []
        for i in range(self.clientCount):
            client = MySQLClient(config, self.sqlQueue, self.ack)
            self.clientList.append(client)
    def getCheckpoint(self):
        return self.ackQueue.getCheckpoint()

    def flush(self):
        self.condition.acquire()
        if self.maxTs != self.getCheckpoint():
            self.condition.wait()
        self.condition.release()

    def ack(self, index):
        self.condition.acquire()
        self.checkpoint = self.ackQueue.ack(index)
        if self.checkpoint == self.maxTs:
            self.condition.notify()
        self.condition.release()

    def send(self, sqlItem):
        ts = sqlItem[0]
        self.maxTs = ts
        sql = sqlItem[1]
        index = self.ackQueue.put(ts)
        self.sqlQueue.put((index, sql))
    def start(self):
        for client in self.clientList:
            client.start()

class MySQLClient(threading.Thread):
    def __init__(self, config, sqlQueue, ackFunc):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.conf = config
        self.sqlQueue = sqlQueue
        self.ackFunc = ackFunc
        self.conn = self.getconn()

    def run(self):
        while 1:
            (index,sql) = self.sqlQueue.get()
            self.execute(sql)
            self.ackFunc(index)

    def getconn(self):
        while True:
            try:
                conn = pymysql.connect(host=self.conf.get('mysql','host'),
                                        port=int(self.conf.get('mysql','port')),
                                        user=self.conf.get('mysql','user'),
                                        password=self.conf.get('mysql','password'),
                                        db=self.conf.get('mysql','database'),
                                        charset='utf8mb4',
                                        autocommit=True)
                return conn
            except Exception,e:
                print str(e)
                print traceback.format_exc()
                print "获取连接失败，重试"
                time.sleep(2)
                continue

    def execute(self,sql):
        while True:
            try:
                #print sql
                cur = self.conn.cursor()
                res = cur.execute(sql)
                break
            except Exception, e:
                error_info = str(e) + "\nsql=" +sql[:10000]
                print error_info
                self.conn = self.getconn()
                time.sleep(3)
                continue 

