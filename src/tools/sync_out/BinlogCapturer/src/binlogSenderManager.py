#!/usr/bin/python
#-*- coding:utf8 -*-

import sys
import os

import threading
import Queue
from mysqlSender import MysqlSender
from fileSender import fileSender
from kafkaSender import KafkaSender

class BinlogSenderManager(threading.Thread):
    def __init__(self,config, Queue, ackFunc):
        self.sendTypeList = list(set(config.get('type').split(','))
        self.senderList = []
        self.ackCallBack = ackFunc
        self.onceSendCount = 100
        self.ackIDUpdateLock = threading.Lock()
        self.ackIDCountDict = {}
        for sendType in self.sendTypeList:
            if sendType == 'kafka':
                sender = KafkaSender(config)
                self.senderList.append(sender)
            elif sendType == 'mysql':
                sender = MysqlSender(config)
                self.senderList.append(sender)
            elif sendType == 'file':
                sender = FileSender(config)
                self.senderList.append(sender)
            else:
                continue
        if len(self.senderList) == 0:
            raise Exception("no sender init")
    def onSenderAck(self, ackTsList):
        self.ackIDUpdateLock.acquire()
        maxAckID = 0
        for ackTs in ackTsList:
            if ackTs not in self.ackIDCountDict:
                continue
            self.ackIDCountDict[ackTs] += 1
        for key, count in self.ackIDCountDict.items():
            if count >= len(self.senderList):
                maxAckID = key
            else:
                break
        if maxAckID > 0:
            self.ackCallBack(maxAckID)
        self.ackIDUpdateLock.release()
        
    def init(self):
        for sender in self.senderList:
            if not sender.init():
                return False
        self.ackThread = threading.Thread(target = self.ackThreadFunc)
        self.ackThread.setDaemon(True)
        return True

    def start(self):
        self.ackThread.start()
        for sender in self.senderList:
            sender.start():
        senderCount = len(self.senderList)
        while not self._stop:
            for i in range(0,1000):
                blog = self.queue.get()
                binlogList.append(blog)
                self.ackCount.append(0)
                if maxID < blog['commitTs']:
                    maxID = blog['commitTs']
                self.ackIDCountDict[blog['commitTs']] = 0
                if self.queue.empty():
                    break
            for sender in self.senderList:
                sender.send(binlogList)

