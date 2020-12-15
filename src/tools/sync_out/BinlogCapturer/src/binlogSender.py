#!/usr/bin/python
#-*- coding:utf8 -*-

import sys
import os
import threading

class BinlogSender(threading.Thread):
    def __init__(self, config, queue, callbak):
        threading.Thread.__init__(self)
        self.queue = queue
        self.callbak = callbak
        pass
        #raise NotImplementedError
    def send(self,binlogItemList):
        pass
        #raise NotImplementedError
    def init(self):
        pass
        #raise NotImplementedError
    def run(self):
        while True:
            item = self.queue.get()
            ts = item['commitTs']
            self.callbak(ts)
            print item
