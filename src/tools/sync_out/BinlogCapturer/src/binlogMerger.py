#!/usr/bin/python
#-*- coding:utf8 -*-

import threading
import Queue
import time


class BinlogMerger(threading.Thread):

    def __init__(self, inQueueDict, outQueue):
        threading.Thread.__init__(self)
        self.inQueueDict = inQueueDict
        self.outQueue = outQueue
        self.priorityQueue = Queue.PriorityQueue()
        self.setDaemon(True)
        self._stop = False
    def run(self):
        for rid,que in self.inQueueDict.items():
            item = que.get()
            ts = item['commitTs']
            self.priorityQueue.put((ts, (item, que)))
        while not self._stop:
            ts, item = self.priorityQueue.get()
            binlog = item[0]
            que = item[1]
            self.outQueue.put(binlog)
            newbinlog = que.get()
            self.priorityQueue.put((newbinlog["commitTs"],(newbinlog, que)))

