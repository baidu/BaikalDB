#!/usr/bin/python
#-*- coding:utf8 -*-
import threading
import Queue

class AckQueue:
    def __init__(self, maxsize = 2000):
        self.maxsize = maxsize
        self.dataList = [0 for i in range(0,maxsize)]
        self.checkpoint = 0
        self.dataIndex = 0
        self.ackIndex = 0
        self.ackList = [False for i in range(0,maxsize)]
        self.cond = threading.Condition()

    def getCheckpoint(self):
        return self.checkpoint

    def put(self, v):
        self.cond.acquire()
        while (self.dataIndex - self.ackIndex) >=self.maxsize:
            self.cond.wait()
        self.dataList[self.dataIndex % self.maxsize] = v
        self.ackList[self.dataIndex % self.maxsize] = False
        ret = self.dataIndex
        self.dataIndex += 1
        self.cond.release()
        return ret

    def ack(self, index):
        self.cond.acquire()
        if index < self.ackIndex or index >= self.dataIndex:
            res = self.checkpoint
            self.cond.release()
            return res
        self.ackList[index % self.maxsize] = True
        while  self.ackIndex < self.dataIndex and self.ackList[self.ackIndex % self.maxsize] == True:
            self.checkpoint = self.dataList[self.ackIndex % self.maxsize]
            self.ackIndex += 1
        self.cond.notify()
        res = self.checkpoint
        self.cond.release()
        return res

import time        
class consumer(threading.Thread):
    def __init__(self, queue, ackFunc):
        self.queue = queue
        self.ackFunc = ackFunc
        threading.Thread.__init__(self)
        self.setDaemon(True)
    def run(self):
        while True:
            (ind, sql) = self.queue.get()
            self.ackFunc(ind)
            time.sleep(0.005)

if __name__ == '__main__':
    ackQueue = AckQueue(10000)
    sqlQueue = Queue.Queue(10000)
    consList = []
    for i in range(0,20):
        con = consumer(sqlQueue, ackQueue.ack)
        con.start()
        consList.append(con)
    cnt = 1;
    while True:
        ind = ackQueue.put(cnt)
        sqlQueue.put((ind, "fsdfasd " + str(cnt)))
        if cnt % 10000 == 0:
            print "checkpoint:",ackQueue.getCheckpoint()
        cnt += 1

