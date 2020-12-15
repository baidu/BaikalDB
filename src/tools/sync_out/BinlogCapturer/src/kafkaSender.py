#!/usr/bin/python
#-*- coding:utf8 -*-
import threading
import Queue
from kafka import KafkaProducer
from ackQueue import AckQueue


class KafkaSender(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.sqlQueue = Queue.Queue(100)
        self.config = config
        self.kafkaServers = self.config.get('kafka','servers').split(',')
        self.kafkaTopic = self.config.get('kafka','topic')
        self.initKafkaProducer()
        self.ackQueue = AckQueue(300)
        self.condition = threading.Condition()
        self.maxTs = 0

    def initKafkaProducer(self):
        self.kafkaProducer = KafkaProducer(bootstrap_servers = self.kafkaServers, acks='all', batch_size = 1 * 1024 * 1024, linger_ms = 500)

    def sendCallBack(self, *args, **kwargs):
        index = args[0].timestamp
        ck = self.ackQueue.ack(index)
        self.condition.acquire()
        if ck == self.maxTs:
            self.condition.notify()
        self.condition.release()

    def getCheckpoint(self):
        return self.ackQueue.getCheckpoint()

    def flush(self):
        self.condition.acquire()
        while self.maxTs != self.getCheckpoint():
            self.condition.wait()
        self.condition.release()

    def send(self, sqlItem):
        ts = sqlItem[0]
        self.maxTs = ts
        sql = sqlItem[1]
        index = self.ackQueue.put(ts)
        self.sqlQueue.put((index, sql))

    def run(self):
        while True:
            item = self.sqlQueue.get()
            index = item[0]
            sql = str(item[1])
            future = self.kafkaProducer.send(self.kafkaTopic, value = sql, timestamp_ms = index)
            future.add_callback(self.sendCallBack)
