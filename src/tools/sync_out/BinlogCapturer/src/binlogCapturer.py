#!/usr/bin/python
#-*- coding:utf8 -*-

import threading
import Queue
import ConfigParser
import time
import os
import sys
import signal
import multiprocessing

from binlogReader import BinlogReader
from binlogParser import BinlogParser
from binlogProcessor import BinlogProcessor
from binlogFilter import BinlogFilter
#from binlogSender import BinlogSender
#from fileSender import BinlogSender
from schemaManager import SchemaManager
from senderManager import SenderManager
# from server import ServerHandler
from BaseHTTPServer import HTTPServer,BaseHTTPRequestHandler

class BinlogCapturer:
    def __init__(self, configPath = './conf/config.cfg'):
        self.configPath = configPath
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.configPath)
        self.loadBinlogReaderTimestamps()
        self.initSchemaManager()
        self.initQueues()
        self.initWorks()
        self._stop = False

    def initSchemaManager(self):
        self.metaServerList = self.config.get('global','metaServer')
        self.schemaManager = SchemaManager(self.config.get('global','binlogTableID'), self.metaServerList)
        self.schemaManager.init()

        self.schemaManager.setSchemaUpdateFunc(self.tableSchemaUpdateFunc)
        self.schemaManager.setRegionUpdateFunc(self.regionInfoUpdateFunc)

    def loadBinlogReaderTimestamps(self):
        self.binlogCheckPointPath = self.config.get('global','binlogCheckPointPath')
        lastReadTs = 0
        for line in open(self.binlogCheckPointPath):
            ts = int(line)
            if ts > lastReadTs:
                lastReadTs = ts
        self.binlogReadCheckPoint = lastReadTs
        self.lastUpdateCheckPoint = time.time()

    def initQueues(self):
        self.binlogReaderQueue = multiprocessing.Queue(2000)
        self.binlogParserQueue = multiprocessing.Queue(2000)
        self.binlogSenderQueue = multiprocessing.Queue(2000)
        self.binlogCheckPointQueue = multiprocessing.Queue(2000)


    def initWorks(self):
        self.binlogReaderDict = {}
        self.regionID = self.config.getint('global','binlogRegionID')
        self.regionInfo = self.schemaManager.getRegionInfoByRegionID(self.regionID)
        self.binlogReader = BinlogReader(self.regionInfo, self.binlogReaderQueue, self.binlogReadCheckPoint, self.schemaManager)

        self.binlogParser = BinlogParser(self.configPath, self.binlogReaderQueue, self.binlogParserQueue)
        self.binlogProcessor = BinlogProcessor(self.schemaManager)
        self.binlogFilter = BinlogFilter(self.config.get('global','filterRuleDict'))
        self.binlogSender = SenderManager(self.config.get('global','senderConfig'), self.binlogSenderQueue, self.binlogCheckPointQueue)

    def senderCallBack(self, ts):
        self.binlogReadCheckPoint = ts
        tmpFile = self.binlogCheckPointPath + '.tmp'
        with open(tmpFile, 'w') as f:
            f.write(str(ts) + '\n')
        os.rename(tmpFile, self.binlogCheckPointPath)
        readerSize = self.binlogReaderQueue.qsize()
        senderSize = self.binlogSenderQueue.qsize()
        f = open('queueSize.txt','w')
        f.write(str(readerSize) + '\t' + str(senderSize))
        f.close()
    def processBinlogsThreadFunc(self):
        while not self._stop:
            self.binlogProcess()
    def binlogProcess(self):
        while not self._stop:
            item = self.binlogParserQueue.get()
            self.binlogProcessor.process(item)
            if self.binlogFilter.filter(item):
                continue
            self.binlogSenderQueue.put(item)
        
    def startProcessBinlogsThread(self):
        self.processBinlogThread = threading.Thread(target = self.processBinlogsThreadFunc)
        self.processBinlogThread.setDaemon(True)
        self.processBinlogThread.start()
    def tableSchemaUpdateFunc(self, insertDict, updateDict, deleteDict):
        #当table schema更新时，此函数被调用
        return
        for tableID, schema in insertDict.items():
            self.binlogParser.updateTableSchema(schema)
        for tableID, schema in updateDict.items():
            self.binlogParser.updateTableSchema(schema)

    def regionInfoUpdateFunc(self, insertDict, updateDict, deleteDict):
        #当binlog的region变更时，此函数被调用
        #若binlog store 切主，需要更新binlogStore的storeClient
        for rid, regionInfo in updateDict.items():
            if rid != self.regionID:
                continue

            self.binlogReader.updateRegionInfo(regionInfo)

    def updateCheckpointThreadFunc(self):
        while True:
            checkpoint = self.binlogCheckPointQueue.get()
            if checkpoint == 0:
                continue
            self.binlogReadCheckPoint = checkpoint
            tmpFile = self.binlogCheckPointPath + '.tmp'
            with open(tmpFile, 'w') as f:
                f.write(str(self.binlogReadCheckPoint) + '\n')
            os.rename(tmpFile, self.binlogCheckPointPath)
            # readerSize = self.binlogReaderQueue.qsize()
            # parserSize = self.binlogParserQueue.qsize()
            # senderSize = self.binlogSenderQueue.qsize()
            # print "%d\t%d\t%d\t%d" % (readerSize, parserSize, senderSize, self.binlogReadCheckPoint)
            # f = open('queueSize.txt','w')
            # f.write(str(readerSize) + '\t' + str(senderSize))
            # f.close()
            

    def startUpdateCheckpointThread(self):
        self.updateCheckpointThread = threading.Thread(target = self.updateCheckpointThreadFunc)
        self.updateCheckpointThread.start()

    def start(self):
        self.schemaManager.start()
        self.binlogReader.start()
        self.binlogParser.start()
        self.binlogSender.start()
        
        self.httpThread = threading.Thread(target=self.httpFunc)
        self.httpThread.setDaemon(True)
        self.httpThread.start()
        
        self.startProcessBinlogsThread()
        self.startUpdateCheckpointThread()
        self.threadMonitor()

    def stop(self):
        self.binlogReader.stop()
        self.binlogParser.terminate()
        self.binlogSender.terminate()
        pid = os.getpid()
        os.kill(pid, signal.SIGKILL)

    def threadMonitor(self):
        self.threadList = []
        self.threadList.append(self.processBinlogThread)
        self.threadList.append(self.binlogReader)
        while True:
            if not self.binlogReader.isAlive():
                self.stop()
            if not self.binlogParser.isAlive():
                self.stop()
            if not self.binlogSender.isAlive():
                self.stop()
            time.sleep(1)

    def httpFunc(self):
        self.port = int(self.config.get('global','httpPort'))
        print self.port
        self.http_server = HTTPServer(('0.0.0.0',self.port),ServerHandler)
        self.http_server.serve_forever()

capturer = BinlogCapturer()

class ServerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print "get"
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        wor = '[status]\n\n'     
        wor += 'binlogReaderQueue.size:' + str(capturer.binlogReaderQueue.qsize()) + '\n'
        wor += 'binlogParserQueue.size:' + str(capturer.binlogParserQueue.qsize()) + '\n'
        wor += 'binlogSenderQueue.size:' + str(capturer.binlogSenderQueue.qsize()) + '\n'
        wor += 'binlogReadCheckPoint:' + str(capturer.binlogReadCheckPoint) + '\n'        
        self.wfile.write(wor)

def signal_kill_handler(signum, handler):
   print "kill:",signum
   capturer.stop()

def register_signal_handler():
    signal.signal(signal.SIGINT, signal_kill_handler)
    signal.signal(signal.SIGCHLD, signal_kill_handler)
    #signal.signal(signal.SIGKILL, signal_kill_handler)

if __name__ == '__main__':  
    register_signal_handler()
    print os.getpid()
    capturer.start()

