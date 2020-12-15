#!/usr/bin/python
#-*- coding:utf8 -*-

from storeClient import StoreClient
import threading
import Queue
import time
from schemaManager import SchemaManager

class BinlogReader(threading.Thread):

    def __init__(self, regionInfo, binlogQueue, startTs, schemaManager):
        self.queue = binlogQueue
        self.regionInfo = regionInfo
        self.regionId = regionInfo['region_id']
        self.readStartTimestamp = startTs
        self.schemaManager = schemaManager
        self.initStoreLock = threading.Lock()
        self.perReadCount = 1000
        self._stop = False
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.initStoreClient()

    def initStoreClient(self):
        self.storeClient = StoreClient(self.regionInfo["leader"])

    def updateRegionInfo(self, regionInfo):
        self.regionInfo = regionInfo
        self.initStoreClient()

    def run(self):
        while not self._stop:
            errCode, binlogList = self.storeClient.get_binlog(self.regionInfo['region_id'], self.regionInfo['version'],
                                                                self.readStartTimestamp, self.perReadCount)
            if errCode == 0:
                if len(binlogList) == 0:
                    time.sleep(0.1)
                for binlog in binlogList:
                    self.readStartTimestamp = binlog["commitTs"] + 1
                    self.queue.put(binlog)
            elif errCode == 1:
                #更新storeAddr
                self.regionInfo = self.schemaManager.updateAndGetRegionInfo(self.regionId)
                if self.regionInfo == None:
                    break
                self.initStoreClient()
                time.sleep(1)
            else:
                print "binlogReader ERROR:",errCode
                #发生错误
                break
    def stop(self):
        self._stop = True


def GetRegionInfo(region_id):
    return None
    regionInfo = {"region_id":3,"table_name":"test_blog","table_id":3,"partition_id":0,"replica_num":3,"version":1,"conf_version":2,"peers":["10.100.217.151:8111","10.100.217.152:8111"],"leader":"10.100.217.151:8111","status":"IDLE","used_size":0,"log_index":20,"can_add_peer":True,"parent":0,"timestamp":1603878078,"num_table_lines":0,"main_table_id":3,"is_binlog_region":True,"partition_num":1} 
    return regionInfo

