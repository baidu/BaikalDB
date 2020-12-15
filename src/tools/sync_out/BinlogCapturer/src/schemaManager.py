#!/usr/bin/python
#-*- coding:utf8 -*-

import time
import threading

from metaClient import MetaClient

class SchemaManager(threading.Thread):

    def __init__(self, blogTableId, metaAddr):
        threading.Thread.__init__(self)
        self.metaClient = MetaClient(metaAddr)
        self.binlogRegionInfoDict = {}
        self.binlogTableID = int(blogTableId)
        self.tableSchemaDict = {}
        self.onSchemaUpdate = None
        self.onRegionUpdate = None
        self.lock = threading.Lock()
        self.setDaemon(True)
        self._stop = False
    def setSchemaUpdateFunc(self, schemaUpdateFunc):
        self.onSchemaUpdate = schemaUpdateFunc
    def setRegionUpdateFunc(self, regionUpdateFunc):
        self.onRegionUpdate = regionUpdateFunc

    def stop(self):
        self._stop = True
    def updateAndGetSchema(tableID):
        self.updateTableSchema()
        return self.getTableSchemaByTableID(tableID)

    def updateAndGetRegionInfo(self, regionID):
        self.updateBinlogRegionInfo()
        return self.getRegionInfoByRegionID(regionID)

    def run(self):
        time.sleep(5)
        while not self._stop:
            self.updateSchemaAndRegion()
            time.sleep(10)
    def updateSchemaAndRegion(self):
        insertDict, updateDict, deleteDict = self.updateTableSchema()
        if insertDict != {} or updateDict != {} or deleteDict != {}:
            if self.onSchemaUpdate != None:
                self.onSchemaUpdate(insertDict,updateDict,deleteDict)
        insertDict, updateDict, deleteDict = self.updateBinlogRegionInfo()
        if insertDict != {} or updateDict != {} or deleteDict != {}:
            if self.onRegionUpdate != None:
                self.onRegionUpdate(insertDict,updateDict,deleteDict)
    def init(self):
        self.updateTableSchema()
        self.updateBinlogRegionInfo()

    def getRegionInfoByRegionID(self, regionID):
        regionInfo = None
        self.lock.acquire()
        if regionID in self.binlogRegionInfoDict:
            regionInfo =  self.binlogRegionInfoDict[regionID]
        self.lock.release()
        return regionInfo

    def getTableSchemaByTableID(self, tableID):
        schema = None
        self.lock.acquire()
        if tableID in self.tableSchemaDict:
            schema =  self.tableSchemaDict[tableID]
        self.lock.release()
        return schema

    def getTableSchemaList(self):
        schemaList = []
        self.lock.acquire()
        for tid, schema in self.tableSchemaDict.items():
            schemaList.append(schema)
        self.lock.release()
        return schemaList

    def updateTableSchema(self):
        self.lock.acquire()
        tableSchemaList = self.metaClient.getTableSchema()
        updateSchemaDict = {}
        insertSchemaDict = {}
        deleteSchemaDict = {}
        newTableDict = {}
        for schema in tableSchemaList:
            tableID = schema['table_id']
            if 'binlog_info' not in schema or 'binlog_table_id' not in schema['binlog_info'] or self.binlogTableID != schema['binlog_info']['binlog_table_id']:
                continue
            newTableDict[tableID] = 1
            primaryKey = []
            for index in schema['indexs']:
                if index['index_name'] == 'primary_key':
                    primaryKey = index['field_names']
            schema['primary_key'] = primaryKey
            if tableID not in self.tableSchemaDict:
                self.tableSchemaDict[tableID] = schema
                insertSchemaDict[tableID] = schema
            else:
                if self.tableSchemaDict[tableID]['version'] != schema['version']:
                    self.tableSchemaDict[tableID] = schema
                    updateSchemaDict[tableID] = schema

        for tableID, schema in self.tableSchemaDict.items():
            if tableID not in newTableDict:
                deleteSchemaDict[tableID] = schema
                del self.tableSchemaDict[tableID]
        
        self.lock.release()
        return insertSchemaDict,updateSchemaDict,deleteSchemaDict

    def updateBinlogRegionInfo(self):
        self.lock.acquire()
        regionInfoList = self.metaClient.getRegionInfo()
        insertRegionDict = {}
        updateRegionDict = {}
        deleteRegionDict = {}
        newRegionDict = {}
        for region in regionInfoList:
            if region['table_id'] != self.binlogTableID:
                continue
            regionID = region['region_id']
            newRegionDict[regionID] = 1
            if regionID not in self.binlogRegionInfoDict:
                self.binlogRegionInfoDict[regionID] = region
                insertRegionDict[regionID] = region

        for regionID, region in self.binlogRegionInfoDict.items():
            if regionID not in newRegionDict:
                deleteRegionDict[regionID] = region
                del self.binlogRegionInfoDict[regionID]
        self.lock.release()
        return insertRegionDict, updateRegionDict, deleteRegionDict



def schemaUpdate(insert, update, delete):
    print "on schemaUpdate"
    print insert
    print update
    print delete
def regionUpdate(insert, update, delete):
    print "on regionUpdate"
    print insert
    print update
    print delete

