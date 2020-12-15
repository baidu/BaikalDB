#!/usr/bin/python
#-*- coding:utf8 -*-

import sys
import os
import time

CURRENT_PATH = os.getcwd()

sys.path.append(os.path.join(CURRENT_PATH, 'protoout'))

import base64
import threading
import multiprocessing
import ConfigParser

from store import interface_pb2
from dynamicMessageParser import DynamicMessageParser
from schemaManager import SchemaManager

class BinlogParser(multiprocessing.Process):
    def __init__(self, configPath, inQueue, outQueue):
        multiprocessing.Process.__init__(self)
        self.configPath = configPath
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.configPath)
        self.inQueue = inQueue
        self.outQueue = outQueue
    def init(self):
        self.lock = threading.Lock()
        self.schemaDict = {}
        self.messageParser = DynamicMessageParser()
        self.initSchemaManager()
        self.UpdateSchemas()

    def initSchemaManager(self):
        self.metaServerList = self.config.get('global','metaServer')
        self.schemaManager = SchemaManager(self.config.get('global','binlogTableID'), self.metaServerList)
        self.schemaManager.init()

        self.schemaManager.setSchemaUpdateFunc(self.tableSchemaUpdateFunc)

    def isAlive(self):
        return self.is_alive()
    def tableSchemaUpdateFunc(self, insertDict, updateDict, deleteDict):
        for tableID, schema in insertDict.items():
            self.binlogParser.updateTableSchema(schema)
        for tableID, schema in updateDict.items():
            self.binlogParser.updateTableSchema(schema)


    def run(self):
        self.init()
        while True:
            blogItem = self.inQueue.get()
            commitTs = blogItem['commitTs']
            for item in self.parse(blogItem['binlogStr']):
                item['commitTs'] = commitTs
                self.outQueue.put(item)

    def updateTableSchema(self, schema):
        self.lock.acquire()
        self.messageParser.addTable(schema)
        self.lock.release()
    def UpdateSchemas(self):
        for schema in self.schemaManager.getTableSchemaList():
            self.messageParser.addTable(schema)

    def parseInsertRows(self, tableId, rows):
        res = []
        for row in rows:
            fieldDict = self.parseOneRows(tableId, row)
            item = {}
            item['type'] = "INSERT"
            item['value'] = fieldDict
            item['table_id'] = tableId
            res.append(item)
        return res

    def parseDeletedRows(self, tableId, rows):
        res = []
        for row in rows:
            fieldDict = self.parseOneRows(tableId, row)
            item = {}
            item['type'] = "DELETE"
            item['value'] = fieldDict
            item['table_id'] = tableId
            res.append(item)
        return res

    def parseOneRows(self, tableId, row):
        self.lock.acquire()
        tryTimes = 3
        errCode = 0
        value = None
        while tryTimes >= 0:
            tryTimes -= 1
            errCode, value = self.messageParser.parseFromString(tableId, row)
            if errCode == 0:
                break
            else:
                self.lock.release()
                self.UpdateSchemas()
                print "parse binlog faild! so update schema!"
                self.lock.acquire()
        self.lock.release()
        return value

    def parseUpdateRows(self, tableId, oldRows, newRows):
        lenold = len(oldRows)
        lennew = len(newRows)
        if lenold != lennew:
            #TODO 解析异常处理，不止一处
            return []
        res = []
        for i in range(0, lenold):
            oldValue = self.parseOneRows(tableId, oldRows[i])
            newValue = self.parseOneRows(tableId, newRows[i])
            item = {}
            item['type'] = "UPDATE"
            item['value'] = newValue
            item['old_value'] = oldValue
            item['table_id'] = tableId
            res.append(item)
        return res
        
   
    def parse(self,binlogStr): 
        msg = interface_pb2.StoreReq()
        record = base64.b64decode(binlogStr)
        msg.ParseFromString(record)
        if msg.binlog.type != 0:
            return []
        res = []
        for mutation in msg.binlog.prewrite_value.mutations:
            binlogType = mutation.sequence[0]
            tableId = mutation.table_id
            if binlogType == 0:
                #insert binlog
                rows = mutation.insert_rows
                res.extend(self.parseInsertRows(tableId, rows))
            elif binlogType == 1:
                oldRows = mutation.deleted_rows
                newRows = mutation.insert_rows
                res.extend(self.parseUpdateRows(tableId, oldRows, newRows))
            elif binlogType == 2:
                rows = mutation.deleted_rows
                res.extend(self.parseDeletedRows(tableId, rows))
        return res



