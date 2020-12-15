#!/usr/bin/python
#-*- coding:utf8 -*-

import urllib
import urllib2
import time
import traceback
import json

class MetaClient:
    def __init__(self, addrList):
        self.addrList = addrList.split(',')
        self.leader = self.addrList[0]
        self.addrIndex = 0
        self.tableSchemaQuery = '{"op_type" : "QUERY_SCHEMA"}'
        self.regionQuery = '{"op_type" : "QUERY_REGION"}'
        self.getLeaderQuery = '{"op_type" : "GetLeader","region_id":0}'

    def getLeader(self):
        res = None
        for addr in self.addrList:
            url = 'http://%s/%s' % (addr, 'MetaService/raft_control')
            try:
                req = urllib2.Request(url, self.getLeaderQuery)
                response = urllib2.urlopen(req)
                res = response.read()
                break
            except Exception,e:
                print str(e)
                continue
        if res == None:
            return None
        try:
            jsres = json.loads(res)
            leader = jsres['leader']
            return leader
        except Exception,e:
            print traceback.format_exc()
            return None
            

    def getTableSchema(self):
        res, errMsg = self.post('/MetaService/query', self.tableSchemaQuery)
        try:
            jsres = json.loads(res)
            if 'schema_infos' not in jsres:
                return []
            return jsres['schema_infos']
        except Exception,e:
            print traceback.format_exc()
            return []


    def getRegionInfo(self):
        res, errMsg = self.post('/MetaService/query', self.regionQuery)
        try:
            jsres = json.loads(res)
            if 'region_infos' not in jsres:
                return []
            return jsres['region_infos']
        except Exception,e:
            print traceback.format_exc()
            return []

                

    def post(self,uri,data):
        res = None
        tryTimes = 3
        errorMsg = ''
        while tryTimes:
            tryTimes -= 1
            try:
                url = 'http://%s/%s' % (self.getLeader(), uri)
                req = urllib2.Request(url, data)
                response = urllib2.urlopen(req)
                res = response.read()
            except Exception,e:
                time.sleep(1)
                continue
            break
        if res == None:
            print traceback.format_exc()
            return res, errorMsg
        return res,''

