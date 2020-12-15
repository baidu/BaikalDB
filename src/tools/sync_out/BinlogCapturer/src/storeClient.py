#!/usr/bin/python
#-*- coding:utf8 -*-

import urllib
import urllib2
import time
import traceback
import json

class StoreClient:
    def __init__(self, addr):
        self.addr = addr
        self.binlogQueryTpl = '{ \
                "binlog_desc":{ \
                    "read_binlog_cnt":%(count)d, \
                    "binlog_ts":%(start_ts)d \
                }, \
                "op_type":"OP_READ_BINLOG", \
                "region_id":%(region_id)d, \
                "region_version":%(region_version)d \
            }'
        self.binlogUri = '/StoreService/query_binlog'

    def get_binlog(self,region_id, region_version, start_ts, count):
        params = {
            "region_id":region_id,
            "region_version":region_version,
            "start_ts":start_ts,
            "count":count
        }
        queryData = self.binlogQueryTpl % params
        res, errCode = self.post(self.binlogUri, queryData)
        if res == None:
            return 1, []
        try:
            jsres = json.loads(res)
            if 'binlogs' not in jsres or 'commit_ts' not in jsres:
                return 0, []
            res = []
            if len(jsres['binlogs']) != len(jsres['commit_ts']):
                return 0, []
            length = len(jsres['binlogs'])
            for i in range(0, length):
                binlogItem = {"binlogStr":jsres['binlogs'][i],"commitTs":jsres['commit_ts'][i]}
                res.append(binlogItem)
            return 0, res
        except Exception,e:
            print traceback.format_exc()
            return 2,[]


    def post(self,uri,data):
        res = None
        errorCode = 0
        try:
            url = 'http://%s/%s' % (self.addr, uri)
            req = urllib2.Request(url, data)
            response = urllib2.urlopen(req)
            res = response.read()
        except Exception,e:
            if type(e) == urllib2.URLError:
                errorCode = 1
        if res == None:
            print traceback.format_exc()
            return res, errorCode
        return res,errorCode

