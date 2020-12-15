#!/usr/bin/python
#-*- coding:utf8 -*-

import sys
import os

CURRENT_PATH = os.getcwd()

sys.path.append(os.path.join(CURRENT_PATH, 'protoout'))


class BinlogProcessor:
    def __init__(self, schemaManager):
        self.schemaManager = schemaManager

    def process(self,item):
        tid = int(item['table_id'])
        item['table'] = self.schemaManager.tableSchemaDict[tid]['table_name']
        item['database'] = self.schemaManager.tableSchemaDict[tid]['database']
        item['primary_key'] = self.schemaManager.tableSchemaDict[tid]['primary_key']
        
