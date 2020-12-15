#!/usr/bin/python
#-*- coding:utf8 -*-

import sys
import os

CURRENT_PATH = os.getcwd()

sys.path.append(os.path.join(CURRENT_PATH, 'protoout'))


class BinlogFilter:
    def __init__(self, filterRulePath):
        self.ruleFilePath = filterRulePath
        self.initFilterRuleDict()

    def initFilterRuleDict(self):
        self.filterRuleDict = {}
        for line in open(self.ruleFilePath):
            ops = line.split('\t')
            if len(ops) != 4:
                continue
            db = ops[0].strip()
            fi = ops[1].strip()
            op = ops[2].strip()
            va = ops[3].strip()
            exp = 'value.get("%s",None) %s %s' % (fi, op, va)
            if db not in self.filterRuleDict:
                self.filterRuleDict[db] = []
            self.filterRuleDict[db].append(exp)
            

    def filter(self,item):
        db = item['database']
        if db not in self.filterRuleDict:
            return False
        value = item['value']
        for exp in self.filterRuleDict[db]:
            if eval(exp) == False:
                return True
        return False
