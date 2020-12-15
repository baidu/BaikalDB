#!/usr/bin/python
#-*- coding:utf8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import random
import pymysql
import commands
import ConfigParser
import io
import time
import subprocess
import fileinput
from datetime import datetime

def get_pk(mysqlSor, tbname):
    sql = 'show create table %s;' % tbname
    mysqlSor.execute(sql)
    pk = []
    for row in mysqlSor.fetchall():
	lines = str(row[1]).split('\n')
	for line in lines:
	    line = line.strip()
	    if line.find('PRIMARY KEY') == 0:
		pk = line[12:].rstrip(',')
		pk = eval(pk.replace('`','\''))
		if type(pk) == str:
		    pk = [pk]
    return pk

def get_tables(mysqlConf):
    conn = pymysql.connect(**mysqlConf)
    sor = conn.cursor()
    sql = 'show tables;'
    sor.execute(sql)
    tblist = []
    for row in sor.fetchall():
	tname = str(row[0])
	tblist.append(tname)
    pkDict = {}
    for tb in tblist:
	pk = get_pk(sor, tb)
	pkDict[tb] = pk
    return pkDict

def dump(conf, tb, pklist, outdir, limit = 100000):
    conn = pymysql.connect(**conf)
    orderflist = []
    for pk in pklist:
	orderflist.append(pk + ' desc')
    orderby = ','.join(orderflist)
    once = 10000
    begin = 0
    wherevalue = ''
    wherevformat = ''
    wherekey = ''
    where = ''
    cnt = 0
    outf = open('%s/%s.sql' % (outdir,tb),'w')
    indexList = []
    while True:
	if begin == 0:
	    #sql = '/*{"full_export":true}*/ select * from %s order by %s limit %d;' % (tb, orderby, once)
	    sql = 'select * from %s order by %s limit %d;' % (tb, orderby,once)
	else:
	    sql = 'select * from %s where %s order by %s limit %d;' % (tb, where,orderby, once)
	print sql
	sor = conn.cursor()
	sql_head = 'replace into %s values ' % tb
        valueList = []
        sor.execute(sql)
	col = sor.description
	collist = []
	for c in col:
	    collist.append(c[0])
	if len(indexList) == 0:
	    for pk in pklist:
		ind = collist.index(pk)
		indexList.append(ind)
            flist = ['%s' for i in col]
            formatStr = '(' + ','.join(flist) + ')'
        rows = sor.fetchall()
        if len(rows) == 0:
            return 
	curcnt = 0
        for row in rows:
            cnt += 1
	    curcnt += 1
	    pkv = []
	    v = sor.mogrify(formatStr, row)
            valueList.append(v)
            if len(valueList) == 1000:
                sql = sql_head + ','.join(valueList)
                valueList = []
                outf.write(sql + ';\n')
        if len(valueList) != 0:
	    sql = sql_head + ','.join(valueList)
	    outf.write(sql + ';\n')
	    valueList = []
	time.sleep(0.01)
	if begin >= limit or curcnt < once:
	    break
	begin += once
	if wherekey == '':
	    if len(indexList) == 1:
	        wherekey = '`%s`' % pklist[0]
	    else:
	        pkklist = []
	        for pk in pklist:
	            pkklist.append('`' + pk + '`')
	        wherekey = '(%s)' % ','.join(pkklist)
	    tmpvlist = ['%s' for i in indexList]
	    wherevformat = ','.join(tmpvlist)
	vlist = []
	for ind in indexList:
	    vlist.append(rows[-1][ind])
	wherevalue = sor.mogrify(wherevformat, vlist)
	if len(indexList) != 1:
	    wherevalue = '(' + wherevalue + ')'
	where = wherekey + ' < ' + wherevalue
	

def main(configfile):
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    mysqlConf = {"host":config.get('source','host'),
		"port":int(config.get('source','port')),
		"user":config.get('source','user'),
		"password":config.get('source','password'),
		"database":config.get('source','database'),
		"charset":"utf8"}

    

    outdir = config.get('global','outdir')
    limit = int(config.get('global','limit'))
    pkDict = get_tables(mysqlConf)
    for tablename, pk in pkDict.items():
	print tablename, pk
        print datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
	print "begin dump table ", tablename
	dump(mysqlConf,tablename, pk, outdir, limit)
        print datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
	print "end dump table ", tablename
	


if __name__ == "__main__":
    main(sys.argv[1])
