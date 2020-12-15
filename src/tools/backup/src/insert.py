#-*- coding:utf8 -*-
import os
import random
import pymysql
import commands
import ConfigParser
import io
import sys
import time
import subprocess
import fileinput
from datetime import datetime


def insert(conf, fname):
    conn = pymysql.connect(**conf)
    sor = conn.cursor()
    cnt = 0
    for line in open(fname):
	sor.execute(line.rstrip(';'))
	cnt += 1
	if cnt % 10 == 0:
	    time.sleep(0.01)
	print cnt

def main(configfile):
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    mysqlConf = {"host":config.get('dest','host'),
		"port":int(config.get('dest','port')),
		"user":config.get('dest','user'),
		"password":config.get('dest','password'),
		"database":config.get('dest','database'),
		"autocommit":True,"charset":"utf8"}

    outdir = config.get('global','outdir')
    for fname in os.listdir(outdir):
	if not fname.endswith('.sql'):
	    continue
        print datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
	print "begin insert table ", fname
	insert(mysqlConf,os.path.join(outdir,fname))
        print datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
	print "end insert table ", fname
	


if __name__ == "__main__":
    main(sys.argv[1])
