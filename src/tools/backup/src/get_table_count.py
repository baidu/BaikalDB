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


def get_tables(sourceConf):
    conn = pymysql.connect(**sourceConf)
    sor = conn.cursor()
    sql = 'show tables;'
    sor.execute(sql)
    tblist = []
    for row in sor.fetchall():
	tname = str(row[0])
	tblist.append(tname)
    return tblist

def get_count(sourceConf, tb):
    conn = pymysql.connect(**sourceConf)
    sor = conn.cursor()
    sql = 'select count(*) from %s;' % tb
    sor.execute(sql)
    count = sor.fetchone()[0]
    return count

def main(configfile):
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    sourceConf = {"host":config.get('source','host'),
		"port":int(config.get('source','port')),
		"user":config.get('source','user'),
		"password":config.get('source','password'),
		"database":config.get('source','database')}

    tables = get_tables(sourceConf)
    dbcount = 0
    for tb in tables:
	count = get_count(sourceConf, tb)
	print tb, count
        dbcount += count
    print len(tables), dbcount

if __name__ == "__main__":
    main(sys.argv[1])
