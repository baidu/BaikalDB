#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import multiprocessing
import time
import sys

HOST=sys.argv[1]
USER="root"
PASSWORD="****"
DATABASE="Baikaltest"
PORT=eval(sys.argv[2])
resource_tag=''
namespace="FENGCHAO"

def worker3(start1, end):
    start = time.time()
    print(int(start1), int(end))
    db = MySQLdb.connect(HOST, USER, PASSWORD, DATABASE, port=PORT)
    cursor = db.cursor()
    delta = end - start1
    for i in range(int(start1),int(end)):
        sql="replace Baikaltest.studentCstorePy(id,name1,name2,age1,age2,class1,class2,address,height) values(%d, 'baikal%d','baidu%d',%d, %d, %d, %d,'baidu%d',%d)"%(i,i,i,i,i,i,i,i,i)
        sql += ",(%d, 'baikal%d','baidu%d',%d, %d, %d, %d,'baidu%d',%d)"%(i+delta,i+delta,i+delta,i+delta,i+delta,i+delta,i+delta,i+delta,i+delta)
        try:
            cursor.execute(sql)
            if i%10 == 0:
                db.commit()
        except:
            pass
    db.commit()
    db.close()
    end = time.time()
    print("%s", end - start)

def create_table():
    '''建表'''
    db = MySQLdb.connect(HOST, USER, PASSWORD, DATABASE, port=PORT)
    cursor = db.cursor()
    drop_table_if_exist = "drop table if exists Baikaltest.studentCstorePy;"
    try:
        cursor.execute(drop_table_if_exist)
    except:
        pass
    sql = '''
    CREATE TABLE `Baikaltest`.`studentCstorePy` (
        `id` int(10) NOT NULL ,
        `name1` varchar(1024) NOT NULL ,
        `name2` varchar(1024) NOT NULL ,
        `age1` int(10) NOT NULL ,
        `age2` int(10) NOT NULL ,
        `class1` int(10) NOT NULL ,
        `class2` int(10) NOT NULL ,
        `address` varchar(1024) NOT NULL ,
        `height` int(10) NOT NULL ,
        PRIMARY KEY (`id`),
        UNIQUE KEY `name1_key` (`name1`),
        UNIQUE KEY `name2_key` (`name2`),
        KEY GLOBAL `age1_key` (`age1`),
        KEY GLOBAL `age2_key` (`age2`),
        UNIQUE KEY GLOBAL `class1_key` (`class1`),
        UNIQUE KEY GLOBAL `class2_key` (`class2`),
        KEY `address_key` (`address`)
    ) ENGINE=Rocksdb DEFAULT CHARSET=utf8 AVG_ROW_LENGTH=500 COMMENT='{"resource_tag":"%s", "replica_num":3, "region_split_lines":209715, "namespace":"%s"}';
    '''%(resource_tag,namespace)
    try:
        cursor.execute(sql)
    except:
        return False
    time.sleep(100) # sleep 200s wait region inited
    return True

def check_rows_number():
    '''检查主表和全局二级索引表的行数是否匹配'''
    db = MySQLdb.connect(HOST, USER, PASSWORD, DATABASE, port=PORT)
    cursor = db.cursor()
    all_rows_sql = "select count(*) from Baikaltest.studentCstorePy";
    class1_rows_sql = "select count(*) from Baikaltest.studentCstorePy where class1>0";
    class2_rows_sql = "select count(*) from Baikaltest.studentCstorePy where class2>0";
    age1_rows_sql = "select count(*) from Baikaltest.studentCstorePy where age1>0";
    age2_rows_sql = "select count(*) from Baikaltest.studentCstorePy where age2>0";
    ret = []
    cursor.execute(all_rows_sql)
    all_rows = cursor.fetchall()
    ret.append(all_rows[0][0])
    cursor.execute(class1_rows_sql)
    class1_rows = cursor.fetchall()
    ret.append(class1_rows[0][0])
    cursor.execute(class2_rows_sql)
    class2_rows = cursor.fetchall()
    ret.append(class2_rows[0][0])
    cursor.execute(age1_rows_sql)
    age1_rows = cursor.fetchall()
    ret.append(age1_rows[0][0])
    cursor.execute(age2_rows_sql)
    age2_rows = cursor.fetchall()
    ret.append(age2_rows[0][0])
    # print ret
    cnt = 0
    for it in ret:
        if it == ret[0]:
            cnt += 1
    if cnt != 5:
        print "check data consistent failed"
        return False
    else:
        print "check data consistent success"
        return True

def run():
    # 建表
    if create_table() == False:
        print("create table failed")
        return False
    # 插入数据
    record = []
    p1 = multiprocessing.Process(target=worker3, args=(1000000,1100000))
    record.append(p1)
    p2 = multiprocessing.Process(target=worker3, args=(3000000,3100000))
    record.append(p2)
    p3 = multiprocessing.Process(target=worker3, args=(6000000,6100000))
    record.append(p3)
    p4 = multiprocessing.Process(target=worker3, args=(70000000,70100000))
    record.append(p4)
    p2.start()
    p1.start()
    p3.start()
    p4.start()
    for i in record:
        i.join() 
    # 检查数据一致性
    check_rows_number()

if __name__ == '__main__':
    run()
