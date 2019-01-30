#!/bin/bash

#set -x
workpath=`pwd`
cd $workpath
source ./config.conf

#baikaldb or mysql

if [[ $1 = "baikaldb" ]];
then
    common_file="lua/oltp_common.lua"
    storage_engine="rocksdb"
else
    common_file="lua/oltp_common.lua"
    storage_engine="innodb"
fi

cd ..
echo `pwd`

# cleanup
./sysbench --auto-inc=off --create_secondary=true --db-ps-mode=auto \
 --mysql-host=$host --mysql-port=$port --mysql-user=$user --mysql-password=$passwd \
 --mysql-storage-engine=$storage_engine --table_size=$table_size --tables=$tables $common_file cleanup
