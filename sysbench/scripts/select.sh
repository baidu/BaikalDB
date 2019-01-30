#!/bin/bash

#set -x
workpath=`pwd`
cd $workpath
source ./config.conf

#baikaldb or mysql

if [[ $1 = "baikaldb" ]];
then
    common_file="select_random_ranges.lua"
    storage_engine="rocksdb"
else
    common_file="select_random_ranges.lua"
    storage_engine="innodb"
fi

cd ../lua
echo `pwd`
# run read-only

../sysbench --auto-inc=off --create_secondary=true --db-ps-mode=auto \
 --mysql-host=$host --mysql-port=$port --mysql-user=$user --mysql-password=$passwd \
 --mysql-storage-engine=$storage_engine --table_size=$table_size --tables=$tables \
 --report-interval=$interval --percentile=95 \
 --events=$events --time=$time --threads=$threads --rand-type=uniform $common_file run
