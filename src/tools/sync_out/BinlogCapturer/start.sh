#!/bin/bash

database=`awk -F '=' '/\[global\]/{a=1}a==1&&$1~/database/{print $2;exit}' conf/config.cfg`
binlogRegionID=`awk -F '=' '/\[global\]/{a=1}a==1&&$1~/binlogRegionID/{print $2;exit}' conf/config.cfg`

pid=`ps -ef | grep "\-\-id\=$binlogRegionID \-\-database\=$database" | grep -v "grep" | awk '{print$2}'`
echo $pid
if [ "$pid" ]; then
    echo "stop capturer: "$pid
    kill -9 $pid
fi

nohup python src/binlogCapturer.py --id=$binlogRegionID --database=$database > run.info 2>&1 &
