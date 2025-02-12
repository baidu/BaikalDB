#!/usr/bin/env bash

#/***************************************************************************
# * 
# * Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
# * 
# **************************************************************************/
 
 
 
#/**
# * @file run.sh
# * @date 2016/08/01 18:36:40
# * @brief 
# *  
# **/

source download_conf.sh

#日志函数
function log_message()
{
    #$1 日志级别
    #$2 日志信息
    case "$1" in 
        "i") 
            mes_class="[ info ]\t"
            ;;
        "f") 
            mes_class="[ fatal]\t"
            ;;
    esac
    mes_date=`date "+%Y-%m-%d %H:%M:%S"`
    echo -e "${mes_date}\t${mes_class}$2" >>$log_filename

}

yestoday=$(date -d "1 day ago" "+%Y%m%d")
if [[ $1 != "" ]]; then
    yestoday=$1
fi
has_done=false
while true;do
    $hadoop fs -ls "$hdfs_path/$yestoday/.done"
    if [[ $? -eq 0 ]];then
        log_message "i" "task has done"
        has_done=true
        break
    fi
    log_message "f" "task not done,retry"
    sleep $sleep_per_task_check
done
if [[ ! $has_done ]];then
    log_message "f" "task has not done,exit"
    exit -1
fi

start_tm=$(date +%s)
rm $data_path -rf
mkdir -p $data_path
$hadoop fs -get "$hdfs_path/$yestoday/*" $data_path
if [[ $? != 0 ]]; then
    log_message "f" "download fail"
    exit -1
fi
end_tm=$(date +%s)
((cost=end_tm-start_tm))
log_message "i" "download over, cost:$cost s"
./importer
end_tmm=$(date +%s)
((cost=end_tmm-end_tm))
log_message "i" "import over, cost:$cost s"


#/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
