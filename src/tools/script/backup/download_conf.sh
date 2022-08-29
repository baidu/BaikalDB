#!/usr/bin/env sh
#/***************************************************************************
# * 
# * Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
# * 
# **************************************************************************/
 
 
 
#/**
# * @file conf.sh
# * @date 2016/08/01 19:23:47
# * @brief 
# *  
# **/

#配置
home_dir="$HOME"
#日志目录
script_dir=$(pwd)
log_dir="${script_dir}/log"
#日志文件
log_filename="${log_dir}/download_schedule_new.log"
#检查任务是否完成的次数
retry_task_check=1000
#每次检查任务间隔时间
sleep_per_task_check=30

hadoop="$HOME/hadoop-client/hadoop/bin/hadoop"
hdfs_path="/app/ecom/fcr/roi/ocpc/ocpc_data/yewuduan_v3"
data_path="${script_dir}/data"















#/y vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
