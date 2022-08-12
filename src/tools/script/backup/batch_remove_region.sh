#!/bin/sh
#Created on 2017-11-22 
#测试场景：在store删除无用的region

sh remove_region.sh 127.0.0.1:8222  32862
sh remove_region.sh 127.0.0.1:8222  32863
