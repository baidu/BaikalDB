#!/bin/sh
# 更新update_ttl_duration
# 使用方法: sh update_ttl_duration.sh 10.1.1.1:8111 NAMESPACE DATABASE TABLE 1000000

echo -e "\n"
#创建table
curl -d '{
    "op_type":"OP_UPDATE_TTL_DURATION",
    "table_info": {
        "table_name": "'$4'",
        "database": "'$3'",
        "namespace_name": "'$2'",
        "ttl_duration": '$5'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

