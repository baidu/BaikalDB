#!/bin/sh
#Created on 2017-11-22 

echo -e "查看从instance角度和raft角度 store上存储的region的diff"
curl -d '{
    "op_type": "QUERY_DIFF_REGION_IDS",
    "instance_address": "'$2'"
}' http://$1/MetaService/query
echo -e "\n"

