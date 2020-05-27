#!/bin/sh
# 查看region所属的txn状态
echo -e "query region txns state"
echo 'param: address region_id'
region_id=$2
curl -d '{
    "op_type" : "OP_TXN_QUERY_STATE",
    "region_id": '$region_id',
    "region_version": 1,
    "force": true
}' http://$1/StoreService/query
echo -e "\n"
