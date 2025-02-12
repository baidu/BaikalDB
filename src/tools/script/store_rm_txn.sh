#!/bin/sh
# 查看region所属的txn状态

# show store_txn storeAddress regionID region_version

echo -e "query region txns state"
echo 'param: address region_id'
region_id=$2
curl -d '{
    "op_type" : "OP_TXN_COMPLETE",
    "region_id": '$region_id',
    "region_version": 0,
    "rollback_txn_ids": ['$3'],
    "force": true
}' http://$1/StoreService/query
echo -e "\n"
