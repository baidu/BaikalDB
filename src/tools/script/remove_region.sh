#!/bin/sh
#Created on 2017-11-22 
#测试场景：在store删除无用的region

echo -e "remove region to store"
echo 'param: address region_id'
region_id=$2
curl -d '{
    "region_id": '$region_id',
    "force": true
}' http://$1/StoreService/remove_region
echo -e "\n"


