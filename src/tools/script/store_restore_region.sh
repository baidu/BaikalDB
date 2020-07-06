#!/bin/sh
#Created on 2017-11-22 

echo 'param: address region_id'
curl -d '{
    "force": true
}' http://$1/StoreService/restore_region
echo -e "\n"


