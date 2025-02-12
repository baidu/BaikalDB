#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

# handle split_region regionID splitKey

echo -e "\n"
echo -e "split region, 分配一个region_id\n"
curl -d '{
    "op_type":"OP_SPLIT_REGION",
    "region_split": {
        "region_id": 1,
        "split_key": "not used"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"


