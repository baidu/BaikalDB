#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
echo -e "restore region through lower and upper region_id"
curl -d '{
    "op_type":"OP_RESTORE_REGION",
    "restore_region": {
        "restore_region_id":'$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"


