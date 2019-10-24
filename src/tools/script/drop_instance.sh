#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

#删除实例
echo -e "drop 实例, 有哪些字段就会更新哪些字段\n"
echo 'param: meta_server_address store_address'
drop_instance=$2
curl -d '{
    "op_type": "OP_DROP_INSTANCE",
    "instance": {
        "address" : "'$2'",
        "physical_room": "njjs"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

