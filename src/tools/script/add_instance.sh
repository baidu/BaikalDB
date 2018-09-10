#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

#插入物理机房
echo -e "增加实例\n"
curl -d '{
    "op_type": "OP_ADD_INSTANCE",
    "instance": {
        "address" : "127.0.0.1:8011",
        "capacity" : 107374182400,
        "used_size" : 0,
        "resource_tag" :"",
        "physical_room" :"idc",
        "status": 1
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "QUERY_INSTANCE",
    "instance_address": "127.0.0.1:8011"
}' http://$1/MetaService/query
