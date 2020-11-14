#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "测试场景：添加逻辑机房bj nj hz\n"
curl -d '{
    "op_type": "OP_ADD_LOGICAL",
    "logical_rooms": {
        "logical_rooms" : ["bj", "nj", "hz", "yq", "gz"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#插入物理机房
echo -e "插入物理机房\n"
curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "bj",
        "physical_rooms" : ["tc", "yf01", "m1", "bjyz", "cq01", "cq02", "cp01", "dbl01", "st01", "bjhw"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "nj",
        "physical_rooms" : ["nj01", "nj02", "nj03", "njjs"]
    }
}' http://$1/MetaService/meta_manager

echo -e "\n"
curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "hz",
        "physical_rooms" : ["hz01"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo -e "\n"
curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "gz",
        "physical_rooms" : ["gzbh", "gzhl", "gzns", "gzhxy"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo -e "\n"
curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "yq",
        "physical_rooms" : ["yq01"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
#查询逻辑机房
echo -e "查询逻辑机房\n"
curl -d '{
    "op_type" : "QUERY_LOGICAL"
}' http://$1/MetaService/query
echo -e "\n"

#查询物理机房
echo -e "查询物理机房\n"
curl -d '{
    "op_type" : "QUERY_PHYSICAL"
}' http://$1/MetaService/query
echo -e "\n"

##查询测试实例
echo -e "查询查询实例\n"
curl -d '{
    "op_type" : "QUERY_INSTANCE"
}' http://$1/MetaService/query
echo -e "\n"

