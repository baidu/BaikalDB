#!/bin/sh
#验证场景：完成的meta_server流程

echo -e "测试场景：添加逻辑机房bj nj hz\n"
curl -d '{
    "op_type": "OP_ADD_LOGICAL",
    "logical_rooms": {
        "logical_rooms" : ["bj", "nj", "gz"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#插入物理机房
echo -e "插入物理机房\n"
curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "bj",
        "physical_rooms" : ["bj"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "nj",
        "physical_rooms" : ["nj"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "gz",
        "physical_rooms" : ["gz"]
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

