#!/bin/sh
#验证场景：完成的meta_server流程

echo -e "add logic idc: bj nj hz\n"
curl -s -d '{
    "op_type": "OP_ADD_LOGICAL",
    "logical_rooms": {
        "logical_rooms" : ["bj", "nj", "gz"]
    }
}' http://$1/MetaService/meta_manager


#插入物理机房
echo -e "add physical idc: bj nj gz\n"
curl -s -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "bj",
        "physical_rooms" : ["bj"]
    }
}' http://$1/MetaService/meta_manager

curl -s -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "nj",
        "physical_rooms" : ["nj"]
    }
}' http://$1/MetaService/meta_manager

curl -s -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "gz",
        "physical_rooms" : ["gz"]
    }
}' http://$1/MetaService/meta_manager

#查询逻辑机房
echo -e "query logical idc\n"
curl -s -d '{
    "op_type" : "QUERY_LOGICAL"
}' http://$1/MetaService/query

#查询物理机房
echo -e "\nquery physical idc\n"
curl -s -d '{
    "op_type" : "QUERY_PHYSICAL"
}' http://$1/MetaService/query

##查询测试实例
echo -e "\nquery instance\n"
curl -s -d '{
    "op_type" : "QUERY_INSTANCE"
}' http://$1/MetaService/query

