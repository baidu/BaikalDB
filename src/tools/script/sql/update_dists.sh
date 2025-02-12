#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

# handle update_dists TableName
# '{
# "replica_num": 3,
# "main_logical_room" : "bj",
# "dists": [
# {
# "logical_room": "bj",
# "count" : 3
# }]
#}'

echo -e "\n"
#创建table
echo -e "update replica dists\n"
curl -d '{
    "op_type":"OP_UPDATE_DISTS",
    "table_info": {
        "table_name": "'$2'",
        "database": "FC_Word",
        "namespace_name": "FENGCHAO",
        "replica_num": 3,
        "main_logical_room" : "bd",
         "dists": [
            {
                "logical_room": "bd",
                "count" : 2
            },
            {
                "logical_room": "bj",
                "count" : 1
            }
         ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

