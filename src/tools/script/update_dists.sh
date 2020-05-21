#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "update replica dists\n"
curl -d '{
    "op_type":"OP_UPDATE_DISTS",
    "table_info": {
        "table_name": "'$2'",
        "database": "test",
        "namespace_name": "TEST",
        "replica_num": 3,
         "dists": [
            {
                "logical_room": "nj",
                "count" : 3
            }
         ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

