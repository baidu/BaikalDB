#!/bin/sh

# handle main_logical_room Table MainLogicalRoom

echo -e "更新表主机房"
curl -d '{
    "op_type": "OP_UPDATE_MAIN_LOGICAL_ROOM",
    "table_info": {
        "table_name": "t1",
        "database": "TEST",
        "namespace_name": "TEST",
        "main_logical_room": "bj"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
