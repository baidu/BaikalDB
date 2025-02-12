#!/bin/sh
#用来恢复误删的表 
echo -e "\n"
curl -d '{
   "op_type":"OP_RESTORE_TABLE",
    "table_info": {
        "table_name": "test",
        "database": "TEST",
        "namespace_name": "TEST"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

