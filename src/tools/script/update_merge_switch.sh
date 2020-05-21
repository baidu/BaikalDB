#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
curl -d '{
    "op_type":"OP_UPDATE_SCHEMA_CONF",
    "table_info": {
        "table_name": "recognized_clicker",
        "database": "FC_Ganyu",
        "namespace_name": "FENGCHAO",
        "schema_conf":{
            "need_merge": true
        }
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

