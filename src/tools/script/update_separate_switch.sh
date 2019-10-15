#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
curl -d '{
    "op_type":"OP_UPDATE_SCHEMA_CONF",
    "table_info": {
        "table_name": "region_merge_not_sep",
        "database": "TEST",
        "namespace_name": "TEST",
        "schema_conf":{
            "storage_compute_separate": true
        }
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

