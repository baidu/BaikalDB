#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程
#bool need_merge
#bool storage_compute_separate
#bool select_index_by_cost
#optional float filter_ratio 

echo -e "\n"
curl -d '{
    "op_type":"OP_UPDATE_SCHEMA_CONF",
    "table_info": {
        "table_name": "t_sort",
        "database": "TEST",
        "namespace_name": "TEST",
        "schema_conf":{
            "select_index_by_cost": true
        }
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

