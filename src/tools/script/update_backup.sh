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
        "table_name": "tb_online_html",
        "database": "jz_b_render",
        "namespace_name": "FENGCHAO",
        "schema_conf":{
            "backup_table": "BT_AUTO"
        }
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

