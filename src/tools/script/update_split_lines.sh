#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
curl -d '{
    "op_type":"OP_UPDATE_SPLIT_LINES",
    "table_info": {
        "table_name": "ideainfo",
        "database": "TEST",
        "namespace_name": "TEST",
        "region_split_lines": 250000
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

