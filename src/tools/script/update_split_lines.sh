#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "update byte_size_per_record\n"
curl -d '{
    "op_type":"OP_UPDATE_BYTE_SIZE",
    "table_info": {
        "table_name": "h5_backup_tmp",
        "database": "CIP_C",
        "namespace_name": "FENGCHAO",
        "region_split_lines": 1000000
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询table
curl -d '{
    "op_type" : "QUERY_SCHEMA"
}' http://$1/MetaService/query
