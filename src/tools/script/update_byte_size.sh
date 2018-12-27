#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "update byte_size_per_record\n"
curl -d '{
    "op_type":"OP_UPDATE_BYTE_SIZE",
    "table_info": {
        "table_name": "invoice_record",
        "database": "DRFN",
        "namespace_name": "FENGCHAO",
        "byte_size_per_record": 50
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

