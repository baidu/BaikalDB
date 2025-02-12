#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

# handle table_resource_tag TableName NewResourceTag

echo -e "\n"
#创建table
echo -e "创建table\n"
curl -d '{
    "op_type":"OP_MODIFY_RESOURCE_TAG",
    "table_info": {
        "table_name": "'$3'",
        "database": "'$2'",
        "namespace_name": "DMP",
        "resource_tag" : "DMPxinghe-yq"
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

