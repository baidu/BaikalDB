#!/bin/sh
# 打开、关闭计算存储分离
# 使用方法: sh update_separate_switch.sh 10.1.1.1:8111 NAMESPACE DATABASE TABLE conf false/true

echo -e "\n"
curl -d '{
    "op_type":"OP_UPDATE_SCHEMA_CONF",
    "table_info": {
        "table_name": "'$4'",
        "database": "'$3'",
        "namespace_name": "'$2'",
        "schema_conf":{
            "'$5'": '$6' 
        }
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

