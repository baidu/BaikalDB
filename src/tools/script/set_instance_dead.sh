#!/bin/sh
#Created on 2017-11-22 
#测试场景：在store删除无用的region

echo -e "set instance dead, 迁移走所有的region"
echo -e "使用前提是该store不再上报心跳"
echo 'param: meta_server_address, store_address'
store=$2
curl -d '{
    "op_type": "OP_SET_INSTANCE_MIGRATE",
    "instance": {
        "address" : "'$store'"
     }
}' http://$1/MetaService/meta_manager
echo -e "\n"


