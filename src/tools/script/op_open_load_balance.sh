#!/bin/sh
#Created on 2017-11-22 

echo -e "打开meta server的负载均衡策略，但是切主之后会自动关闭"
curl -d '{
    "op_type": "OP_OPEN_LOAD_BALANCE",
    "resource_tags": ["cip-yz"]
}' http://$1/MetaService/meta_manager
echo -e "\n"

