#!/bin/sh
#Created on 2017-11-22 

# handle load_balance ResourceTag open/close
# handle migrate ResourceTag open/close

# OP_OPEN_LOAD_BALANCE 负载均衡
# OP_OPEN_MIGRATE 故障迁移
echo -e "打开meta server的负载均衡策略，但是切主之后会自动关闭"
curl -d '{
    "op_type": "OP_OPEN_MIGRATE",
    "resource_tags": ["'$2'"]
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_OPEN_LOAD_BALANCE",
    "resource_tags": ["'$2'"]
}' http://$1/MetaService/meta_manager
echo -e "\n"

