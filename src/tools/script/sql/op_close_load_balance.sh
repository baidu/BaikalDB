#!/bin/sh
# sh op_close_load_balance.sh 10.1.1.1:8000 resource_tag

# handle load_balance ResourceTag open/close
# handle migrate ResourceTag open/close

# OP_CLOSE_LOAD_BALANCE 负载均衡
# OP_CLOSE_MIGRATE 故障迁移
curl -d '{
    "op_type": "OP_CLOSE_LOAD_BALANCE",
    "resource_tags": ["'$2'"]
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_CLOSE_MIGRATE",
    "resource_tags": ["'$2'"]
}' http://$1/MetaService/meta_manager
echo -e "\n"

