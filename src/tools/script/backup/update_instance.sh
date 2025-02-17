#插入物理机房
echo -e "update 实例, 有哪些字段就会更新哪些字段\n"
curl -d '{
    "op_type": "OP_UPDATE_INSTANCE",
    "instance": {
        "address" : "127.0.0.1:8011",
        "status": 1
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "QUERY_INSTANCE",
    "instance_address": "127.0.0.1:8011"
}' http://$1/MetaService/query
