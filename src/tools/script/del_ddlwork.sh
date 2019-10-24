echo -e "查询查询实例\n"
curl -d '{
    "op_type" : "OP_DELETE_DDLWORK",
    "ddlwork_info" : {
        "table_id" : '$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
