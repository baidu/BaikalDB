echo "二级索引"

# handle delete_ddl TableID global/local
# handle suspend_ddl TableID
# handle restart_ddl TableID

curl -d '{
    "op_type" : "OP_DELETE_DDLWORK",
    "ddlwork_info" : {
        "table_id" : '$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo "暂停二级索引任务"
curl -d '{
    "op_type" : "OP_SUSPEND_DDL_WORK",
    "index_ddl_request" : {
        "table_id" : '$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo "重启二级索引任务"
curl -d '{
    "op_type" : "OP_RESTART_DDL_WORK",
    "index_ddl_request" : {
        "table_id" : '$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"


