echo "二级索引"
curl -d '{
    "op_type" : "OP_DELETE_DDLWORK",
    "ddlwork_info" : {
        "table_id" : '$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo "暂停全局二级索引任务"
curl -d '{
    "op_type" : "OP_SUSPEND_DDL_WORK",
    "global_ddl_request" : {
        "table_id" : '$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo "重启全局二级索引任务"
curl -d '{
    "op_type" : "OP_RESTART_DDL_WORK",
    "global_ddl_request" : {
        "table_id" : '$2'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"


