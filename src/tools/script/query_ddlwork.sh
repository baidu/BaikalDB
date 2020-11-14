curl -d '{
    "op_type" : "QUERY_DDLWORK",
    "table_id" : '$2'
}' http://$1/MetaService/query
echo -e "\n"
