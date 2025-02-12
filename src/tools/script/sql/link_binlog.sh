# handle link_binlog srcNamespace srcDB srcTable binlogNamespace binlogDB binlogTable

curl -d '{
    "op_type":"OP_LINK_BINLOG",
        "table_info": {
            "table_name": "'$4'",
            "database": "'$3'",
            "namespace_name": "'$2'"
        },
        "binlog_info": {
            "table_name": "'$6'",
            "database": "'$5'",
            "namespace_name": "'$2'"
        }
}' http://$1/MetaService/meta_manager
