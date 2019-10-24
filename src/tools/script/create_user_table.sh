#!/bin/bash

echo -e "create user table\n"
curl -d '{
    "op_type": "OP_CREATE_TABLE",
    "table_info": {
        "table_name": "user",
        "database": "cluster_status",
        "namespace_name": "CLUSTER_STATUS",
        "fields": [
                    {
                        "field_name" : "user_id",
                        "mysql_type" : "UINT64",
                        "auto_increment" : true
                    },
                    {
                        "field_name" : "namespace",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "username",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "password",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "table_name",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "permission",
                        "mysql_type" : "INT32"
                    }
                  ],
        "indexs": [
                    {
                        "index_name" : "uniq_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["namespace", "username", "table_name"]
                    }
                  ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

