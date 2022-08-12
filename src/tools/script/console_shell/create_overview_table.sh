#!/bin/bash


echo -e "create overview table\n"
curl -d '{
    "op_type": "OP_CREATE_TABLE",
    "table_info": {
        "table_name": "overview",
        "database": "cluster_status",
        "namespace_name": "CLUSTER_STATUS",
        "fields": [ {
                        "field_name" : "id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "capacity",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "normal",
                        "mysql_type" : "INT32"
                    },
                    {
                        "field_name" : "faulty",
                        "mysql_type" : "INT32"
                    },
                    {
                        "field_name" : "row_count",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "region_count",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "dead",
                        "mysql_type" : "INT32"
                    },
                    {
                        "field_name" : "used_size",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "timestamp",
                        "mysql_type" : "TIMESTAMP"
                    }
                  ],
        "indexs": [
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["id"]
                    }
                  ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"


