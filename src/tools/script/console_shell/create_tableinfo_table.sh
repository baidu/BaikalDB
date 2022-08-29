#!/bin/bash

echo -e "create region table\n"
curl -d '{
    "op_type": "OP_CREATE_TABLE",
    "table_info": {
        "table_name": "table_info",
        "database": "cluster_status",
        "namespace_name": "CLUSTER_STATUS",
        "fields": [
                    {
                        "field_name" : "table_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "namespace_name",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "database",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "table_name",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "resource_tag",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "status",
                        "mysql_type" : "INT32"
                    },
                    {
                        "field_name" : "version",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "create_time",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "region_count",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "row_count",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "byte_size_per_record",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "max_field_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "region_size",
                        "mysql_type" : "INT64"
                    }
                  ],
        "indexs": [
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["table_id"]
                    }
                  ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
