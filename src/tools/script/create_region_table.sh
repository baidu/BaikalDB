#!/bin/bash

echo -e "create region table\n"
curl -d '{
    "op_type": "OP_CREATE_TABLE",
    "table_info": {
        "table_name": "region",
        "database": "cluster_status",
        "namespace_name": "CLUSTER_STATUS",
        "fields": [
                    {
                        "field_name" : "table_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "region_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "parent",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "table_name",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "partition_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "create_time",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "start_key",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "end_key",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "raw_start_key",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "peers",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "leader",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "status",
                        "mysql_type" : "INT32"
                    },
                    {
                        "field_name" : "replica_num",
                        "mysql_type" : "INT32"
                    },
                    {
                        "field_name" : "version",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "conf_version",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "log_index",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "used_size",
                        "mysql_type" : "INT64"
                    }
                  ],
        "indexs": [
                    {
                        "index_name" : "primary_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["region_id"]
                    },
                    {
                        "index_name" : "key_key",
                        "index_type" : "I_KEY",
                        "field_names": ["raw_start_key"]
                     }
                  ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
