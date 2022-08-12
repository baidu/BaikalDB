#!/bin/bash

echo -e "create instance table\n"
curl -d '{
    "op_type": "OP_CREATE_TABLE",
    "table_info": {
        "table_name": "instance",
        "database": "cluster_status",
        "namespace_name": "CLUSTER_STATUS",
        "fields": [
                    {
                        "field_name" : "resource_tag",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "address",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "logical_room",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "physical_room",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "status",
                        "mysql_type" : "INT32"
                    },
                    {
                        "field_name" : "used_size",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "region_count",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "peer_count",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "leader_count",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "capacity",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "version",
                        "mysql_type" : "STRING"
                    }
                  ],
        "indexs": [
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["address"]
                    }
                  ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

