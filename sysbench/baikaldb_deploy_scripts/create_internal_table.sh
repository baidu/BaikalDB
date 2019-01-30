#!/bin/sh
curl -d '{
    "op_type": "OP_CREATE_TABLE",
    "table_info": {
        "table_name": "__baikaldb_instance",
        "database": "baikaldb",
        "namespace_name": "INTERNAL",
        "fields": [ 
                    {
                        "field_name" : "instance_id",
                        "mysql_type" : "UINT64",
                        "auto_increment" : true
                    }
                    ],
        "indexs": [ 
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["instance_id"]
                    }
                    ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
