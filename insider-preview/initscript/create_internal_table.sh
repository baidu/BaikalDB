#!/bin/
curl -d '{
    "op_type":"OP_CREATE_NAMESPACE",
    "namespace_info":{
        "namespace_name": "INTERNAL",
        "quota": 1048576
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type":"OP_CREATE_DATABASE",
    "database_info": {
        "database":"baikaldb",
        "namespace_name":"INTERNAL",
        "quota": 524288
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_CREATE_TABLE",
    "table_info": {
        "table_name": "__baikaldb_instance",
        "database": "baikaldb",
        "namespace_name": "INTERNAL",
        "replica_num": 1,
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
