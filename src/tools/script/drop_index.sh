#!/bin/sh
#Created on 2021-09-02 

echo -e "\n"
echo -e "drop index\n"
curl -d '{
   "op_type":"OP_DROP_INDEX",
    "table_info": {
        "table_name": "t",
        "database": "TestDB",
        "namespace_name": "TEST",
        "indexs": [ {
                        "index_name" : "idx_name"
                    }
                  ]

    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
