#!/bin/sh
#Created on 2020-09-18
#测试场景：完成的meta_server流程

echo -e "\n"
echo -e "修改索引状态\n"
echo -e "IHS_NORMAL : 普通索引\n"
echo -e "IHS_DISABLE : 屏蔽索引\n"
curl -d '{
    "op_type":"OP_SET_INDEX_HINT_STATUS",
    "table_info": {
        "table_name": "TEST",
        "database": "TEST",
        "namespace_name": "TEST",
        "indexs": [ {
                "index_name" : "TEST",
                "hint_status" : "IHS_DISABLE"
            }
        ]

    }
}' http://$1/MetaService/meta_manager
echo -e "\n"