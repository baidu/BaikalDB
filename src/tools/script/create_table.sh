#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "创建table\n"
curl -d '{
    "op_type":"OP_CREATE_TABLE",
    "table_info": {
        "table_name": "ideacontent",
        "database": "TestDB",
        "namespace_name": "TEST_NAMESPACE",
        "fields": [ {
                        "field_name" : "account_id",
                        "mysql_type" : "UINT32"
                    },
                    {
                        "field_name" : "campaign_id",
                        "mysql_type" : "UINT32"
                    },
                    {
                        "field_name" : "adgroup_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "creative_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "magic_id",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "new_creative_title",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "new_creative_desc",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "new_creative_desc2",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "new_creative_url",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "new_creative_showurl",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "new_creative_miurl",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "new_creative_mshowurl",
                        "mysql_type" : "STRING"
                    },
                    {
                        "field_name" : "pc_version",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "m_version",
                        "mysql_type" : "INT64"
                    }
                    ],
        "indexs": [ {
                        "index_name" : "primary_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["account_id", "campaign_id", "adgroup_id", "creative_id"]
                    },
                    {
                        "index_name" : "creative_id_key",
                        "index_type" : "I_UNIQ",
                        "field_names" : ["creative_id"]
                    },
                    {
                        "index_name" : "magic_id_key",
                        "index_type" : "I_KEY",
                        "field_names": ["magic_id"]
                    },
                    {
                        "index_name" : "magic_creative_union_key",
                        "index_type" : "I_UNIQ",
                        "field_names": ["magic_id", "creative_id"]
                    },
                    {
                        "index_name" : "title_key",
                        "index_type" : "I_FULLTEXT",
                        "field_names": ["new_creative_title"]
                    },
                    {
                        "index_name" : "new_creative_desc",
                        "index_type" : "I_FULLTEXT",
                        "field_names": ["new_creative_desc"]
                    },
                    {
                        "index_name" : "new_creative_desc2",
                        "index_type" : "I_FULLTEXT",
                        "field_names": ["new_creative_desc2"]
                    },
                    {
                        "index_name" : "new_creative_url",
                        "index_type" : "I_FULLTEXT",
                        "field_names": ["new_creative_url"]
                    },
                    {
                        "index_name" : "new_creative_showurl",
                        "index_type" : "I_FULLTEXT",
                        "field_names": ["new_creative_showurl"]
                    },
                    {
                        "index_name" : "new_creative_miurl",
                        "index_type" : "I_FULLTEXT",
                        "field_names": ["new_creative_miurl"]
                    },
                    {
                        "index_name" : "new_creative_mshowurl",
                        "index_type" : "I_FULLTEXT",
                        "field_names": ["new_creative_mshowurl"]
                    }
                    ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#查询table
curl -d '{
    "op_type" : "QUERY_SCHEMA" 
}' http://$1/MetaService/query
