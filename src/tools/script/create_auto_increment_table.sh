#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
echo -e "创建table\n"
curl -d '{
    "op_type":128,
    "table_info": {
        "table_name": "TestTB",
        "database": "TestDB",
        "namespace_name": "TEST_NAMESPACE",
        "fields": [ 
                    {
                        "field_name" : "eventday",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "timeseries",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "eventid",
                        "mysql_type" : 6,
                        "auto_increment" : true
                    },
                    {
                        "field_name" : "ideaid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "unitid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "planid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "userid",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "ideaname",
                        "mysql_type" : "STRING"
                    }
                    ],
        "indexs": [ 
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["eventday", "ideaid", "timeseries", "eventid"]
                    },
                    {
                        "index_name" : "userid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "userid"]
                    },
                    {
                        "index_name" : "planid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "planid"]
                    },
                    {
                        "index_name" : "unitid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "unitid"]
                    }
                    ]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo -e "schema set auto_increment start value\n"
curl -d '{
    "op_type":128,
    "table_info": {
        "table_name": "TestTB",
        "database": "TestDB",
        "namespace_name": "TEST_NAMESPACE",
        "fields": [ 
                    {
                        "field_name" : "eventday",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "timeseries",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "eventid",
                        "mysql_type" : 6,
                        "auto_increment" : true
                    },
                    {
                        "field_name" : "ideaid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "unitid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "planid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "userid",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "ideaname",
                        "mysql_type" : "STRING"
                    }
                    ],
        "indexs": [ 
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["eventday", "ideaid", "timeseries", "eventid"]
                    },
                    {
                        "index_name" : "userid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "userid"]
                    },
                    {
                        "index_name" : "planid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "planid"]
                    },
                    {
                        "index_name" : "unitid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "unitid"]
                    }
                    ],
        "auto_increment_increment" : 10000
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo -e "one table only has one auto increment field\n"
curl -d '{
    "op_type":128,
    "table_info": {
        "table_name": "TestTB",
        "database": "TestDB",
        "namespace_name": "TEST_NAMESPACE",
        "fields": [ 
                    {
                        "field_name" : "eventday",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "timeseries",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "eventid",
                        "mysql_type" : 6,
                        "auto_increment" : true
                    },
                    {
                        "field_name" : "ideaid",
                        "mysql_type" : 6,
                        "auto_increment" : true
                    },
                    {
                        "field_name" : "unitid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "planid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "userid",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "ideaname",
                        "mysql_type" : "STRING"
                    }
                    ],
        "indexs": [ 
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["eventday", "ideaid", "timeseries", "eventid"]
                    },
                    {
                        "index_name" : "userid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "userid"]
                    },
                    {
                        "index_name" : "planid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "planid"]
                    },
                    {
                        "index_name" : "unitid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "unitid"]
                    }
                    ],
        "auto_increment_increment" : 10000
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo -e "auto increment field can not be null\n"
curl -d '{
    "op_type":128,
    "table_info": {
        "table_name": "TestTB",
        "database": "TestDB",
        "namespace_name": "TEST_NAMESPACE",
        "fields": [ 
                    {
                        "field_name" : "eventday",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "timeseries",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "eventid",
                        "mysql_type" : 6,
                        "auto_increment" : true,
                        "can_null" : true
                    },
                    {
                        "field_name" : "ideaid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "unitid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "planid",
                        "mysql_type" : 6
                    },
                    {
                        "field_name" : "userid",
                        "mysql_type" : "INT64"
                    },
                    {
                        "field_name" : "ideaname",
                        "mysql_type" : "STRING"
                    }
                    ],
        "indexs": [ 
                    {
                        "index_name" : "priamry_key",
                        "index_type" : "I_PRIMARY",
                        "field_names": ["eventday", "ideaid", "timeseries", "eventid"]
                    },
                    {
                        "index_name" : "userid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "userid"]
                    },
                    {
                        "index_name" : "planid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "planid"]
                    },
                    {
                        "index_name" : "unitid_key",
                        "index_type" : "I_KEY",
                        "field_names": ["eventday", "unitid"]
                    }
                    ],
        "auto_increment_increment" : 10000
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

echo -e "gen_id"
curl -d '{
    "op_type": "OP_GEN_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 1,
        "count": 10,
        "start_id" : 20011
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
