curl -d '{
    "namespace": "namespace1",
    "database": "database2",
    "table_name": "table3",
    "fields": [
        {
            "field_name": "userid",
            "mysql_type": "MYSQL_TYPE_UINT",
            "can_null": false,
            "field_id": 1
        },
        {
            "field_name": "username",
            "mysql_type": "MYSQL_TYPE_STRING",
            "can_null": true,
            "field_id": 2
        }
    ],
    "indexs": [
        {
            "index_name": "pk_idx",
            "index_type": 1,
            "field_ids": [1],
            "is_global": false
        }
    ],
    "table_id": 623,
    "version": 22,
    "namespace_id": 33,
    "database_id": 44
}' http://localhost:8012/StoreService/add_table

curl -d '{
    "region_id": 1,
    "table_ids": [623],
    "namespace_id": 33,
    "database_id": 44,
    "version": 22,
    "start_key": "start_key",
    "end_key": "end_key",
    "peers": [
        "10.101.25.72:8010",
        "10.101.25.72:8011",
        "10.101.25.72:8012"
    ]
}' http://localhost:8010/StoreService/add_region


curl -d '{
    "region_id": 2,
    "table_id": 623,
    "value": [
        {
            "data_type":"MYSQL_TYPE_UINT",
            "uint32_value": 630158
        },
        {
            "data_type":"MYSQL_TYPE_STRING",
            "string_value": "630158_user"
        }
    ]
}' http://localhost:8011/StoreService/insert_request

curl -d '{
    "region_id": 2,
    "table_id": 623,
    "value": [
        {
            "data_type":"MYSQL_TYPE_UINT",
            "uint32_value": 630158
        },
        {
            "data_type":"MYSQL_TYPE_STRING",
            "is_null": true
        }
    ]
}' http://localhost:8010/StoreService/select_request

curl -d '{
    "region_id": 2,
    "table_id": 623,
    "value": [
        {
            "data_type":"MYSQL_TYPE_UINT",
            "uint32_value": 630158
        },
        {
            "data_type":"MYSQL_TYPE_STRING",
            "is_null": true
        }
    ]
}' http://localhost:8010/StoreService/delete_request


curl -d '{
    "op_type": "GetLeader",
    "region_id": 1
}' http://localhost:8010/StoreService/region_raft_control


curl -d '{
    "op_type": "TransLeader",
    "region_id": 1,
    "new_leader": "10.101.25.72:8012"
}' http://localhost:8010/StoreService/region_raft_control

curl -d '{
    "op_type": "SnapShot",
    "region_id": 1
}' http://localhost:8010/StoreService/region_raft_control


curl -d '{
    "op_type": "ShutDown",
    "region_id": 1
}' http://localhost:8010/StoreService/region_raft_control
