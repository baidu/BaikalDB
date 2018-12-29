curl -d '{
    "op_type" : "TransLeader",
    "region_id" : 1,
    "new_leader" : "10.143.17.41:8110"
}' http://10.143.17.41:8130/StoreService/region_raft_control
