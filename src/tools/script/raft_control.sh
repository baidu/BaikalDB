#Created on 2017-12-23 

echo -e "RemovePeer\n"
curl -d '{
    "op_type" : "SetPeer",
    "region_id" : 0,
    "old_peers" : ["127.0.0.1:8211", "127.0.0.1:8110"],
    "new_peers" : ["127.0.0.1:8211", "127.0.0.1:8110", "127.0.0.1:8110"]
}' http://127.0.0.1:8110/MetaService/raft_control
echo -e "\n" 

echo -e "AddPeer\n"
curl -d '{
    "op_type" : "SetPeer",
    "region_id" : 0,
    "old_peers" : ["127.0.0.1:8010", "127.0.0.1:8011"],
    "new_peers" : ["127.0.0.1:8010", "127.0.0.1:8011", "127.0.0.1:8012"]
}' http://127.0.0.1:8010/MetaService/raft_control
echo -e "\n" 


#只有在系统奔溃的情况下可用，正常情况下不起作用
echo -e "Force set peer\n"
curl -d '{
    "op_type" : "SetPeer",
    "region_id" : 0,
    "new_peers" : ["127.0.0.1:8110"],
    "force" : true
}' http://127.0.0.1:8110/MetaService/raft_control

echo -e  "transferLeader,只能发送到leader机器上\n"
curl -d '{
    "op_type" : "TransLeader",
    "region_id" : 0,
    "new_leader" : "127.0.0.1:8011"
}' http://127.0.0.1:8012/MetaService/raft_control
echo -e "\n" 

echo -e "触发做snapshot, leader and follower都可以做\n"
curl -d '{
    "op_type" : "SnapShot",
    "region_id" : 0
}' http://127.0.0.1:8010/MetaService/raft_control
echo -e "\n" 

echo -e "GetLeader, leader and follower都可以做\n"
curl -d '{
    "op_type" : "GetLeader",
    "region_id" : 0
}' http://127.0.0.1:8010/MetaService/raft_control
echo -e "\n" 

echo -e "ShutDwon\n"
curl -d '{
    "op_type" : "ShutDown",
    "region_id" :0
}' http://127.0.0.1:8010/MetaService/raft_control
echo -e "\n" 

echo -e "Vote(ms)\n"
curl -d '{
    "op_type" : "Vote",
    "region_id" : 0,
    "election_time" : 10
}' http://127.0.0.1:8012/MetaService/raft_control
echo -e "\n" 

echo -e "ResetVoteTime(ms)\n"
curl -d '{
    "op_type" : "ResetVoteTime",
    "region_id" : 0,
    "election_time" : 1000
}' http://127.0.0.1:8012/MetaService/raft_control
echo -e "\n" 

