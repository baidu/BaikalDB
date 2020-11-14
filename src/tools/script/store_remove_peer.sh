#Created on 2017-12-23 
#测试场景： region_raft_control

#只有在系统崩溃的情况下可用，正常情况下不起作用
echo -e "RemovePeer\n"
echo 'param: address'
curl -d '{
    "op_type" : "SetPeer",
    "region_id" : 2260710,
    "old_peers" : ["127.0.0.1:8219", " 127.0.0.1:8219", "127.0.0.1:8219"],
    "new_peers" : ["127.0.0.1:8219", " 127.0.0.1:8219"]
}' http://$1/StoreService/region_raft_control
echo -e "\n" 


