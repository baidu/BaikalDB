#Created on 2017-12-23 
#测试场景： region_raft_control

#只有在系统崩溃的情况下可用，正常情况下不起作用
echo -e "RemovePeer\n"
echo 'param: address'
curl -d '{
    "op_type" : "SetPeer",
    "region_id" : 1467772,
    "old_peers" : ["10.150.79.36:8210", "10.150.79.45:8210", "10.151.126.33:8210"],
    "new_peers" : ["10.150.79.36:8210", "10.150.79.45:8210"]
}' http://$1/StoreService/region_raft_control
echo -e "\n" 


