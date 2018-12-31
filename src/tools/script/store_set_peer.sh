#Created on 2017-12-23 
#测试场景： region_raft_control

#只有在系统崩溃的情况下可用，正常情况下不起作用
echo -e "Force set peer\n"
echo 'param: address region_id new_peer'
region_id=$2
new_peer=$1
curl -d '{
    "op_type" : "SetPeer",
    "region_id" : '$region_id',
    "new_peers" : ["'$new_peer'"],
    "force" : true
}' http://$1/StoreService/region_raft_control


