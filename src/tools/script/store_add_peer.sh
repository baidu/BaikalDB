#Created on 2017-12-23 
#测试场景： region_raft_control

echo -e "sender to leader to add_peer\n, 1 init region 2 add_peer"
echo 'param: address region_id'
region_id=$2
curl -d '{
    "region_id" : '$region_id',
    "old_peers" : ["10.146.55.17:8222", "10.150.64.18:8222", "10.151.93.34:8222"],
    "new_peers" : ["10.146.55.17:8222", "10.150.64.18:8222", "10.151.93.34:8222", "10.150.53.18:8222"]
}' http://$1/StoreService/add_peer


