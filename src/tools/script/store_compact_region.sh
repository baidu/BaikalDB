#Created on 2017-12-23 
#测试场景： 主动调用做compaction, 不传region_id, 代表整个store做compact

echo -e "compact_region\n"
echo 'param: address'
curl -d '{
    "region_id":[200504],
    "compact_raft_log": false
}' http://$1/StoreService/compact_region
echo -e "\n" 


