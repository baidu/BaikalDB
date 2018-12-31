#Created on 2017-12-23 
#测试场景： 主动调用做snapshot, 不传region_id, 代表整个store做snapshot

echo -e "snapshot_region\n"
echo 'param: address'
curl -d '{
}' http://$1/StoreService/snapshot_region
echo -e "\n" 


