#Created on 2017-12-23 
#测试场景： 主动调用做snapshot, 不传region_id, 代表整个store做snapshot

echo -e "snapshot_region\n"
echo 'param: address'
curl -d '{
    "region_ids":[585]
}' http://$1/StoreService/manual_split_region
echo -e "\n" 


