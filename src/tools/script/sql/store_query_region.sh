#Created on 2017-12-23 
#测试场景： 不传region_id, 返回整个机房的region信息

# show store region regionID

echo -e "query_region\n"
echo 'param: address'
curl -d '{
    "region_ids":[9525031,9526477]
}' http://$1/StoreService/query_region
echo -e "\n" 


