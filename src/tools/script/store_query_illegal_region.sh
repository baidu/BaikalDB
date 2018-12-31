#Created on 2017-12-23 
#测试场景： 不传region_id, 返回整个机房的region信息中leader是0.0.0.0的region

echo -e "query_region\n"
echo 'param: address'
curl -d '{
}' http://$1/StoreService/query_illegal_region
echo -e "\n" 


