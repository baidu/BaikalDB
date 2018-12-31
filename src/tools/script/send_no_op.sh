#Created on 2017-12-23 
#测试场景： region_raft_control

echo -e "send no_op\n"
echo 'param: address region_id region_version'
region_id=$2
region_version=$3
curl -d '{
    "op_type" : "OP_NONE",
    "region_id" : '$region_id',
    "region_version" : '$region_version'
}' http://$1/StoreService/query


