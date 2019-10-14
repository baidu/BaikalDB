#!/bin/sh
#Created on 2017-12-15
#作用：目前metaserver提供了7类查询接口，分别用来查询逻辑机房、物理机房、实例信息、权限信息#       namespace datable table信息

echo -e '\n'
echo -e "查询当前系统中某个或全部逻辑机房下有哪些物理机房"
curl -d '{
    "op_type" : "QUERY_LOGICAL",
    "logical_room" : "bj" 
}' http://$1/MetaService/query

echo -e '\n'
echo -e "查询某个或全部物理机房信息，包括所属的逻辑机房和物理机房下机器的信息"
curl -d '{
    "op_type" : "QUERY_PHYSICAL",
    "physical_room" : "bjyz"
}' http://$1/MetaService/query

echo -e '\n'
echo -e "查询某个或者全部store信息，如果查询全部，则不显示region的分布信息"
curl -d '{
    "op_type" : "QUERY_INSTANCE",
    "instance_address" : "127.0.0.1:8010"
}' http://$1/MetaService/query

echo -e '\n'
echo -e "查询某个或者全部的用户权限信息"
curl -d '{
    "op_type" : "QUERY_USERPRIVILEG",
    "user_name" : "test"
}' http://$1/MetaService/query

echo -e '\n'
echo -e "查询某个或者全部的namespace"
curl -d '{
    "op_type" : "QUERY_NAMESPACE",
    "namespace_name" : "Test"
}' http://$1/MetaService/query

echo -e '\n'
echo -e "查询某个或者全部的database"
curl -d '{
    "op_type" : "QUERY_DATABASE",
    "database" : "TEST"
}' http://$1/MetaService/query

echo -e '\n'
echo -e "查询某个或者全部的table"
curl -d '{
    "op_type" : "QUERY_SCHEMA",
    "table_name" : "ideacontent"
}' http://$1/MetaService/query

echo -e '\n'
echo -e "查询某个或者全部的region"
curl -d '{
    "op_type" : "QUERY_REGION",
    "region_id" : 1
}' http://$1/MetaService/query
