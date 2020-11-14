#!/bin/bash
  

echo -e "query namespace\n"
curl -d '{
    "op_type" : "QUERY_NAMESPACE",
     "namespace_name" : "CLUSTER_STATUS"
}' http://$1/MetaService/query
echo -e "\n"

echo -e "query database\n"
curl -d '{
    "op_type" : "QUERY_DATABASE",
    "database" : "cluster_status",
    "namespace_name":"CLUSTER_STATUS"
}' http://$1/MetaService/query
echo -e "\n"

echo -e "query user\n"
curl -d '{
    "op_type" : "QUERY_USERPRIVILEG",
    "user_name" : "test"
}' http://$1/MetaService/query
echo -e "\n"

echo -e 'query table\n'
curl -d '{
    "op_type" : "QUERY_SCHEMA",
    "namespace_name" : "CLUSTER_STATUS"
}' http://$1/MetaService/query
echo -e "\n"

echo -e 'query instance\n'
curl -d '{
    "op_type" : "QUERY_INSTANCE",
    "namespace_name" : "CLUSTER_STATUS"
}' http://$1/MetaService/query
echo -e "\n"
