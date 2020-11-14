#!/bin/bash
echo -e "query region peers status\n"
echo 'param: meta address'
curl -d '{
    "op_type" : "QUERY_REGION_PEER_STATUS"
}' http://$1/MetaService/query
echo -e "\n"
