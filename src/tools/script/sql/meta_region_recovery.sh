#!/bin/bash

# handle restore region recoveryType resourceTag

echo -e '\n'
echo -e "bad region recovery"
curl -d '{
    "op_type" : "OP_RECOVERY_ALL_REGION",
    "resource_tags" : ["qa","qadisk"]
}' http://$1/MetaService/meta_manager |tee result_`date +%Y_%m_%d_%k_%M_%S`
echo -e '\n'
