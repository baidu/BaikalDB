#!/bin/bash

table_id=$2

curl -d '{
          "region_ids" : [],
          "clear_all_txns" : true,
          "table_id" : '$table_id',
          "txn_timeout" : 100
}' http://$1/StoreService/query_region

echo -e '\n'
