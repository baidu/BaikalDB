#!/bin/bash

basename=$2
table_name=$3
index_name=$4

curl -d '{
          "op_type" : "OP_DROP_INDEX",
          "table_info" : {
                  "database" : "'$databasename'",
                  "table_name" : "'$table_name'",
                  "namespace_name" : "FENGCHAO",
                  "indexs" : [
                             {"index_name" : "'$index_name'"}
                  ]
          }
}' http://$1/MetaService/meta_manager

echo -e '\n'
