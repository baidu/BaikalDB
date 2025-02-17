#!/bin/bash


curl -d '{
          "region_id" : '$2'
}' http://$1/StoreService/get_applied_index

echo -e '\n'
