#!/bin/sh
#Created on 2017-11-22 

echo -e "打开log_index是0 删除region 和 region_id所在的table id不存在删除region开关"
curl -d '{
    "op_type": "OP_OPEN_UNSAFE_DECISION"
}' http://$1/MetaService/meta_manager
echo -e "\n"

