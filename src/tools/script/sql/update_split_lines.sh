#!/bin/sh
# 更新split_lines
# 使用方法: sh update_split_lines.sh 10.1.1.1:8111 NAMESPACE DATABASE TABLE 1000000

# handle split_lines Table SplitLines

echo -e "\n"
#创建table
curl -d '{
    "op_type":"OP_UPDATE_SPLIT_LINES",
    "table_info": {
        "table_name": "'$4'",
        "database": "'$3'",
        "namespace_name": "'$2'",
        "region_split_lines": '$5'
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

