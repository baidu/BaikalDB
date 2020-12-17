#!/bin/sh
#Created on 2017-11-22 
#测试场景：完成的meta_server流程

echo -e "\n"
#创建table
curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 89,
        "start_id": 100
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 149,
        "start_id": 192198
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 80,
        "start_id": 298985
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 154,
        "start_id": 225931
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 143,
        "start_id": 68059
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 99,
        "start_id": 100
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 76,
        "start_id": 100
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 87,
        "start_id": 27768
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 70,
        "start_id": 100
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 93,
        "start_id": 100
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 91,
        "start_id": 26713
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 137,
        "start_id": 178587 
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 161,
        "start_id": 225925
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_ID_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 84,
        "start_id": 33596956
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

#更新自增ID
curl -d '{
    "op_type": "OP_UPDATE_FOR_AUTO_INCREMENT",
    "auto_increment": {
        "table_id": 1,
        "force": true,
        "start_id": 100
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"
