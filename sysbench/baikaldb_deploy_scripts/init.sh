#!/bin/sh
echo -e "\n"
curl -d '{
    "op_type": "OP_ADD_LOGICAL",
    "logical_rooms": {
        "logical_rooms" : ["bj"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "OP_ADD_PHYSICAL",
    "physical_rooms": {
        "logical_room" : "bj",
        "physical_rooms" : ["tc"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type" : "QUERY_LOGICAL"
}' http://$1/MetaService/query
echo -e "\n"


curl -d '{
    "op_type" : "QUERY_PHYSICAL"
}' http://$1/MetaService/query
echo -e "\n"

echo -e "\n"
curl -d '{
    "op_type" : "QUERY_INSTANCE"
}' http://$1/MetaService/query
echo -e "\n"

echo -e "namespace\n"
curl -d '{
    "op_type":"OP_CREATE_NAMESPACE",
    "namespace_info":{
        "namespace_name": "FENGCHAO",
        "quota": 1048576
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type":"OP_CREATE_NAMESPACE",
    "namespace_info":{
        "namespace_name": "INTERNAL",
        "quota": 1048576
    }
}' http://$1/MetaService/meta_manager


curl -d '{
    "op_type" : "QUERY_NAMESPACE"
}' http://$1/MetaService/query
echo -e "\n"


echo -e "database\n"
curl -d '{
    "op_type":"OP_CREATE_DATABASE",
    "database_info": {
        "database":"Feed",
        "namespace_name":"FENGCHAO",
        "quota": 524288
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type":"OP_CREATE_DATABASE",
    "database_info": {
        "database":"baikaldb",
        "namespace_name":"INTERNAL",
        "quota": 524288
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"


curl -d '{
    "op_type" : "QUERY_DATABASE"
}' http://$1/MetaService/query
echo -e "\n"


curl -d '{
    "op_type":"OP_CREATE_USER",
    "user_privilege" : {
        "username" : "root",
        "password" : "root",
        "namespace_name" : "FENGCHAO",
        "privilege_database" : [{
                                    "database" : "TEST",
                                    "database_rw" : 2                            
                                }],
        "bns":["preonline", "offline"],
        "ip":["127.0.0.1", "127.0.0.2"]
    }
}' http://$1/MetaService/meta_manager
echo -e "\n"


curl -d '{
    "op_type" : "QUERY_USERPRIVILEG"
}' http://$1/MetaService/query
echo -e "\n"
