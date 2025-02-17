
# handle instance_param resource_tag_or_address key value (is_meta, 默认false)

#插入物理机房
echo -e "update instance param\n"
curl -d '{
    "op_type": "OP_UPDATE_INSTANCE_PARAM",
    "instance_params" : [{
	"resource_tag_or_address" : "qa",
	"params" : [
	{
	    "key" : "use_token_bucket",
	    "value" : "1"	    
	},
	{
	    "key" : "get_token_weight",
	    "value" : "5"	    
	},
	{
	    "key" : "token_bucket_burst_window_ms",
	    "value" : "10"	    
	},
	{
	    "key" : "token_num_acquired_each_time",
	    "value" : "0"	    
	},
	{
	    "key" : "max_tokens_per_second",
	    "value" : "100000"	    
	},
	{
	    "key" : "sql_extended_burst_percent",
	    "value" : "80"	    
	}
	]
    }]
}' http://$1/MetaService/meta_manager
echo -e "\n"

curl -d '{
    "op_type": "QUERY_INSTANCE_PARAM",
    "instance_address": "127.0.0.1:8011"
}' http://$1/MetaService/query
