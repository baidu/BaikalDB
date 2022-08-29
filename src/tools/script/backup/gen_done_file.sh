#!/bin/bash
#if [ $# -ne 1 ];then
#    echo param1: stream_name
#    exit 1
#fi
#mysql -h10.94.160.61 -uroot -pfast -P8121 -Dfengmai -e "select * from baikaldb_level" > out
#mysql -hsmartbns.fcdb7113-bmi0000.xdb.all.serv -ubaikaldb_read -p3dJ_1xvrVG2y -P5616 -DFC_Fengmai
WATT_PARAM_HOST="smartbns.fcdb7113-bmi0000.xdb.all.serv"
WATT_PARAM_PORT="5616"
WATT_PARAM_USER="baikaldb_read"
WATT_PARAM_PASSWORD="3dJ_1xvrVG2y"
WATT_PARAM_DB="FC_Fengmai"
WATT_PARAM_TABLE="baikaldb_level"
WATT_BASE_PATH="/home/work/afs_cluster/mnt"

while getopts ":s:d:t:" opt
do
    case $opt in
        s)
        echo "stream:$OPTARG"
        OPT_STREAM_NAME=$OPTARG
        ;;
        d)
        echo "database:$OPTARG"
        OPT_DATABASE=$OPTARG
        ;;
        t)
        echo "table:$OPTARG"
        OPT_TABLE=$OPTARG
        ;;
        ?)
        echo "unkonwn param"
        exit 1;;
    esac
done

PROJECT_PATH=$(cd `dirname $0`; pwd)
echo $PROJECT_PATH
DATE=$(date +%Y%m%d%H%m%S)
echo $DATE
if [ X$OPT_STREAM_NAME != X ];then
    IMPORT_DIR=${OPT_STREAM_NAME}"_"$DATE
elif [ X$OPT_DATABASE != X ] && [ X$OPT_TABLE != X ];then
    IMPORT_DIR=${OPT_DATABASE}"_"${OPT_TABLE}"_"$DATE
else
    echo "invalid param stream:$OPT_STREAM_NAME database:$OPT_DATABASE table:$OPT_TABLE"
    exit 1
fi

WORK_DIR=$PROJECT_PATH/$IMPORT_DIR
DATA_DIR=$PROJECT_PATH/data/$IMPORT_DIR
echo $WORK_DIR
if [ ! -d $WORK_DIR ];then
    mkdir -p $WORK_DIR
    mkdir -p $DATA_DIR
else
    echo "maybe done, date:"$DATE
    exit 1
fi

function fail_quit() {
    cd $PROJECT_PATH  
    rm -r $WORK_DIR
    exit 1
}

function get_column_single() {
    local column_name=$1
    local idx=$(head -1 $MYSQL_OUT_FILE | awk -F "\t" -v name="${column_name}" '{for(i=0;i<NF;i++){if($i==name){print i}}}')     
    echo $(cat $MYSQL_OUT_FILE | sed -n '2,2p' | awk -F "\t" -v i="${idx}" '{print $i}')   
}

function get_column_multi() {
    local column_name=$1
    local idx=$(head -1 $MYSQL_OUT_FILE | awk -F "\t" -v name="${column_name}" '{for(i=0;i<NF;i++){if($i==name){print i}}}')     
    echo $(cat $MYSQL_OUT_FILE | sed '1d' | awk -F "\t" -v i="${idx}" '{print $i}')   
}

function check_base() {
    local shard_num=$1
    local group_num=$2
    local stream_path=$3
    for ((i=0;i<$shard_num;i++))
    {
        if [ ! -d $stream_path/$i ];then
            echo "$stream_path/$i is not dir"
            return 1
        fi
        if [ ! -d $stream_path/$i/merge ];then
            echo "$stream_path/$i/merge is not dir"
            return 1
        fi
        local bid_count=$(ls $stream_path/$i | grep "bid.info.n" | grep -v "md5" | wc -l)
        if [ $bid_count -ne $group_num ];then
            echo "path:$stream_path/$i bid_count:$bid_count vs group_num:$group_num base invalid"
            return 1
        fi
    }
    return 0
}

#############################################################
cd $WORK_DIR
MYSQL_OUT_FILE="mysql_out_file"
if [ X$OPT_STREAM_NAME != X ];then
    mysql -h$WATT_PARAM_HOST -P$WATT_PARAM_PORT -u$WATT_PARAM_USER -p$WATT_PARAM_PASSWORD -D$WATT_PARAM_DB -e "select * from $WATT_PARAM_TABLE where stream in ('$OPT_STREAM_NAME')" > mysql_out_tmp
    if [ $? -ne 0 ];then
        echo "mysql exec failed"
        fail_quit
    fi
elif [ X$OPT_DATABASE != X ] && [ X$OPT_TABLE != X ];then
    mysql -h$WATT_PARAM_HOST -P$WATT_PARAM_PORT -u$WATT_PARAM_USER -p$WATT_PARAM_PASSWORD -D$WATT_PARAM_DB -e "select * from $WATT_PARAM_TABLE where baikaldb_dbname in ('$OPT_DATABASE') and baikaldb_tablename in ('$OPT_TABLE')" > mysql_out_tmp
    if [ $? -ne 0 ];then
        echo "mysql exec failed"
        fail_quit
    fi
else
    echo "invalid param stream:$OPT_STREAM_NAME database:$OPT_DATABASE table:$OPT_TABLE"
    fail_quit
fi
cat mysql_out_tmp | grep -v baidu_dba > $MYSQL_OUT_FILE
rm -f mysql_out_tmp
if [ $(cat $MYSQL_OUT_FILE | wc -l) -le 1 ];then
    echo "no data in mysql, stream:$OPT_STREAM_NAME"
    fail_quit
fi
PRODUCT=$(get_column_single "product")
STREAM=$(get_column_single "stream")
PROFILE=$(get_column_single "profile")
DATABASES=($(get_column_multi "baikaldb_dbname"))
TABLES=($(get_column_multi "baikaldb_tablename"))
LEVEL_VALUES=($(get_column_multi "level"))
FIELD_JSON=($(get_column_multi "field_json"))
SHARD_NUM=$(get_column_single "shard_num")
GROUP_NUM=$(get_column_single "group_num")
CHARSET=$(get_column_single "charset")
if [ X$PRODUCT = X ] || [ X$STREAM = X ] || [ X$PROFILE = X ] || [ X$SHARD_NUM = X ] || [ X$GROUP_NUM = X ] || [ X$DATABASES = X ] || [ X$TABLES = X ] || [ X$LEVEL_VALUES = X ];then
    echo product:$PRODUCT
    echo stream:$STREAM
    echo profile:$PROFILE
    echo shard_num:$SHARD_NUM
    echo group_num:$GROUP_NUM
    echo datebases:$DATABASES
    echo tables:$TABLES
    echo level_values:$LEVEL_VALUES
    fail_quit
fi

STREAM_PATH=$WATT_BASE_PATH/$PRODUCT-$STREAM-$PROFILE
echo stream_path:$STREAM_PATH
if [ ! -d $STREAM_PATH ];then
    echo stream_path:$STREAM_PATH is not dir
    fail_quit
fi
check_base $SHARD_NUM $GROUP_NUM $STREAM_PATH
if [ $? -ne 0 ];then
    echo "check base failed"
    fail_quit
fi

for ((shard_dir=0;shard_dir<$SHARD_NUM; shard_dir++))
{
    ln -s $STREAM_PATH/$shard_dir/merge $DATA_DIR/$shard_dir
}

done_json="{\"path\": \"./$IMPORT_DIR\", \"charset\":\"$CHARSET\", \"update\": ["
table_size=${#TABLES[@]}
echo table_size:$table_size
for ((table_idx=0;table_idx<${table_size};table_idx++))
{
    table_json="{\"filter_field\": \"level\",\"filter_value\": \"${LEVEL_VALUES[$table_idx]}\",\"fields\": ${FIELD_JSON[$table_idx]},\"db\": \"${DATABASES[$table_idx]}\", \"table\": \"${TABLES[$table_idx]}\"}"
    echo table_idx:$table_idx table_json:$table_json
    table_index=$[${table_size}-1]
    if [ $table_idx -lt ${table_index} ];then
        done_json=$done_json$table_json,
    else
        done_json=$done_json$table_json
    fi
}

done_json=$done_json"]}"

cd $PROJECT_PATH

echo $done_json | python -mjson.tool > ./$IMPORT_DIR.done
