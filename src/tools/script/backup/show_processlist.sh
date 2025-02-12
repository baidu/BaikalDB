#!/bin/sh
#you can add 'bns user password' to this array
bns_user_password_array=(
#             bns                                user        password
    'group.opera-ps-baikaldb-000-bj.FENGCHAO.all baikal_user 59ACp2Bl#'
    'group.opera-ps-baikaldb-000-ct.FENGCHAO.all baikal_user 59ACp2Bl#'
    'group.opera-e0-baikaldb-000-yz.FENGCHAO.all root        root'
)
#print sql info to this file
OUTPUTFILENAME="sql_proccesslist"
TIME=60

#############################################################################
array_size=${#bns_user_password_array[@]}
#echo "array_size:$array_size"
for array in "${bns_user_password_array[@]}"
{
    array_line=($array)
    #echo "array_line:${array_line[@]}"
    #echo "bns:${array_line[0]};user:${array_line[1]};password:${array_line[2]}"
    TMP_IPS=($(get_instance_by_service -a ${array_line[0]} | awk -F ' ' '{print $2}'))
    if [ ${#TMP_IPS[@]} -eq 0 ];then
        echo "get instance by service failed, bns_name:${array_line[0]}"
        exit 1
    fi
    IPS=(${IPS[@]} ${TMP_IPS[@]})
    PORTS=(${PORTS[@]} $(get_instance_by_service -a ${array_line[0]} | awk -F ' ' '{print $4}'))
    STATUS=(${STATUS[@]} $(get_instance_by_service -a ${array_line[0]} | awk -F ' ' '{print $5}'))
    HOSTNAMES=(${HOSTNAMES[@]} $(get_instance_by_service -a ${array_line[0]} | awk -F ' ' '{print $9}'))
    ip_size=${#IPS[@]}
    for ((i=0;i<ip_size;i++))
    {
        USERS=(${USERS[@]} ${array_line[1]})
        PASSWORDS=(${PASSWORDS[@]} ${array_line[2]})
    }
}
#echo "IPS:${IPS[@]}"
#echo "PORTS:${PORTS[@]}"
#echo "USERS:${USERS[@]}"
#echo "PASSWORDS:${PASSWORDS[@]}"
#echo "STATUS:${STATUS[@]}"
#echo "HOSTNAMES:${HOSTNAMES[@]}"

index=0
ip_size=${#IPS[@]}
#echo "ip_size:$ip_size"
for ip in ${IPS[@]}
do
    echo "${HOSTNAMES[$index]} doing..."
    echo "##########"${HOSTNAMES[$index]} "BEGIN ##########" >> $OUTPUTFILENAME
    if [ ${STATUS[$index]} -ne 0 ];then
        echo "${HOSTNAMES[$index]} is faulty"
        continue
    fi
    mysql -h$ip -P${PORTS[$index]} -u${USERS[$index]} -p${PASSWORDS[$index]} -e "show processlist" > ${HOSTNAMES[$index]}.tmpfile1
    if [ $? -ne 0 ];then
        echo "mysql exec failed, ip:$ip, port:${PORTS[$index]}, user:${USERS[$index]}, password:${PASSWORDS[$index]}"
        exit 1
    fi
    grep -v Sleep ${HOSTNAMES[$index]}.tmpfile1 > ${HOSTNAMES[$index]}.tmpfile2
    while read line
    do
        sql=`echo $line | awk -F ' ' '{print $8}'`
        if [ X$sql = X ];then
            echo "line:$line has not sql info"
            continue
        elif [ "$sql" = "NULL" ];then
            continue
        fi

        time_s=`echo $line | awk -F ' ' '{print $6}'`
        if [ X$time_s = X ];then
            echo "line:$line has not Time info"
            continue
        elif [ $time_s = "Time" ];then
            echo $line >> $OUTPUTFILENAME
        elif [ $time_s -ge $TIME ];then
            echo $line >> $OUTPUTFILENAME
        fi
    done < ${HOSTNAMES[$index]}.tmpfile2
    echo "##########"${HOSTNAMES[$index]} "END ##########" >> $OUTPUTFILENAME
    rm -rf ${HOSTNAMES[$index]}.tmpfile*
    let index++
done
echo "print sql info to $OUTPUTFILENAME"
exit 0


