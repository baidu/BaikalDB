#!/bin/bash
function print_with_red() {
    echo -e "\033[31m "${*}"\033[0m"
}

function print_with_green() {
    echo -e "\033[32m "${*}"\033[0m"
}

function print_with_yellow() {
    echo -e "\033[33m "${*}"\033[0m"
}

function progress_bar() {
    local progress=$1
    local bar_length=50
    local fill_char="#"
    local empty_char="-"
    
    local filled_length=$((bar_length * progress / 100))
    local empty_length=$((bar_length - filled_length))
    
    printf "\r["
    printf "%${filled_length}s" | tr ' ' "$fill_char"
    printf "%${empty_length}s" | tr ' ' "$empty_char"
    printf "] %d%%" $progress
}

function conf_init() {
    rm -rf log/*
    begin_time=$(jq -r '.begin_time' step1/done)
    end_time=$(jq -r '.end_time' step1/done)
    meta_bns=$(jq -r '.bns' step1/done)
    namespace=$(jq -r '.namespace' step1/done)
    db_tables=$(jq -r '.db_tables |  map(tostring) | join(",")' step1/done)
    signs=$(grep '"signs"' step1/done | sed 's/.*"signs" *: *\[ *\([0-9,]*\) *\].*/\1/')
    charset=$(grep "charset" conf/baikal_client.conf | awk -F" " '{print $2}')

    while true
    do
        # step1 输入参数
        print_with_green "Input cluster: [PS, PAP, BAIJIA]"
        print_with_green "default_meta: $meta_bns, default_namespace: $namespace"
        cluster="DEFAULT"
        read input_cluster
        input_cluster=$(echo $input_cluster | tr '[a-z]' '[A-Z]')
        if [ -n "$input_cluster" ]; then
            cluster="$input_cluster"
        fi

        case $cluster in
            DEFAULT)
                break
                ;;
            PS)
                meta_bns="group.opera-ps-baikalMeta-000-bj.FENGCHAO.all"
                namespace="FENGCHAO"
                baikal_client_file="conf/baikal_client.conf.ps"
                break
                ;;
            PAP)
                meta_bns="group.opera-pap-baikalMeta-000-bj.FENGCHAO.all"
                namespace="FENGCHAO"
                baikal_client_file="conf/baikal_client.conf.pap"
                break
                ;;
            BAIJIA)
                meta_bns="group.opera-online-baikalMeta-000-bj.BZ.all"
                namespace="FENGCHAO"
                baikal_client_file="conf/baikal_client.conf.baijia"
                break
                ;;
        esac
    done

    print_with_green "Input begin time, default: $begin_time"
    read input_begin_time
    if [ -n "$input_begin_time" ]; then
        begin_time="$input_begin_time"
    fi

    print_with_green "Input end time, default: $end_time"
    read input_end_time
    if [ -n "$input_end_time" ]; then
        end_time="$input_end_time"
    fi

    print_with_green "Input database.table, default: $db_tables"
    read input_db_tables
    if [ -n "$input_db_tables" ]; then
        db_tables="$input_db_tables"
    fi

    print_with_green "Input charset, default: $charset"
    read input_charset
    if [ -n "$input_charset" ]; then
        charset="$input_charset"
    fi

    print_with_green "Input signs split with ',', default: $signs"
    read input_signs
    if [ -n "$input_signs" ]; then
        signs="$input_signs"
    fi

    # 替换conf文件
    sed -i  "s/\"begin_time\"[[:space:]]*:[[:space:]]*\"[^\"]*\"/\"begin_time\" : \"$begin_time\"/" step1/done
    sed -i  "s/\"end_time\"[[:space:]]*:[[:space:]]*\"[^\"]*\"/\"end_time\" : \"$end_time\"/" step1/done
    sed -i  "s/\"bns\"[[:space:]]*:[[:space:]]*\"[^\"]*\"/\"bns\" : \"$meta_bns\"/" step1/done
    sed -i  "s/\"namespace\"[[:space:]]*:[[:space:]]*\"[^\"]*\"/\"namespace\" : \"$namespace\"/" step1/done
    sed -i  "s/\"db_tables\"[[:space:]]*:[[:space:]]*\[\"[^\"]*\"\]/\"db_tables\" : [\"$db_tables\"]/" step1/done
    sed -i  "s/\"signs\"[[:space:]]*:[[:space:]]*\[[^]]*\]/\"signs\" : [$signs]/" step1/done
    sed -i  "s/^charset:.*/charset: $charset/" conf/baikal_client.conf
    sed -i  "s/\"db_shards\"[[:space:]]*:[[:space:]]*\[[^]]*\]/\"db_shards\" : [0]/" step1/done
    sed -i  "s/\"db_shard_num\"[[:space:]]*:[[:space:]]*[0-9]*/\"db_shard_num\" : 0/" step1/done

    if [ "$cluster" != "DEFAULT" ]; then
        cp $baikal_client_file conf/baikal_client.conf
    fi
    
    if [[ "$db_tables" =~ ^FC_Word ]]; then
        sed -i  "s/\"db_shards\"[[:space:]]*:[[:space:]]*\[[^]]*\]/\"db_shards\" : [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]/" step1/done
        sed -i  "s/\"db_shard_num\"[[:space:]]*:[[:space:]]*[0-9]*/\"db_shard_num\" : 16/" step1/done
    fi
    if [[ "$db_tables" =~ ^FC_Feed ]]; then
        sed -i  "s/\"db_shards\"[[:space:]]*:[[:space:]]*\[[^]]*\]/\"db_shards\" : [0,1,2,3]/" step1/done
        sed -i  "s/\"db_shard_num\"[[:space:]]*:[[:space:]]*[0-9]*/\"db_shard_num\" : 4/" step1/done
    fi

    cat step1/done
    print_with_green "check json_file, type Y/N"
    while true
    do
        read rd_input
        [[ $rd_input == [nN] ]] && exit 0
        [[ $rd_input == [yY] ]] && break
        print_with_green "type Y/N"
    done
}

function exe_step1() {
    # step1
    rm -rf step1_output_path
    mkdir step1_output_path
    ./bin/flash_back -input_path=step1 -output_path=step1_output_path &
    task_pid=$!
    while true
    do
        if ! ps -p $task_pid > /dev/null; then
            echo "Task has finished."
            break
        fi
        sleep 5
        grep_line=$(cat step1_output_path/shard0* 2>/dev/null | wc -l)
        echo "Step1 is doing, grep line $grep_line, please wait...."
    done
    print_with_green "【重要】check flash_back step1 line number $grep_line, type Y/N"
    while true
    do
        read rd_input
        [[ $rd_input == [nN] ]] && exit 0
        [[ $rd_input == [yY] ]] && break
        print_with_green "type Y/N"
    done
}

function exe_step2() {
    # step2
    rm -rf step2_output_path
    mkdir step2_output_path
    ./bin/flash_back -input_path=step1_output_path -output_path=step2_output_path &
    task_pid=$!
    while true
    do
        if ! ps -p $task_pid > /dev/null; then
            echo "Task has finished."
            break
        fi
        sleep 5
        grep_line=$(cat step2_output_path/shard0* 2>/dev/null | wc -l)
        echo "Step2 is doing, grep line $grep_line, please wait...."
    done
    print_with_green "【重要】check flash_back step2 line number $grep_line, type Y/N"
    while true
    do
        read rd_input
        [[ $rd_input == [nN] ]] && exit 0
        [[ $rd_input == [yY] ]] && break
        print_with_green "type Y/N"
    done
}

function exe_step3() {
    # step3
    rm -rf step3_output_path
    mkdir step3_output_path
    ./bin/flash_back -input_path=step2_output_path -output_path=step3_output_path &
    task_pid=$!
    file_size_sum=0
    while true
    do
        if ! ps -p $task_pid > /dev/null; then
            echo "Task has finished."
            break
        fi
        sleep 5

        files=($(ls step2_output_path/shard0_*))
        if [ $file_size_sum == 0 ]; then
            while true
            do
                total_file_size_array=()
                for file in "${files[@]}"
                do
                    file_size=$(tac log/capture.log.wf | grep -m 1 "$file.*file_size" | grep -o "file_size: [0-9]*" | awk -F' ' '{print $2}')
                    total_file_size_array[${#total_file_size_array[*]}]=${file_size}
                done

                if [ ${#total_file_size_array[@]} -eq ${#files[@]} ]; then
                    break
                fi
            done

            for element in "${total_file_size_array[@]}"
            do
                file_size_sum=$((file_size_sum + element))
            done
        fi

        while true
        do
            cur_pos_array=()
            for file in "${files[@]}"
            do
                cur_pos=$(tac log/capture.log.wf | grep -m 1 "read_and_exec.*$file" | grep -o "pos: [0-9]*" | awk -F' ' '{print $2}')
                cur_pos_array[${#cur_pos_array[*]}]=${cur_pos}
            done

            if [ ${#cur_pos_array[@]} -eq ${#files[@]} ]; then
                break
            fi
        done
        cur_pos_sum=0
        for element in "${cur_pos_array[@]}"
        do
            cur_pos_sum=$((cur_pos_sum + element))
        done

        # 计算进度百分比
        progress=$((cur_pos_sum * 100 / file_size_sum))
        # 调用进度条函数
        progress_bar $progress
    done
    
    cat step3_output_path/result
    print_with_green "【重要】确认是否全部执行成功, 有不成功需要手动执行./bin/flash_back -input_path=step3_output_path -output_path=step4_output_path"
    while true
    do
        read rd_input
        [[ $rd_input == [nN] ]] && exit 0
        [[ $rd_input == [yY] ]] && break
        print_with_green "type Y/N"
    done
}

function flash_back() {
    print_with_green " ----------------------------------------------------------------------------------------------"
    print_with_green "| handbook: https://ku.baidu-int.com/knowledge/HFVrC7hq1Q/gv0WKdV3ZF/cNwHzaEw_E/fGqJnvxoHeWTXT |"
    print_with_green " ----------------------------------------------------------------------------------------------"
    conf_init
    exe_step1
    exe_step2
    exe_step3
}

function main() {
    flash_back
    return 0
}
main "$@"
