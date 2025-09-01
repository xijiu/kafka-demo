#!/bin/bash

##################################################
# 常规检查项的巡检脚本
##################################################

currentTime=$(date +"%Y%m%d-%H%M%S")

# 日志目录
SCRIPT_DIR=$(dirname "$0")
SCRIPT_FILE_NAME=$(echo $0 | sed 's/\..*//g')
logFile=$SCRIPT_DIR/kafka_check.log


echo "SCRIPT_FILE_NAME $SCRIPT_FILE_NAME"

mkdir -p $(dirname "$logFile")

EchoNull() {
    echo "" >>$logFile
    echo ""
}

Info() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mINFO $1\033[0m" >>$logFile
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mINFO $1\033[0m"
}

# 定义匹配的天数
MATCH_DAY=1

# 生成时间关键字，比如^(2023-07-25\|2023-07-24\|2023-07-23)
DAYS=$(date -d "0 days" +%Y-%m-%d)

for ((i = 1; i <= $MATCH_DAY; i++)); do
    DAY=$(date -d "-$i days" +%Y-%m-%d)
    DAYS="$DAYS\\|$DAY"
done

EchoNull
EchoNull

Info "********************租户Kafka实例巡检开始********************"
num=$(kubectl get kafkainstance -A | grep kafka | wc -l)
Info "Total kafka instance num: $num"
if [ $num -gt 0 ]; then
    kubectl get kafkainstance -A | awk '{print $1,$2}' | grep kafka >./kafka_instance_list
    while read res; do
        EchoNull
        podOk=0
        ns=$(echo ${res} | cut -d " " -f 1)
        kafkaInstanceName=$(echo ${res} | cut -d " " -f 2)

        Info "----------Kafka实例 ${kafkaInstanceName} 巡检开始----------"

        eipEnabled=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.config.eipConfig.eipEnabled}')

        if [ "$eipEnabled" = "false" ]; then
            json_data=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.brokerVPCStatus.portsInfo}')

            # 获取 portsInfo 的长度
            ports_count=$(echo "$json_data" | jq '.brokerVPCStatus.portsInfo | length')

            # 循环每个 port
            for ((i=0; i<ports_count; i++)); do
                # 提取当前 port 的信息
                port=$(echo "$json_data" | jq ".brokerVPCStatus.portsInfo[$i]")

                # 判断是否包含 EIPId 和 EIPAddress
                has_eip_id=$(echo "$port" | jq 'has("EIPId")')
                has_eip_addr=$(echo "$port" | jq 'has("EIPAddress")')

                if [ "$has_eip_id" = "true" ] || [ "$has_eip_addr" = "true" ]; then
                    echo "  ✅ 包含 EIP：EIPId=$eip_id, EIPAddress=$eip_addr"
                fi
            done
            echo "eipEnabled is true"
            # 这里添加当值为 true 时的处理逻辑
        fi


        diskSizeStr=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.persistency.cap}')
        diskSizeGi=$(echo "$diskSizeStr" | grep -oE '[0-9]+')


        Info "----------Kafka实例 ${kafkaInstanceName} 巡检结束----------"
    done <./kafka_instance_list

fi
rm -rf ./kafka_instance_list
EchoNull
Info "********************租户Kafka实例巡检结束********************"







