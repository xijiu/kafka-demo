#!/bin/bash

##################################################
# 常规检查项的巡检脚本
##################################################

currentTime=$(date +"%Y%m%d-%H%M%S")

# 日志目录
SCRIPT_DIR=$(dirname "$0")
SCRIPT_FILE_NAME=$(echo $0 | sed 's/\..*//g')
#logFile=$SCRIPT_DIR/kafka_check_$currentTime.log
logFile=$SCRIPT_DIR/kafka_check.log

admin_config="/opt/bitnami/kafka/config/admin.properties"

echo "logFile $logFile"
echo "SCRIPT_FILE_NAME $SCRIPT_FILE_NAME"

mkdir -p $(dirname "$logFile")

EchoNull() {
    echo "" >>$logFile
    echo ""
}

Error() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[31mERROR $1 \033[0m" >>$logFile
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[31mERROR $1 \033[0m"
}

Warn() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[33mWARN $1 \033[0m" >>$logFile
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[33mWARN $1 \033[0m"
}

Info() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mINFO $1\033[0m" >>$logFile
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mINFO $1\033[0m"
}

Debug() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mDEBUG $1\033[0m" >>$logFile
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mDEBUG $1\033[0m"
}

topicSizeGB=""

function genericAdminConfigIfSASLEnable() {
    ns=$1
    kafkaInstanceName=$2

    saslEnabled=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.config.saslEnabled}')
    Info "Kafka instance: ${ns} ${kafkaInstanceName} SASL enable:$saslEnabled. "
    if [ x"$saslEnabled" != x"true" ]; then
        return
    fi

    usernameTmp=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.services[?(@.name == "brokerService")].env[?(@.name == "KAFKA_CLIENT_USERS")].value}')
    passwordTmp=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.services[?(@.name == "brokerService")].env[?(@.name == "KAFKA_CLIENT_PASSWORDS")].value}')

    kubectl -n ${ns} exec $kafkaInstanceName-0-0 -c kafka -- /bin/sh -c "cat <<EOF > $admin_config
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$usernameTmp\" password=\"$passwordTmp\";
EOF"

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

        saslEnabled=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.config.saslEnabled}')
        if [ x"$saslEnabled" == x"true" ]; then
            genericAdminConfigIfSASLEnable $ns $kafkaInstanceName
        fi

        runningStatus=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.status.status}')
        if [ x"$runningStatus" != x"Running" ]; then
            Error "Kafka instance: ${ns} ${kafkaInstanceName} status:$runningStatus. 请检查该实例，若是创建中的实例请忽略"
            Info "----------Kafka实例 ${kafkaInstanceName} 巡检结束----------"
            continue
        fi
        crDeleteTimestap=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.metadata.deletionTimestamp}')
        if [ x"$crDeleteTimestap" != x"" ]; then
            Warn "Kafka instance: ${ns} ${kafkaInstanceName} is deleting. "
            Info "----------Kafka实例 ${kafkaInstanceName} 巡检结束----------"
            continue
        fi

        unHealthPodCount=$(sudo kubectl get po --no-headers -n ${ns} | grep ${kafkaInstanceName} | grep -Ev "(Running|Completed|Terminating|Evicted)" -c)
        if [ "$unHealthPodCount" -gt 0 ]; then
            podOk=1
            Error "Kafka instance: ${ns}  ${kafkaInstanceName} pod状态检测:异常，请检查pod情况"
            sudo kubectl get po --no-headers -n ${ns} | grep ${kafkaInstanceName} | grep -Ev "(Running|Completed|Terminating|Evicted)"
            Info "----------Kafka实例 ${kafkaInstanceName} 巡检结束----------"
            continue
        fi

        agentIp=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.agentVPCStatus.portsInfo[0].ipv4Address}')
        if [ x"$agentIp" == x ]; then
            podOk=2
            Error "Kafka instance: ${ns} ${kafkaInstanceName} agentIp is null. 请检查实例状态"
            Info "----------Kafka实例 ${kafkaInstanceName} 巡检结束----------"
            continue
        fi

        curlPodName=$(kubectl -n ${ns} get po | grep ${kafkaInstanceName} | grep Running | grep -v doko | awk '{print $1}' | head -n 1)

        topicResponse=$(kubectl -n ${ns} exec -q $curlPodName -- /bin/bash -c "curl -s  http://${agentIp}:8081/agent/v1.0/kafka/topic/list?pageIndex=1\&pageSize=30000")

        topicCount=$(echo "$topicResponse" | grep -o '"totalCount": *[0-9]*' | sed 's/"totalCount": *//')

        Info "Topic总数：$topicCount"

        topicInfos=()
        while IFS= read -r line; do
            topicInfos+=("$line")
        done < <(echo "$topicResponse" | grep -o '{[^}]*"name":[^}]*"partitionNum":[^}]*"replicasNum":[^}]*}')

        # 初始化 partitionNum 总和
        totalPartitions=0
        # 初始化一个数组用于存储很久没有消息写入的 topic
        declare -a topic_info_array
        # 遍历 topics 并检查 replicasNum 是否为 1，同时计算 partitionNum 总和
        for topic in "${topicInfos[@]}"; do
            # 提取 name 字段的值
            name=$(echo "$topic" | grep -o '"name": *"[^"]*"' | sed 's/"name": *"//;s/"//')
            # 提取 partitionNum 字段的值
            partitionNum=$(echo "$topic" | grep -o '"partitionNum": *[0-9]*' | sed 's/"partitionNum": *//')
            # 提取 replicasNum 字段的值
            replicasNum=$(echo "$topic" | grep -o '"replicasNum": *[0-9]*' | sed 's/"replicasNum": *//')

            # 累加 partitionNum
            totalPartitions=$((totalPartitions + partitionNum))

            # 如果 replicasNum 为 1，输出警告信息
            if [ "$replicasNum" -eq 1 ]; then
                podOk=3
                Warn "Kafainstance:${ns}  $kafkaInstanceName has topic $name replicasNum = 1,请通知业务方，高可用会存在问题，需要将topic副本数至少调整到2"
            fi

            # 查询 Topic 的位点信息和消息保留时长
            topicDetailResponse=$(kubectl -n ${ns} exec -q $curlPodName -- /bin/bash -c "curl -s  http://${agentIp}:8081/agent/v1.0/kafka/topic/detail?topicName=$name")

            totalLag=0
            accumulativeMessageNum=0

            # 提取每个 partition 的 JSON 块
            while IFS= read -r partition; do
                # 提取 offsetMax 的值
                logEndOffset=$(echo "$partition" | grep -o '"offsetMax": *[0-9]*' | sed 's/"offsetMax": *//')
                # 提取 offsetMin 的值
                logStartOffset=$(echo "$partition" | grep -o '"offsetMin": *[0-9]*' | sed 's/"offsetMin": *//')

                # 计算 lagSize
                lagSize=$((logEndOffset - logStartOffset))
                # 累加 totalLag
                totalLag=$((totalLag + lagSize))
                # 累加 accumulativeMessageNum
                accumulativeMessageNum=$((accumulativeMessageNum + logEndOffset))
            done < <(echo "$topicDetailResponse" | grep -o '{[^}]*"offsetMax":[^}]*"offsetMin":[^}]*}')

            echo "Total Lag: $totalLag"
            echo "Accumulative Message Num: $accumulativeMessageNum"

            if [ "$totalLag" -eq 0 ]; then
#                retentionMs=$(echo "$topicDetailResponse" | grep -o '"retention.ms": *[0-9]*' | sed 's/"retention.ms": *//')
                retentionMs=$(echo "$topicDetailResponse" | grep -o '"retention.ms":"[^"]*' | sed 's/"retention.ms":"//')
                retentionHours=$(echo "$retentionMs" | awk '{printf "%.2f", $1 / (1000 * 60 * 60)}')
                topic_info_array+=("$name,$totalLag,$accumulativeMessageNum,$retentionHours")
            fi
        done

        # 输出 partitionNum 的总和
        Info "Partition总数：$totalPartitions"
        if [ "$totalPartitions" -ge 2000 ]; then
            podOk=4
            Warn "Kafainstance:${ns}  $kafkaInstanceName topicCount:$topicCount totalPartitions:$totalPartitions，分区过多会降低Kafka性能，若实例总流量较低，请忽略"
        fi

        if [ "$podOk" -eq 0 ]; then
            Info "巡检通过"
        fi
        Info "----------Kafka实例 ${kafkaInstanceName} 巡检结束----------"
        EchoNull
        EchoNull
    done <./kafka_instance_list

fi
rm -rf ./kafka_instance_list
EchoNull
Info "********************租户Kafka实例巡检结束********************"
