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
request.timeout.ms=180000
EOF"

    portsInfo=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.brokerVPCStatus.portsInfo}')
    hostContent=$(echo $portsInfo | grep -o '"ipv4Address":"[^"]*"' | sed 's/"ipv4Address":"//;s/"//' | awk '{print $0" "$0}')

    allHostContent=$(kubectl -n "${ns}" exec "$kafkaInstanceName-0-0" -c kafka -- /bin/sh -c "cat /etc/hosts")

    if echo "$allHostContent" | grep -q "$hostContent"; then
        echo "host内容已填充，不再需要额外添加"
    else
        echo "host内容缺失，将进行添加，要添加的内容为 $hostContent"
        kubectl -n "${ns}" exec "$kafkaInstanceName-0-0" -c kafka -- /bin/sh -c "echo '$hostContent' >> /etc/hosts "
    fi
}

declare -A groupAndLagMap
declare -A groupAndTopicMap
function groupSubscribeTopicNum() {
    ns=$1
    kafkaInstanceName=$2
    enableSASL=$3

    allTopicSizeContent=""
    if [ "$enableSASL" = "true" ]; then
        allTopicSizeContent=$(kubectl -n "${ns}" exec "$kafkaInstanceName-0-0" -c kafka -- /bin/sh -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9093 --command-config $admin_config --describe --all-groups")
    else
        allTopicSizeContent=$(kubectl -n "${ns}" exec "$kafkaInstanceName-0-0" -c kafka -- /bin/sh -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9093 --describe --all-groups")
    fi

    # 逐个删除键值对
    for key in "${!groupAndLagMap[@]}"; do
        unset groupAndLagMap[$key]
    done
    for key in "${!groupAndTopicMap[@]}"; do
        unset groupAndTopicMap[$key]
    done

    declare -A groupAndLagMapTmp
    declare -A groupAndTopicMapTmp
    while IFS= read -r line; do
        groupId=$(echo "$line" | awk '{print $1}')
        topicName=$(echo "$line" | awk '{print $2}')
        lag=$(echo "$line" | awk '{print $6}')
        if [[ $lag =~ ^[0-9]+$ ]]; then
            # 检查键是否存在
            if [[ -v groupAndLagMapTmp[$groupId] ]]; then
                # 如果键存在，对其值进行累加
                ((groupAndLagMapTmp[$groupId]+=lag))
            else
                # 如果键不存在，初始化该键的值
                groupAndLagMapTmp[$groupId]=$lag
            fi

            if [[ -v groupAndTopicMapTmp[$groupId] ]]; then
                # 如果键存在，对其值进行累加
                groupAndTopicMapTmp[$groupId]=${groupAndTopicMapTmp[$groupId]}",$topicName"
            else
                # 如果键不存在，初始化该键的值
                groupAndTopicMapTmp[$groupId]=$topicName
            fi
        fi
    done <<< "$(echo "$allTopicSizeContent")"

    for key in "${!groupAndLagMapTmp[@]}"; do
        # 将源关联数组的值赋给目标关联数组相同的键
        groupAndLagMap[$key]=${groupAndLagMapTmp[$key]}
    done
    for key in "${!groupAndTopicMapTmp[@]}"; do
        # 将源关联数组的值赋给目标关联数组相同的键
        groupAndTopicMap[$key]=${groupAndTopicMapTmp[$key]}
    done

    topicCount=$(echo "$allTopicSizeContent" | grep -v '^GROUP' | awk '{print $2}' | grep -v '^$' | sort | uniq | wc -l)
    return "$topicCount"
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

        if [ $# -gt 0 -a "$1" != "$kafkaInstanceName" ]; then
            continue
        fi

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

        # 以下是Group查询
        groupResponse=$(kubectl -n ${ns} exec -q $curlPodName -- /bin/bash -c "curl -s  http://${agentIp}:8081/agent/v1.0/kafka/consumergroup/list?pageIndex=1\&pageSize=30000")
        groupCount=$(echo "$groupResponse" | grep -o '"totalCount": *[0-9]*' | sed 's/"totalCount": *//')

        Info "Group总数：$groupCount"

        groupInfos=()
        while IFS= read -r line; do
            groupInfos+=("$line")
        done < <(echo "$groupResponse" | grep -o '{[^}]*"groupId":[^}]*"state":[^}]*"memberNum":[^}]*}')

        totalGroupMember=0

        groupSubscribeTopicNum $ns $kafkaInstanceName $saslEnabled

        for group in "${groupInfos[@]}"; do
            groupId=$(echo "$group" | grep -o '"groupId": *"[^"]*"' | sed 's/"groupId": *"//;s/"//')
            state=$(echo "$group" | grep -o '"state": *"[^"]*"' | sed 's/"state": *"//;s/"//')
            memberNum=$(echo "$group" | grep -o '"memberNum": *[0-9]*' | sed 's/"memberNum": *//')

            totalGroupMember=$((totalGroupMember + memberNum))

            groupLagNum=0
            if [[ -v groupAndLagMap[$groupId] ]]; then
                groupLagNum="${groupAndLagMap[$groupId]}"
            fi

            subscribingTopicNum=0
            if [[ -v groupAndTopicMap[$groupId] ]]; then
                groupTopics="${groupAndTopicMap[$groupId]}"
                split_string=$(echo "$groupTopics" | tr ',' '\n')
                unique_string=$(echo "$split_string" | sort | uniq)
                subscribingTopicNum=$(echo "$unique_string" | wc -l)
            fi

            Info "groupId $groupId, 状态 $state, Consumer数量 $memberNum, 堆积量 $groupLagNum, 订阅topic数 $subscribingTopicNum"

            # 如果 lag 大于 100 且 state 为 Stable，输出警告信息
            if [ "$state" == "Stable" ] && [ "$groupLagNum" -ge 100 ]; then
                Warn "Kafainstance:${ns}  $kafkaInstanceName has group $groupId 消费堆积: $groupLagNum，请通知业务方查看消费是否异常"
                podOk=5
            fi
            # 如果 lag 大于 100 且 state 为 Stable，输出警告信息
            if [ "$state" == "Stable" ] && [ "$subscribingTopicNum" -ge 5 ]; then
                Warn "group $groupId 订阅Topic数量: $subscribingTopicNum，请通知业务方减少订阅topic"
                podOk=6
            fi
        done

        Info "全部consumer个数为 $totalGroupMember"

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
