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
request.timeout.ms=180000
EOF"

}

groupLagNum=0

function groupSubscribeTopicNum() {
    ns=$1
    kafkaInstanceName=$2
    enableSASL=$3
    groupName=$4

    allTopicSizeContent=""
    if [ "$enableSASL" = "true" ]; then
        allTopicSizeContent=$(kubectl -n "${ns}" exec "$kafkaInstanceName-0-0" -c kafka -- /bin/sh -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9093 --command-config $admin_config --describe --group $groupName")
    else
        allTopicSizeContent=$(kubectl -n "${ns}" exec "$kafkaInstanceName-0-0" -c kafka -- /bin/sh -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9093 --describe --group $groupName")
    fi

    lag_sum=0
    while IFS= read -r line; do
        lag=$(echo "$line" | awk '{print $5}')
        if [[ $lag =~ ^[0-9]+$ ]]; then
            ((lag_sum += lag))
        fi
    done <<< "$(echo "$allTopicSizeContent" | grep -v '^GROUP')"
    groupLagNum=$lag_sum

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

        for group in "${groupInfos[@]}"; do
            groupId=$(echo "$group" | grep -o '"groupId": *"[^"]*"' | sed 's/"groupId": *"//;s/"//')
            state=$(echo "$group" | grep -o '"state": *"[^"]*"' | sed 's/"state": *"//;s/"//')
            memberNum=$(echo "$group" | grep -o '"memberNum": *[0-9]*' | sed 's/"memberNum": *//')
#            subscribingTopicNum=$(echo "$group" | grep -o '"subscribingTopicNum": *[0-9]*' | sed 's/"subscribingTopicNum": *//')
            GROUP_IDS="[\"$groupId\"]"

            totalGroupMember=$((totalGroupMember + memberNum))

            groupSubscribeTopicNum $ns $kafkaInstanceName $saslEnabled $groupId
            subscribingTopicNum=$?

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
