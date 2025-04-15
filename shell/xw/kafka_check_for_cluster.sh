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

        replicaNum=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.replicasStatus}')


        saslEnabled=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.config.saslEnabled}')
        if [ x"$saslEnabled" == x"true" ]; then
            genericAdminConfigIfSASLEnable $ns $kafkaInstanceName
        fi

        diskSizeStr=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.persistency.cap}')
        diskSizeGi=$(echo "$diskSizeStr" | grep -oE '[0-9]+')

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
        Info "Topic总数 ：$topicCount"

        # 以下是Group查询
        groupResponse=$(kubectl -n ${ns} exec -q $curlPodName -- /bin/bash -c "curl -s  http://${agentIp}:8081/agent/v1.0/kafka/consumergroup/list?pageIndex=1\&pageSize=30000")

        groupCount=$(echo "$groupResponse" | grep -o '"totalCount": *[0-9]*' | sed 's/"totalCount": *//')
        Info "Group总数 ：$groupCount"

        for ((i = 1000; i < 1000 + $replicaNum; i++)); do
            index=$(expr $i - 1000)
            diskResponse=$(kubectl -n ${ns} exec -q "$kafkaInstanceName-$index-0" -- /bin/bash -c "du -sh /bitnami/kafka/data")
            diskSizeMStr=$(kubectl -n ${ns} exec -q "$kafkaInstanceName-$index-0" -- /bin/bash -c "du -m /bitnami/kafka/data")
            diskSizeM=$(echo "$diskSizeMStr" | grep "/bitnami/kafka/data$" | awk '{print $1}')
            percentage=$(awk "BEGIN {printf \"%.2f\", ($diskSizeM * 100) / ($diskSizeGi * 1024)}")
            Info "broker $i, 占用磁盘空间：$diskResponse, 总磁盘空间：$diskSizeStr, 磁盘使用率：$percentage%"

            cpuLoadStr=$(kubectl -n ${ns} exec -q "$kafkaInstanceName-$index-0" -- /bin/bash -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; sh /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool --object-name java.lang:type=OperatingSystem --jmx-url service:jmx:rmi:///jndi/rmi://localhost:5555/jmxrmi --attributes ProcessCpuLoad --one-time true")
            cpuLoad=$(echo "$cpuLoadStr" | awk 'NR==2 {split($0, arr, ","); print arr[2]}')
            totalLoadStr=$(kubectl -n ${ns} exec -q "$kafkaInstanceName-$index-0" -- /bin/bash -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; sh /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool --object-name java.lang:type=OperatingSystem --jmx-url service:jmx:rmi:///jndi/rmi://localhost:5555/jmxrmi --attributes AvailableProcessors --one-time true")

            totalLoad=$(echo "$totalLoadStr" | awk 'NR==2 {split($0, arr, ","); print arr[2]}')
            cpuPercentage=$(awk "BEGIN {printf \"%.2f\", ($cpuLoad * 100) / ($totalLoad)}")

            memoryLoadStr=$(kubectl -n ${ns} exec -q "$kafkaInstanceName-$index-0" -- /bin/bash -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; sh /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.JmxTool --object-name java.lang:type=Memory --jmx-url service:jmx:rmi:///jndi/rmi://localhost:5555/jmxrmi --attributes HeapMemoryUsage --one-time true")
            memoryLoadStr=$(echo "$memoryLoadStr" | awk 'NR==2')
            memoryUsed=$(echo "$memoryLoadStr" | awk -F 'used=' '{split($2, arr, /[},]/); print arr[1]}')
            memoryCommitted=$(echo "$memoryLoadStr" | awk -F 'committed=' '{split($2, arr, /[},]/); print arr[1]}')
            memoryPercentage=$(awk "BEGIN {printf \"%.2f\", ($memoryUsed * 100) / ($memoryCommitted)}")


            Info "broker $i,  cpu使用 $cpuLoad, cpu总大小 $totalLoad, cpu使用率 $cpuPercentage%, 堆内存使用 $memoryUsed, 堆内存总大小 $memoryCommitted, 堆内存使用率 $memoryPercentage%"
        done


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
