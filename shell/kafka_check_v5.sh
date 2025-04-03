#!/bin/bash

##################################################(50个)
# Author:       wubing
# Description:  检查Kafka管控和实例服务
# Create:       2024-09-20
# Update:       2024-10-21:更新日志目录
# Example：     直接执行
##################################################(50个)

currentTime=$(date +"%Y%m%d-%H%M%S") 

# 日志目录
SCRIPT_DIR=$(dirname "$0")
SCRIPT_FILE_NAME=`echo $0|sed 's/\..*//g'`
logFile=$SCRIPT_DIR/../log/kafka_check_v5_$currentTime.log
mkdir -p $(dirname "$logFile")

EchoNull() {
    echo "" >> $logFile
}

Error() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[31mERROR $1 \033[0m" >> $logFile
}
   
Warn() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[33mWARN $1 \033[0m" >> $logFile
}
   
Info() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mINFO $1\033[0m" >> $logFile
}
 
Debug() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mDEBUG $1\033[0m" >> $logFile
}


# 定义匹配的天数
MATCH_DAY=1

# 生成时间关键字，比如^(2023-07-25\|2023-07-24\|2023-07-23)
DAYS=$(date -d "0 days" +%Y-%m-%d) 

for ((i=1; i<=$MATCH_DAY; i++)); do
  DAY=$(date -d "-$i days" +%Y-%m-%d)
  DAYS="$DAYS\\|$DAY" 
done

Info "********************1.组件ccos-kafka巡检开始********************"
ccosKafkaOk=0
unHealthPodCount=$(sudo kubectl get po --no-headers -n ccos-cmq-kafka | grep -Ev "(Running|Completed|Terminating|Evicted)" -c)
if [ "$unHealthPodCount" -gt 0 ]; then
    ccosKafkaOk=1
    Error "ccos-kafka 实例pod状态检测:异常,请查看pod异常情况"
    sudo kubectl get po -n ccos-cmq-kafka | grep -Ev "(Running|Completed|Terminating|Evicted)"
fi
#检查ccos-kafka实例日志
ccosKafkaList=($(kubectl get kafkacluster | awk '{print $1}' | grep -v NAME))

for ccosKafka in "${ccosKafkaList[@]}"; do
    ccosKafkaPodList=($ccosKafka-0-0-0 $ccosKafka-1-0-0 $ccosKafka-2-0-0)
    for ccosKafkaPod in "${ccosKafkaPodList[@]}"; do
        errorCount=$(kubectl -n ccos-cmq-kafka logs $ccosKafkaPod -c kafka | grep "$DAYS"| grep ERROR -c)
        if [ "$errorCount" -ge 50 ] && [ "$errorCount" -lt 100 ]; then
            ccosKafkaOk=2
            ERROR "组件Kafka实例Pod:ccos-cmq-kafka下 pod:$ccosKafkaPod has $errorCount error. 请查看具体错误日志!"
        fi    
    done
done
if [ "$ccosKafkaOk" -eq 0 ]; then
    Info "巡检通过"
fi

Info "********************1.组件ccos-kafka巡检结束********************"
EchoNull
EchoNull
Info "********************2.产品Kafka管控服务巡检开始********************"
manageOk=0
unHealthPodCount=$(sudo kubectl get po --no-headers -n paas-kafka-product| grep kafka-instance | grep -Ev "(Running|Completed|Terminating|Evicted)" -c)
if [ "$unHealthPodCount" -gt 0 ]; then
    manageOk=1
    Error "Kafka instance operator pod状态检测:异常,请查看pod异常情况"
    sudo kubectl get po -n paas-kafka-product| grep kafka-instance | grep -Ev "(Running|Completed|Terminating|Evicted)"
fi

unHealthPodCount=$(sudo kubectl get po --no-headers -n paas-kafka-product | grep kafka-product | grep -Ev "(Running|Completed|Terminating|Evicted)" -c)
if [ "$unHealthPodCount" -gt 0 ]; then
    manageOk=2
    Error "Kafka产品operator pod状态检测:异常,请查看pod异常情况"
    sudo kubectl get po -n paas-kafka-product | grep kafka-product | grep -Ev "(Running|Completed|Terminating|Evicted)"
fi

unHealthPodCount=$(sudo kubectl get po --no-headers -n paas | grep kafka-service | grep -Ev "(Running|Completed|Terminating|Evicted)" -c)
if [ "$unHealthPodCount" -gt 0 ]; then
    manageOk=3
    Error "Kafka管控服务pod状态检测:异常,请查看pod异常情况"
    sudo kubectl get po -n paas | grep kafka-service | grep -Ev "(Running|Completed|Terminating|Evicted)"
fi

# 检查当天日志是否有ERROR
managePodList=($(kubectl -n paas get po | grep kafka-service | grep Running | grep -Ev "doko|exporter|zookeeper|/1"| awk '{print $1}'))
for mPod in "${managePodList[@]}"; do

    errorCount=$(kubectl -n $ns logs $mPod -c kafka | grep "$DAYS"| grep -Ev "identity-service-req|INFO|INF" |grep -i error -c)
    if [ "$errorCount" -ge 20 ] && [ "$errorCount" -lt 50 ]; then
        manageOk=4
        Error "Kafka管控服务 namespace:paas pod:$mPod has $errorCount error. 请查看具体错误!"
    fi
done
if [ "$manageOk" -eq 0 ]; then
    Info "巡检通过"
fi

Info "********************2.租户Kafka管控服务巡检结束********************"
EchoNull
EchoNull
Info "********************3.租户Kafka实例巡检开始********************"
num=$(kubectl get kafkainstance -A |grep kafka| wc -l )
Info "Total kafka instance num: $num"
if [ $num -gt 0 ]
then
    kubectl get kafkainstance -A | awk '{print $1,$2}' | grep kafka >./kafka_instance_list
    while read res
    do
        EchoNull
        podOk=0
        ns=$(echo ${res} | cut -d " " -f 1)
        kafkaInstanceName=$(echo ${res} | cut -d " " -f 2)
        
        Info "----------Kafka实例${kafkaInstanceName}巡检开始----------"
        crStatus=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.status.status}')
        if [ x"$crStatus" != x"Running" ];then
            Error "Kafka instance: ${ns} ${kafkaInstanceName} status:$crStatus. 请检查该实例，若是创建中的实例请忽略"
            continue
        fi
        crDeleteTimestap=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.metadata.deletionTimestamp}')
        if [ x"$crDeleteTimestap" != x"" ];then
            Warn "Kafka instance: ${ns} ${kafkaInstanceName} is deleting. "
            continue
        fi
        
        unHealthPodCount=$(sudo kubectl get po --no-headers -n ${ns} | grep  ${kafkaInstanceName} | grep -Ev "(Running|Completed|Terminating|Evicted)" -c)
        if [ "$unHealthPodCount" -gt 0 ]; then
            podOk=1
            Error "Kafka instance: ${ns}  ${kafkaInstanceName} pod状态检测:异常，请检查pod情况"
            sudo kubectl get po --no-headers -n ${ns} | grep  ${kafkaInstanceName} | grep -Ev "(Running|Completed|Terminating|Evicted)"
            continue
        fi
        
        agentIp=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.agentVPCStatus.portsInfo[0].ipv4Address}')
        if [ x"$agentIp" == x ];then
            podOk=2
            Error "Kafka instance: ${ns} ${kafkaInstanceName} agentIp is null. 请检查实例状态"
            continue
        fi        
        
        curlPodName=$(kubectl -n ${ns} get po | grep ${kafkaInstanceName} | grep Running | grep -v doko| awk '{print $1}'| head -n 1)
        
        topicResponse=$(kubectl -n ${ns} exec -q $curlPodName -- /bin/bash -c "curl -s  http://${agentIp}:8081/agent/v1.0/kafka/topic/list?pageIndex=1\&pageSize=3000")
      
        topicCount=$(echo $topicResponse | jq '.data.totalCount')

        mapfile -t topicInfos < <(echo "$topicResponse" | jq -c '.data.item[]')
        # 初始化 partitionNum 总和
        totalPartitions=0
        # 遍历 topics 并检查 replicasNum 是否为 1，同时计算 partitionNum 总和
        for topic in "${topicInfos[@]}"; do
            name=$(echo "$topic" | jq -r '.name')
            partitionNum=$(echo "$topic" | jq -r '.partitionNum')
            replicasNum=$(echo "$topic" | jq -r '.replicasNum')

            # 累加 partitionNum
            totalPartitions=$((totalPartitions + partitionNum))

            # 如果 replicasNum 为 1，输出警告信息
            if [ "$replicasNum" -eq 1 ]; then
                podOk=3
                Warn "Kafainstance:${ns}  $kafkaInstanceName has topic $name replicasNum = 1,请通知业务方，高可用会存在问题，需要将topic副本数至少调整到2"
            fi
        done

        # 输出 partitionNum 的总和
        if [ "$totalPartitions" -ge 2000 ]; then
          podOk=4
          Warn "Kafainstance:${ns}  $kafkaInstanceName topicCount:$topicCount totalPartitions:$totalPartitions，分区过多会降低Kafka性能，若实例总流量较低，请忽略"
        fi
        
        groupResponse=$(kubectl -n ${ns} exec -q $curlPodName -- /bin/bash -c "curl -s  http://${agentIp}:8081/agent/v1.0/kafka/consumerguoup/list?pageIndex=1\&pageSize=3000")

        mapfile -t groupInfos < <(echo "$groupResponse" | jq -c '.data.item[]')

        totalGroupMember=0
        # 遍历 group 并检查 group 消费者个数是否大于50，同时计算 Member 总和
        for group in "${groupInfos[@]}"; do
            groupId=$(echo "$group" | jq -r '.groupId')
            state=$(echo "$group" | jq -r '.state')
            memberNum=$(echo "$group" | jq -r '.memberNum')
            lag=$(echo "$group" | jq -r '.lag')
            totalGroupMember=$((totalGroupMember + memberNum))
            # 如果 lag 大于 500，输出警告信息
            if [ "$state" == "Stable" ] && [ "$lag" -ge 500 ]; then
                Warn "Kafainstance:${ns}  $kafkaInstanceName has group $groupId 消费堆积: $lag，请通知业务方查看消费是否异常"
                podOk=5
            fi
            if [ "$memberNum" -ge 50 ]; then
                Warn "Kafainstance:${ns}  $kafkaInstanceName has group $groupId memeberNum $memberNum，同消费组的消费者过多会占用网络连接资源"
                podOk=6
            fi
        done
        #Info "Kafainstance:${ns}  $kafkaInstanceName has totalGroupMember:$totalGroupMember"
        
        # 检查当天日志是否有ERROR
        kafkaPodList=($(kubectl -n $ns get po | grep $kafkaInstanceName | grep Running | grep -Ev "doko|exporter|zookeeper|/1"| awk '{print $1}'))
        for kafkaPod in "${kafkaPodList[@]}"; do
        
            errorCount=$(kubectl -n $ns logs $kafkaPod -c kafka | grep "$DAYS"| grep ERROR -c)
            if [ "$errorCount" -ge 100 ]; then
                podOk=7
                Error "Kafainstance:${ns}  $kafkaInstanceName kafka pod:$kafkaPod has $errorCount log error. 请使用kubectl -n ${ns} logs $kafkaPod -c kafka 查看具体错误日志!"
            fi
        done
        if [ "$podOk" -eq 0 ];then
            Info "巡检通过"
        fi
        Info "----------Kafka实例${kafkaInstanceName}巡检结束----------"
    done < ./kafka_instance_list
 
fi
rm -rf ./kafka_instance_list
EchoNull
Info "********************3.租户Kafka实例巡检结束********************"