#!/bin/bash

# 检查是否有副本进度落后的脚步

green='\033[32m'
red='\033[0;31m'
yellow='\033[0;33m'
NC='\033[0m' # No Color

function check_kafka(){
    echo "********************平台ccos-kafka数据一致性检查******************** "

    local kafkaInstance=("cluster" "dts-kafka-cluster" "sec-kafkainstance")
    local ns="ccos-cmq-kafka"
    local connectedCount=0
    local disconnectedCount=0

    for clusterCr in ${kafkaInstance[@]}
        do
          crStatus=$(kubectl -n ${ns} get kafkacluster ${clusterCr} -o template --template={{.status.phase}})
          if [ x"$crStatus" != x"Running" ];then
            echo "  VV ${clusterCr} kafkacluster cluster状态, 请检查: ${clusterCr} kafka集群状态"
            ((disconnectedCount++))
          fi
          topicCount=$(kubectl -n ${ns} exec $clusterCr-0-0-0  -c kafka -- /bin/sh -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092" | wc -l)
          if [ $? -ne 0 ]; then
            echo "  VV $clusterCr-0-0-0  pod功能异常, 请检查: $$clusterCr-0-0-0"
            ((disconnectedCount++))
          fi
          if [ $topicCount == 0 ]; then
            # LOG_INFO "  $clusterCr kafka集群没有topic, 不需要检查"
            continue
          fi

          topic_replicas=$(kubectl -n ${ns} exec ${clusterCr}-0-0-0   -c kafka -- /bin/sh -c "unset JMX_PORT; unset KAFKA_OPTS; unset KAFKA_JMX_OPTS; /opt/bitnami/kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092" | grep -E "Replicas" |grep "Isr" |  awk '{print $1,$2,$7,$8,$9,$10}')
          # while read res1;do
          while read line;do
            if [ -z "$line" ]; then
                continue
            fi
            topicName=$(echo ${line} | cut -d " " -f 2)
            replicas=$(echo ${line} | cut -d " " -f 4)
            replicasCount=$(echo ${replicas} | tr ',' '\n' | wc -l)
            isr=$(echo ${line} | cut -d " " -f 6)
            if [ x"${isr}" == "x" ]; then
              echo "  ${clusterCr} kafkacluster topic:${topicName} 中follower未同步完成(isr中无follower)，请检查!!! 注：如果是非管理集群请忽略此报错 "
              ((disconnectedCount++))
            fi
            isrCount=$(echo ${isr} | tr ',' '\n' | wc -l)
            if [ "${isrCount}" -le 1 ] && [ "${replicasCount}" -ne 1 ];then
              echo "  ${clusterCr} kafkacluster topic:${topicName} 中follower未同步完成(isr中只有leader)，请检查!!! 注：如果是非管理集群请忽略此报错 "
              ((disconnectedCount++))
            fi
          done <<< ${topic_replicas}

          # LOG_INFO "  ${clusterCr} kafka集群topic副本同步正常"
    done
    # LOG_INFO "  检查 ccos-kafka 实例集群同步状态正常"

    if [ $disconnectedCount -eq 0 ]; then
      echo "  平台ccos-kafka数据一致性正常"
    fi

    echo "********************平台ccos-kafka数据一致性检查完成********************"
    echo ""
    echo ""
}

check_kafka