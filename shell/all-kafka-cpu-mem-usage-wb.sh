#!/bin/bash

##########################################################################
# 作者：wubing
# 版本：v1.0
# 日期：2024-10-14
# 描述：获取本集群中的Kafka的cpu/mem等资源
##########################################################################

#!/bin/bash

Error() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[31mERROR\033[0m $1"
}
   
Warn() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[33mWARN\033[0m $1"
}
   
Info() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[34mINFO\033[0m $1"
}
 
Debug() {
    echo -e "[$(date +"%Y-%m-%d %H:%M:%S")] \033[32mDEBU\033[0m $1"
}

timestamp=$(date +"%Y%m%d_%H%M%S")
filename="./kafka_benchmark_${timestamp}.csv"


secretname=$(kubectl  get sa -n ccos-monitoring prometheus-k8s -o jsonpath='{range .secrets[*]}{.name}{"\n"}{end}' | grep prometheus-k8s-token-)
token="Bearer $(kubectl get secret $secretname -n ccos-monitoring -o template --template='{{.data.token}}' | base64 --decode)"

  # 获取Prometheus的接口请求地址
thanos_url=$(kubectl get route -n ccos-monitoring thanos-querier  --no-headers | awk '{print $2}')

function get_department_name() {
    local id=$1

    SQL_QUERY="SET NAMES utf8mb4; SELECT \`department_name_path\` FROM \`u_department\` WHERE \`id\`='${id}';"

    RESPONSE=$(kubectl exec -n ccos-mysql ccos-mysql-0-0 -c mysql -- mysql -urds_manager -pJRi435wtZj -Daccount_operation -e "${SQL_QUERY}" -S /rds/mysql.sock 2>/dev/null | awk 'NR==2')

    echo "$RESPONSE"
}

function get_res_department() {
    local res_id=$1

    SQL_QUERY="SET NAMES utf8mb4; SELECT \`department_id\` FROM \`resource\` WHERE \`code\`='${res_id}';"

    department_id=$(kubectl exec -n ccos-mysql ccos-mysql-0-0 -c mysql -- mysql -urds_manager -pJRi435wtZj -Dresourcecenter -e "${SQL_QUERY}" -S /rds/mysql.sock 2>/dev/null | awk 'NR==2')

    get_department_name "$department_id"
}


function prometheus_query() {
    local query=$1

    local data=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=$query")
    local data_length=`echo "${data}"|jq '.data.result|length'`
    local data_rate=''
    if [[ "${data_length}" -gt 0 ]]; then
        data_rate=$(echo "${data}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        data_rate=$(printf "%.2f" "$data_rate")
    fi

   echo $data_rate
}

cos_query_service=$(kubectl -n paas describe deploy kafka-service  | grep cos_query_service |awk '{print $2}')
kafka_pod_name=$(kubectl get pod -n paas | grep kafka-service | head -1 |awk '{print $1}')
# 7天前毫秒时间戳
startTime=$(date -d "7 days ago" +%s%3N)
# 当前毫秒时间戳
endTime=$(date +%s%3N)

function cos_prometheus_query() {
    local querySql=$1

    data=$(kubectl -n paas exec -q $kafka_pod_name -- curl -s -X POST -H "Content-Type: application/json" "http://$cos_query_service/cos-query/v2.0/metrics/query_range" -d '{ "start": '$startTime', "end": '$endTime', "step": 604800, "compositeQuery": { "queryType": "promql", "panelType": "table", "promQueries": { "A": { "query": "'$querySql'" } } } }')

    local data_length=`echo "${data}"|jq '.data.result|length'`
    local data_rate=''
    if [[ "${data_length}" -gt 0 ]]; then
        data_rate=$(echo "${data}" | jq '.data.result[0].series[0].values[1].value' | sed 's/"//g')
    fi

   echo $data_rate
}



echo "一级部门,二级部门,三级部门,实例ID,实例名称,实例Pod名称,内网IP,弹性公网IP,规格(CPU/内存/磁盘),平均/最大/P90/P99CPU使用率%,平均/最大/P90/P99内存使用率%,磁盘使用率%,平均/最大/P90/P99生产流量Mbps,平均/最大/P90/P99消费流量Mbps" > "$filename"

totalCount=$(kubectl get kafkainstance -A |grep kafka| wc -l )
Info "Total kafka instance num: $totalCount"
count=0
if [ $totalCount -gt 0 ]
then
    kubectl get kafkainstance -A | awk '{print $1,$2}' | grep kafka >./allKafkaListForResource
    while read res
    do
        department_level1=''
        department_level2=''
        department_level3=''


        ((count++))
        ns=$(echo ${res} | cut -d " " -f 1)
        kafkaInstanceName=$(echo ${res} | cut -d " " -f 2)
        instanceId=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.config.instanceId}')

        department_name=$(get_res_department "$instanceId")
        if [[ -n "$department_name" ]]; then
            IFS='/' read -r department_level1 department_level2 department_level3 <<< "$department_name"
        fi

        #Info "Check kafka instance: ${ns} ${kafkaInstanceName}"
        crStatus=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.status.status}')
        if [ x"$crStatus" != x"Running" ];then
            Warn "Kafka instance: ${ns} ${kafkaInstanceName} status:$crStatus. 导出csv文件会忽略该实例，若需要统计，请检查该实例"
            kafkaInstanceName=$kafkaInstanceName"(非运行状态)"
            echo "$department_level1,$department_level2,$department_level3,$kafkaInstanceName,$displayName,$vpcIp,$eip,$specification,$cpu_usage,$mem_usage,$diskUsageRate,$productionFlow,$consumptionFlow" >> "$filename"
            echo "进度完成 $count/$totalCount"
            continue
        fi
        crDeleteTimestap=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.metadata.deletionTimestamp}')
        if [ x"$crDeleteTimestap" != x"" ];then
            Warn "Kafka instance: ${ns} ${kafkaInstanceName} 正在删除，导出的csv文件会忽略该实例. "
            kafkaInstanceName=$kafkaInstanceName"(删除状态)"
            echo "$department_level1,$department_level2,$department_level3,$kafkaInstanceName,$displayName,$vpcIp,$eip,$specification,$cpu_usage,$mem_usage,$diskUsageRate,$productionFlow,$consumptionFlow" >> "$filename"
            echo "进度完成 $count/$totalCount"
            continue
        fi
        replicasNum=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.services[2].replicas}')
        cpu=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.services[2].resources.limits.cpu}')
        mem=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.services[2].resources.limits.memory}')
        disk=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.spec.persistency.cap}' | tr -d 'Gi')
        specification=${cpu}C/${mem}/${disk}G
        for ((i=0; i<$replicasNum; i++))
        do
            vpcIp=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.brokerVPCStatus.portsInfo['$i'].ipv4Address}')
            eip=$(kubectl -n ${ns} get kafkainstance ${kafkaInstanceName} -o jsonpath='{.status.brokerVPCStatus.portsInfo['$i'].EIPAddress}')
            podName="${kafkaInstanceName}-${i}-0"
            usedDisk=$(kubectl -n ${ns} exec -q ${podName} -c kafka  -- /bin/sh -c  "du -shb /bitnami/kafka/data/ |awk '{print \$1}'")
            if [ x"$usedDisk" == x"0" ]; then
                diskUsageRate=0
            else
                diskUsageRate=$(expr $usedDisk \* 100 / $disk / 1073741824 )
            fi 
            
            # 平均使用率
            cur_promql_cpu_usage_rate="avg_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"}[5m]))by(namespace,container,pod)[168h:])/max(kube_pod_container_resource_limits{resource=\"cpu\",namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"})by(namespace,pod,container) *100"
            cpu_usage=$(prometheus_query $cur_promql_cpu_usage_rate)

            # 最大使用率
            cur_promql_cpu_max_rate="max_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"}[5m]))by(namespace,container,pod)[168h:])/max(kube_pod_container_resource_limits{resource=\"cpu\",namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"})by(namespace,pod,container) *100"
            cpu_usage+='/'$(prometheus_query $cur_promql_cpu_max_rate)
            # P90使用率
            cur_promql_cpu_90_rate="quantile_over_time(0.9,sum(rate(container_cpu_usage_seconds_total{namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"}[5m]))by(namespace,container,pod)[168h:])/max(kube_pod_container_resource_limits{resource=\"cpu\",namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"})by(namespace,pod,container) *100"
            cpu_usage+='/'$(prometheus_query $cur_promql_cpu_90_rate)
            # P99使用率
            cur_promql_cpu_99_rate="quantile_over_time(0.99,sum(rate(container_cpu_usage_seconds_total{namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"}[5m]))by(namespace,container,pod)[168h:])/max(kube_pod_container_resource_limits{resource=\"cpu\",namespace=\"${ns}\",pod=\"${podName}\",container=\"kafka\"})by(namespace,pod,container) *100"
            cpu_usage+='/'$(prometheus_query $cur_promql_cpu_99_rate)



            # 平均使用率
            curl_promql_mem_usage="avg_over_time(sum(container_memory_working_set_bytes{namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\",container!=\"\",container!=\"POD\"})by(namespace,pod,container)[168h:])/max(kube_pod_container_resource_limits{resource=\"memory\",namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\"})by(namespace,pod,container)*100"
            mem_usage=$(prometheus_query $curl_promql_mem_usage)
            # 最大使用率
            curl_promql_mem_max_usage="max_over_time(sum(container_memory_working_set_bytes{namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\",container!=\"\",container!=\"POD\"})by(namespace,pod,container)[168h:])/max(kube_pod_container_resource_limits{resource=\"memory\",namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\"})by(namespace,pod,container)*100"
            mem_usage+='/'$(prometheus_query $curl_promql_mem_max_usage)

            # P90使用率
            curl_promql_mem_90_usage="quantile_over_time(0.9,sum(container_memory_working_set_bytes{namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\",container!=\"\",container!=\"POD\"})by(namespace,pod,container)[168h:])/max(kube_pod_container_resource_limits{resource=\"memory\",namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\"})by(namespace,pod,container)*100"
            mem_usage+='/'$(prometheus_query $curl_promql_mem_90_usage)
            # P99使用率
            curl_promql_mem_99_usage="quantile_over_time(0.99,sum(container_memory_working_set_bytes{namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\",container!=\"\",container!=\"POD\"})by(namespace,pod,container)[168h:])/max(kube_pod_container_resource_limits{resource=\"memory\",namespace=~\"${ns}\",pod=~\"${podName}\",container=~\"kafka\"})by(namespace,pod,container)*100"
            mem_usage+='/'$(prometheus_query $curl_promql_mem_99_usage)


            # 获取每个broker流量
            # 生产速率 pod网络的流量并不能代表kafka生产和消费，没有考虑复制的流量
            # curl_promql_recive_bytes="avg_over_time(sum(rate(container_network_receive_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # recive_bytes=$(prometheus_query $curl_promql_recive_bytes)
            # # 最大生产速率
            # curl_promql_recive_bytes_max="max_over_time(sum(rate(container_network_receive_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # recive_bytes+='/'$(prometheus_query $curl_promql_recive_bytes_max)
            # # P99生产速率
            # curl_promql_recive_bytes_99="quantile_over_time(0.99,sum(rate(container_network_receive_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # recive_bytes+='/'$(prometheus_query $curl_promql_recive_bytes_99)
            # # P90使生产速率
            # curl_promql_recive_bytes_90="quantile_over_time(0.9,sum(rate(container_network_receive_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # recive_bytes+='/'$(prometheus_query $curl_promql_recive_bytes_90)

            # # 消费速率
            # curl_promql_send_bytes="avg_over_time(sum(rate(container_network_transmit_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # send_bytes=$(prometheus_query $curl_promql_send_bytes)
            # # 最大消费速率
            # curl_promql_send_bytes_max="max_over_time(sum(rate(container_network_transmit_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # send_bytes+='/'$(prometheus_query $curl_promql_send_bytes_max)
            # # P99消费速率
            # curl_promql_send_bytes_99="quantile_over_time(0.99,sum(rate(container_network_transmit_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # send_bytes+='/'$(prometheus_query $curl_promql_send_bytes_99)
            # # P90消费速率
            # curl_promql_send_bytes_90="quantile_over_time(0.9,sum(rate(container_network_transmit_bytes_total{namespace=~\"${ns}\",pod=~\"${podName}\"}[5m]))by(pod,namespace)[168h:])/1024/1024"
            # send_bytes+='/'$(prometheus_query $curl_promql_send_bytes_90)
            
            # 生产速率
            curl_promql_recive_bytes="round(avg_over_time(kafka_cluster_bytes_in_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            recive_bytes=$(cos_prometheus_query $curl_promql_recive_bytes)
            curl_promql_recive_bytes_max="round(max_over_time(kafka_cluster_bytes_in_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            recive_bytes+='/'$(cos_prometheus_query $curl_promql_recive_bytes_max)
            curl_promql_recive_bytes_90="round(quantile_over_time(0.9,kafka_cluster_bytes_in_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            recive_bytes+='/'$(cos_prometheus_query $curl_promql_recive_bytes_90)
            curl_promql_recive_bytes_99="round(quantile_over_time(0.99,kafka_cluster_bytes_in_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            recive_bytes+='/'$(cos_prometheus_query $curl_promql_recive_bytes_99)

            # 消费速率
            curl_promql_send_bytes="round(avg_over_time(kafka_cluster_bytes_out_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            send_bytes=$(cos_prometheus_query $curl_promql_send_bytes)
            curl_promql_send_bytes_max="round(max_over_time(kafka_cluster_bytes_out_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            send_bytes+='/'$(cos_prometheus_query $curl_promql_send_bytes_max)
            curl_promql_send_bytes_90="round(quantile_over_time(0.9,kafka_cluster_bytes_out_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            send_bytes+='/'$(cos_prometheus_query $curl_promql_send_bytes_90)
            curl_promql_send_bytes_99="round(quantile_over_time(0.99,kafka_cluster_bytes_out_one_minute_rate{instance_name=\\\"$kafkaInstanceName\\\",tenant_id=\\\"$ns\\\"}[168h])/1024/1024,0.1)"
            send_bytes+='/'$(cos_prometheus_query $curl_promql_send_bytes_99)



            # 输出到csv文件
            echo "$department_level1,$department_level2,$department_level3,$instanceId,$kafkaInstanceName,$podName,$vpcIp,$eip,$specification,$cpu_usage,$mem_usage,$diskUsageRate,$recive_bytes,$send_bytes" >> "$filename"
        done
    
    echo "进度完成 $count/$totalCount"
    echo "生产和消费流量统计的都是集群流量，请不要认为是单个kafka broker的pod的流量"
    done < ./allKafkaListForResource
    rm -rf ./allKafkaListForResource
fi