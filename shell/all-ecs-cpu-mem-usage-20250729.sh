#!/bin/bash

##########################################################################
# 作者：刘宏俊
# 版本：v2.6
# 日期：2024-11-12
# 描述：获取本地region中所有ECS实例资源使用情况
# v2 增加关联CKE集群列/P90统计
# v2.1 合并cpu/内存 avg/max/p90/p99使用率
# v2.2 增加磁盘使用率/IOPS
# v2.3 区分是否安装vmtools
# v2.4 磁盘利用率修改基础监控query
# v2.5 增加创建时间；去掉dm-磁盘
# v2.6 应孔祥辉要求，输出格式调整 —— 平均/最大单独成列；系统盘和数据盘（取最大）单独成列
# v2.7 thanos-querier拆分，需要关联代码适配
# v2.8 2025-04-15 
#      1、CPU内存除P90/P99外，再增加P50/P75/P95；
#      2、统计周期可选，过去一天（对应给客户日报），过去7天（对应给客户周报）
#      ---如果需要过去一天的usage信息，需要添加参数 -d 
#      例：sh all-ecs-cpu-mem-usage.sh -d
# v2.9 2025-07-29 
#  资源使用率采集频率调整
# v2.10 2025-07-30 
#  资源使用率5*8支持
#  例：sh all-ecs-cpu-mem-usage.sh -f
##########################################################################

#!/bin/bash

if [[ $# -gt 0 ]]; then
    period_parm=$1
    if [ "$period_parm" == "-d" ]; then
        period='1d'
        day_of_week_58=''
    elif [ "$period_parm" == "-f" ]; then
        period='7d'
        day_of_week_58=" and on() (1<=day_of_week()<=5 and 0<=hour()<10)"
    else
        period='7d'
        day_of_week_58=''
    fi
else
    period='7d'
    day_of_week_58=''
fi


get_kubectl_route() {
    local namespace=$1
    local service_name=$2
    kubectl get route "$service_name" -n "$namespace" -o=jsonpath='{.spec.host}'
}


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

function get_cke_cluster_name() {
    local id=$1

    SQL_QUERY="SET NAMES utf8mb4; SELECT \`name\` FROM \`clusters\` WHERE \`resource_id\`='${id}';"

    RESPONSE=$(kubectl exec -n ccos-mysql ccos-mysql-0-0 -c mysql -- mysql -urds_manager -pJRi435wtZj -Dcke_service -e "${SQL_QUERY}" -S /rds/mysql.sock 2>/dev/null | awk 'NR==2')

    echo "$RESPONSE"
}

function prometheus_query() {
    local query=$*

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

function reorg_disk_info() {
    input=$1

    # 解析输入并重组
    declare -A disks

    # 按 "/" 分割各个部分
    IFS='/' read -r -a parts <<< "$input"

    # 遍历每个部分
    for part in "${parts[@]}"; do
        # 按 "|" 分割 vda:值 和 vdb:值
        IFS='|' read -r -a pairs <<< "$part"

        for pair in "${pairs[@]}"; do
            # 按 ":" 分割盘名和值
            disk=$(echo "$pair" | cut -d':' -f1)
            value=$(echo "$pair" | cut -d':' -f2)

            # 将值累加到对应的盘名
            if [[ -z "${disks[$disk]}" ]]; then
                disks[$disk]="$value"
            else
                disks[$disk]="${disks[$disk]}/$value"
            fi
        done
    done

    result=""
    for disk in "${!disks[@]}"; do
        result+="$disk:${disks[$disk]}|"
    done

    result=${result%|}

    # 输出结果
    echo "$result"

}

function find_ecs_pod() {
    local ecsId=$1

    for ((i=0; i<${#ecs_pods[@]}; i+=2)); do
            namespace=${ecs_pods[i]}
            pod=${ecs_pods[i+1]}
            
        if [[ "$pod" == *"$ecsId"* ]]; then
            echo "$namespace" "$pod"
        fi
    done
}

function calc_disk_usage() {
    local ecsId=$1
    local sys_usage_rate=''
    local max_data_usage_rate=0
    
    capacity_query="max(kubevirt_vmi_filesystem_capacity_bytes_total{name=\"${ecsId}\"}) by (disk_name)"
    capacity_data=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${capacity_query}")

    # 查询当前时间点的已使用容量，并过滤 ECS
    used_query="max(kubevirt_vmi_filesystem_used_bytes{name=\"${ecsId}\"}) by (disk_name)"
    used_data=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${used_query}")

    capacity_data=$(echo "$capacity_data" | jq -c '.data.result[] | {name: .metric.disk_name, value: .value[1] | tonumber}')
    used_data=$(echo "$used_data" | jq -c '.data.result[] | {name: .metric.disk_name, value: .value[1] | tonumber}')

    capacity=$(echo "$capacity_data" | jq -s '
        map({name: .name, value: .value}) |
        group_by(.name[0:3]) |
        map({prefix: .[0].name[0:3], total: map(.value) | add})
    ')

    used=$(echo "$used_data" | jq -s '
        map({name: .name, value: .value}) |
        group_by(.name[0:3]) |
        map({prefix: .[0].name[0:3], total: map(.value) | add})
    ')

    for i in $(echo "$capacity" | jq -c '.[]'); do
        prefix=$(echo "$i" | jq -r '.prefix')

        if [[ $prefix == vd* ]]; then
            capacity_value=$(echo "$i" | jq -r '.total')
            used_value=$(echo "$used" | jq -r --arg prefix "$prefix" '.[] | select(.prefix==$prefix)' |  jq -r '.total')
            usage_rate=$(awk "BEGIN {printf \"%.2f\", $used_value / $capacity_value * 100}")
            if [[ $prefix == vda* ]]; then
                sys_usage_rate=$usage_rate
            else
                #if (( $(echo "$usage_rate > $max_data_usage_rate" | bc -l) )); then
                #    max_data_usage_rate=$usage_rate
                #fi
                # 使用 python3 进行比较
                result=$(python3 -c "print('less' if $max_data_usage_rate < $usage_rate else ('greater' if $max_data_usage_rate > $usage_rate else 'equal'))")
                if [[ $result == less ]]; then
                    max_data_usage_rate=$usage_rate
                fi
            fi
        fi
    done

    if [[ -z "$sys_usage_rate" ]]; then
        echo "未获取到数据,未获取到数据"
    else
        echo "$sys_usage_rate,$max_data_usage_rate"
    fi
}


function get_eip_input_data_rate() {
    local eip_id=$1
    local recv_mbps=''

    avg_recv_query="avg_over_time((sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_input_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${avg_recv_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        avg_recv_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        recv_mbps=$(awk "BEGIN {printf \"%.2f\", $avg_recv_bytes / 125000}")
    fi

    max_recv_query="max_over_time((sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_input_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${max_recv_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        max_recv_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        recv_mbps+=','$(awk "BEGIN {printf \"%.2f\", $max_recv_bytes / 125000}")
    fi

    p50_recv_query="quantile_over_time(0.5,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_input_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p50_recv_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p50_recv_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        recv_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p50_recv_bytes / 125000}")
    fi

    p75_recv_query="quantile_over_time(0.75,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_input_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p75_recv_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p75_recv_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        recv_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p75_recv_bytes / 125000}")
    fi

    p90_recv_query="quantile_over_time(0.9,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_input_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p90_recv_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p90_recv_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        recv_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p90_recv_bytes / 125000}")
    fi

    p95_recv_query="quantile_over_time(0.95,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_input_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p95_recv_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p95_recv_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        recv_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p95_recv_bytes / 125000}")
    fi

    p99_recv_query="quantile_over_time(0.99,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_input_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p99_recv_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p99_recv_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        recv_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p99_recv_bytes / 125000}")
    fi
    echo $recv_mbps
}


function get_eip_output_data_rate() {
    local eip_id=$1
    local send_mbps=''

    avg_send_query="avg_over_time((sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_output_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${avg_send_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        avg_send_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        send_mbps=$(awk "BEGIN {printf \"%.2f\", $avg_send_bytes / 125000}")
    fi

    max_send_query="max_over_time((sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_output_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${max_send_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        max_send_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        send_mbps+=','$(awk "BEGIN {printf \"%.2f\", $max_send_bytes / 125000}")
    fi

    p50_send_query="quantile_over_time(0.5,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_output_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p50_send_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p50_send_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        send_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p50_send_bytes / 125000}")
    fi

    p75_send_query="quantile_over_time(0.75,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_output_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p75_send_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p75_send_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        send_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p75_send_bytes / 125000}")
    fi

    p90_send_query="quantile_over_time(0.9,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_output_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p90_send_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p90_send_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        send_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p90_send_bytes / 125000}")
    fi

    p95_send_query="quantile_over_time(0.95,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_output_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p95_send_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p95_send_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        send_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p95_send_bytes / 125000}")
    fi

    p99_send_query="quantile_over_time(0.99,(sum by (resourceType, resourceId, clusterName, clusterZone, clusterRegion)(irate(vpc_eip_output_bytes_total{resourceId=\"${eip_id}\"}[1m]))$day_of_week_58)[$period:])"
    response=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${p99_send_query}")
    if [[ $(echo "$response" | jq -r '.data.result | length') -gt 0 ]]; then
        p99_send_bytes=$(echo $response | jq -r '.data.result[].value[1]')
        send_mbps+=','$(awk "BEGIN {printf \"%.2f\", $p99_send_bytes / 125000}")
    fi

    echo $send_mbps

}

timestamp=$(date +"%Y%m%d_%H%M%S")
filename="./ecs_benchmark_${timestamp}.csv"

namespace_ecs="ccos-product-ecs"
service_names=("ecs-service")
ECS_MANAGE_ROUTE=$(get_kubectl_route "$namespace_ecs" "${service_names[0]}")
secretname=$(kubectl  get sa -n ccos-monitoring prometheus-k8s -o jsonpath='{range .secrets[*]}{.name}{"\n"}{end}' | grep prometheus-k8s-token-)
token="Bearer $(kubectl get secret $secretname -n ccos-monitoring -o template --template='{{.data.token}}' | base64 --decode)"

thanos_querier_bussiness=$(kubectl get route -n ccos-monitoring |grep thanos-querier-business)
if [[ -z "$thanos_querier_bussiness" ]]; then
    echo "prometheus not parted by bussiness!!!"
    # 获取Prometheus的接口请求地址
    thanos_url=$(kubectl get route -n ccos-monitoring thanos-querier  --no-headers | awk '{print $2}')  
else
    echo "prometheus parted by bussiness!!!"
    # 获取Prometheus的接口请求地址
    thanos_url=$(kubectl get route -n ccos-monitoring thanos-querier-business  --no-headers | awk '{print $2}')  
fi

json_data=$(curl -s -k "https://$ECS_MANAGE_ROUTE/compute/ecs/v1/instances?orderBy=created_at:desc")
total_count=$(echo "$json_data" | jq '.data.total_count')
count=0

#echo "一级部门,二级部门,三级部门,ECS ID,ECS名称,创建时间,关联CKE集群名称,规格CPU/内存,内网IP,弹性公网IP,平均/最大/P50/P75/P90/P95/P99 CPU使用率%,平均/最大/P50/P75/P90/P95/P99内存使用率%,磁盘配置,磁盘使用率%,平均/最大/P50/P75/P90/P95/P99磁盘读IOPS,平均/最大/P50/P75/P90/P95/P99磁盘写IOPS,EIP带宽M,平均/最大/P50/P75/P90/P95/P99 EIP入向流量Mbps,平均/最大/P50/P75/P90/P95/P99 EIP出向流量Mbps" > "$filename"
echo "一级部门,二级部门,三级部门,ECS ID,ECS名称,创建时间,关联CKE集群名称,规格CPU/内存,内网IP,弹性公网IP,平均CPU使用率%,最大CPU使用率%,P50 CPU使用率%,P75 CPU使用率%,P90 CPU使用率%,P95 CPU使用率%,P99 CPU使用率%,平均内存使用率%,最大内存使用率%,P50内存使用率%,P75内存使用率%,P90内存使用率%,P95内存使用率%,P99内存使用率%,磁盘配置,系统盘使用率%,数据盘使用率%(最大),平均磁盘读IOPS,最大磁盘读IOPS,P50磁盘读IOPS,P75磁盘读IOPS,P90磁盘读IOPS,P95磁盘读IOPS,P99磁盘读IOPS,平均磁盘写IOPS,最大磁盘写IOPS,P50磁盘写IOPS,P75磁盘写IOPS,P90磁盘写IOPS,P95磁盘写IOPS,P99磁盘写IOPS,EIP带宽M,平均EIP入向流量Mbps,最大EIP入向流量Mbps,P50 EIP入向流量Mbps,P75 EIP入向流量Mbps,P90 EIP入向流量Mbps,P95 EIP入向流量Mbps,P99 EIP入向流量Mbps,平均EIP出向流量Mbps,最大EIP出向流量Mbps,P50 EIP出向流量Mbps,P75 EIP出向流量Mbps,P90 EIP出向流量Mbps,P95 EIP出向流量Mbps,P99 EIP出向流量Mbps" > "$filename"

echo "$json_data" | jq -r '
    .data.virtual_machines[] |
    [
        .departmentName,
        .tenantId,
        .ecsId,
        .displayName,
        .createdAt,
        .status,
        .vCpus,
        .ram,
        (.networkMappings | map(.ipv4Address) | join("|")),
        .eipMappings[0].eip_id,
        (.eipMappings | map(.ipAddress) | join("|")),
        (.eipMappings[0] | "\(.bandwidthInSize)|\(.bandwidthOutSize)"),
        (.blockDeviceMappings | map(.volumeSize) | join("|")),
        (.blockDeviceMappings | map(.volumeType) | join("|"))
    ] | @csv
' | while IFS=, read -r departmentName tenantId ecsId displayName createdAt status vcpu mem_size internal_ips eip_id eips eip_bandwidth volume_sizes volume_types; do
    department_level1=''
    department_level2=''
    department_level3=''
    ckeName=''
    cpu_usage=''
    mem_usage=''

    ((count++))
    
    if [[ "$volume_types" == *"local-pool"* ]]; then
        echo "${ecsId} 使用本地盘（如DM ECS），跳过统计"
        continue
    fi

    mem_size=$(awk -v bytes="$mem_size" 'BEGIN {print int((bytes / (1024^3)) + 0.999)}')

    tenantId=$(echo $tenantId | sed 's/"//g')
    ecsId=$(echo $ecsId | sed 's/"//g')
    internal_ips=$(echo $internal_ips | sed 's/"//g')
    eip_id=$(echo $eip_id | sed 's/"//g')
    eips=$(echo $eips | sed 's/"//g')
    eip_bandwidth=$(echo $eip_bandwidth | sed 's/"//g')
    displayName=$(echo $displayName | sed 's/"//g')
    volume_sizes=$(echo $volume_sizes | sed 's/"//g')
    disk_type=$(echo $disk_type | sed 's/"//g')
    createdAt=$(echo $createdAt | sed 's/"//g' | sed 's/\.[0-9]*[+].*//')

    department_name=$(get_res_department "$ecsId")
    if [[ -n "$department_name" ]]; then
        IFS='/' read -r department_level1 department_level2 department_level3 <<< "$department_name"
    else
        department_level1=$(echo $departmentName | sed 's/"//g')
    fi

    #CKE所属ECS节点关联CKE名称
    if [[ "$displayName" == "cke-"* ]]; then
        ckeId=$(echo $displayName | cut -d'-' -f1,2)
        ckeName=$(get_cke_cluster_name "$ckeId")
    fi

    #判断ECS运行状态
    if [[ "$status" != *"Running"* ]]; then
        ecsId=$ecsId"(非运行状态)"
        echo "$department_level1,$department_level2,$department_level3,$ecsId,$displayName,$createdAt,$ckeName,$vcpu"c"/$mem_size"g",$internal_ips,$eips,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,," >> "$filename"
        echo "进度完成 $count/$total_count"
        continue
    fi

    # CPU平均利用率（一周内）
    avg_cpu_query="avg_over_time((clamp_max(avg(vm_cpu_base_vCPUusage{name=\"${ecsId}\"})by(name,namespace),100)$day_of_week_58)[$period:30s])"
    cpu_usage=$(prometheus_query $avg_cpu_query)

    # CPU最大利用率（一周内）
    max_cpu_query="max_over_time((clamp_max(avg(vm_cpu_base_vCPUusage{name=\"${ecsId}\"})by(name,namespace),100)$day_of_week_58)[$period:30s])"
    cpu_usage+=','$(prometheus_query $max_cpu_query)

    # CPU P50利用率（一周内）
    p50_cpu_query="quantile_over_time(0.5,clamp_max(avg(vm_cpu_base_vCPUusage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    cpu_usage+=','$(prometheus_query $p50_cpu_query)

    # CPU P75利用率（一周内）
    p75_cpu_query="quantile_over_time(0.75,clamp_max(avg(vm_cpu_base_vCPUusage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    cpu_usage+=','$(prometheus_query $p75_cpu_query)
    
    # CPU P90利用率（一周内）
    p90_cpu_query="quantile_over_time(0.9,clamp_max(avg(vm_cpu_base_vCPUusage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    cpu_usage+=','$(prometheus_query $p90_cpu_query)

    # CPU P95利用率（一周内）
    p95_cpu_query="quantile_over_time(0.95,clamp_max(avg(vm_cpu_base_vCPUusage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    cpu_usage+=','$(prometheus_query $p95_cpu_query)

    # CPU P99利用率（一周内）
    p99_cpu_query="quantile_over_time(0.99,clamp_max(avg(vm_cpu_base_vCPUusage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    cpu_usage+=','$(prometheus_query $p99_cpu_query)



    # 内存平均利用率（一周内）
    avg_mem_query="avg_over_time((clamp_max(avg(vm_memory_usage{name=\"${ecsId}\"})by(name,__name__),100)$day_of_week_58)[$period:30s])"
    mem_usage=$(prometheus_query $avg_mem_query)

    # 内存最大利用率（一周内）
    max_mem_query="max_over_time((clamp_max(avg(vm_memory_usage{name=\"${ecsId}\"})by(name,__name__),100)$day_of_week_58)[$period:30s])"
    mem_usage+=','$(prometheus_query $max_mem_query)

    # 内存 P50利用率（一周内）
    p50_mem_query="quantile_over_time(0.5,clamp_max(avg(vm_memory_usage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    mem_usage+=','$(prometheus_query $p50_mem_query)

    # 内存 P75利用率（一周内）
    p75_mem_query="quantile_over_time(0.75,clamp_max(avg(vm_memory_usage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    mem_usage+=','$(prometheus_query $p75_mem_query)

    # 内存 P90利用率（一周内）
    p90_mem_query="quantile_over_time(0.9,clamp_max(avg(vm_memory_usage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    mem_usage+=','$(prometheus_query $p90_mem_query)

    # 内存 P95利用率（一周内）
    p95_mem_query="quantile_over_time(0.95,clamp_max(avg(vm_memory_usage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    mem_usage+=','$(prometheus_query $p95_mem_query)

    # 内存 P99利用率（一周内）
    p99_mem_query="quantile_over_time(0.99,clamp_max(avg(vm_memory_usage{name=\"${ecsId}\"}$day_of_week_58)by(name,namespace),100)[$period:30s])"
    mem_usage+=','$(prometheus_query $p99_mem_query)

    
    recv_mbps=''
    send_mbps=''
    if [[ -n "$eip_id" ]]; then
        # EIP入向流量Mbps
        recv_mbps=$(get_eip_input_data_rate $eip_id)

        # EIP出向流量Mbps
        send_mbps=$(get_eip_output_data_rate $eip_id)
    fi

    # 磁盘平均读速率（一周内）
    namespace="tenant-"$tenantId
    disk_read_iops=''
    avg_disk_read_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time(sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_read_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_read_iops=$(echo "$avg_disk_read_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')


    max_disk_read_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time(sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_read_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_read_iops+=','$(echo "$max_disk_read_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p50_disk_read_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.5,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_read_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_read_iops+=','$(echo "$p50_disk_read_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p75_disk_read_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.75,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_read_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_read_iops+=','$(echo "$p75_disk_read_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p90_disk_read_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_read_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_read_iops+=','$(echo "$p90_disk_read_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p95_disk_read_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.95,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_read_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_read_iops+=','$(echo "$p95_disk_read_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p99_disk_read_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_read_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_read_iops+=','$(echo "$p99_disk_read_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    #disk_read_iops=$(reorg_disk_info $disk_read_iops)

    # 磁盘平均读速率（一周内）
    namespace="tenant-"$tenantId
    disk_write_iops=''
    avg_disk_write_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time(sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_write_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_write_iops=$(echo "$avg_disk_write_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')


    max_disk_write_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time(sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_write_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_write_iops+=','$(echo "$max_disk_write_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p50_disk_write_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.5,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_write_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_write_iops+=','$(echo "$p50_disk_write_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p75_disk_write_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.75,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_write_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_write_iops+=','$(echo "$p75_disk_write_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p90_disk_write_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_write_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_write_iops+=','$(echo "$p90_disk_write_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p95_disk_write_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.95,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_write_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_write_iops+=','$(echo "$p95_disk_write_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')

    p99_disk_write_iops=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9,sum by(resourceId,resourceType,disk_name)(irate(kubevirt_vmi_storage_iops_write_total{resourceId=\"${namespace}_${ecsId}\"}[3m])$day_of_week_58)[$period:30s])")
    disk_write_iops+=','$(echo "$p99_disk_write_iops" | jq -r '.data.result | map("\(.metric.disk_name):\((.value[1] | tonumber | floor))") | join("|")')
    #disk_write_iops=$(reorg_disk_info $disk_write_iops)




    # 计算磁盘利用率
    disk_usage_rate=$(calc_disk_usage $ecsId)
    
    echo "$department_level1,$department_level2,$department_level3,$ecsId,$displayName,$createdAt,$ckeName,$vcpu"c"/$mem_size"g",$internal_ips,$eips,$cpu_usage,$mem_usage,$volume_sizes,$disk_usage_rate,$disk_read_iops,$disk_write_iops,$eip_bandwidth,$recv_mbps,$send_mbps" >> "$filename"

    echo "进度完成 $count/$total_count"
done



round(avg_over_time(kafka_cluster_bytes_in_one_minute_rate{instance_name="kafka-gbjiqe8yl9",tenant_id="tenant-1962328467399778053"}[168h] and (day_of_week() >= 1 and day_of_week() <= 5 and hour() >= 0 and hour() < 10))/1024/1024,0.1)

round(avg_over_time(kafka_cluster_bytes_in_one_minute_rate{instance_name="kafka-gbjiqe8yl9",tenant_id="tenant-1962328467399778053"}[168h])and (day_of_week() >= 1 and day_of_week() <= 5 and hour() >= 0 and hour() < 10)  # 再过滤时间条件
  / 1024 / 1024,
0.1
)