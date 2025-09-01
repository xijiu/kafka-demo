#!/bin/bash
 
##########################################################################
# 作者：yangxinqi
# 版本：v2
# 日期：2024-10-28
# 描述：获取本地region中所有ES实例资源使用情况
##########################################################################
 
 
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
 
 
timestamp=$(date +"%Y%m%d_%H%M%S")
filename="./es_benchmark_${timestamp}.csv"
 
namespace_dbops="ccos-dbops"
route_dbops_names="dbops-service"
DBOPS_ROUTE=$(get_kubectl_route "$namespace_dbops" "${route_dbops_names}")
 
secretname=$(kubectl  get sa -n ccos-monitoring prometheus-k8s -o jsonpath='{range .secrets[*]}{.name}{"\n"}{end}' | grep prometheus-k8s-token-)
token="Bearer $(kubectl get secret $secretname -n ccos-monitoring -o template --template='{{.data.token}}' | base64 --decode)"
 
  # 获取Prometheus的接口请求地址
thanos_url=$(kubectl get route -n ccos-monitoring  thanos-querier-business --no-headers | awk '{print $2}')
  # 获取云资源中心的接口请求地址
#  local resource_center_agent_url=$(${command_} -n ccos-ops-resource-management get route resource-center-agent-route --no-headers|awk '{print$2}')
  # ecs的接口
 
json_data=$(curl -s -k "https://$DBOPS_ROUTE/api/dbops/v1/info/pageList?pageNum=1&pageSize=10000&serviceCode=ES")
total_count=$(echo "$json_data" | jq '.data.total')
count=0
 
echo "一级部门,二级部门,三级部门,实例 ID,实例名称,内网IP,弹性公网IP,规格(CPU|内存|节点数|磁盘）,平均CPU使用率%,最大CPU使用率%,P50 CPU使用率%,P75 CPU使用率%,P90 CPU使用率%,P95 CPU使用率%,P99 CPU使用率%,平均堆内存使用率%,最大堆内存使用率%,P50 堆内存使用率%,P75 堆内存使用率%,P90 堆内存使用率%,P95 堆内存使用率%,P99 堆内存使用率%,平均堆外内存使用率%,最大堆外内存使用率%,P50 堆外内存使用率%,P75 堆外内存使用率%,P90 堆外内存使用率%,P95 堆外内存使用率%,P99 堆外内存使用率%,平均内存使用率%,最大内存使用率%,P50 内存使用率%,P75 内存使用率%,P90 内存使用率%,P95 内存使用率%,P99 内存使用率%,磁盘使用率%,平均网络流入带宽Mbps,最大网络流入带宽Mbps,P50 网络流入带宽Mbps,P75 网络流入带宽Mbps,P90 网络流入带宽Mbps,P95 网络流入带宽Mbps,P99 网络流入带宽Mbps,平均网络流出带宽Mbps,最大网络流出带宽Mbps,P50 网络流出带宽Mbps,P75 网络流出带宽Mbps,P90 网络流出带宽Mbps,P95 网络流出带宽Mbps,P99 网络流出带宽Mbps" > "$filename"
 
echo "$json_data" | jq -r '
    .data.list[] |
    [
        .departmentName,
        .dbInstanceId,
        .name,
        .state
    ] | @csv
' | while IFS=, read -r departmentName dbInstanceId name state; do
    department_level1=''
    department_level2=''
    department_level3=''
 
    ((++count))
    instanceId=$(echo $dbInstanceId | sed 's/"//g')
    instanceDetailJsonData=$(curl -s -k "https://$DBOPS_ROUTE/api/dbops/v1/info/detail?dbInstanceId=$instanceId&serviceCode=ES") 
    displayName=$(echo $name | sed 's/"//g')
    internal_ips=$(echo "$instanceDetailJsonData" | jq '.data.connectionUrl')
    if [ ! -z "$internal_ips" ]; then
      internal_ips=$(echo $internal_ips | sed 's/"//g')
    fi
    eips=$(echo "$instanceDetailJsonData" | jq '.data.eipIp')
    if [ ! -z "$eips" ]; then
      eips=$(echo $eips | sed 's/"//g')
    fi
    instanceClass=$(echo "$instanceDetailJsonData" | jq '.data.dbInstanceClass')
    if [ ! -z "$instanceClass" ]; then
      instanceClass=$(echo $instanceClass | sed 's/"//g')
    fi
    cpuStr=$(echo "$instanceDetailJsonData" | jq '.data.cpu')Core
    memroyStr=$(echo "$instanceDetailJsonData" | jq '.data.memoryLimit')
    if [ ! -z "$memroyStr" ]; then
      memroyStr=$(echo $memroyStr | sed 's/"//g')
    fi
    disk=$(echo "$instanceDetailJsonData" | jq '.data.dbInstanceStorage')GiB
    nodeNum=$(echo "$instanceDetailJsonData" | jq '.data.nodeNum')
    department_name=$(get_res_department "$dbInstanceId")
    if [[ -n "$department_name" ]]; then
        IFS='/' read -r department_level1 department_level2 department_level3 <<< "$department_name"
    else
        department_level1=$(echo $departmentName | sed 's/"//g')
    fi
 
     
    # CPU平均利用率（一周内）
    cpu=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time((clamp_max(avg(cecloud_elasticsearch_node_cpu_usage_percent{instanceId=\"${instanceId}\"})by(instanceId,namespace),100))[7d:])")
    cpu_data_length=`echo "${cpu}"|jq '.data.result|length'`
    avg_cpu_rate='0'
    if [[ "${cpu_data_length}" -gt 0 ]]; then
        avg_cpu_rate=$(echo "${cpu}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        avg_cpu_rate=$(printf "%.2f" "$avg_cpu_rate")
    fi
    # CPU最大利用率（一周内）
    cpu=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time((clamp_max(avg(cecloud_elasticsearch_node_cpu_usage_percent{instanceId=\"${instanceId}\"})by(instanceId,namespace),100))[7d:])")
    cpu_data_length=`echo "${cpu}"|jq '.data.result|length'`
    max_cpu_rate='0'
    if [[ "${cpu_data_length}" -gt 0 ]]; then
        max_cpu_rate=$(echo "${cpu}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        max_cpu_rate=$(printf "%.2f" "$max_cpu_rate")
    fi
 
    # CPU P50利用率（一周内）
    cpu=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
        --data-urlencode "query=quantile_over_time(0.5, clamp_max(avg(cecloud_elasticsearch_node_cpu_usage_percent{instanceId=\"${instanceId}\"}) by (instanceId, namespace), 100)[7d:])")
    cpu_data_length=`echo "${cpu}"|jq '.data.result|length'`
    p50_cpu_rate='0'
    if [[ "${cpu_data_length}" -gt 0 ]]; then
        p50_cpu_rate=$(echo "${cpu}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p50_cpu_rate=$(printf "%.2f" "$p50_cpu_rate")
    fi    
  
    # CPU P75利用率（一周内）
    cpu=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
        --data-urlencode "query=quantile_over_time(0.75, clamp_max(avg(cecloud_elasticsearch_node_cpu_usage_percent{instanceId=\"${instanceId}\"}) by (instanceId, namespace), 100)[7d:])")
    cpu_data_length=`echo "${cpu}"|jq '.data.result|length'`
    p75_cpu_rate='0'
    if [[ "${cpu_data_length}" -gt 0 ]]; then
        p75_cpu_rate=$(echo "${cpu}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p75_cpu_rate=$(printf "%.2f" "$p75_cpu_rate")
    fi
 
    # CPU P90利用率（一周内）
    cpu=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
        --data-urlencode "query=quantile_over_time(0.9, clamp_max(avg(cecloud_elasticsearch_node_cpu_usage_percent{instanceId=\"${instanceId}\"}) by (instanceId, namespace), 100)[7d:])")
    cpu_data_length=`echo "${cpu}"|jq '.data.result|length'`
    p90_cpu_rate='0'
    if [[ "${cpu_data_length}" -gt 0 ]]; then
        p90_cpu_rate=$(echo "${cpu}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p90_cpu_rate=$(printf "%.2f" "$p90_cpu_rate")
    fi
 
    # CPU P95利用率（一周内）
    cpu=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
        --data-urlencode "query=quantile_over_time(0.95, clamp_max(avg(cecloud_elasticsearch_node_cpu_usage_percent{instanceId=\"${instanceId}\"}) by (instanceId, namespace), 100)[7d:])")
    cpu_data_length=`echo "${cpu}"|jq '.data.result|length'`
    p95_cpu_rate='0'
    if [[ "${cpu_data_length}" -gt 0 ]]; then
        p95_cpu_rate=$(echo "${cpu}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p95_cpu_rate=$(printf "%.2f" "$p95_cpu_rate")
    fi 
 
    # CPU P99利用率（一周内）
    cpu=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
        --data-urlencode "query=quantile_over_time(0.99, clamp_max(avg(cecloud_elasticsearch_node_cpu_usage_percent{instanceId=\"${instanceId}\"}) by (instanceId, namespace), 100)[7d:])")
    cpu_data_length=`echo "${cpu}"|jq '.data.result|length'`
    p99_cpu_rate='0'
    if [[ "${cpu_data_length}" -gt 0 ]]; then
        p99_cpu_rate=$(echo "${cpu}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p99_cpu_rate=$(printf "%.2f" "$p99_cpu_rate")
    fi
 
    # 堆内存平均利用率（一周内）
    heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time((clamp_max(avg(sum(cecloud_elasticsearch_node_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId),100))[7d:])")
   heap_mem_data_length=`echo "${heap_mem}"|jq '.data.result|length'`
    avg_heap_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        avg_heap_mem_rate=$(echo "${heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        avg_heap_mem_rate=$(printf "%.2f" "$avg_heap_mem_rate")
    fi
 
    # 堆内存最大利用率（一周内）
   heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time((clamp_max(avg(sum(cecloud_elasticsearch_node_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId),100))[7d:])")
   heap_mem_data_length=`echo "${heap_mem}"|jq '.data.result|length'`
    max_heap_mem_rate='0'
    if [[ "${heap_mem_data_length}" -gt 0 ]]; then
        max_heap_mem_rate=$(echo "${heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        max_heap_mem_rate=$(printf "%.2f" "$max_heap_mem_rate")
    fi
 
    # 堆内存 P50利用率（一周内）
   heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.5, clamp_max(avg(sum(cecloud_elasticsearch_node_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   heap_mem_data_length=`echo "${heap_mem}"|jq '.data.result|length'`
    p50_heap_mem_rate='0'
    if [[ "${heap_mem_data_length}" -gt 0 ]]; then
        p50_heap_mem_rate=$(echo "${heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p50_heap_mem_rate=$(printf "%.2f" "$p50_heap_mem_rate")
    fi
 
    # 堆内存 P75利用率（一周内）
   heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.75, clamp_max(avg(sum(cecloud_elasticsearch_node_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   heap_mem_data_length=`echo "${heap_mem}"|jq '.data.result|length'`
    p75_heap_mem_rate='0'
    if [[ "${heap_mem_data_length}" -gt 0 ]]; then
        p75_heap_mem_rate=$(echo "${heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p75_heap_mem_rate=$(printf "%.2f" "$p75_heap_mem_rate")
    fi
         
   # 堆内存 P90利用率（一周内）
   heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9, clamp_max(avg(sum(cecloud_elasticsearch_node_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   heap_mem_data_length=`echo "${heap_mem}"|jq '.data.result|length'`
    p90_heap_mem_rate='0'
    if [[ "${heap_mem_data_length}" -gt 0 ]]; then
        p90_heap_mem_rate=$(echo "${heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p90_heap_mem_rate=$(printf "%.2f" "$p90_heap_mem_rate")
    fi
     
   # 堆内存 P95利用率（一周内）
   heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.95, clamp_max(avg(sum(cecloud_elasticsearch_node_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   heap_mem_data_length=`echo "${heap_mem}"|jq '.data.result|length'`
    p95_heap_mem_rate='0'
    if [[ "${heap_mem_data_length}" -gt 0 ]]; then
        p95_heap_mem_rate=$(echo "${heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p95_heap_mem_rate=$(printf "%.2f" "$p95_heap_mem_rate")
    fi       
 
    # 堆内存 P99利用率（一周内）
   heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.99, clamp_max(avg(sum(cecloud_elasticsearch_node_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   heap_mem_data_length=`echo "${heap_mem}"|jq '.data.result|length'`
    p99_heap_mem_rate='0'
    if [[ "${heap_mem_data_length}" -gt 0 ]]; then
        p99_heap_mem_rate=$(echo "${heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p99_heap_mem_rate=$(printf "%.2f" "$p99_heap_mem_rate")
    fi
 
    # 堆外内存平均利用率（一周内）
    off_heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time((clamp_max(avg(sum(cecloud_elasticsearch_node_off_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_off_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId),100))[7d:])")
   off_heap_mem_data_length=`echo "${off_heap_mem}"|jq '.data.result|length'`
    avg_off_heap_mem_rate='0'
    if [[ "${off_mem_data_length}" -gt 0 ]]; then
        avg_off_heap_mem_rate=$(echo "${off_heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        avg_off_heap_mem_rate=$(printf "%.2f" "$avg_off_heap_mem_rate")
    fi
 
    # 堆外内存最大利用率（一周内）
   off_heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time((clamp_max(avg(sum(cecloud_elasticsearch_node_off_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_off_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId),100))[7d:])")
   off_heap_mem_data_length=`echo "${off_heap_mem}"|jq '.data.result|length'`
    max_off_heap_mem_rate='0'
    if [[ "${off_heap_mem_data_length}" -gt 0 ]]; then
        max_off_heap_mem_rate=$(echo "${off_heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        max_off_heap_mem_rate=$(printf "%.2f" "$max_off_heap_mem_rate")
    fi
 
    # 堆外内存 P50利用率（一周内）
   off_heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.5, clamp_max(avg(sum(cecloud_elasticsearch_node_off_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_off_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   off_heap_mem_data_length=`echo "${off_heap_mem}"|jq '.data.result|length'`
    p50_off_heap_mem_rate='0'
    if [[ "${off_heap_mem_data_length}" -gt 0 ]]; then
        p50_off_heap_mem_rate=$(echo "${off_heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p50_off_heap_mem_rate=$(printf "%.2f" "$p50_off_heap_mem_rate")
    fi
 
    # 堆外内存 P75利用率（一周内）
   off_heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.75, clamp_max(avg(sum(cecloud_elasticsearch_node_off_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_off_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   off_heap_mem_data_length=`echo "${off_heap_mem}"|jq '.data.result|length'`
    p75_off_heap_mem_rate='0'
    if [[ "${off_heap_mem_data_length}" -gt 0 ]]; then
        p75_off_heap_mem_rate=$(echo "${off_heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p75_off_heap_mem_rate=$(printf "%.2f" "$p75_off_heap_mem_rate")
    fi
 
    # 堆外内存 P90利用率（一周内）
   off_heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9, clamp_max(avg(sum(cecloud_elasticsearch_node_off_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_off_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   off_heap_mem_data_length=`echo "${off_heap_mem}"|jq '.data.result|length'`
    p90_off_heap_mem_rate='0'
    if [[ "${off_heap_mem_data_length}" -gt 0 ]]; then
        p90_off_heap_mem_rate=$(echo "${off_heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p90_off_heap_mem_rate=$(printf "%.2f" "$p90_off_heap_mem_rate")
    fi
 
    # 堆外内存 P95利用率（一周内）
   off_heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.95, clamp_max(avg(sum(cecloud_elasticsearch_node_off_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_off_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   off_heap_mem_data_length=`echo "${off_heap_mem}"|jq '.data.result|length'`
    p95_off_heap_mem_rate='0'
    if [[ "${off_heap_mem_data_length}" -gt 0 ]]; then
        p95_off_heap_mem_rate=$(echo "${off_heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p95_off_heap_mem_rate=$(printf "%.2f" "$p95_off_heap_mem_rate")
    fi
 
   # 堆外内存 P99利用率（一周内）
   off_heap_mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.99, clamp_max(avg(sum(cecloud_elasticsearch_node_off_heap_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_off_heap_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
   off_heap_mem_data_length=`echo "${off_heap_mem}"|jq '.data.result|length'`
    p99_off_heap_mem_rate='0'
    if [[ "${off_heap_mem_data_length}" -gt 0 ]]; then
        p99_off_heap_mem_rate=$(echo "${off_heap_mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p99_off_heap_mem_rate=$(printf "%.2f" "$p99_off_heap_mem_rate")
    fi
 
   # 内存平均利用率（一周内）
    mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time((clamp_max(avg(sum(cecloud_elasticsearch_node_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId),100))[7d:])")
    mem_data_length=`echo "${mem}"|jq '.data.result|length'`
    avg_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        avg_mem_rate=$(echo "${mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        avg_mem_rate=$(printf "%.2f" "$avg_mem_rate")
    fi
 
    # 内存最大利用率（一周内）
    mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time((clamp_max(avg(sum(cecloud_elasticsearch_node_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId),100))[7d:])")
    mem_data_length=`echo "${mem}"|jq '.data.result|length'`
    max_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        max_mem_rate=$(echo "${mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        max_mem_rate=$(printf "%.2f" "$max_mem_rate")
    fi     
     
    # 内存 P50利用率（一周内）
    mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.5, clamp_max(avg(sum(cecloud_elasticsearch_node_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
    mem_data_length=`echo "${mem}"|jq '.data.result|length'`
    p50_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        p50_mem_rate=$(echo "${mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p50_mem_rate=$(printf "%.2f" "$p50_mem_rate")
    fi
 
    # 内存 P75利用率（一周内）
    mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.75, clamp_max(avg(sum(cecloud_elasticsearch_node_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
    mem_data_length=`echo "${mem}"|jq '.data.result|length'`
    p75_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        p75_mem_rate=$(echo "${mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p75_mem_rate=$(printf "%.2f" "$p75_mem_rate")
    fi
 
    # 内存 P90利用率（一周内）
    mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9, clamp_max(avg(sum(cecloud_elasticsearch_node_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
    mem_data_length=`echo "${mem}"|jq '.data.result|length'`
    p90_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        p90_mem_rate=$(echo "${mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p90_mem_rate=$(printf "%.2f" "$p90_mem_rate")
    fi
     
    # 内存 P95利用率（一周内）
    mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.95, clamp_max(avg(sum(cecloud_elasticsearch_node_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
    mem_data_length=`echo "${mem}"|jq '.data.result|length'`
    p95_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        p95_mem_rate=$(echo "${mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p95_mem_rate=$(printf "%.2f" "$p95_mem_rate")
    fi
 
   # 内存 P99利用率（一周内）
    mem=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.99, clamp_max(avg(sum(cecloud_elasticsearch_node_memory_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_memory_all_bytes{instanceId=\"${instanceId}\"})*100)by(instanceId), 100)[7d:])")
    mem_data_length=`echo "${mem}"|jq '.data.result|length'`
    p99_mem_rate='0'
    if [[ "${mem_data_length}" -gt 0 ]]; then
        p99_mem_rate=$(echo "${mem}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p99_mem_rate=$(printf "%.2f" "$p99_mem_rate")
    fi
 
 
    # 磁盘使用率
    disk_usage_rate_query="sum(cecloud_elasticsearch_node_disk_usage_bytes{instanceId=\"${instanceId}\"})/sum(cecloud_elasticsearch_node_disk_all_bytes{instanceId=\"${instanceId}\"})"
    disk_usage=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" --data-urlencode "query=${disk_usage_rate_query}")
    disk_usage_length=`echo "${disk_usage}"|jq '.data.result|length'`
    disk_usage_rate=''
    if [[ "${disk_usage_length}" -gt 0 ]]; then
        disk_usage_rate=$(echo "${disk_usage}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        disk_usage_rate=$(printf "%.2f" "$disk_usage_rate")
    fi
     
    # 网络平均进入流量mbps（一周内）
    net_in=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time(avg(sum(rate(cecloud_elasticsearch_node_net_in_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_in_length=`echo "${net_in}"|jq '.data.result|length'`
    avg_net_in_rate='0'
    if [[ "${net_in_length}" -gt 0 ]]; then
        avg_net_in_rate=$(echo "${net_in}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        avg_net_in_rate=${avg_net_in_rate}
        avg_net_in_rate=$(printf "%.2f" "$avg_net_in_rate")
    fi
 
    # 网络最大进入流量mbps（一周内）
    net_in=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time(avg(sum(rate(cecloud_elasticsearch_node_net_in_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_in_length=`echo "${net_in}"|jq '.data.result|length'`
    max_net_in_rate='0'
    if [[ "${net_in_length}" -gt 0 ]]; then
        max_net_in_rate=$(echo "${net_in}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        max_net_in_rate=${max_net_in_rate}
        max_net_in_rate=$(printf "%.2f" "$max_net_in_rate")
    fi
 
    # 网络P50进入流量mbps（一周内）
    net_in=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.5, avg(sum(rate(cecloud_elasticsearch_node_net_in_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_in_length=`echo "${net_in}"|jq '.data.result|length'`
    p50_net_in_rate='0'
    if [[ "${net_in_length}" -gt 0 ]]; then
        p50_net_in_rate=$(echo "${net_in}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p50_net_in_rate=${p50_net_in_rate}
        p50_net_in_rate=$(printf "%.2f" "$p50_net_in_rate")
    fi
 
    # 网络P75进入流量mbps（一周内）
    net_in=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.75, avg(sum(rate(cecloud_elasticsearch_node_net_in_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_in_length=`echo "${net_in}"|jq '.data.result|length'`
    p75_net_in_rate='0'
    if [[ "${net_in_length}" -gt 0 ]]; then
        p75_net_in_rate=$(echo "${net_in}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p75_net_in_rate=${p75_net_in_rate}
        p75_net_in_rate=$(printf "%.2f" "$p75_net_in_rate")
    fi 
 
    # 网络P90进入流量mbps（一周内）
    net_in=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9, avg(sum(rate(cecloud_elasticsearch_node_net_in_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_in_length=`echo "${net_in}"|jq '.data.result|length'`
    p90_net_in_rate='0'
    if [[ "${net_in_length}" -gt 0 ]]; then
        p90_net_in_rate=$(echo "${net_in}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p90_net_in_rate=${p90_net_in_rate}
        p90_net_in_rate=$(printf "%.2f" "$p90_net_in_rate")
    fi
     
    # 网络P95进入流量mbps（一周内）
    net_in=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.95, avg(sum(rate(cecloud_elasticsearch_node_net_in_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_in_length=`echo "${net_in}"|jq '.data.result|length'`
    p95_net_in_rate='0'
    if [[ "${net_in_length}" -gt 0 ]]; then
        p95_net_in_rate=$(echo "${net_in}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p95_net_in_rate=${p95_net_in_rate}
        p95_net_in_rate=$(printf "%.2f" "$p95_net_in_rate")
    fi
 
   # 网络P99进入流量mbps（一周内）
    net_in=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.99, avg(sum(rate(cecloud_elasticsearch_node_net_in_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_in_length=`echo "${net_in}"|jq '.data.result|length'`
    p99_net_in_rate='0'
    if [[ "${net_in_length}" -gt 0 ]]; then
        p99_net_in_rate=$(echo "${net_in}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p99_net_in_rate=${p99_net_in_rate}
        p99_net_in_rate=$(printf "%.2f" "$p99_net_in_rate")
    fi
     
     # 网络平均流出流量mbps（一周内）
    net_out=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=avg_over_time(avg(sum(rate(cecloud_elasticsearch_node_net_out_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_out_length=`echo "${net_out}"|jq '.data.result|length'`
    avg_net_out_rate='0'
    if [[ "${net_out_length}" -gt 0 ]]; then
        avg_net_out_rate=$(echo "${net_out}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        avg_net_out_rate=${avg_net_out_rate}
        avg_net_out_rate=$(printf "%.2f" "$avg_net_out_rate")
    fi
 
    # 网络最大流出流量mbps（一周内）
    net_out=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=max_over_time(avg(sum(rate(cecloud_elasticsearch_node_net_out_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_out_length=`echo "${net_out}"|jq '.data.result|length'`
    max_net_out_rate='0'
    if [[ "${net_out_length}" -gt 0 ]]; then
        max_net_out_rate=$(echo "${net_out}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        max_net_out_rate=${max_net_out_rate}
        max_net_out_rate=$(printf "%.2f" "$max_net_out_rate")
    fi
 
    # 网络P50流出流量mbps（一周内）
    net_out=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.5, avg(sum(rate(cecloud_elasticsearch_node_net_out_bytes{instanceId=\"${instanceId}\"}))/1024/1024)by(instanceId)[7d:])")
    net_out_length=`echo "${net_out}"|jq '.data.result|length'`
    p50_net_out_rate='0'
    if [[ "${net_out_length}" -gt 0 ]]; then
        p50_net_out_rate=$(echo "${net_out}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p50_net_out_rate=${p50_net_out_rate}
        p50_net_out_rate=$(printf "%.2f" "$p50_net_out_rate")
    fi
 
    # 网络P75流出流量mbps（一周内）
    net_out=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.75, avg(sum(rate(cecloud_elasticsearch_node_net_out_bytes{instanceId=\"${instanceId}\"}))/1024/1024)by(instanceId)[7d:])")
    net_out_length=`echo "${net_out}"|jq '.data.result|length'`
    p75_net_out_rate='0'
    if [[ "${net_out_length}" -gt 0 ]]; then
        p75_net_out_rate=$(echo "${net_out}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p75_net_out_rate=${p75_net_out_rate}
        p75_net_out_rate=$(printf "%.2f" "$p75_net_out_rate")
    fi 
 
    # 网络P90流出流量mbps（一周内）
    net_out=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.9, avg(sum(rate(cecloud_elasticsearch_node_net_out_bytes{instanceId=\"${instanceId}\"}))/1024/1024)by(instanceId)[7d:])")
    net_out_length=`echo "${net_out}"|jq '.data.result|length'`
    p90_net_out_rate='0'
    if [[ "${net_out_length}" -gt 0 ]]; then
        p90_net_out_rate=$(echo "${net_out}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p90_net_out_rate=${p90_net_out_rate}
        p90_net_out_rate=$(printf "%.2f" "$p90_net_out_rate")
    fi
     
    # 网络P95流出流量mbps（一周内）
    net_out=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.95, avg(sum(rate(cecloud_elasticsearch_node_net_out_bytes{instanceId=\"${instanceId}\"}))/1024/1024)by(instanceId)[7d:])")
    net_out_length=`echo "${net_out}"|jq '.data.result|length'`
    p95_net_out_rate='0'
    if [[ "${net_out_length}" -gt 0 ]]; then
        p95_net_out_rate=$(echo "${net_out}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p95_net_out_rate=${p95_net_out_rate}
        p95_net_out_rate=$(printf "%.2f" "$p95_net_out_rate")
    fi 
 
   # 网络P99流出流量mbps（一周内）
    net_out=$(curl -H "Authorization: $token" -skG "https://${thanos_url}/api/v1/query" \
          --data-urlencode "query=quantile_over_time(0.99, avg(sum(rate(cecloud_elasticsearch_node_net_out_bytes{instanceId=\"${instanceId}\"}[2m]))/1024/1024)by(instanceId)[7d:])")
    net_out_length=`echo "${net_out}"|jq '.data.result|length'`
    p99_net_out_rate='0'
    if [[ "${net_out_length}" -gt 0 ]]; then
        p99_net_out_rate=$(echo "${net_out}" | jq '.data.result[0].value[1]' | sed 's/"//g')
        p99_net_out_rate=${p99_net_out_rate}
        p99_net_out_rate=$(printf "%.2f" "$p99_net_out_rate")
    fi
     
     
    echo "$department_level1,$department_level2,$department_level3,$instanceId,$displayName,$internal_ips,$eips,$instanceClass|$cpuStr|$memroyStr|$nodeNum|$disk,$avg_cpu_rate,$max_cpu_rate,$p50_cpu_rate,$p75_cpu_rate,$p90_cpu_rate,$p95_cpu_rate,$p99_cpu_rate,$avg_heap_mem_rate,$max_heap_mem_rate,$p50_heap_mem_rate,$p75_heap_mem_rate,$p90_heap_mem_rate,$p95_heap_mem_rate,$p99_heap_mem_rate,$avg_off_heap_mem_rate,$max_off_heap_mem_rate,$p50_off_heap_mem_rate,$p75_off_heap_mem_rate,$p90_off_heap_mem_rate,$p95_off_heap_mem_rate,$p99_off_heap_mem_rate,$avg_mem_rate,$max_mem_rate,$p50_mem_rate,$p75_mem_rate,$p90_mem_rate,$p95_mem_rate,$p99_mem_rate,$disk_usage_rate,$avg_net_in_rate,$max_net_in_rate,$p50_net_in_rate,$p75_net_in_rate,$p90_net_in_rate,$p95_net_in_rate,$p99_net_in_rate,$avg_net_out_rate,$max_net_out_rate,$p50_net_out_rate,$p75_net_out_rate,$p90_net_out_rate,$p95_net_out_rate,$p99_net_out_rate" >> "$filename"
 
    echo "进度完成 $count/$total_count"
done
