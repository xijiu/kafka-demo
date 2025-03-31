#!/bin/bash

# 批量删除所有topic
# 如果没有开启SASL，那么执行命令 sh delete_all_topics.sh
# 如果开启SASL，那么需要添加此集群的账号密码，执行命令 sh delete_all_topics.sh kafka-f2hfo7ndk2 f2hfo7nun4

KAFKA_HOME=/opt/bitnami/kafka
BROKER=localhost:9092

# unset端口，让5555可用
unset JMX_PORT
unset KAFKA_OPTS
unset KAFKA_JMX_OPTS

admin_config="$KAFKA_HOME/config/admin.properties"
username=""
password=""
useSasl=false
if [ "$#" -lt 2 ]; then
  echo "username and password not provided. Skipping SASL."
else
  useSasl=true
  cat <<EOF > $admin_config
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$1" password="$2";
EOF
  echo "use SASL, and file '$admin_config' created successfully."
fi


# 构建基础命令
base_cmd="$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BROKER --list"
base_delete_cmd="$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BROKER"
if $useSasl; then
  base_cmd="$base_cmd --command-config $admin_config"
  base_delete_cmd="$base_delete_cmd --command-config $admin_config"
fi

echo "list topic cmd is $base_cmd"

delete_topic_num=0
delete_topic_content=""
# 遍历所有Topic并删除
while read -r topic; do
  if [[ "$topic" == __* ]]; then
    echo "Skipping system topic: $topic"
    continue
  fi
  ((delete_topic_num++))
  if [[ "$delete_topic_content" == "" ]]; then
    delete_topic_content=$topic
  else
    delete_topic_content="$delete_topic_content,$topic"
  fi
done < <($base_cmd)

echo "final: delete_topic_num is $delete_topic_num"

if [ $delete_topic_num -eq 0 ]; then
  echo "topic number is 0, it going to exit"
  exit
fi

echo "going to delete topic $delete_topic_content"
$base_delete_cmd --delete --topic "$delete_topic_content"
echo ""
echo "delete topic finished, delete topic number $delete_topic_num"