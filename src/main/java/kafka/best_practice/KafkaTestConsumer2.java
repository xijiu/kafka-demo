package kafka.best_practice;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaTestConsumer2 {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestConsumer2.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // 设置Kafka集群的接入点，多个IP用逗号隔开
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.253.246.47:9095,10.253.246.48:9095,10.253.246.46:9095");
        // 如果消息带有key，那么需要设置序列化方式，推荐使用 ByteArraySerializer 以提高性能
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 消息value的序列化方式，推荐使用 ByteArraySerializer 以提高性能
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 设置默认情况下，在什么位置开始消费消息
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 是否开启自动提交位点
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        // 是否开启自动创建Topic
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        // 设置消费组，用于存储位点信息
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        // 每次拉取消息时，在不超过超时的情况下，最小拉取的消息总大小
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        // 每次拉取消息时，在不超过超时的情况下，最大拉取的消息总大小
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1000000");
        // 执行poll()命令的时候，一次最多poll的消息条数
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.cestc.cmq.kafka.show.MyConsumerInterceptors");

//        // 注意：如果实例开启了SASL，那么还需要设置以下3个配置
//        // 配置SASL的认证机制，当前集群256、512均支持
//        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
//        // 配置加密协议，如果开启了SASL且没有开启TLS的话，需要将其设置为SASL_PLAINTEXT
//        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//        // SASL的具体配置信息，需要把账号及密码替换为真实的账号密码
//        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        List<String> topics = Collections.singletonList("topic1");
        kafkaConsumer.subscribe(topics);


        List<TopicPartition> partitionList = Collections.singletonList(new TopicPartition("theTopic", 0));
        // 消费暂停
        kafkaConsumer.pause(partitionList);
        // 消费恢复
        kafkaConsumer.resume(partitionList);

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() > 0) {
                consumerRecords.iterator().forEachRemaining(record -> {
                    System.out.println("offset: " + record.offset() + ", value " + record.value());
                });
            } else {
                log.info("empty poll");
            }
        }
    }
}
