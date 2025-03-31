package kafka.best_practice;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaTestConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestConsumer.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // 设置Kafka集群的接入点，多个IP用逗号隔开
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.253.246.46:9095,10.253.246.47:9095,10.253.246.43:9095");
        // 如果消息带有key，那么需要设置序列化方式，推荐使用 ByteArraySerializer 以提高性能
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 消息value的序列化方式，推荐使用 ByteArraySerializer 以提高性能
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 是否开启自动提交位点
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 设置消费组，用于存储位点信息
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group3");

        // 注意：如果实例开启了SASL，那么还需要设置以下3个配置
        // 配置SASL的认证机制，当前集群256、512均支持
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        // 配置加密协议，如果开启了SASL且没有开启TLS的话，需要将其设置为SASL_PLAINTEXT
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        // SASL的具体配置信息，需要把账号及密码替换为真实的账号密码
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username='user1' password='user1';");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        String theTopic = "t5";
        kafkaConsumer.subscribe(Collections.singletonList(theTopic));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() > 0) {
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("topic: {}", record.topic());
                    log.info("partition: {}", record.partition());
                    log.info("offset: {}", record.offset());
                    log.info("msg key: {}", record.key());
                    log.info("msg value: {}", record.value());
                }
            } else {
                log.info("empty poll");
            }
        }
    }
}
