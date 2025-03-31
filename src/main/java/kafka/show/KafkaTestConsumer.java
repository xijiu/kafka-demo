package kafka.show;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

public class KafkaTestConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestConsumer.class);


    public static void main(String[] args) throws InterruptedException {
        consume();
    }

    private static KafkaConsumer<byte[], byte[]> createConsumer(int groupId) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.253.248.3:9095,10.253.248.9:9095,10.253.120.108:9095");
//        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + groupId);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
//        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.cestc.cmq.kafka.show.MyConsumerInterceptors");
        setSaslConfig(props);
        return new KafkaConsumer<>(props);
    }

    public static void setSaslConfig(Properties properties) {
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", "kafka-ej0klvnizo").replace("testPassWord", "ej0klvnzcq"));
    }


    public static void consume() throws InterruptedException {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumer(1);
        String theTopic = "topic2";

//        TopicPartition topicPartition = new TopicPartition(theTopic, 0);
//        kafkaConsumer.assign(Collections.singleton(topicPartition));
//        kafkaConsumer.seek(topicPartition, 0);

        kafkaConsumer.subscribe(Collections.singletonList(theTopic));

        while (true) {
            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() > 0) {
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    System.out.println(now() + " receive msg!!! offset is " + record.offset() + ", partition " + record.partition());
                }
            } else {
                log.info("empty poll");
            }
        }
    }

    private static String now() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        // 创建 DateTimeFormatter 对象，指定日期格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        // 将 LocalDateTime 对象格式化为字符串
        return currentDateTime.format(formatter);
    }
}
