package org.kafka.demo.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.kafka.demo.KafkaClientMetricStat;
import org.kafka.demo.tool.CommonTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaConsumerSimpleMultiTopicTest {

    private static final String BOOTSTRAP_SERVERS = "10.253.246.12:9095,10.253.246.11:9095,10.253.246.10:9095";
    private static final boolean USE_SASL = false;
    private static final Set<String> TOPIC_NAMES = Stream.of(
            "topic1", "topic2", "topic3"
    ).collect(Collectors.toSet());
    private static final String GROUP_NAME = "group1";
    private static final String USER_NAME = "kafka-egwiwwcls9";
    private static final String PASSWORD = "__CIPHER__V0uCjSXxAa1QMVNDn1fjyT46tfIq/OGDDlQ=";

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerSimpleMultiTopicTest.class);

    @Test
    public void test2() {
        KafkaClientMetricStat.start();
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumer()) {
            kafkaConsumer.subscribe(TOPIC_NAMES);

            while (true) {
                ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                if (consumerRecords.count() > 0) {
                    for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
//                        System.out.printf("receive msg, topic %s, partition %d, offset is %d, msg time %s, current time %s %n",
//                                record.topic(), record.partition(), record.offset(), CommonTools.formatTimestamp(record.timestamp()), CommonTools.now());
                    }
                } else {
                    System.out.println("empty poll");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1");
//        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
//        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.cestc.cmq.kafka.show.MyConsumerInterceptors");
        setSaslConfig(props);
        return new KafkaConsumer<>(props);
    }

    public static void setSaslConfig(Properties properties) {
        if (USE_SASL) {
            properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", "kafka-ej0klvnizo").replace("testPassWord", "ej0klvnzcq"));
        }
    }



}
