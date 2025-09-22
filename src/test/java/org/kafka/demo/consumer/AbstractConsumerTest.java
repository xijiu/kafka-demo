package org.kafka.demo.consumer;

import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.demo.tool.CommonTools;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public abstract class AbstractConsumerTest {

    private final ConsumerParams consumerParams = consumerParamsBuilder();

    protected ConsumerParams consumerParamsBuilder() {
        return ConsumerParams.builder().build();
    }

    protected KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(genericProperties());
    }

    protected KafkaConsumer<byte[], byte[]> createConsumerForByteArr() {
        return new KafkaConsumer<>(genericProperties());
    }


    private Properties genericProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, consumerParams.bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerParams.deserializerClass);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerParams.deserializerClass);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerParams.group);

        if (consumerParams.useSasl) {
            props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", consumerParams.username).replace("testPassWord", consumerParams.password));
        }

        props.putAll(consumerParams.specialProperties);
        return props;
    }

    /**
     * 简单输出一次消费到的消息内容，只包含消息的元数据等
     */
    protected <T1, T2> void printConsumerRecords(ConsumerRecords<T1, T2> consumerRecords) {
        if (consumerRecords.count() > 0) {
            for (ConsumerRecord<T1, T2> record : consumerRecords) {
                CommonTools.printlnWithTimestamp(String.format("receive msg, topic %s, partition %d, offset is %d, msg time %s",
                        record.topic(), record.partition(), record.offset(), CommonTools.formatTimestamp(record.timestamp())));
            }
        } else {
            CommonTools.printlnWithTimestamp("empty poll");
        }
    }

    @Builder
    @ToString
    protected static class ConsumerParams {
        @Builder.Default
        private String bootstrapServers = "localhost:9092";
        @Builder.Default
        private String group = "defaultGroup";
        @Builder.Default
        private Class<?> deserializerClass = StringDeserializer.class;
        @Builder.Default
        private boolean useSasl = false;
        @Builder.Default
        private String username = "";
        @Builder.Default
        private String password = "";
        @Builder.Default
        private Map<String, Object> specialProperties = new HashMap<>();
    }
}
