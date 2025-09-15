package org.kafka.demo.producer;

import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public abstract class AbstractProducerTest {

    private final ProducerParams producerParamsBuilder = producerParamsBuilder();

    protected ProducerParams producerParamsBuilder() {
        return ProducerParams.builder().build();
    }

    protected KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(genericProperties());
    }

    protected KafkaProducer<byte[], byte[]> createProducerForByteArr() {
        return new KafkaProducer<>(genericProperties());
    }


    private Properties genericProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, producerParamsBuilder.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerParamsBuilder.serializerClass);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerParamsBuilder.serializerClass);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.format("%d", 1024*1024*50));

        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        if (producerParamsBuilder.useSasl) {
            props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", producerParamsBuilder.username).replace("testPassWord", producerParamsBuilder.password));
        }

        props.putAll(producerParamsBuilder.specialProperties);
        return props;
    }

    @Builder
    @ToString
    protected static class ProducerParams {
        @Builder.Default
        private String bootstrapServers = "localhost:9092";
        @Builder.Default
        private Class<?> serializerClass = StringSerializer.class;
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
