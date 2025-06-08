package org.kafka.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public abstract class AbstractProducerTest {

    protected KafkaProducer<String, String> createProducer() {
        return createProducer("localhost:9092");
    }

    protected KafkaProducer<String, String> createProducer(String bootstrapServers) {
        return createProducer(bootstrapServers, false, null, null);
    }

    protected KafkaProducer<String, String> createProducer(String bootstrapServers, boolean useSasl, String username, String password) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.format("%d", 1024*1024*50));

        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        setSaslConfig(props, useSasl, username, password);
        return new KafkaProducer<>(props);
    }

    public static void setSaslConfig(Properties properties, boolean useSasl, String username, String password) {
        if (!useSasl) {
            return;
        }
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", username).replace("testPassWord", password));
    }
}
