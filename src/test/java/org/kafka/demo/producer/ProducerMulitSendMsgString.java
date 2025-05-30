package org.kafka.demo.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class ProducerMulitSendMsgString {

    private static final String TOPIC_NAME = "topicA";
    private static final String BOOTSTRAP_SERVERS = "10.253.246.21:9095,10.253.246.14:9095,10.253.246.13:9095";
    private static final boolean USE_SASL = false;
    private static final String USER_NAME = "kafka-egwiwwcls9";
    private static final String PASSWORD = "__CIPHER__V0uCjSXxAa1QMVNDn1fjyT46tfIq/OGDDlQ=";

    @Test
    public void sendMsg() throws Exception {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            threads.add(new Thread(() -> {
                KafkaProducer<String, String> kafkaProducer = createProducer();
                try {
                    send(kafkaProducer);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static void send(KafkaProducer<String, String> kafkaProducer) throws Exception {
        String msgContent = "abcasdfasdfasdfljflasjdflajdflasjdfladlasfj8328239293892389238982398293839238923829382382";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, msgContent.toString());
        while (true) {
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                    System.out.println("send error !!!");
                } else {
                    System.out.printf("send success!!! topic %s, partition %s, offset %s, timestamp %s %n",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                }
            });
            Thread.sleep(10);
        }
    }

    public static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.format("%d", 1024*1024*50));

        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        setSaslConfig(props);
        return new KafkaProducer<>(props);
    }

    public static void setSaslConfig(Properties properties) {
        if (!USE_SASL) {
            return;
        }
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", "kafka-ffzctvzpen").replace("testPassWord", "ffzctw06hp"));
    }
}
