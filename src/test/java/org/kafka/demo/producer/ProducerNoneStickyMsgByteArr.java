package org.kafka.demo.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class ProducerNoneStickyMsgByteArr {

    private static final String BOOTSTRAP_SERVERS = "10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095";
    private static final boolean USE_SASL = false;
    private static final String USER_NAME = "kafka-egwiwwcls9";
    private static final String PASSWORD = "__CIPHER__V0uCjSXxAa1QMVNDn1fjyT46tfIq/OGDDlQ=";

    private static byte[] SINGLE_MSG = new byte[1024 * 1];


    @Test
    public void sendMsg() throws Exception {
        singleSendMsg();
    }

    private static void singleSendMsg() throws Exception {
        KafkaProducer<byte[], byte[]> kafkaProducer = createProducer();

        String topicName = "big_topic";
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topicName, SINGLE_MSG);
        send(kafkaProducer, record1);

        System.out.println("topic " + topicName + " send over!!!!");
        kafkaProducer.close();
    }

    private static void send(KafkaProducer<byte[], byte[]> kafkaProducer, ProducerRecord<byte[], byte[]> record) throws Exception {
        while (true) {
            for (int i = 0; i < 500; i++) {
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                        System.out.println("send error !!!");
                    } else {
                        System.out.printf(" send success!!! partition %s, offset is %s %n", metadata.partition(), metadata.offset());
                    }
                });
            }
            Thread.sleep(1000);
        }
    }

    public static KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024);
//        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1024");

        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.xxxxx.cmq.kafka.show.MyProducerInterceptors");
//        setSaslConfig(props);
        return new KafkaProducer<>(props);
    }

    public static void setSaslConfig(Properties properties) {
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", "kafka-ffzctvzpen").replace("testPassWord", "ffzctw06hp"));
    }
    




    private static String now() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        // 创建 DateTimeFormatter 对象，指定日期格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        // 将 LocalDateTime 对象格式化为字符串
        return currentDateTime.format(formatter);
    }
}
