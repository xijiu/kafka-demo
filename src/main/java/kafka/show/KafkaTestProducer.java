package kafka.show;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class KafkaTestProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTestProducer.class);
    private static byte[] SINGLE_MSG = new byte[1024];


    public static void main(String[] args) throws Exception {
        sendMsg();
    }
    
    public static void sendMsg() throws Exception {
        singleSendMsg();
    }

    public static KafkaProducer<byte[], String> createProducer() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.253.246.41:9095,10.253.246.42:9095,10.253.246.43:9095,10.253.246.5:9095");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-sync-send-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.format("%d", 1024*1024*10));

        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.cestc.cmq.kafka.show.MyProducerInterceptors");
//        setSaslConfig(props);
        return new KafkaProducer<>(props);
    }

    public static void setSaslConfig(Properties properties) {
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", "kafka-ffzctvzpen").replace("testPassWord", "ffzctw06hp"));
    }
    
    private static void singleSendMsg() throws Exception {
        KafkaProducer<byte[], String> kafkaProducer = createProducer();

        String topicName = "test1";
        ProducerRecord<byte[], String> record1 = new ProducerRecord<>(topicName, "producer_test");
        send(kafkaProducer, record1);

        System.out.println("topic " + topicName + " send over!!!!");
        kafkaProducer.close();
    }

    private static void send(KafkaProducer<byte[], String> kafkaProducer, ProducerRecord<byte[], String> record) throws Exception {
        while (true) {
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                    log.info("send error !!!");
                } else {
                    log.info(" send success!!! offset is {}", metadata.offset());
                }
            });
            Thread.sleep(10);
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
