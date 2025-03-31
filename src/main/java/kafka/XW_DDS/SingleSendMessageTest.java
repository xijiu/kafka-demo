package kafka.XW_DDS;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class SingleSendMessageTest {
    
    private static byte[] SINGLE_MSG = new byte[1024];


    public static void main(String[] args) {
        sendMsg();
    }
    
    public static void sendMsg() {
        singleSendMsg();
    }

    public static KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.253.246.17:9095,10.253.246.6:9095,10.253.246.7:9095");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-sync-send-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.format("%d", 1024*1024*10));

        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        setSaslConfig(props);
        return new KafkaProducer<>(props);
    }

    public static void setSaslConfig(Properties properties) {
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username='testUser' password='testPassWord';";
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig.replace("testUser", "kafka-eyuqkjcsd0").replace("testPassWord", "eyuqkjcjg2"));
    }
    
    private static void singleSendMsg() {
        KafkaProducer<byte[], byte[]> kafkaProducer = createProducer();

        String topicName = "topic1";
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topicName, 0, null, SINGLE_MSG);
        send(kafkaProducer, record1);

        System.out.println("topic " + topicName + " send over!!!!");
    }

    private static void send(KafkaProducer<byte[], byte[]> kafkaProducer, ProducerRecord<byte[], byte[]> record) {
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                System.out.println("send error!!!");
            } else {
                System.out.println("send success!!! offset is " + metadata.offset());
            }
        });
    }
}
