package kafka.XW_DDS;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

public class TestKafkaKerberos {

    public static void main(String[] args) {
        // 消费者
        testConsumer();

        // 生产者
        testProducer();
    }

    private static void testConsumer() {
        System.setProperty("java.security.auth.login.config", "F:\\test\\kerberos\\kafka-client-jaas.conf");
        System.setProperty("java.security.krb5.conf", "F:\\test\\kerberos\\krb5.conf");
        Properties props = new Properties();
        props.put("bootstrap.servers", "monkey:9092");
        props.put("group.id", "test_group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // sasl
        props.put("sasl.mechanism", "GSSAPI");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka-server");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        String topic = "test";
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, partition = %d, key = %s, value = %s%n",
                            record.offset(), record.partition(), record.key(), record.value());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void testProducer() {
        // JAAS配置文件路径和Kerberos配置文件路径
        System.setProperty("java.security.auth.login.config", "F:\\test\\kerberos\\kafka-client-jaas.conf");
        System.setProperty("java.security.krb5.conf", "F:\\test\\kerberos\\krb5.conf");
        // kafka属性配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "monkey:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // kerberos安全认证
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka-server");

        String topic = "test";
        String msg = "this is a test msg";
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        // 发送消息记录
        Future<RecordMetadata> future = kafkaProducer.send(record);
        try {
            RecordMetadata metadata = future.get();
            System.out.printf("Message sent to Kafka topic=%s, partition=%d, offset=%d\n", metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
        kafkaProducer.close();
    }
}