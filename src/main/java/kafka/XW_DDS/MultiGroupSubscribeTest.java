package kafka.XW_DDS;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MultiGroupSubscribeTest {

    public static void main(String[] args) throws InterruptedException {
        for (int i = 10; i < 100; i++) {
            consume(i);
        }
//        consumeAssign();
//        commitOffset();
    }

    private static KafkaConsumer<byte[], byte[]> createConsumer(int groupId) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.253.246.12:9095,10.253.246.13:9095,10.253.246.14:9095");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group" + groupId);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return new KafkaConsumer<>(props);
    }


    public static void consumeAssign() throws InterruptedException {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumer(1);
        String theTopic = "topic1";



        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(432365);
        TopicPartition topicPartition = new TopicPartition("topic1", 0);
        map.put(topicPartition, offsetAndMetadata);

        kafkaConsumer.assign(Collections.singleton(topicPartition));

        while (true) {
            System.out.println("begin consume");
            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1));
            if (consumerRecords.count() > 0) {
//                kafkaConsumer.commitSync();
                System.out.println("num : " + consumerRecords.count());
            } else {
                System.out.println("empty poll");
            }
//            try {
//                kafkaConsumer.commitSync(map);
//                System.out.println("commitSync");
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
            System.out.println("begin sleep");
            Thread.sleep(65000);
            System.out.println("end sleep");
        }
    }

    public static void consume(int groupId) throws InterruptedException {
        System.out.println("groupId : " + groupId);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumer(groupId);
        String theTopic = "topic1";

        kafkaConsumer.subscribe(Collections.singleton(theTopic));

        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0);
        map.put(new TopicPartition("topic1", 0), offsetAndMetadata);



        while (true) {
            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.count() > 0) {
//                kafkaConsumer.commitSync();
                System.out.println("num : " + consumerRecords.count());
            } else {
                System.out.println("empty poll");
            }
            try {
                kafkaConsumer.commitSync(map);
            } catch (Exception e) {
                e.printStackTrace();
            }
            break;
        }
    }
    
    public static void commitOffset() {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumer(1);
        String theTopic = "topic1";

        kafkaConsumer.subscribe(Collections.singleton(theTopic));

        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
//        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(305261);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(100);
        map.put(new TopicPartition("topic1", 0), offsetAndMetadata);

        kafkaConsumer.commitSync(map);

        System.out.println("end");
    }
}
