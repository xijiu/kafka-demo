package kafka.XW_DDS;//package com.xxxxx.cmq.kafka.XW_DDS;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.serialization.ByteArrayDeserializer;
//import org.apache.kafka.common.serialization.ByteArraySerializer;
//
//import java.time.Duration;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//
//@Slf4j
//public class XWTimeoutExceptionTest {
//
//    public static void main(String[] args) throws InterruptedException {
//        log.info("XWTimeoutExceptionTest test !!!!!");
//        log.error("XWTimeoutExceptionTest test !!!!! aaaaaaaaa");
////        consume();
//        consumeAssign();
////        commitOffset();
//    }
//
//    private static KafkaConsumer<byte[], byte[]> createConsumer() {
//        Properties props = new Properties();
//        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
//        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
//        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1");
//        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
//        return new KafkaConsumer<>(props);
//    }
//
//    public static KafkaProducer<byte[], byte[]> createProducer() {
//        Properties props = new Properties();
//        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-sync-send-producer");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.format("%d", 1024*1024*10));
//
//        props.put(ProducerConfig.RETRIES_CONFIG, 1);
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//        props.put(ProducerConfig.ACKS_CONFIG, "1");
//        return new KafkaProducer<>(props);
//    }
//
//
//    public static void consumeAssign() throws InterruptedException {
//        KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumer();
//        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
//        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(432365);
//        TopicPartition topicPartition = new TopicPartition("topic1", 0);
//        map.put(topicPartition, offsetAndMetadata);
//
//        kafkaConsumer.assign(Collections.singleton(topicPartition));
//
//
//        int fetchTimes = 0;
//
//        while (true) {
//            log.info("begin consume");
//            System.out.println("begin consume");
//            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1));
//            if (consumerRecords.count() > 0) {
////                kafkaConsumer.commitSync();
//                System.out.println("num : " + consumerRecords.count());
//            } else {
//                System.out.println("empty poll");
//            }
//
//            System.out.println("fetch times is : " + fetchTimes++);
//
//            if (fetchTimes % 10 == 0) {
//                System.out.println("begin create producer");
//                KafkaProducer<byte[], byte[]> producer = createProducer();
//                long beginTime = System.currentTimeMillis();
//                while (true) {
//                    producer.send(new ProducerRecord<>("topic2", 0, new byte[1], new byte[2]));
//                    Thread.sleep(1000);
//                    if (System.currentTimeMillis() - beginTime > 80_000) {
//                        break;
//                    }
//                }
//            }
//            Thread.sleep(300);
//            System.out.println("end sleep");
//        }
//    }
//}
