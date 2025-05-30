//package org.kafka.demo.producer.spring;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.support.Acknowledgment;
//import org.springframework.stereotype.Service;
//
//@Slf4j
//@Service
//public class SpringKafkaConsumer {
//
//    @KafkaListener(topics = "my-topic", groupId = "my-group")
//    public void listen(ConsumerRecords<?, ?> records, Acknowledgment ack) {
//        for (ConsumerRecord<?, ?> record : records) {
//            log.info("received record, topic {}, partition {}, offset {}, msg timestamp {}", record.topic(), record.partition(), record.offset(), record.timestamp());
//            long begin = System.currentTimeMillis();
//            doSomeBusiness(record);
//            log.info("cost time {}", System.currentTimeMillis() - begin);
//        }
//    }
//
//    private void doSomeBusiness(ConsumerRecord<?, ?> record) {
//        // do some things
//    }
//}