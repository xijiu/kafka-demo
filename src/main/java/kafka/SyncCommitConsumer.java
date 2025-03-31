package kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class SyncCommitConsumer {
    
    public static void consume() {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = UtilTools.createConsumer();
        CountDownLatch latch = new CountDownLatch(1);
        String theTopic = UtilTools.getTopic();
        
        kafkaConsumer.subscribe(Collections.singleton(theTopic), new ConsumerRebalanceListener() {
           @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("rebalance occurs ==> onPartitionsRevoked, partitions are " + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("rebalance occurs ==> onPartitionsAssigned, partitions are " + partitions);
            }
        });
        
        // 注册 Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered. Closing Kafka Consumer...");
            try {
                latch.await();
                kafkaConsumer.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Kafka Consumer closed.");
        }));
        
        while (true) {
            try {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(10));
                if (!records.isEmpty()) {
                    kafkaConsumer.commitAsync();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
}
