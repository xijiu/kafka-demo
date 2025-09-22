package org.kafka.demo.consumer.rebalance;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.kafka.demo.consumer.AbstractConsumerTest;
import org.kafka.demo.tool.CommonTools;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaRebalanceTest extends AbstractConsumerTest {

    private static final String TOPIC_NAME = "topic12";

    @Override
    protected ConsumerParams consumerParamsBuilder() {
        return ConsumerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .group("group2")
                .specialProperties(Map.of(
                        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.kafka.demo.consumer.rebalance.MyAssignor"
                ))
                .build();
    }

    @Test
    public void rebalanceTest() throws Exception {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread(() -> {
                try (KafkaConsumer<String, String> kafkaConsumer = createConsumer()) {
                    kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));

                    while (true) {
                        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                        printConsumerRecords(consumerRecords);
                        if (consumerRecords.count() > 0) {
                            kafkaConsumer.commitAsync();
                        }
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void addNewConsumer() throws Exception {
        try (KafkaConsumer<String, String> kafkaConsumer = createConsumer()) {
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(consumerRecords);
                if (consumerRecords.count() > 0) {
                    kafkaConsumer.commitSync();
                }
            }
        }
    }

}
