package org.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.kafka.demo.tool.CommonTools;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class MultiKafkaConsumerTest extends AbstractConsumerTest {

    private static final String TOPIC_NAME = "big_topic";

    @Override
    protected AbstractConsumerTest.ConsumerParams consumerParamsBuilder() {
        return AbstractConsumerTest.ConsumerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .group("group2")
                .specialProperties(Map.of(
                        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1,
                        ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1
                ))
                .build();
    }

    @Test
    public void multiConsumer() throws Exception {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            Thread thread = new Thread(this::singleConsume);
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private void singleConsume() {
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumerForByteArr()) {
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));

            while (true) {
                ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(consumerRecords);
                if (consumerRecords.count() > 0) {
                    kafkaConsumer.commitSync();
                }
                CommonTools.sleepMilliseconds(1000);
            }
        }
    }
}
