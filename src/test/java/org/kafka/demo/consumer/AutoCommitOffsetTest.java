package org.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

@Slf4j
public class AutoCommitOffsetTest extends AbstractConsumerTest {

    private static final String TOPIC_NAME = "topic_simple";

    @Override
    protected ConsumerParams consumerParamsBuilder() {
        return ConsumerParams.builder()
                .bootstrapServers("10.255.225.74:9095,10.255.225.75:9095,10.255.225.76:9095")
                .group("group3")
                .specialProperties(Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"))
                .build();
    }

    @Test
    public void test2() {
        try (KafkaConsumer<String, String> kafkaConsumer = createConsumer()) {
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
                printConsumerRecords(consumerRecords);
                System.out.println("prepare commit begin");
                kafkaConsumer.commitSync();
                System.out.println("prepare commit end");
            }
        }
    }
}