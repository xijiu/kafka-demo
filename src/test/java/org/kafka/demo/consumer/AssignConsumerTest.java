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
public class AssignConsumerTest extends AbstractConsumerTest {

    private static final String TOPIC_NAME = "topic_simple";

    @Override
    protected ConsumerParams consumerParamsBuilder() {
        return ConsumerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .group("group2")
                .specialProperties(Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"))
                .build();
    }

    @Test
    public void test2() {
        try (KafkaConsumer<String, String> kafkaConsumer = createConsumer()) {
            kafkaConsumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, 0)));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
                printConsumerRecords(consumerRecords);
                if (consumerRecords.count() > 0) {
                    kafkaConsumer.commitSync();
                }
            }
        }
    }

}
