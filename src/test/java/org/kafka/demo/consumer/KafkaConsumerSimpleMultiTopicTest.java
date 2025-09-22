package org.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.kafka.demo.KafkaClientMetricStat;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class KafkaConsumerSimpleMultiTopicTest extends AbstractConsumerTest {

    private static final Set<String> TOPIC_NAMES = Stream.of(
            "topic1", "topic2", "topic3"
    ).collect(Collectors.toSet());

    @Override
    protected AbstractConsumerTest.ConsumerParams consumerParamsBuilder() {
        return AbstractConsumerTest.ConsumerParams.builder()
                .bootstrapServers("10.253.246.12:9095,10.253.246.11:9095,10.253.246.10:9095")
                .group("group1")
                .build();
    }

    @Test
    public void test2() {
        KafkaClientMetricStat.start();
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = createConsumerForByteArr()) {
            kafkaConsumer.subscribe(TOPIC_NAMES);

            while (true) {
                ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(consumerRecords);
            }
        }
    }
}
