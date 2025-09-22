package org.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.kafka.demo.tool.CommonTools;

import java.time.Duration;
import java.util.regex.Pattern;

@Slf4j
public class KafkaConsumerMultiTopicsTest extends AbstractConsumerTest {

    private static final String TOPIC_NAME_PREFIX = "batch_create_topic_test_";

    @Override
    protected AbstractConsumerTest.ConsumerParams consumerParamsBuilder() {
        return AbstractConsumerTest.ConsumerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .group("group1")
                .build();
    }

    @Test
    public void test2() {
        try (KafkaConsumer<String, String> kafkaConsumer = createConsumer()) {
            kafkaConsumer.subscribe(Pattern.compile(TOPIC_NAME_PREFIX + ".*"));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(consumerRecords);
            }
        }
    }
}
