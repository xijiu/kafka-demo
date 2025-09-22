package org.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class KafkaConsumerTest2 extends AbstractConsumerTest {

    private static final String TOPIC_NAME = "topic2";


    @Override
    protected AbstractConsumerTest.ConsumerParams consumerParamsBuilder() {
        return AbstractConsumerTest.ConsumerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .group("group2")
                .build();
    }

    @Test
    public void test() {
        try (KafkaConsumer<String, String> kafkaConsumer = createConsumer()) {
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(consumerRecords);
            }
        }
    }

}
