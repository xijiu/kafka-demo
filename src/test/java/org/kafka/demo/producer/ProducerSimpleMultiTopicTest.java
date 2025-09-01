package org.kafka.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.kafka.demo.tool.CommonTools;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ProducerSimpleMultiTopicTest extends AbstractProducerTest {

    private static final String BOOTSTRAP_SERVERS = "10.253.246.12:9095,10.253.246.11:9095,10.253.246.10:9095";
    private static final Set<String> TOPIC_NAMES = Stream.of(
            "topic1", "topic2", "topic3"
    ).collect(Collectors.toSet());
    private static final String MSG_CONTENT = "test_content";

    private static final boolean USE_SASL = false;
    private static final String USER_NAME = "kafka-gfg0vmvzuw";
    private static final String PASSWORD = "gfg0vmwgxy";

    @Test
    public void sendMsg() {
        try (KafkaProducer<String, String> kafkaProducer = createProducer(BOOTSTRAP_SERVERS, USE_SASL, USER_NAME, PASSWORD)) {
            send(kafkaProducer);
        }
    }

    private static void send(KafkaProducer<String, String> kafkaProducer) {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            for (String topicName : TOPIC_NAMES) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, MSG_CONTENT);
                long begin = System.currentTimeMillis();
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send error !!!", exception);
                    } else {
//                        long cost = System.currentTimeMillis() - begin;
//                        log.info("send msg succeed, topic {}, partition {}, offset {}, msg born timestamp {}, callback elapsed time {} ms",
//                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), cost);
                    }
                });
            }
            CommonTools.sleepMilliseconds(10);
        }
    }
}
