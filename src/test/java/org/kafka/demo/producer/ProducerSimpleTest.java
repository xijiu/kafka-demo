package org.kafka.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.kafka.demo.tool.CommonTools;

@Slf4j
public class ProducerSimpleTest extends AbstractProducerTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "topic1";
    private static final String MSG_CONTENT = "test_content";

    private static final boolean USE_SASL = false;
    private static final String USER_NAME = "kafka-gfg0vmvzuw";
    private static final String PASSWORD = "gfg0vmwgxy";

    @Test
    public void sendMsg2() {
        sendMsg();
    }

    @Test
    public void sendMsg() {
        try (KafkaProducer<String, String> kafkaProducer = createProducer(BOOTSTRAP_SERVERS, USE_SASL, USER_NAME, PASSWORD)) {
            send(kafkaProducer);
        }
    }

    private static void send(KafkaProducer<String, String> kafkaProducer) {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, MSG_CONTENT);
            long begin = System.currentTimeMillis();
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("send error !!!", exception);
                } else {
                    long cost = System.currentTimeMillis() - begin;
                    log.info("send msg succeed, topic {}, partition {}, offset {}, msg born timestamp {}, callback elapsed time {} ms",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), cost);
                }
            });
            CommonTools.sleepMilliseconds(5000);
        }
    }
}
