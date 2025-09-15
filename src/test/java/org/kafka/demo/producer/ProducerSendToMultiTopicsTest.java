package org.kafka.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.kafka.demo.tool.CommonTools;

@Slf4j
public class ProducerSendToMultiTopicsTest extends AbstractProducerTest {

    private static final String TOPIC_PREFIX = "batch_create_topic_test_";
    private static final String MSG_CONTENT = "test_content";

    @Override
    protected ProducerParams producerParamsBuilder() {
        return ProducerParams.builder()
                .bootstrapServers("localhost:9092")
                .serializerClass(StringSerializer.class)
                .useSasl(false)
                .username("")
                .password("")
                .build();
    }

    @Test
    public void sendMsg() {
        try (KafkaProducer<String, String> kafkaProducer = createProducer()) {
            send(kafkaProducer);
        }
    }

    private static void send(KafkaProducer<String, String> kafkaProducer) {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            for (int j = 0; j < 10; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_PREFIX + j, null, MSG_CONTENT);
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
            }
            CommonTools.sleepMilliseconds(5000);
        }
    }
}
