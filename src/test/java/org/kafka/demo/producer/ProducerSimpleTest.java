package org.kafka.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.kafka.demo.tool.CommonTools;

import java.util.Map;

@Slf4j
public class ProducerSimpleTest extends AbstractProducerTest {

    private static final String TOPIC_NAME = "topic12";
    private static final String MSG_CONTENT = "test_content";

    @Override
    protected ProducerParams producerParamsBuilder() {
        return ProducerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .specialProperties(Map.of(ProducerConfig.BATCH_SIZE_CONFIG, "1"))
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
            CommonTools.sleepMilliseconds(200);
        }
    }
}
