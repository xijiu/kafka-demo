package org.kafka.demo.producer.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.kafka.demo.producer.AbstractProducerTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ProducerInterceptorTest extends AbstractProducerTest {

    private static final String TOPIC_NAME = "topic_simple";
    private static final String MSG_CONTENT = "test_content";

    @Override
    protected ProducerParams producerParamsBuilder() {
        List<String> interceptors = new ArrayList<>();
        interceptors.add("org.kafka.demo.producer.interceptor.InterceptorTest1");
        interceptors.add("org.kafka.demo.producer.interceptor.InterceptorTest2");
        return ProducerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .specialProperties(Map.of(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors))
                .build();
    }

    @Test
    public void sendMsg() {
        try (KafkaProducer<String, String> kafkaProducer = createProducer()) {
            send(kafkaProducer);
        }
    }

    private static void send(KafkaProducer<String, String> kafkaProducer) {
        for (int i = 0; i < 3; i++) {
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
        }
    }
}
