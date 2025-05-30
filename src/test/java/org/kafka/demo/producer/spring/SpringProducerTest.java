package org.kafka.demo.producer.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kafka.demo.KafkaClientMetricStat;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.Resource;

@Slf4j
@RunWith(SpringRunner.class)
@EnableKafka
@SpringBootTest(classes = {SpringConfig.class})
public class SpringProducerTest {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    public void produceTest() throws Exception {

        KafkaClientMetricStat.start();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            long begin = System.currentTimeMillis();
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("topicA", "content");
            future.addCallback(new ListenableFutureCallback<>() {

                @Override
                public void onSuccess(SendResult<String, String> result) {
                    long cost = System.currentTimeMillis() - begin;
                    RecordMetadata recordMetadata = result.getRecordMetadata();
                    log.info("send msg succeed, topic {}, partition {}, offset {}, msg born timestamp {}, callback elapsed time {} ms",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp(), cost);
                }

                @Override
                public void onFailure(Throwable ex) {
                    log.error("send msg failed, topic {}, msg content {}", "topic1", "msg content", ex);
                }
            });

            Thread.sleep(10000);
        }

        System.out.println("begin wait");
        Thread.sleep(Long.MAX_VALUE);
    }

}
