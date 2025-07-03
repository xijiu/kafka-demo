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

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

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
            log.info("prepare send one msg");

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("topicA", "content");
            future.whenComplete((result, exception) -> {
                // 处理异常
                if (exception != null) {
                    log.error("send msg failed, topic {}, msg content {}", "topic1", "msg content", exception);
                } else {
                    // 正常消息发送记录
                    long cost = System.currentTimeMillis() - begin;
                    RecordMetadata recordMetadata = result.getRecordMetadata();
                    log.info("send msg succeed, topic {}, partition {}, offset {}, msg born timestamp {}, callback elapsed time {} ms",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp(), cost);
                }
            });

            Thread.sleep(5000);
        }

        System.out.println("begin wait");
        Thread.sleep(Long.MAX_VALUE);
    }

}
