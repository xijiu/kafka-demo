package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class SpringKafka {

    public static void main(String[] args) {
        KafkaTemplate<String, String> kafkaTemplate = null;

        long begin = System.currentTimeMillis();
        kafkaTemplate.send("aa", "ddd").whenComplete((result, exception) -> {
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
    }
}
