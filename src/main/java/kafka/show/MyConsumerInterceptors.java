package kafka.show;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MyConsumerInterceptors implements ConsumerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(MyConsumerInterceptors.class);

    // 消费的拦截
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            record.value();
            record.offset();
            record.topic();
            record.partition();
            record.timestamp();
            log.info("");
        }
        return null;
    }

    // 提交位点的拦截
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
