package org.kafka.demo.consumer.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kafka.demo.tool.CommonTools;

import java.util.Map;

public class ConsumerInterceptorTest1 implements ConsumerInterceptor<String, String> {
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        records.iterator().forEachRemaining(record -> System.out.printf(CommonTools.now() + " on consume, topic %s, partition %s, offset %s %n", record.topic(), record.partition(), record.offset()));
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, metadata) -> System.out.printf(CommonTools.now() + " on commit, topic %s, partition %s, offset %s %n", tp.topic(), tp.partition(), metadata.offset()));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
