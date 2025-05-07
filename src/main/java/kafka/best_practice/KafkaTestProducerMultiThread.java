package kafka.best_practice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;

public class KafkaTestProducerMultiThread {

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = initKafkaProducer();
        Header header = new Header() {
            @Override
            public String key() {
                return "key1";
            }

            @Override
            public byte[] value() {
                return "value1".getBytes();
            }
        };
        Headers headers = new RecordHeaders(Collections.singletonList(header));
        ProducerRecord<String, String> record = new ProducerRecord<>("topic1", 0, "test_key", "test_value", headers);
        kafkaProducer.send(record);


        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> consumerRecord = iterator.next();
            Headers headers1 = consumerRecord.headers();
            for (Header header2 : headers1) {
                header2.key();
                header2.value();
            }
        }


        long beginTime = System.currentTimeMillis();
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                // 发送出现异常，通常需要在日志中记录异常信息
                System.out.println("send error!!!" + exception);
            } else {
                // 真正的发送成功，说明消息已经被broker收到
                long cost = System.currentTimeMillis() - beginTime;
                System.out.println("send msg success!!!, time cost " + cost + " ms");
            }
        });
    }

    private static KafkaProducer<String, String> initKafkaProducer() {
        return null;
    }
}
