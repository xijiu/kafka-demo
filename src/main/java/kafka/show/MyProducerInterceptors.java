package kafka.show;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyProducerInterceptors implements ProducerInterceptor {
    // 消息还未发送出去时的拦截器，在方法内部，我们可以修改消息的内容，例如时间戳、header等
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    // 消息已经发送至broker，给业务的回调通知，我们可以知道该条消息的各类元数据
    // 例如topic、分区、时间戳、位点等
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println(metadata.partition());
        System.out.println(metadata.offset());
        System.out.println(metadata.timestamp());
        System.out.println(metadata.topic());
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
