package kafka.best_practice;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;

import java.util.Properties;

public class KafkaTestProducer2 {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name());

    }
}
