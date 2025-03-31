package kafka.best_practice;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaTestProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置Kafka集群的接入点，多个IP用逗号隔开
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.253.246.46:9095,10.253.246.47:9095,10.253.246.43:9095");
        // 如果消息带有key，那么需要设置序列化方式，推荐使用 ByteArraySerializer 以提高性能
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 消息value的序列化方式，推荐使用 ByteArraySerializer 以提高性能
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 发送批次大小，默认为16K，如果单条消息较大，建议提高此值
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        // 聚批的时长，建议将此值设置为 > 100，牺牲部分延迟以提高聚批能力
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // ACKS参数，有3种可供选择，分别为-1、0、1。此处建议设置为1即可
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        // 注意：如果实例开启了SASL，那么还需要设置以下3个配置
        // 配置SASL的认证机制，当前集群256、512均支持
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        // 配置加密协议，如果开启了SASL且没有开启TLS的话，需要将其设置为SASL_PLAINTEXT
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        // SASL的具体配置信息，需要把账号及密码替换为真实的账号密码
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username='user1' password='user1';");

        // 实例化一个 KafkaProducer
        KafkaProducer<byte[], String> kafkaProducer = new KafkaProducer<>(properties);
        // 创建一条消息，这条消息要发送给“topic1”，其内容为“test_value”
        ProducerRecord<byte[], String> record = new ProducerRecord<>("t5", "test_value");
        // 执行发送逻辑，因为kafka是异步发送，因此此时消息可能还未发出
        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    // 发送出现异常，通常需要在日志中记录异常信息
                    System.out.println("send error!!!" + exception);
                } else {
                    // 真正的发送成功，说明消息已经被broker收到
                    // 消息发送的topic name
                    System.out.println("send success!!! topic is " + metadata.topic());
                    // 消息的offset
                    System.out.println("send success!!! offset is " + metadata.offset());
                    // 消息的partition分区信息
                    System.out.println("send success!!! partition is " + metadata.partition());
                    // broker存储这条消息的时间
                    System.out.println("send success!!! timestamp is " + metadata.timestamp());
                }
            });
        }
        // 优雅关闭，正常生产代码中应该使用 try() {} 模块进行优雅关闭
        kafkaProducer.close();
    }
}
