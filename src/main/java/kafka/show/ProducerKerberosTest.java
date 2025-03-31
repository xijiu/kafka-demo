package kafka.show;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.serialization.*;

import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ProducerKerberosTest {
    private final String bootstrapConfig;
    private final String topic;
    private final int partitions;

    protected ProducerKerberosTest(String bootstrapConfig, String topic, int partitions) {
        this.bootstrapConfig = bootstrapConfig;
        this.topic = topic;
        this.partitions = partitions;
    }

    public static void main(String[] args) throws Exception {
//		String servers = "10.253.246.47:9095,10.253.246.46:9095,10.253.246.48:9095";
		String servers = "kafka-eye3hncisi-0-0:9095,kafka-eye3hncisi-1-0:9095,kafka-eye3hncisi-2-0:9095";
        ProducerKerberosTest producerTest1 = new ProducerKerberosTest(servers, "topic1", 1);
        producerTest1.produce();
    }

    public void produce() throws Exception {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getCommonProperties());
//        ByteBuffer byteBuffer = ByteBuffer.allocate(1048000);
//        String msg = new String(byteBuffer.array());


        AtomicInteger index = new AtomicInteger();
        Timer timer = new Timer();
		System.out.println("begin send");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                int partition = index.get() % partitions;
				String message = "Kerberos test message ::: send to partition " + index.get();
                Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<>(topic, partition, null, message));
                try {
                    RecordMetadata recordMetadata = future.get();
                    System.out.println(new Date() + ": 消息体: " + message + " TOPIC: " + topic + " PARTITION: " + recordMetadata.partition() + " OFFSET: " + recordMetadata.offset());
                    index.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, 1);
    }

    private Properties getCommonProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapConfig);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "163840");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "500");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
		
		// 配置Kerberos
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "GSSAPI"); //,SCRAM-SHA-512
		props.put("sasl.kerberos.service.name", "kafka");
	    System.setProperty("java.security.krb5.conf", "/Users/likangning/OpenSource/KafkaXWDDS/src/main/resources/krb5.conf");
		System.setProperty("java.security.auth.login.config", "/Users/likangning/OpenSource/KafkaXWDDS/src/main/resources/kafka-client-jaas.conf");
        System.setProperty("sun.security.krb5.debug", "true");
//		System.setProperty("java.security.debug", "all");
        return props;
    }
}
