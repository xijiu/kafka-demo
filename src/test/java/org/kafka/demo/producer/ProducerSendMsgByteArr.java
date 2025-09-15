package org.kafka.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;


public class ProducerSendMsgByteArr extends AbstractProducerTest {

    protected ProducerParams producerParamsBuilder() {
        return ProducerParams.builder()
                .bootstrapServers("10.255.225.107:9095,10.255.225.108:9095,10.255.225.106:9095")
                .serializerClass(ByteArraySerializer.class)
                .useSasl(false)
                .username("")
                .password("")
                .build();
    }

    private static byte[] SINGLE_MSG = new byte[1024 * 1];


    @Test
    public void sendMsg() throws Exception {
        singleSendMsg();
    }

    private void singleSendMsg() throws Exception {
        KafkaProducer<byte[], byte[]> kafkaProducer = createProducerForByteArr();

        String topicName = "big_topic";
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topicName, SINGLE_MSG);
        send(kafkaProducer, record1);

        System.out.println("topic " + topicName + " send over!!!!");
        kafkaProducer.close();
    }

    private static void send(KafkaProducer<byte[], byte[]> kafkaProducer, ProducerRecord<byte[], byte[]> record) throws Exception {
        while (true) {
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                    System.out.println("send error !!!");
                } else {
                    System.out.printf(" send success!!! partition %s, offset is %s %n", metadata.partition(), metadata.offset());
                }
            });
            Thread.sleep(1000);
        }
    }
}
