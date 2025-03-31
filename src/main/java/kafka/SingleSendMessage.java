package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SingleSendMessage {
    
    private static String TOPIC;
    private static long MSG_SIZE = 1 * 1024;
//    private static long MSG_TOTAL_NUM = 1000_0000;
    private static byte[] SINGLE_MSG = new byte[8];


    public static void main(String[] args) {
        sendMsg();
    }
    
    public static void sendMsg() {
        initConfig();

        singleSendMsg();
    }
    
     private static void initConfig() {
        TOPIC = UtilTools.getTopic();
        MSG_SIZE = UtilTools.getMsgSize() != 0 ? (int) UtilTools.getMsgSize() : MSG_SIZE;
//        MSG_TOTAL_NUM = UtilTools.getMsgNum() != 0 ? (int) UtilTools.getMsgNum() : MSG_TOTAL_NUM;
    }
    
    private static void singleSendMsg() {
        KafkaProducer<byte[], byte[]> kafkaProducer = UtilTools.createProducer();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, SINGLE_MSG);

        int count = 0;
        while (true) {

            System.out.println("send one msg");
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("exception is : " + exception);
                }
                System.out.println("receive callback, metadata is " + metadata);
                System.out.println("receive callback, serializedKeySize is " + metadata.serializedKeySize());
                System.out.println("receive callback, serializedValueSize is " + metadata.serializedValueSize());
            });

            count++;
            if (count % 1_0000 == 0) {
                System.out.println("Simply send message: already sent " + count + " messages");
            }
        }
    }
}
