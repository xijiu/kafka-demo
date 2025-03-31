package kafka.XW_DDS;

import com.cestc.cmq.kafka.UtilTools;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LocalSendTest {

    public static void main(String[] args) {
        UtilTools.properties.put("bootstrap-servers", "localhost:9092");
//        UtilTools.properties.put("bootstrap-servers", "10.253.119.253:9095,10.253.119.35:9095,10.253.119.34:9095");
//        UtilTools.properties.put("bootstrap-servers", "localhost:9092");
        UtilTools.properties.put("useSASL", Boolean.FALSE.toString());
        UtilTools.properties.put("linger-ms", "1");
        UtilTools.properties.put("msg-num", "10000000");
//        UtilTools.properties.put("batch-size", "1024000");
        singleSendMsg();
    }


    private static void singleSendMsg() {
        byte[] bytes = new byte[1024];
        KafkaProducer<byte[], byte[]> kafkaProducer = UtilTools.createProducer();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic1", bytes);

        int count = 0;
        while (true) {
            System.out.println("send one msg");
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("exception is : " + exception);
                }
            });
            count++;
            if (count % 1_0000 == 0) {
                System.out.println("Simply send message: already sent " + count + " messages");
            }
        }
    }
}
