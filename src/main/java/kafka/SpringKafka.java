package kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.SuccessCallback;

public class SpringKafka {

    public static void main(String[] args) {
        KafkaTemplate<String, String> kafkaTemplate = null;

        kafkaTemplate.send("aa", "ddd").addCallback(new SuccessCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {

            }
        }, new FailureCallback() {
            @Override
            public void onFailure(Throwable ex) {

            }
        });
    }
}
