package org.kafka.demo.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class InterceptorTest2 implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        System.out.println("InterceptorTest2 ::: onSend");
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("InterceptorTest2 ::: onAcknowledgement");
    }

    @Override
    public void close() {
        System.out.println("InterceptorTest2 ::: close");

    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("InterceptorTest2 ::: configure");
    }
}