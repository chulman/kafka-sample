package com.chulm.kafka.producer_example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(metadata != null){
            /*
             * 정상처리
             */
        } else {
            /*
             * 예외처리
             */
        }

    }
}
