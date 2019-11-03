package com.chulm.kafka.stream_example;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleTopologyExample {

    private static Logger log = LoggerFactory.getLogger(SimpleTopologyExample.class);


    public static void main(String[] args) {

        Properties props =  StreamProperties.getProperties();
        //데이터의 흐름으로 구성된 토폴로지를 정의할 빌더
        final StreamsBuilder builder = new StreamsBuilder();

        //streams-*input에서 streams-*output으로 데이터 흐름을 정의한다.
        /*
         * KStream<String, String> source = builder.stream("streams-plaintext-input");
           source.to("streams-pipe-output");
         */
        builder.stream("streams-plaintext-input").to("streams-pipe-output");

        //최종적인 토폴로지 생성
        final Topology topology = builder.build();

        //만들어진 토폴로지 확인
        log.info("Topology info = {}",topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        try {
            streams.start();
            System.out.println("topology started");

        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
//        streams.close();
    }


}
