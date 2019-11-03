package com.chulm.kafka.stream_example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamProperties {

    public static Properties getProperties(){
        /*
         * 카프카 스트림 파이프 프로세스에 필요한 설정값
         * StreamsConfig의 자세한 설정값은
         * https://kafka.apache.org/10/documentation/#streamsconfigs 참고
         */
        Properties props = new Properties();
        //카프카 스트림즈 애플리케이션을 유일할게 구분할 아이디
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        //스트림즈 애플리케이션이 접근할 카프카 브로커정보
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094,localhost:9095");
        //데이터를 어떠한 형식으로 Read/Write할지를 설정(키/값의 데이터 타입을 지정) - 문자열
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
