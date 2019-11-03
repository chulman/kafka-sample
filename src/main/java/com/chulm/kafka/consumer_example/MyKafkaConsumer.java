package com.chulm.kafka.consumer_example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class MyKafkaConsumer {

    private final static String TOPIC = "my-example-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    static Properties props = new Properties();
    static  {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        //특정 파티션을 할당하기 위해서는 기존과는 다른 그룹아이디 값을 주어야한다. 왜냐하면 컨슈머 그룹내에 같이 있게되면 서로의
        //오프셋을 공유하기 때문에 종료된 컨슈머의 파티션을 다른 컨슈머가 할당받아 메시지를 이어서 가져가게 되고, 오프셋을 커밋하게 된다.

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SpecificKafkaConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }


    private static Consumer<Long, String> createConsumer() {
        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);
        return consumer;
    }

    public static void runConsumer()  {
        final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 100;   int noRecordsCount = 0;


        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));

        poll(consumer, giveUp, noRecordsCount);
    }

    static void poll(Consumer<Long, String> consumer, int giveUp, int noRecordsCount) {
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(1000l));

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.err.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });


            /**
             *
             * 특정 로직 수행
             */
            consumer.commitSync();
        }
        consumer.close();
        System.err.println("DONE");
    }

}
