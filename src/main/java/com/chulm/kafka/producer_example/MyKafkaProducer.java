package com.chulm.kafka.producer_example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ref http://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html
 */
public class MyKafkaProducer {

    private final static String TOPIC = "my-example-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";


    //https://kafka.apache.org/documentation/#producerconfigs
    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        //카프카 서버의 부트스트랩 주소를 설정한다.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");

        // key.serializer
        // 메시지는 키 값의 쌍으로 이뤄진 형태로 카프카 브로커에게 전송된다.
        // 브로커는 이 키값이 바이트 배열로 돼있다고 가정한다.
        // 그래서 프로듀서에게 어떠한 직렬화 클래스가 키값을 바이트 배열로 변환할때 사용됬는 지 알려주어야한다
        // 카프카는 내장된 직렬화 클래스를 제공한다.(ByteArraySerializer, StringSerializer, IntegerSerializer / org.apache.kafka.common.serialization)

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG,  1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 20000);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 24568545);

        return new KafkaProducer<>(props);
    }


    public static void runProducerSync(final int sendMessageCount) throws Exception {

        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC, index,
                                "Hello Mom " + index);

                // record 기록 시 key value, timestamp 기록
                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void runProducerAsync(final int sendMessageCount) throws InterruptedException {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC, index, "Hello Mom " + index);

                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        System.out.printf("sent record(key=%s value=%s) " +
                                        "meta(partition=%d, offset=%d) time=%d\n",
                                record.key(), record.value(), metadata.partition(),
                                metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                });
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        } finally {
            producer.flush();
            producer.close();
        }
    }

}
