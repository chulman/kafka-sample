package com.chulm.kafka.stream_example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class StringToJsonStreamExample {

    private static Logger log = LoggerFactory.getLogger(SimpleTopologyExample.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Properties props = StreamProperties.getProperties();

        StreamsBuilder builder = new StreamsBuilder();
         builder.stream("json-source-input", Consumed.with(Serdes.String(), Serdes.String()))
         .flatMapValues(value -> {
            Map map = new HashMap<String, Object>();
            map.put("key", value);
            String json = "";
            try {
                json = mapper.writeValueAsString(map);
                System.err.println("---------->"+json);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return Arrays.asList(json);
        })
        .to("json-sink-out");

        //최종적인 토폴로지 생성
        final Topology topology = builder.build();

        //만들어진 토폴로지 확인
        log.info("Topology info = {}", topology.describe());


        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);


        try {
            kafkaStreams.start();
            System.out.println("topology started");
        } catch (Throwable e) {
            System.exit(1);
        }

        try {
            DataProducer.runProduce();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        try {
            JsonConsumer.consume();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}


class DataProducer {

    public static void runProduce() throws ExecutionException, InterruptedException {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaDataProducer");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        long time = System.currentTimeMillis();
        final ProducerRecord<String, String> record = new ProducerRecord("json-source-input", String.valueOf(time), "test data01 ");

        // record 기록 시 key value, timestamp 기록
        RecordMetadata metadata = producer.send(record).get();

        System.err.printf("sent record(key=%s value=%s) " +
                "meta(partition=%d, offset=%d) \n", record.key(), record.value(), metadata.partition(), metadata.offset());

    }
}


class JsonConsumer {

    public static void consume() throws ExecutionException, InterruptedException {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaJsonConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        final Consumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList("json-sink-out"));

        while(true){
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(0));


            consumerRecords.forEach(record -> {
                System.err.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
        }
    }
}