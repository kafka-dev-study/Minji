package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerASync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer configuration setting
        // key : null, value : "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer, value.serializer.class
        // props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        // props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        // <key, value>
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // ProducerRecord object creation
        // topicName, key(생략 가능), value
        // ssh : kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world 2");

        // KafkaProducer message send
        // send할 때마다 콜백 객체가 만들어지는 것
        // 메인 스레드가 아닌 다른 스레드를 호출해서 값이 채워지기 때문에 비동기적으로 불려지는 것
//        kafkaProducer.send(producerRecord, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                if(exception == null) {
//                    logger.info("\n ##### recordMetadata received ##### \n " +
//                            "partition: " + metadata.partition() + "\n" +
//                            "offset: " + metadata.offset() + "\n" +
//                            "timestamp: " + metadata.timestamp() + "\n");
//                } else {
//                    logger.error("exception error from broker ", exception.getMessage());
//                }
//            }
//        });

        // 위 방식도 가능하나 람다식 사용하는 방법도 있음
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if(exception == null) {
                logger.info("\n ##### record Metadata received ##### \n " +
                        "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset() + "\n" +
                        "timestamp: " + metadata.timestamp() + "\n");
            } else {
                logger.error("exception error from broker ", exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}