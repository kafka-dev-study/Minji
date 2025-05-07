package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncWithKey {
    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncWithKey.class.getName());

    public static void main(String[] args) {

        String topicName = "multipart-topic";

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
        // key 20개 만들어 넣기
        for(int seq=0;seq<20;seq++) {
            // key -> String 타입으로 만들기 -> 현재 <String, String> 이니까
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world "+seq);

            logger.info("seq:"+seq); // 이렇게 하면 seq 번호 찍고 정보 나오고 이러지 않고 / 번호 쭉 나오고 정보 쭉 나오고 끝남

            // KafkaProducer message send
            // 위 방식도 가능하나 람다식 사용하는 방법도 있음
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("\n ##### record Metadata received ##### \n " +
                            "partition: " + metadata.partition() + "\n" +
                            "offset: " + metadata.offset() + "\n" +
                            "timestamp: " + metadata.timestamp() + "\n");
                } else {
                    logger.error("exception error from broker ", exception.getMessage());
                }
            });
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}