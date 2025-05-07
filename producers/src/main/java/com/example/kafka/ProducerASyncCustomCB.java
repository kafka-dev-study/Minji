package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncCustomCB {
    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCB.class.getName());

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        // KafkaProducer configuration setting
        // key : null, value : "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer, value.serializer.class
        // props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        // props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        // <key, value>
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        // ProducerRecord object creation
        // key 20개 만들어 넣기
        for(int seq=0;seq<20;seq++) {
            // key -> Integer 타입으로 만들기
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world "+seq);

            // logger.info("seq:"+seq); // 이렇게 하면 seq 번호 찍고 정보 나오고 이러지 않고 / 번호 쭉 나오고 정보 쭉 나오고 끝남
            CustomCallback callback = new CustomCallback(seq);
            // KafkaProducer message send
            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}