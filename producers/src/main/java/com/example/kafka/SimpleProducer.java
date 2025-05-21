package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer configuration setting
        // key : null, value : "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer, value.serializer.class
        // props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        // props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object creation
        // <key, value>
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // ProducerRecord object creation
        // topicName, key(생략 가능), value
        // ssh : kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "Hello World 2");

        // KafkaProducer message send
        // ssh : kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic --from-beginning
        kafkaProducer.send(producerRecord);

        // KafkaProducer close
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}