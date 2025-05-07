package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

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
        // send가 비동기이니까 future 객체에 리턴한다 (레코드 메타 데이터 채워서 리턴할거임)
        // 동기화를 할거면 get 호출 (future의 get을 호출하면 block함)
        // Future<RecordMetadata> future = kafkaProducer.send(producerRecord); 보통은 이렇게 하지 않는다 ~!
        // RecordMetadata recordMetadata = future.get();
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ##### recordMetadata received ##### \n " +
                    "partition: " + recordMetadata.partition() + "\n" +
                    "offset: " + recordMetadata.offset() + "\n" +
                    "timestamp: " + recordMetadata.timestamp() + "\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}
