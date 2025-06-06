package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {
    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMillis, int intervalMillis,
                                        int intervalCount, boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterseq = 0;

        // seed값을 고정하여 Random 객체와 Faker 객체를 생성.
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while(iterseq++ != iterCount){
            HashMap<String, String> pMessage = pizzaMessage .produce_msg(faker, random, iterseq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                    pMessage.get("key"), pMessage.get("value"));

            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if((intervalCount > 0)&&(iterseq % intervalCount == 0)) {
                try {
                    logger.info("##### IntervalCount: "+intervalCount+
                            "intervalMillis: "+interIntervalMillis+" #####");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if(interIntervalMillis > 0){
                try {
                    logger.info("interintervalMillis: "+interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage,
                                   boolean sync) {
        if(!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message: " + pMessage.get("key") + "\n" +
                            "partition: " + metadata.partition() + "\n" +
                            "offset: " + metadata.offset() + "\n");
                } else {
                    logger.error("exception error from broker ", exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message: " + pMessage.get("key") + "\n" +
                        "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset() + "\n");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        String topicName = "pizza-topic";

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

        // interCount가 -1 -> 무한루프
        sendPizzaMessage(kafkaProducer, topicName,
                -1, 100,
                100, 100, true);

        kafkaProducer.close();
    }
}