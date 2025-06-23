package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AdvancedKafkaConsumer {
    public static void main(String[] args) throws Exception {
        String topicName = "user-log";

        // Kafka consumer 설정
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "user-log-consumer-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 수동 커밋

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println("=== CLICK 활동 로그만 필터링 출력 ===");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(record.value());
                    String activityType = jsonNode.get("activityType").asText();

                    if ("CLICK".equalsIgnoreCase(activityType)) {
                        System.out.printf(
                                "[CLICK 로그] Key=%s\nValue=%s\nPartition=%d, Offset=%d\n\n",
                                record.key(), record.value(), record.partition(), record.offset()
                        );
                    }
                } catch (Exception e) {
                    System.err.println("JSON 파싱 에러: " + e.getMessage());
                }
            }

            // 수동 커밋
            consumer.commitSync();
        }
    }
}