package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Scanner;

public class AdvancedKafkaProducer {
    public static void main(String[] args) throws Exception {
        String topicName = "user-log";

        // Kafka producer 설정
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        ObjectMapper objectMapper = new ObjectMapper(); // Jackson 라이브러리 사용

        System.out.println("=== 사용자 활동 로그 입력 (exit 입력 시 종료) ===");

        while (true) {
            System.out.print("사용자 ID: ");
            String userId = scanner.nextLine().trim();
            if ("exit".equalsIgnoreCase(userId)) break;

            System.out.print("활동 타입(activityType): ");
            String activityType = scanner.nextLine().trim();

            System.out.print("활동 설명(description): ");
            String description = scanner.nextLine().trim();

            // JSON 형식으로 직렬화
            ActivityLog log = new ActivityLog(userId, activityType, description);
            String jsonValue = objectMapper.writeValueAsString(log);

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, userId, jsonValue);

            // 비동기 전송 + 콜백
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("[전송완료] Partition=%d, Offset=%d\n", metadata.partition(), metadata.offset());
                } else {
                    System.err.println("전송 실패: " + exception.getMessage());
                }
            });
        }

        producer.close();
        scanner.close();
    }

    // 사용자 활동 로그 객체
    static class ActivityLog {
        public String userId;
        public String activityType;
        public String description;

        public ActivityLog(String userId, String activityType, String description) {
            this.userId = userId;
            this.activityType = activityType;
            this.description = description;
        }
    }
}