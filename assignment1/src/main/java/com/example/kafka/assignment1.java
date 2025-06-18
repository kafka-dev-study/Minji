package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class assignment1 {
    public static final Logger logger = LoggerFactory.getLogger(assignment1.class.getName());

    public static void main(String[] args) {
        // 사용할 kafka 토픽 이름
        String topicName = "multipart-topic";

        /*
        1. 프로듀서 설정
        - bootstrap.servers 주소 설정
        - KafkaProducer 인스턴스 생성 -> key, value string 타입 & StringSerializer 사용하여 직렬화
         */
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        /*
        2. 메시지 전송
        - 비동기 방식으로 메시지 전송
        - "hello world"와 숫자 조합 -> 0 ~ 19
        - 메시지 key -> 숫자를 문자열로 변환한 값
        - value -> "hello world {seq}"
         */
        for(int seq=0;seq<20;seq++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world "+seq);

            /*
            5. 각 메시지 전송 시, 메시지의 순서 번호(seq) 로깅
             */
            logger.info("seq:"+seq);

            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    /*
                    3. 비동기 전송 결과 처리
                    - 성공적으로 전송 시 메타데이터(파티션, 오프셋, 타임스탬프) 로깅
                    - 전송 실패 시 에러 메시지 로깅
                     */
                    logger.info("\n ##### record Metadata received ##### \n " +
                            "partition: " + metadata.partition() + "\n" +
                            "offset: " + metadata.offset() + "\n" +
                            "timestamp: " + metadata.timestamp() + "\n");
                } else {
                    logger.error("exception error from broker ", exception.getMessage());
                }
            });
        }

        /*
        4. 프로듀서 종료
        - 메시지 전송 완료 -> 3초 대기 후 종료
         */
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
