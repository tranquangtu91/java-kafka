package com.github.kafka.example.producer;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class KafkaProducerConfig {
    @Value("${kafka.producer.cluster}")
    private String cluster;
    @Value("${kafka.clusterA.broker}")
    private String brokerUrl;
    @Value("${kafka.clusterA.username}")
    private String username;
    @Value("${kafka.clusterA.password}")
    private String password;
    @Value("${kafka.clusterA.ack:all}")
    private String ackMode;
}
