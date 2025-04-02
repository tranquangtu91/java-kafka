package com.github.kafka.example.producer;

import com.github.kafka.managers.producer.KafkaProducerManagerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaProducerManagerImpl kafkaProducerManager;

    @Autowired
    public KafkaProducerService(KafkaProducerConfig producerConfig) {
        this.kafkaProducerManager = new KafkaProducerManagerImpl(
                producerConfig.getBootstrapServers(),
                producerConfig.getUsername(),
                producerConfig.getPassword(),
                producerConfig.getAckMode()
        );
    }

    public void sendMessage(String topic, String key, String message) {
        kafkaProducerManager.sendMessage(topic, key, message);
    }
}
