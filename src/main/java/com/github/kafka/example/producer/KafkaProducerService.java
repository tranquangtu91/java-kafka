package com.github.kafka.example.producer;

import com.github.kafka.managers.producer.KafkaProducerManagerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaProducerManagerImpl kafkaProducerManager = new KafkaProducerManagerImpl();

    @Autowired
    public KafkaProducerService(KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerManager.registerCluster(
                kafkaProducerConfig.getCluster(),
                kafkaProducerConfig.getBrokerUrl(),
                kafkaProducerConfig.getUsername(),
                kafkaProducerConfig.getPassword(),
                kafkaProducerConfig.getAckMode()
        );
    }

    public void sendMessage(String cluster, String topic, String key, String message) {
        kafkaProducerManager.sendMessage(cluster, topic, key, message);
    }
}
