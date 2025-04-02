package com.github.kafka.managers.producer;

public interface KafkaProducerManager {
    public void sendMessage(String topic, String key, String message);
}
