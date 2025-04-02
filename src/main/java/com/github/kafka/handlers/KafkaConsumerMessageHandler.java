package com.github.kafka.handlers;

public interface KafkaConsumerMessageHandler {
    void onMessage(String topic, String key, String value);
}
