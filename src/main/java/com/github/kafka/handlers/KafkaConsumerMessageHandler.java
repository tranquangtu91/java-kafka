package com.github.kafka.handlers;

public interface KafkaConsumerMessageHandler {
    // callback
    void onMessage(String topic, String key, String value);
}
