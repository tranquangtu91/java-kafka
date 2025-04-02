package com.github.kafka.managers;

import com.github.kafka.handlers.KafkaConsumerMessageHandler;

import java.util.List;

public interface KafkaConsumerManager {
    void registerHandler(
            String cluster,
            List<String> topics,
            String username,
            String password,
            String server,
            String groupId,
            KafkaConsumerMessageHandler handler
    );

    void startConsumers();
}
