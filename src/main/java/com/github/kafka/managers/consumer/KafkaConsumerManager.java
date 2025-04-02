package com.github.kafka.managers.consumer;

import com.github.kafka.handlers.KafkaConsumerMessageHandler;

import java.util.List;

public interface KafkaConsumerManager {
    // register callback for a consumer
    void registerHandler(
            String cluster,
            List<String> topics,
            String username,
            String password,
            String server,
            String groupId,
            KafkaConsumerMessageHandler handler
    );

    // start all consumers
    void startConsumers();
}
