package com.github.kafka.managers.producer;

public interface KafkaProducerManager {
    public void registerCluster(String cluster, String brokerUrl, String username, String password, String ackMode);
    public void sendMessage(String cluster, String topic, String key, String message);
}
