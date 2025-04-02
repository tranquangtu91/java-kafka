package com.github.kafka.config;

import com.github.kafka.handlers.KafkaConsumerMessageHandler;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class KafkaConsumerConfig {
    private String cluster;
    private List<String> topics;
    private String username;
    private String password;
    private String server;
    private String groupId;
    private List<KafkaConsumerMessageHandler> handlers;

    public KafkaConsumerConfig(String cluster, List<String> topics, String username, String password, String server, String groupId) {
        this.cluster = cluster;
        this.topics = topics;
        this.username = username;
        this.password = password;
        this.server = server;
        this.groupId = groupId;
        this.handlers = new ArrayList<>();
    }

    public void addHandler(KafkaConsumerMessageHandler handler) {
        handlers.add(handler);
    }
}
