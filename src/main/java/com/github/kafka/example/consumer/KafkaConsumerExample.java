package com.github.kafka.example.consumer;

import com.github.kafka.managers.consumer.KafkaConsumerManager;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Component
public class KafkaConsumerExample implements CommandLineRunner {
    @Getter
    public static class KafkaClusterConfig {
        private final String cluster;
        private final String brokers;
        private final String topics;
        private final String username;
        private final String password;
        private final String groupId;

        public KafkaClusterConfig(String cluster, String brokers, String topics, String username, String password, String groupId) {
            this.cluster = cluster;
            this.brokers = brokers;
            this.topics = topics;
            this.username = username;
            this.password = password;
            this.groupId = groupId;
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerExample.class);

    private final KafkaConsumerManager kafkaConsumerManager;
    private final Environment environment;

    @Autowired
    public KafkaConsumerExample(KafkaConsumerManager kafkaConsumerManager, Environment environment) {
        this.kafkaConsumerManager = kafkaConsumerManager;
        this.environment = environment;
    }

    @Override
    public void run(String... args) {
        String kafkaClusters = environment.getProperty("kafka.clusters");
        if (Objects.isNull(kafkaClusters) || kafkaClusters.isEmpty()) {
            throw new IllegalArgumentException("kafka.clusters is required!");
        }

        for (String cluster : kafkaClusters.split(",")) {
            KafkaClusterConfig clusterConfig = getClusterConfig(cluster.trim());
            List<String> topics = Arrays.asList(clusterConfig.getTopics().split(","));
            logger.info("Registering consumer for cluster: " + clusterConfig.getCluster() + " with topics: " + topics + "");

            // register callback handler for each consumer
            kafkaConsumerManager.registerHandler(
                    clusterConfig.getCluster(),
                    topics,
                    clusterConfig.getUsername(),
                    clusterConfig.getPassword(),
                    clusterConfig.getBrokers(),
                    clusterConfig.getGroupId(),
                    (topic, key, value) -> logger.info("Received from topic " + clusterConfig.getCluster() + ": " + topic + " " + value));
        }

        // start consumer
        kafkaConsumerManager.startConsumers();
    }

    // get config for each cluster
    private KafkaClusterConfig getClusterConfig(String cluster) {
        String prefix = "kafka." + cluster + ".";

        return new KafkaClusterConfig(
                cluster,
                getRequiredProperty(prefix + "broker"),
                getRequiredProperty(prefix + "topics"),
                environment.getProperty(prefix + "username"),
                environment.getProperty(prefix + "password"),
                getRequiredProperty(prefix + "group-id")
                );
    }

    private String getRequiredProperty(String key) {
        String value = environment.getProperty(key);
        if (Objects.isNull(value) || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required Kafka property: " + key);
        }
        return value;
    }

}