package com.github.kafka.managers;

import com.github.kafka.handlers.KafkaConsumerMessageHandler;
import com.github.kafka.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KafkaConsumerManagerImpl implements KafkaConsumerManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerManagerImpl.class);
    private final Map<String, KafkaConsumerConfig> consumerConfigs = new HashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Value("${kafka.consumer.poll-duration-ms}")
    private int pollDurationMs;

    @Override
    public void registerHandler(String cluster, List<String> topics, String username, String password, String server, String groupId, KafkaConsumerMessageHandler handler) {
        consumerConfigs.computeIfAbsent(cluster, k -> new KafkaConsumerConfig(cluster, topics, username, password, server, groupId)).addHandler(handler);
    }

    @Override
    public void startConsumers() {
        for (KafkaConsumerConfig config : consumerConfigs.values()) {
            executorService.submit(() -> startKafkaConsumer(config));
        }
    }

    private Properties getConsumerConfig(KafkaConsumerConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getServer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-" + config.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

        // SASL settings
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + config.getUsername() + "\" "
                + "password=\"" + config.getPassword() + "\";");
        return props;
    }

    private void startKafkaConsumer(KafkaConsumerConfig config) {
        Properties props = getConsumerConfig(config);

        try {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            consumer.subscribe(config.getTopics());

            logger.info("Listening on topics: " + config.getTopics() + " from cluster " + config.getCluster());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollDurationMs));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received message from cluster " + config.getCluster() + ": topic=" + record.topic() + ", value=" + record.value());
                    try {
                        for (KafkaConsumerMessageHandler handler : config.getHandlers()) {
                            handler.onMessage(record.topic(), record.key(), record.value());
                        }
                        consumer.commitSync();
                        logger.info("Message from cluster " + config.getCluster() + " processed successfully");
                    } catch (Exception e) {
                        logger.error("Error processing message from cluster " + config.getCluster() + ": " + e.getMessage());
                    }
                }
            }
        } catch (org.apache.kafka.common.KafkaException e) {
            logger.error("KafkaException occurred while subscribing or polling: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("Invalid Kafka properties: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error: " + e.getMessage());
        }
    }
}