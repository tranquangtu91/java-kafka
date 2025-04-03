package com.github.kafka.managers.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaProducerManagerImpl implements KafkaProducerManager {
    private final Map<String, KafkaTemplate<String, String>> kafkaTemplates = new ConcurrentHashMap<>();

    @Override
    public void registerCluster(String cluster, String brokerUrl, String username, String password, String ackMode) {
        // Set up producer properties
        Map<String, Object> producerProps = getKafkaProperties(brokerUrl, username, password, ackMode);

        // Create producer factory
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);

        // Initialize KafkaTemplate
        kafkaTemplates.computeIfAbsent(cluster, (k) -> new KafkaTemplate<>(producerFactory));
    }

    private Map<String, Object> getKafkaProperties(String brokerUrl, String username, String password, String ackMode) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, ackMode);

        if (username != null && password != null) {
            producerProps.put("security.protocol", "SASL_PLAINTEXT");
            producerProps.put("sasl.mechanism", "PLAIN");
            producerProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + username + "\" "
                    + "password=\"" + password + "\";");
        }

        return producerProps;
    }

    @Override
    public void sendMessage(String cluster, String topic, String key, String message) {
        if (kafkaTemplates.containsKey(cluster)) {
            kafkaTemplates.get(cluster).send(topic, key, message);
        }
    }
}
