package com.github.kafka.controllers;

import com.github.kafka.example.producer.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaProducerController {

    @Autowired
    private final KafkaProducerService kafkaProducerService;

    public KafkaProducerController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }


    // API testing publish message to Kafka
    @GetMapping("/produce")
    public String produceMessage(@RequestParam String cluster, @RequestParam String topic, @RequestParam String key, @RequestParam String message) {
        kafkaProducerService.sendMessage(cluster, topic, key, message);
        return "Message sent to Kafka with key: " + key;
    }
}
