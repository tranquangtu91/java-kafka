package com.github.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaIntegrationApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaIntegrationApplication.class, args);
	}

}
