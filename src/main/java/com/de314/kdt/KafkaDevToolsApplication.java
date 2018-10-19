package com.de314.kdt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaDevToolsApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDevToolsApplication.class, args);
	}
}
