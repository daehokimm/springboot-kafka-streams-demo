package me.daehokimm.springbootkafkastreamsdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
public class SpringbootKafkaStreamsDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringbootKafkaStreamsDemoApplication.class, args);
	}

}
