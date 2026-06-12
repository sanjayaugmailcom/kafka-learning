package com.example.kafka_springboot

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaSpringbootApplication

fun main(args: Array<String>) {
	runApplication<KafkaSpringbootApplication>(*args)
}
