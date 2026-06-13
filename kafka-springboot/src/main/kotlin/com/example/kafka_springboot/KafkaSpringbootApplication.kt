package com.example.kafka_springboot

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class KafkaSpringbootApplication

fun main(args: Array<String>) {
	runApplication<KafkaSpringbootApplication>(*args)
}