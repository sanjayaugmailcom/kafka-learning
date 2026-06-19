package com.example

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.Properties

fun main() {
    val props = Properties().apply {
        put("bootstrap.servers", "localhost:9092")
        put("group.id", "payments-consumer-group")
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("auto.offset.reset", "earliest")
    }

    KafkaConsumer<String, String>(props).use { consumer ->
        consumer.subscribe(listOf("payments"))
        println("Listening for messages...")

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            for (record in records) {
                println("Received: ${record.value()} | offset: ${record.offset()}")
            }
        }
    }
}
