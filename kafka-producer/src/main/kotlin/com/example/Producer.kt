package com.example

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

fun main() {
    val props = Properties().apply {
        put("bootstrap.servers", "localhost:9092")
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }

    KafkaProducer<String, String>(props).use { producer ->
        producer.send(ProducerRecord("payments", "key1", "Payment received: $100"))
        println("Message sent!")
    }
}
