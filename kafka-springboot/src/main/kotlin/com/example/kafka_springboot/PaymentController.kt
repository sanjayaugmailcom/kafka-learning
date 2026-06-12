package com.example.kafka_springboot

import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class PaymentController(private val kafka: KafkaTemplate<String, String>) {

    @PostMapping("/payments")
    fun send(@RequestBody message: String): String {
        kafka.send(ProducerRecord("payments", message, message))
        return "Sent: $message"
    }
}
