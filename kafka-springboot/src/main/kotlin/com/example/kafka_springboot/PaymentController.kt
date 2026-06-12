package com.example.kafka_springboot

import tools.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class PaymentController(
    private val kafka: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) {

    @PostMapping("/payments")
    fun send(@RequestBody payment: PaymentRequest): String {
        val json = objectMapper.writeValueAsString(payment)
        kafka.send(ProducerRecord("payments", payment.paymentId, json))
        return "Queued payment ${payment.paymentId}"
    }
}
