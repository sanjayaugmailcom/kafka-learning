package com.example.kafka_springboot

import tools.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class PaymentRetryController(
    private val kafka: KafkaTemplate<String, String>,
    private val failedMessageStore: FailedMessageStore,
    private val objectMapper: ObjectMapper
) {

    @PostMapping("/payments/retry")
    fun retry(): String {
        val messages = failedMessageStore.drainAll()
        messages.forEach { json ->
            val payment = objectMapper.readValue(json, PaymentRequest::class.java)
            kafka.send(ProducerRecord("payments", payment.paymentId, json))
        }
        return "Retried ${messages.size} message(s)"
    }
}
