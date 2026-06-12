package com.example.kafka_springboot

import tools.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Service

@Service
class PaymentConsumer(private val objectMapper: ObjectMapper) {

    @KafkaListener(topics = ["payments"], groupId = "payments-consumer-group", containerFactory = "listenerContainerFactory")
    fun handle(
        json: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        val payment = objectMapper.readValue(json, PaymentRequest::class.java)

        if (payment.amount <= java.math.BigDecimal.ZERO) {
            throw IllegalArgumentException("Invalid amount: ${payment.amount}")
        }

        println("Partition $partition | ${payment.paymentId} | ${payment.amount} ${payment.currency} | ${payment.fromAccount} → ${payment.toAccount}")
    }

    @KafkaListener(topics = ["payments-dlt"], 
        groupId = "payments-dlt-consumer-group", 
        containerFactory = "listenerContainerFactory",
        )
    fun handleDlt(
        json: String,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        println("DLT | Failed message from $topic: $json")
    }
}
