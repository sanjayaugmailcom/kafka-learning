package com.example.kafka_springboot

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header


@Service
class PaymentConsumer {

    @KafkaListener(topics = ["payments"], groupId = "payments-consumer-group")
    fun handle(
        message: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        println("Received on partition $partition: $message")
    }
}
