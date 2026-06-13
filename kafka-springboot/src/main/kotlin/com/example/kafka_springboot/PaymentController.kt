package com.example.kafka_springboot

import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import tools.jackson.databind.ObjectMapper

@RestController
class PaymentController(
    private val paymentRepository: PaymentRepository,
    private val outboxEventRepository: OutboxEventRepository,
    private val objectMapper: ObjectMapper
) {

    @PostMapping("/payments")
    @Transactional
    fun send(@RequestBody request: PaymentRequest): String {
        val payment = Payment(
            paymentId = request.paymentId,
            amount = request.amount,
            currency = request.currency,
            fromAccount = request.fromAccount,
            toAccount = request.toAccount
        )
        paymentRepository.save(payment)

        val outboxEvent = OutboxEvent(
            aggregateId = request.paymentId,
            payload = objectMapper.writeValueAsString(request)
        )
        outboxEventRepository.save(outboxEvent)

        return "Accepted payment ${request.paymentId}"
    }
}
