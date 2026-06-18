package com.example.kafka_springboot

import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import tools.jackson.databind.ObjectMapper
import java.math.BigDecimal
import java.util.UUID

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

    @PostMapping("/payments/bulk")
    @Transactional
    fun sendBulk(): String {
        val currencies = listOf("USD", "AUD", "GBP")
        repeat(10) { i ->
            val request = PaymentRequest(
                paymentId = UUID.randomUUID().toString(),
                amount = BigDecimal("${(i + 1) * 10}.00"),
                currency = currencies[i % currencies.size],
                fromAccount = "acc-bulk-${i + 1}",
                toAccount = "acc-dest-${i + 1}"
            )
            val payment = Payment(
                paymentId = request.paymentId,
                amount = request.amount,
                currency = request.currency,
                fromAccount = request.fromAccount,
                toAccount = request.toAccount
            )
            paymentRepository.save(payment)
            outboxEventRepository.save(
                OutboxEvent(
                    aggregateId = request.paymentId,
                    payload = objectMapper.writeValueAsString(request)
                )
            )
        }
        return "Accepted 10 bulk payments"
    }
}
