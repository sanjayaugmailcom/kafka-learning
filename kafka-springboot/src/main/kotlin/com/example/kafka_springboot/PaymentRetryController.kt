package com.example.kafka_springboot

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import tools.jackson.databind.ObjectMapper

@RestController
class PaymentRetryController(
    private val kafka: KafkaTemplate<String, Any>,
    private val failedMessageStore: FailedMessageStore,
    private val objectMapper: ObjectMapper
) {
    private val schema: Schema = Schema.Parser().parse(
        PaymentRetryController::class.java.getResourceAsStream("/avro/PaymentRequest.avsc")
    )

    @PostMapping("/payments/retry")
    fun retry(): String {
        val messages = failedMessageStore.drainAll()
        messages.forEach { json ->
            val payment = objectMapper.readValue(json, PaymentRequest::class.java)
            val record: GenericRecord = GenericData.Record(schema).apply {
                put("paymentId", payment.paymentId)
                put("amount", payment.amount.toPlainString())
                put("currency", payment.currency)
                put("fromAccount", payment.fromAccount)
                put("toAccount", payment.toAccount)
            }
            kafka.send(ProducerRecord("payments", payment.paymentId, record))
        }
        return "Retried ${messages.size} message(s)"
    }
}
