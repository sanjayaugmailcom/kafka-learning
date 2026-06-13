package com.example.kafka_springboot

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class PaymentController(
    private val kafka: KafkaTemplate<String, Any>
) {
    private val schema: Schema = Schema.Parser().parse(
        PaymentController::class.java.getResourceAsStream("/avro/PaymentRequest.avsc")
    )

    @PostMapping("/payments")
    fun send(@RequestBody payment: PaymentRequest): String {
        val record: GenericRecord = GenericData.Record(schema).apply {
            put("paymentId", payment.paymentId)
            put("amount", payment.amount.toPlainString())
            put("currency", payment.currency)
            put("fromAccount", payment.fromAccount)
            put("toAccount", payment.toAccount)
        }
        kafka.send(ProducerRecord("payments", payment.paymentId, record))
        return "Queued payment ${payment.paymentId}"
    }
}
