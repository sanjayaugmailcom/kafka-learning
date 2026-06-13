package com.example.kafka_springboot

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.ObjectMapper

@Component
class OutboxPublisher(
    private val outboxEventRepository: OutboxEventRepository,
    private val kafka: KafkaTemplate<String, Any>,
    private val objectMapper: ObjectMapper
) {
    private val schema: Schema = Schema.Parser().parse(
        OutboxPublisher::class.java.getResourceAsStream("/avro/PaymentRequest.avsc")
    )

    @Scheduled(fixedDelay = 5000)
    @Transactional
    fun publish() {
        val unpublished = outboxEventRepository.findByPublishedFalse()
        if (unpublished.isEmpty()) return

        unpublished.forEach { event ->
            val payment = objectMapper.readValue(event.payload, PaymentRequest::class.java)
            val record: GenericRecord = GenericData.Record(schema).apply {
                put("paymentId", payment.paymentId)
                put("amount", payment.amount.toPlainString())
                put("currency", payment.currency)
                put("fromAccount", payment.fromAccount)
                put("toAccount", payment.toAccount)
            }
            kafka.send(ProducerRecord("payments", event.aggregateId, record)).get()

            // mark as published so we don't send it again
            outboxEventRepository.save(
                OutboxEvent(
                    id = event.id,
                    aggregateId = event.aggregateId,
                    payload = event.payload,
                    published = true,
                    createdAt = event.createdAt
                )
            )
        }

        println("OutboxPublisher: sent ${unpublished.size} event(s) to Kafka")
    }
}
