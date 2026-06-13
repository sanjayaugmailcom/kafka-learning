package com.example.kafka_springboot

import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Service
import java.math.BigDecimal

@Service
class PaymentConsumer(
    private val failedMessageStore: FailedMessageStore,
    private val processedPaymentStore: ProcessedPaymentStore
) {

    @KafkaListener(topics = ["payments"], groupId = "payments-consumer-group", containerFactory = "listenerContainerFactory")
    fun handle(
        record: GenericRecord,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        val paymentId = record["paymentId"].toString()

        if (!processedPaymentStore.recordIfAbsent(paymentId)) {
            println("Duplicate | Skipping already-processed payment $paymentId")
            return
        }

        val amount = BigDecimal(record["amount"].toString())
        if (amount <= BigDecimal.ZERO) {
            throw IllegalArgumentException("Invalid amount: $amount")
        }

        println("Partition $partition | $paymentId | $amount ${record["currency"]} | ${record["fromAccount"]} → ${record["toAccount"]}")
    }

    @KafkaListener(topics = ["payments-dlt"], groupId = "payments-dlt-consumer-group", containerFactory = "listenerContainerFactory")
    fun handleDlt(
        record: GenericRecord,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        println("DLT | Failed message from $topic: $record")
        failedMessageStore.add(record.toString())
    }
}
