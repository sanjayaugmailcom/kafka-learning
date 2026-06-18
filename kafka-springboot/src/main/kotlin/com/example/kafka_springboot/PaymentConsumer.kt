package com.example.kafka_springboot

import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
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
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        ack: Acknowledgment
    ) {
        val paymentId = record["paymentId"].toString()
        val amount = BigDecimal(record["amount"].toString())

        if (amount <= BigDecimal.ZERO) {
            throw IllegalArgumentException("Invalid amount: $amount")
        }

        if (!processedPaymentStore.recordIfAbsent(paymentId)) {
            println("Duplicate | Skipping already-processed payment $paymentId")
            ack.acknowledge()
            return
        }

        println("Partition $partition | $paymentId | $amount ${record["currency"]} | ${record["fromAccount"]} → ${record["toAccount"]}")
        ack.acknowledge()
    }

    @KafkaListener(topics = ["payment-totals"], groupId = "payment-totals-group")
    fun handleTotals(
        @Header(KafkaHeaders.RECEIVED_KEY) currency: String,
        @Payload count: String
    ) {
        println("COUNT | $currency count = $count (last 60s)")
    }

    @KafkaListener(topics = ["payment-amount-totals"], groupId = "payment-amount-totals-group")
    fun handleAmountTotals(
        @Header(KafkaHeaders.RECEIVED_KEY) currency: String,
        @Payload total: String
    ) {
        println("AGGREGATE | $currency running total = $total")
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
