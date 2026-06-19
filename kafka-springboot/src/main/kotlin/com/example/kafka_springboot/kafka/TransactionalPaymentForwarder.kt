package com.example.kafka_springboot.kafka

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Duration

@Component
class TransactionalPaymentForwarder(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
    @Value("\${spring.kafka.schema-registry-url}") private val schemaRegistryUrl: String
) : CommandLineRunner {

    private val log = LoggerFactory.getLogger(this::class.java)

    override fun run(vararg args: String) {
        // Run in a daemon thread so it doesn't block app shutdown
        Thread(::forwardLoop, "tx-forwarder").also { it.isDaemon = true }.start()
    }

    private fun forwardLoop() {
        val producer = buildProducer()
        val consumer = buildConsumer()

        // Must be called once before any transaction — registers this transactional.id with the broker.
        // If a previous producer with the same id crashed mid-transaction, the broker will fence it here.
        producer.initTransactions()
        consumer.subscribe(listOf("payments"))

        log.info("TransactionalPaymentForwarder started — forwarding high-value payments (>= 500)")

        while (true) {
            val records = consumer.poll(Duration.ofMillis(500))
            if (records.isEmpty) continue

            producer.beginTransaction()
            try {
                records.forEach { record ->
                    val amount = BigDecimal(record.value()["amount"].toString())
                    if (amount >= BigDecimal("500")) {
                        producer.send(ProducerRecord("payments-high-value", record.key(), record.value()))
                        log.info("TX | Forwarding high-value payment ${record.value()["paymentId"]} ($amount)")
                    }
                }

                // This is the key EOS call: the consumer offset commit becomes part of the transaction.
                // Instead of consumer.commitSync(), we hand the offsets to the producer.
                // They commit atomically with the sends — crash between send and here = abort = no duplicate.
                producer.sendOffsetsToTransaction(lastOffsets(records), consumer.groupMetadata())
                producer.commitTransaction()

            } catch (e: Exception) {
                log.error("Transaction failed — aborting. No messages written, no offset committed.", e)
                producer.abortTransaction()
            }
        }
    }

    // Kafka offset semantics: commit = last processed offset + 1 (the next one to read)
    private fun lastOffsets(records: ConsumerRecords<String, GenericRecord>): Map<TopicPartition, OffsetAndMetadata> =
        records.partitions().associateWith { partition ->
            OffsetAndMetadata(records.records(partition).last().offset() + 1)
        }

    private fun buildProducer(): KafkaProducer<String, GenericRecord> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            ProducerConfig.ACKS_CONFIG to "all",              // required for transactions
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,  // required for transactions
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "payment-forwarder-tx-1"
        )
        return KafkaProducer(config)
    }

    private fun buildConsumer(): KafkaConsumer<String, GenericRecord> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "tx-forwarder-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to false,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,       // we commit via the transaction
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed", // ignore uncommitted transactional messages
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        return KafkaConsumer(config)
    }
}
