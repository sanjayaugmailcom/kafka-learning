package com.example.kafka_springboot.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String
) {

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.PARTITIONER_CLASS_CONFIG to RoundRobinPartitioner::class.java
        )
        return DefaultKafkaProducerFactory(config)
    }

    @Bean
    fun kafkaTemplate(pf: ProducerFactory<String, String>) = KafkaTemplate(pf)

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "payments-consumer-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )
        return DefaultKafkaConsumerFactory(config)
    }

    @Bean
    fun errorHandler(kafkaTemplate: KafkaTemplate<String, String>): DefaultErrorHandler {
        val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate) { record, _ ->
            org.apache.kafka.common.TopicPartition("payments-dlt", record.partition())
        }
        // retry 3 times with a 1 second gap, then send to DLT
        return DefaultErrorHandler(recoverer, FixedBackOff(1000L, 3)).apply {
            addNotRetryableExceptions(IllegalArgumentException::class.java)
        }
    }

    @Bean
    fun listenerContainerFactory(
        cf: ConsumerFactory<String, String>,
        errorHandler: DefaultErrorHandler
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.setConsumerFactory(cf)
        factory.setCommonErrorHandler(errorHandler)
        return factory
    }
}
