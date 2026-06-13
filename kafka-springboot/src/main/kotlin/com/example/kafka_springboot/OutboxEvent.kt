package com.example.kafka_springboot

import jakarta.persistence.*
import java.time.Instant

@Entity
@Table(name = "outbox_events")
class OutboxEvent(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    val aggregateId: String,   // the paymentId — used as the Kafka message key
    val payload: String,       // JSON snapshot of the payment
    val published: Boolean = false,
    val createdAt: Instant = Instant.now()
)
