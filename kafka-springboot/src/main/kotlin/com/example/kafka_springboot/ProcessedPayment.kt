package com.example.kafka_springboot

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(name = "processed_payments")
class ProcessedPayment(
    @Id val paymentId: String,
    val processedAt: Instant = Instant.now()
)
