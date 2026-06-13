package com.example.kafka_springboot

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.Instant

@Entity
@Table(name = "payments")
class Payment(
    @Id val paymentId: String,
    val amount: BigDecimal,
    val currency: String,
    val fromAccount: String,
    val toAccount: String,
    val createdAt: Instant = Instant.now()
)
