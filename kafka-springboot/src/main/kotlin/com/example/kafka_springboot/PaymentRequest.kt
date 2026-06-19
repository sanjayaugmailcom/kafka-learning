package com.example.kafka_springboot

import java.math.BigDecimal

data class PaymentRequest(
    val paymentId: String,
    val amount: BigDecimal,
    val currency: String,
    val fromAccount: String,
    val toAccount: String,
    val description: String = ""
)
