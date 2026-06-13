package com.example.kafka_springboot

import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class ProcessedPaymentStore {
    private val seen = ConcurrentHashMap.newKeySet<String>()

    // Returns true if this is the first time we've seen this id (safe to process).
    // Returns false if it's a duplicate (skip it).
    fun recordIfAbsent(paymentId: String): Boolean = seen.add(paymentId)
}
