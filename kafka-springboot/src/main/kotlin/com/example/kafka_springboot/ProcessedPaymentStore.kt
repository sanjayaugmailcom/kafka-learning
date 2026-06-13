package com.example.kafka_springboot

import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional

@Component
class ProcessedPaymentStore(
    private val processedPaymentRepository: ProcessedPaymentRepository
) {
    // Returns true if this is the first time we've seen this id (safe to process).
    // Returns false if it's a duplicate (skip it).
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun recordIfAbsent(paymentId: String): Boolean {
        if (processedPaymentRepository.existsById(paymentId)) return false
        processedPaymentRepository.save(ProcessedPayment(paymentId))
        return true
    }
}
