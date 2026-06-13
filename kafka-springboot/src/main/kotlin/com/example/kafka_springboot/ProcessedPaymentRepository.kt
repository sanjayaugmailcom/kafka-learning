package com.example.kafka_springboot

import org.springframework.data.jpa.repository.JpaRepository

interface ProcessedPaymentRepository : JpaRepository<ProcessedPayment, String>
