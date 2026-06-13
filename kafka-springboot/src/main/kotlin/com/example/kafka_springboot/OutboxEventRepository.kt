package com.example.kafka_springboot

import org.springframework.data.jpa.repository.JpaRepository

interface OutboxEventRepository : JpaRepository<OutboxEvent, Long> {
    fun findByPublishedFalse(): List<OutboxEvent>
}
