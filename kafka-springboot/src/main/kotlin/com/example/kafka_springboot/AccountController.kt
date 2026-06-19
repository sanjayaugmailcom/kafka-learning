package com.example.kafka_springboot

import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/accounts")
class AccountController(
    private val kafka: KafkaTemplate<String, String>,
    private val stringProducerFactory: ProducerFactory<String, String>
) {

    @PostMapping("/{accountId}/balance")
    fun updateBalance(@PathVariable accountId: String, @RequestBody balance: String): String {
        kafka.send("account-balances", accountId, balance)
        return "Balance updated: $accountId = $balance"
    }

    @DeleteMapping("/{accountId}")
    fun deleteAccount(@PathVariable accountId: String): String {
        // KafkaTemplate doesn't allow null values (Kotlin non-nullable bound).
        // Raw producer is Java so null value is permitted — this is a tombstone.
        val producer = stringProducerFactory.createProducer()
        producer.send(ProducerRecord("account-balances", accountId, null))
        producer.flush()
        return "Tombstone sent for $accountId"
    }
}
