package com.example.kafka_springboot

import org.springframework.stereotype.Component
import java.util.concurrent.LinkedBlockingQueue

@Component
class FailedMessageStore {
    private val messages = LinkedBlockingQueue<String>()

    fun add(json: String) { messages.add(json) }

    fun drainAll(): List<String> {
        val drained = mutableListOf<String>()
        messages.drainTo(drained)
        return drained
    }
}
