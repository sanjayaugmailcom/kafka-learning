package com.example.kafka_springboot.kafka

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import java.util.concurrent.atomic.AtomicInteger

class RoundRobinPartitioner : Partitioner {
    private val counter = AtomicInteger(0)

    override fun partition(topic: String, key: Any?, keyBytes: ByteArray?,
                          value: Any?, valueBytes: ByteArray?, cluster: Cluster): Int {
        val partitions = cluster.partitionCountForTopic(topic)
        return counter.getAndIncrement() % partitions
    }

    override fun configure(configs: MutableMap<String, *>) {
        // No configuration needed
    }

    override fun close() {
        // No resources to close
    }
}