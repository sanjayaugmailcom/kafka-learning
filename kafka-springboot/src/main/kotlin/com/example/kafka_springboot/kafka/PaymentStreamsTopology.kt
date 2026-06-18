package com.example.kafka_springboot.kafka

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Aggregator
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Initializer
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.WindowStore
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class PaymentStreamsTopology(
    @Value("\${spring.kafka.streams.properties.schema.registry.url}") private val schemaRegistryUrl: String
) {

    @Bean
    fun paymentCountStream(builder: StreamsBuilder): KStream<String, GenericRecord> {
        // GenericAvroSerde is the Streams-specific Avro serde — handles schema registry
        // internally. Needs to know if it's used for keys (true) or values (false).
        val avroSerde = GenericAvroSerde().apply {
            configure(mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl), false)
        }

        // Step 1: Read from the "payments" topic.
        // The stream is keyed by paymentId (String) with Avro GenericRecord values.
        val stream: KStream<String, GenericRecord> = builder.stream(
            "payments",
            Consumed.with(Serdes.String(), avroSerde)
        )

        stream
            // Step 2: Re-key by currency.
            // selectKey replaces the message key without changing the value.
            // After this, all USD payments share the same key, all AUD payments share another, etc.
            // C# analogy: like .GroupBy(x => x.Currency) but the key change is written back to Kafka.
            .selectKey { _, value -> value.get("currency").toString() }

            // Step 3: Group records that now share the same key (currency).
            // groupByKey triggers a repartition behind the scenes — Kafka Streams creates an
            // internal topic so all records with the same key land on the same partition/instance.
            .groupByKey(Grouped.with(Serdes.String(), avroSerde))

            // Step 4: Apply a 60-second tumbling window.
            // "Tumbling" = non-overlapping fixed-size windows: [0-60s], [60-120s], etc.
            // "NoGrace" = no late-arrival tolerance; events after the window closes are dropped.
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))

            // Step 5: Count events per (currency, window) pair.
            // This writes counts into a local RocksDB state store named "payment-counts-store".
            // The store is backed by a Kafka changelog topic for fault tolerance.
            // C# analogy: like a ConcurrentDictionary<(currency, window), long> that survives restarts.
            .count(
                Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("payment-counts-store")
            )

            // Step 6: Convert KTable back to a KStream so we can write to an output topic.
            .toStream()

            // Step 7: Unwrap the windowed key.
            // The key after windowing is Windowed<String> — it carries both the currency and the
            // window start/end timestamps. We only want the currency string as the final key.
            .map { windowedKey, count -> KeyValue(windowedKey.key(), count.toString()) }

            // Step 8: Write to the output topic.
            .to("payment-totals", Produced.with(Serdes.String(), Serdes.String()))

        return stream
    }

    @Bean
    fun paymentAmountTotalsTable(builder: StreamsBuilder): KTable<String, Double> {
        val avroSerde = GenericAvroSerde().apply {
            configure(mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl), false)
        }

        val table: KTable<String, Double> = builder
            .stream("payments", Consumed.with(Serdes.String(), avroSerde))

            // Re-key by currency so all USD payments share the same key.
            .selectKey { _, value -> value.get("currency").toString() }

            // Group all records that share the same key together.
            .groupByKey(Grouped.with(Serdes.String(), avroSerde))

            // aggregate() is the general form of count().
            // - initializer: starting value for a key we've never seen before (like the seed in C# Aggregate)
            // - adder: called for every new record; receives (key, newValue, currentAccumulator) → newAccumulator
            // - Materialized: names the state store and declares the serdes for key (String) and value (Double)
            .aggregate(
                Initializer<Double> { 0.0 },
                Aggregator<String, GenericRecord, Double> { _, record, runningTotal ->
                    runningTotal + record.get("amount").toString().toDouble()
                },
                Materialized.`as`<String, Double, KeyValueStore<Bytes, ByteArray>>("payment-amount-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double())
            )

        // Write the KTable to an output topic so other services can consume it.
        // toStream() converts KTable → KStream; each update to the table emits a new record.
        table.toStream()
            .map { currency, total -> KeyValue(currency, total.toString()) }
            .to("payment-amount-totals", Produced.with(Serdes.String(), Serdes.String()))

        return table
    }
}
