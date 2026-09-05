# Kafka Payments Platform

An event-driven payments system built on Kafka, Spring Boot, and Kotlin — demonstrating outbox-based event publishing, exactly-once processing, stream aggregation, dead-letter handling, and CDC-style sink integration with Postgres.

## What's in here

| Module | What it is |
|---|---|
| [`kafka-producer/`](kafka-producer) | Standalone Kotlin console app — the first "hello world" producer (plain `KafkaProducer`, string serde). |
| [`kafka-consumer/`](kafka-consumer) | Standalone Kotlin console app — matching bare-bones `KafkaConsumer` poll loop. |
| [`kafka-springboot/`](kafka-springboot) | **The main app.** Spring Boot 4 + Kotlin, PostgreSQL, Avro/Schema Registry, Kafka Streams — this is where the interesting patterns live. |
| [`kafka-connect/`](kafka-connect) | Custom Kafka Connect image + a JDBC sink connector that upserts the `payments` topic into a Postgres table. |
| [`docker-compose.yml`](docker-compose.yml) | Kafka (KRaft, single broker), Schema Registry, Kafka Connect, ksqlDB, Kafka UI, Postgres. |

For the full data flow and where each Kafka concept lives in code, see **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Running it

```bash
docker compose up -d                      # Kafka, Postgres, Schema Registry, Connect, ksqlDB, Kafka UI
./kafka-connect/connectors/register-sink.sh   # register the JDBC sink connector (after Connect is healthy)

cd kafka-springboot
./gradlew bootRun
```

Kafka UI: http://localhost:8081 · Schema Registry: http://localhost:8085 · Connect REST: http://localhost:8083 · ksqlDB: http://localhost:8088

### Endpoints (kafka-springboot, port 8080)

| Endpoint | Does |
|---|---|
| `POST /payments` | Accepts a payment, writes it + an outbox row in one DB transaction. |
| `POST /payments/bulk` | Same, ×10, across a few currencies — useful for seeing Streams windowing/aggregation react. |
| `POST /payments/retry` | Redrives anything that landed in the DLT back onto `payments`. |
| `POST /accounts/{id}/balance` | Publishes a balance update onto the compacted `account-balances` topic. |
| `DELETE /accounts/{id}` | Publishes a tombstone (null value) for that account key. |

Password note: the local Postgres credential is read from `DB_PASSWORD` (defaults to `secret` for local dev) — see [docker-compose.yml](docker-compose.yml) and [application.yaml](kafka-springboot/src/main/resources/application.yaml).

## Known gaps

- **No real tests yet** — `kafka-springboot` only has the default Spring Boot context-load placeholder test. Testcontainers isn't wired in yet.
- **No Debezium/CDC source connector** — only the JDBC *sink* connector exists in `kafka-connect/`; the outbox is currently drained by a polling scheduler rather than log-based CDC.
- **In-memory-only stores** — `FailedMessageStore` (DLT staging) is a `LinkedBlockingQueue` and is lost on restart; `ProcessedPaymentStore` (idempotent-consumer dedup) *is* Postgres-backed.
