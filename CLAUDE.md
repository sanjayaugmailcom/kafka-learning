# Learning Mode — Java Kotlin

I am learning Java/Kotlin to get a job at one of: Atlassian, Canva, Airwallex, MYOB, Culture Amp, Entelect, Linktree, Envato.
I come from an enterprise C#/.NET background (senior level). The primary targets are Atlassian, Canva, Airwallex, MYOB — all Java/Kotlin shops.

## How to teach me
- Explain what each code change does and why, don't just make the edit silently
- When introducing a new concept, give a one-line analogy to C# (.NET)
- Point out idiomatic Kotlin/Java patterns I should know
- Don't write code I haven't seen yet without explaining it first

## Learning roadmap (in priority order)
1. Microservices + event-driven patterns (Kafka)
   - After Kafka: cover distributed systems fundamentals (CAP theorem, saga pattern, circuit breakers) — theory anchored by hands-on Kafka context
2. Distributed systems fundamentals
3. Payment + fintech flows
4. SaaS basics (multi-tenancy, sharding, search, permissions)
5. LeetCode — Data Structures & Algorithms, medium difficulty, 30-40 problems
6. Kotlin basics — console app that prints Hello World (equivalent: C# console app)
7. Spring Boot — web app, no database, hardcoded responses (equivalent: ASP.NET Core minimal API)
8. Spring Boot — REST controllers only, no UI (equivalent: ASP.NET Core Web API with [ApiController])
9. Spring Boot — direct database access with JDBC Template (equivalent: ADO.NET / SqlConnection + SqlCommand)
10. Spring Boot — ORM with Spring Data JPA + Hibernate (equivalent: Entity Framework Core + DbContext)
10. Docker + Kubernetes basics
11. AWS core (SQS, S3, ECS/Lambda)
12. System Design

## Kafka certification
Targeting: Confluent Certified Developer for Apache Kafka (CCDAK)

Covered:
- Producers + consumers
- Consumer groups + rebalancing
- Custom partitioner
- Dead Letter Topic + retry
- Avro + Schema Registry
- Idempotent consumers

Remaining gaps (in priority order):
1. ~~Kafka Streams — KStream, KTable, windowing (heavily tested)~~ ✓ DONE
2. ~~Kafka Connect — source connector (Debezium CDC)~~ ✓ DONE | sink connector — still needed
3. ~~Schema compatibility rules — backward, forward, full~~ ✓ DONE
4. Exactly-once semantics — transactional producer API
5. Log compaction — key-based retention
6. ksqlDB — stream processing with SQL
7. Replication + ISR — in-sync replicas, leader election

Completed:
- Offset management — manual commits, enable.auto.commit, auto.offset.reset
- Producer config deep dive — acks, linger.ms, batch.size, enable.idempotence

## Interview target: high-throughput system design
Be able to explain and partially demonstrate how a 10,000 TPS payment system is built:
- Partitioning for parallelism (already built)
- Producer batching — linger.ms, batch.size (already configured)
- Outbox pattern for async decoupling (already built)
- Debezium CDC to replace polling outbox (done)
- Horizontal scaling — multiple app instances, each owning partitions
- Sharding the DB by account ID to remove the single-DB bottleneck

## Current project
Building a REST API with Spring Boot + Kotlin, PostgreSQL, Docker, tested with JUnit + Testcontainers.
This is the portfolio project to demonstrate skills for job applications.