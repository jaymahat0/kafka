# Kafka Learning

Apache Kafka — core concepts, internals, CLI usage, and Spring Boot integration. Written while learning Kafka alongside backend/Spring Boot development.

---

## Table of Contents

1. [What is Kafka?](#1-what-is-kafka)
2. [Core Concepts](#2-core-concepts)
3. [Architecture](#3-architecture)
4. [Topics, Partitions & Offsets](#4-topics-partitions--offsets)
5. [Producers](#5-producers)
6. [Consumers & Consumer Groups](#6-consumers--consumer-groups)
7. [Replication & Fault Tolerance](#7-replication--fault-tolerance)
8. [Delivery Semantics](#8-delivery-semantics)
9. [ZooKeeper vs KRaft](#9-zookeeper-vs-kraft)
10. [Kafka CLI Cheatsheet](#10-kafka-cli-cheatsheet)
11. [Kafka with Spring Boot](#11-kafka-with-spring-boot)
12. [Kafka Streams (Intro)](#12-kafka-streams-intro)
13. [Kafka Connect (Intro)](#13-kafka-connect-intro)
14. [Common Use Cases](#14-common-use-cases)
15. [Kafka vs Other Messaging Systems](#15-kafka-vs-other-messaging-systems)
16. [Local Setup (Docker)](#16-local-setup-docker)
17. [Resources](#17-resources)

---

## 1. What is Kafka?

Apache Kafka is a **distributed event streaming platform**. It's used to publish, store, and process streams of records (events) in real time. Originally built at LinkedIn, now an Apache project.

At its core, Kafka is:
- A **publish/subscribe messaging system**
- A **distributed commit log** — records are persisted to disk and replicated
- A **stream processing platform** (via Kafka Streams)

Kafka is not a traditional message queue (like RabbitMQ) — it's better thought of as a durable, append-only log that multiple consumers can read independently, at their own pace.

---

## 2. Core Concepts

| Term | Meaning |
|---|---|
| **Broker** | A single Kafka server. A Kafka cluster is made up of multiple brokers. |
| **Topic** | A named stream of records (like a table/category). |
| **Partition** | A topic is split into partitions for parallelism and scalability. |
| **Producer** | Publishes (writes) records to topics. |
| **Consumer** | Subscribes to (reads) records from topics. |
| **Consumer Group** | A set of consumers that share the work of consuming a topic. |
| **Offset** | A unique, sequential ID for each record within a partition. |
| **Replica** | A copy of a partition stored on another broker for fault tolerance. |
| **ISR (In-Sync Replica)** | Replicas that are fully caught up with the partition leader. |
| **ZooKeeper / KRaft** | Coordination layer for cluster metadata (legacy vs modern). |

---

## 3. Architecture

```
                     ┌──────────────────────────────────┐
                     │           Kafka Cluster          │
                     │                                  │
 Producer ───────►   │  Broker 1   Broker 2   Broker 3  │  ────► Consumer Group
                     │   (leader)  (replica)  (replica) │
                     └──────────────────────────────────┘
                                       │
                                       Topic: "orders"
                                       ├── Partition 0
                                       ├── Partition 1
                                       └── Partition 2
```

- Producers write to a **topic**; Kafka decides which **partition** a record goes to (via key hashing or round-robin).
- Each partition has one **leader broker** and zero or more **follower (replica) brokers**.
- Consumers read from partitions, tracking their own **offset** (position).
- Kafka does **not** push data to consumers — consumers **pull** at their own pace.

---

## 4. Topics, Partitions & Offsets

- A **topic** is divided into one or more **partitions**.
- Each partition is an **ordered, immutable, append-only log**.
- Records within a partition are strictly ordered; **ordering is NOT guaranteed across partitions** of the same topic.
- Each record gets an **offset** — a monotonically increasing ID, unique per partition.
- More partitions = more parallelism (more consumers can read concurrently), but also more overhead.

**Key-based partitioning:**
- If a producer sends a record with a key, Kafka hashes the key to consistently route it to the same partition (useful for maintaining order per key, e.g., all events for `user_id=42` go to the same partition).
- If no key is provided, Kafka uses round-robin / sticky partitioning.

**Retention:**
- Kafka retains records for a configurable time (`retention.ms`, default 7 days) or size (`retention.bytes`), **regardless of whether they've been consumed**.
- This is fundamentally different from traditional queues, where a message is deleted once consumed.

---

## 5. Producers

Producers publish records to a topic. Key configuration knobs:

| Config | Purpose |
|---|---|
| `bootstrap.servers` | List of broker addresses to connect to. |
| `key.serializer` / `value.serializer` | How to serialize the key/value (e.g., `StringSerializer`, `JsonSerializer`). |
| `acks` | Durability guarantee (`0`, `1`, `all`) — see [Delivery Semantics](#8-delivery-semantics). |
| `retries` | Number of retry attempts on transient failure. |
| `enable.idempotence` | Prevents duplicate records on retries. |
| `linger.ms` / `batch.size` | Controls batching for throughput. |

**Acks values:**
- `acks=0` — fire and forget, no guarantee.
- `acks=1` — leader broker acknowledges (fast, but risk of loss if leader fails before replication).
- `acks=all` (`-1`) — all in-sync replicas must acknowledge (strongest durability).

---

## 6. Consumers & Consumer Groups

- Consumers **pull** records from topics and track their own offset.
- Consumers belong to a **consumer group** (`group.id`). Kafka distributes partitions among the consumers in a group so that:
  - Each partition is consumed by **exactly one consumer** within a group at a time.
  - Multiple consumer groups can independently consume the **same topic** in full (pub/sub fan-out).
- If a consumer group has more consumers than partitions, extra consumers sit idle.
- If a consumer fails, Kafka triggers a **rebalance**, reassigning its partitions to other consumers in the group.

**Offset management:**
- `enable.auto.commit=true` — offsets committed automatically at intervals (risk of reprocessing or data loss).
- Manual commit (`commitSync()` / `commitAsync()`) gives more control, typically used in production for at-least-once processing.

**Offset reset policy (`auto.offset.reset`):**
- `earliest` — start from the beginning of the partition.
- `latest` — start from new records only (default).
- `none` — throw an error if no offset exists.

---

## 7. Replication & Fault Tolerance

- Each partition has a **replication factor** (commonly 3 in production).
- One replica is the **leader** (handles all reads/writes); others are **followers** that replicate data.
- **ISR (In-Sync Replicas)**: followers that are fully caught up with the leader within `replica.lag.time.max.ms`.
- If the leader broker fails, a new leader is elected from the ISR set — this is why `acks=all` combined with a healthy ISR set is critical for durability.
- `min.insync.replicas` sets the minimum ISR count required for a write to succeed when `acks=all`.

---

## 8. Delivery Semantics

| Semantic | Description | How to achieve |
|---|---|---|
| **At most once** | Records may be lost, never duplicated. | `acks=0`, no retries |
| **At least once** | Records never lost, may be duplicated. | `acks=all` + retries (default, most common) |
| **Exactly once (EOS)** | No loss, no duplication. | Idempotent producer + transactional API (`enable.idempotence=true`, `transactional.id` set) |

Exactly-once semantics in Kafka apply strongly within Kafka-to-Kafka pipelines (e.g., Kafka Streams); exactly-once to external systems requires additional care (idempotent writes, transactional outbox pattern, etc.).

---

## 9. ZooKeeper vs KRaft

- **Legacy architecture**: Kafka relied on **Apache ZooKeeper** for cluster metadata, leader election, and configuration.
- **KRaft (Kafka Raft)**: Since Kafka 3.x (and mandatory from Kafka 4.0), Kafka replaced ZooKeeper with its own **built-in Raft-based consensus protocol**, simplifying deployment (no separate ZooKeeper cluster needed) and improving scalability/failover time.
- New setups should use **KRaft mode**; ZooKeeper mode is deprecated/removed in modern versions.

---

## 10. Kafka CLI Cheatsheet

```bash
# Create a topic
kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe a topic
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092

# Produce messages (console producer)
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092

# Consume messages (console consumer)
kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092 --from-beginning

# List consumer groups
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Describe a consumer group (lag, offsets, assignment)
kafka-consumer-groups.sh --describe --group my-group --bootstrap-server localhost:9092

# Delete a topic
kafka-topics.sh --delete --topic orders --bootstrap-server localhost:9092
```

---

## 11. Kafka with Spring Boot

Dependency (`pom.xml`):

```xml
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
</dependency>
```

**`application.yml`:**

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: my-service-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
```

**Producer:**

```java
@Service
@RequiredArgsConstructor
public class OrderProducer {

  private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

  public void sendOrderEvent(OrderEvent event) {
    kafkaTemplate.send("orders", event.getOrderId(), event);
  }
}
```

**Consumer:**

```java
@Component
public class OrderConsumer {

  @KafkaListener(topics = "orders", groupId = "my-service-group")
  public void consume(OrderEvent event) {
    // process event
  }
}
```

**Notes:**
- `KafkaTemplate` is thread-safe and can be reused across the application.
- `@KafkaListener` methods run on listener container threads managed by Spring — configure concurrency via `ConcurrentKafkaListenerContainerFactory` to match partition count.
- For manual ack mode, set `spring.kafka.listener.ack-mode: MANUAL` and use `Acknowledgment.acknowledge()` in the listener.

---

## 12. Kafka Streams (Intro)

Kafka Streams is a Java library for building stream processing applications directly on top of Kafka (no separate cluster needed).

```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> source = builder.stream("input-topic");
source.mapValues(value -> value.toUpperCase())
        .to("output-topic");
```

Core abstractions: `KStream` (record stream), `KTable` (changelog/table view), windowed aggregations, joins.

---

## 13. Kafka Connect (Intro)

Kafka Connect is a framework for integrating Kafka with external systems (databases, S3, Elasticsearch, etc.) using pre-built **source** and **sink** connectors — without writing custom producer/consumer code.

- **Source connector**: pulls data from an external system into Kafka (e.g., Debezium for CDC from a database).
- **Sink connector**: pushes data from Kafka into an external system (e.g., JDBC sink into Postgres).

---

## 14. Common Use Cases

- Event-driven microservices communication (decoupling services)
- Log aggregation and centralized logging pipelines
- Real-time analytics and monitoring dashboards
- Change Data Capture (CDC) pipelines
- Activity tracking (clickstreams, user events)
- Message queue replacement for high-throughput systems

---

## 15. Kafka vs Other Messaging Systems

| Feature | Kafka | RabbitMQ | AWS SQS |
|---|---|---|---|
| Model | Distributed log | Traditional queue/broker | Managed queue |
| Message retention | Configurable, independent of consumption | Deleted after ack | Deleted after ack (with visibility timeout) |
| Ordering | Per-partition | Per-queue | FIFO queues only |
| Throughput | Very high | Moderate | Moderate |
| Replay | Yes (seek to offset) | No (once consumed, gone) | No |
| Best for | Streaming, event sourcing, high volume | Task queues, RPC-style messaging | Simple decoupled queues, serverless |

---

## 16. Local Setup (Docker)

`docker-compose.yml` (KRaft mode, no ZooKeeper needed):

```yaml
version: "3.8"
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
```

```bash
docker compose up -d
```

---

## 17. Resources

- [Official Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Tutorials](https://developer.confluent.io/)
- [Spring for Apache Kafka Docs](https://docs.spring.io/spring-kafka/reference/)
- *Kafka: The Definitive Guide* (O'Reilly)

---

### Repo Structure (suggested)

```
kafka/
├── README.md
├── kafka-learning-consumer/           # Consumer Service
│    ├── src/..
│    │    ├── controller
│    │    ├── model
│    │    └── service
│    └── pom.xml
└──kafka-learning-producer/            # Producer Service
    ├── src/..
    │    ├── controller
    │    ├── model
    │    └── service
    └── pom.xml
```