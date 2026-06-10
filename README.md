# JDBC Transactional Kafka Consumer

A Spring Boot starter that stores Kafka consumer offsets in a JDBC database **within the same transaction as your business logic**. This guarantees exactly-once processing: if your business transaction rolls back, the offset is not committed either.

[![Maven](https://badges.mvnrepository.com/badge/nl.sophiasoftware/jdbc-transactional-kafka-consumer-spring-boot-starter/badge.svg?label=Maven)](https://mvnrepository.com/artifact/nl.sophiasoftware/jdbc-transactional-kafka-consumer-spring-boot-starter)

## What This Spring Boot Starter Solves

### The Dual-Write Problem

When you consume a Kafka message and process it with a database transaction, you have two separate systems that can each fail independently. Consider this sequence:

1. Your business logic runs and the database transaction **commits** ✅
2. Kafka offset commit **fails** ❌

Kafka now thinks the message was never processed. On restart, your listener receives the same, your business logic runs a second time, potentially creating duplicate records, double charges, or inconsistent state.

The reverse is potentially more dangerous:

1. Kafka offset commits **successfully** ✅
2. Your database transaction **rolls back** ❌

Kafka thinks the message is done, but your business logic never completed. The message is silently lost.

This is the **dual-write problem**: you cannot atomically commit to both Kafka and a database without a distributed transaction.

Instead of letting Kafka manage offsets, this starter stores them in **your own database**, inside the same transaction as your business logic. Either both commit or both roll back. No duplicates. No silent data loss. Exactly-once processing, guaranteed by your database.

### Atomic Backups

Because consumer offsets live in the same database as your business data, a regular database backup captures both **in a single, consistent snapshot**.

When you restore that backup, the offsets it contains describe exactly which Kafka messages had already been processed at the moment the backup was taken, the same moment your business data reflects. Restart your consumers and they pick up right where the backup left off: no missed messages, no reprocessed ones.

Compare this to storing offsets in Kafka itself: after restoring a database backup, your business data is "in the past" while Kafka's committed offsets are still "in the present". You'd have to manually figure out and reset the consumer group's offsets to match the restored point in time; get it wrong and you either reprocess messages already reflected in your data, or skip messages your restored data is missing.

With offsets stored alongside your business data, **restoring the database backup is enough**. There's no separate Kafka offset reset step.

## Installation

```kotlin
implementation("nl.sophiasoftware:jdbc-transactional-kafka-consumer-spring-boot-starter:<version>")
```

See [Maven Central](https://central.sonatype.com/artifact/nl.sophiasoftware/jdbc-transactional-kafka-consumer-spring-boot-starter) for the latest available version.

## Usage

Add `@TransactionalKafkaOffsets` to your `@KafkaListener` method. The listener method must have a `ConsumerRecord` or `ConsumerRecords` as its argument:

```kotlin
@KafkaListener(id = "orders", topics = ["orders"], groupId = "orders")
@TransactionalKafkaOffsets
fun handle(record: ConsumerRecord<String, String>) {
    // your business logic here — offset is committed only if this succeeds
}
```

Batch listeners are also supported:

```kotlin
@KafkaListener(id = "orders", topics = ["orders"], groupId = "orders")
@TransactionalKafkaOffsets
fun handle(records: ConsumerRecords<String, String>) {
    // processes a batch — offsets are committed per partition after successful processing
}
```

### Kafka offset commit (optional)

By default, this starter uses your database as the sole source of truth for offsets and does not commit anything back to Kafka. Kafka's consumer group management and monitoring tools therefore show stale lag.

If you want monitoring tools to reflect accurate consumer lag, add an `Acknowledgment` parameter to your listener. After the database transaction commits successfully, the starter acknowledges the offset back to Kafka:

```kotlin
@KafkaListener(id = "orders", topics = ["orders"], groupId = "orders")
@TransactionalKafkaOffsets
fun handle(record: ConsumerRecord<String, String>, acknowledgment: Acknowledgment) {
    // your business logic here
}
```

Because Spring Kafka only populates the `Acknowledgment` argument when the listener container's ack mode is `MANUAL` (or `MANUAL_IMMEDIATE`), you must configure that yourself — for example globally:

```yaml
spring:
  kafka:
    listener:
      ack-mode: manual
```

or per-listener via a custom `containerFactory`. The starter validates this at startup and fails fast with a clear message if the ack mode is wrong.

This is purely additive: the database remains the authoritative source. If the Kafka acknowledgment fails after the database has committed, the offset is still tracked correctly in your database and the message will not be reprocessed.

## Schema

The starter needs a `kafka_consumer_offsets` table. You can let the starter create it automatically:

```yaml
jdbc-transactional-kafka-consumer:
  schema-initialization: create
```

If you prefer to manage the schema yourself (e.g. via Flyway or Liquibase), set it to `none` (the default) and create the table using the DDL in [`SchemaInitializer.kt`](starter/src/main/kotlin/nl/sophiasoftware/jdbctransactionalkafkaconsumer/SchemaInitializer.kt).
