# JDBC Transactional Kafka Consumer

A Spring Boot starter that stores Kafka consumer offsets in a JDBC database **within the same transaction as your business logic**. This guarantees exactly-once processing: if your business transaction rolls back, the offset is not committed either.

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

## Schema

The starter needs a `kafka_consumer_offsets` table. You can let the starter create it automatically:

```yaml
jdbc-transactional-kafka-consumer:
  schema-initialization: create
```

If you prefer to manage the schema yourself (e.g. via Flyway or Liquibase), set it to `none` (the default) and create the table using the DDL in [`SchemaInitializer.kt`](starter/src/main/kotlin/nl/sophiasoftware/jdbctransactionalkafkaconsumer/SchemaInitializer.kt).
