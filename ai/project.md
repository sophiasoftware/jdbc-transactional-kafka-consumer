# jdbc-transactional-kafka-consumer

## Doel

Spring Boot starter die Kafka consumer offsets transactioneel opslaat in een relationele database (PostgreSQL via JDBC). Hierdoor worden Kafka offsets atomair gecommit samen met de domeinwerkzaamheden van de listener — als de listener faalt, worden zowel de domeinwijzigingen als de offset-update teruggedraaid.

## Opbouw

Het project bestaat uit twee Gradle submodules:

### `starter`

De herbruikbare Spring Boot starter library. Bevat:

- **`TransactionalKafkaOffsets`** — annotatie voor listener-methodes
- **`Aspect`** — AOP-aspect dat de annotatie onderschept, de transactie opent en de offsets opslaat
- **`OffsetsRepository`** — voert UPSERT uit in `kafka_consumer_offsets` via JdbcTemplate
- **`SchemaInitializer`** — maakt de `kafka_consumer_offsets` tabel aan bij opstarten (configureerbaar via `schema-initialization: create/none`)
- **`StoredOffsetRebalanceListener`** — herstelt Kafka consumer naar opgeslagen offsets bij rebalance
- **`ContainerCustomizer`** — past Kafka listener containers aan (manual ack, error handler)
- **`Properties`** — config prefix `jdbc-transactional-kafka-consumer`
- **`AutoConfiguration`** — registreert alle beans

### `sample`

Demo/test applicatie. Bevat:

- **`SampleApplication`** — Spring Boot entry point
- **`SampleListener`** — twee listeners:
  - `handleBatch` op `sample-batch-topic` (batch mode)
  - `handleSingle` op `sample-single-topic` (single record); doet INSERT in `processed_messages` tabel om transactionele rollback te demonstreren
- **`IntegrationTest`** — integratietests met embedded Kafka + PostgreSQL via Testcontainers

## Database schema

```sql
CREATE TABLE kafka_consumer_offsets (
    consumer_group VARCHAR(512) NOT NULL,
    topic          VARCHAR(512) NOT NULL,
    partition_id   INT          NOT NULL,
    offset_id      BIGINT       NOT NULL,
    PRIMARY KEY (consumer_group, topic, partition_id)
)
```

De `processed_messages` tabel wordt aangemaakt in de test setup (`@BeforeEach`):

```sql
CREATE TABLE IF NOT EXISTS processed_messages (key VARCHAR(512), value VARCHAR(512))
```

## Kernmechanisme

Het `@Around`-aspect in `Aspect.kt`:
1. Extraheert de volgende offsets uit de `ConsumerRecord(s)` argumenten
2. Voert de listener uit binnen een `TransactionTemplate`
3. Slaat de offsets op via `OffsetsRepository.saveAll` (vereist bestaande transactie via `MANDATORY`)
4. Bij exception: volledige rollback van transactie (domein-inserts + offset-updates)

## Technologie

- Kotlin 2.x, Spring Boot 4.x, Spring Kafka
- JdbcTemplate (geen JPA/Hibernate)
- Ktlint voor formatting
- Testcontainers (PostgreSQL 17) + embedded Kafka in tests
- AssertK + Awaitility in tests

## CI/CD & security (.github/workflows)

- **`ci.yml`** — draait `./gradlew check` op pull requests
- **`publish.yml`** — publiceert `starter` naar Maven Central bij een `v*`-tag
- **`codeql.yml`** — CodeQL SAST-scan (java-kotlin) op push/PR naar `main` en wekelijks
- **`scorecard.yml`** — OpenSSF Scorecard-analyse op push naar `main` en wekelijks, publiceert resultaten naar de publieke Scorecard API en als SARIF naar code scanning

Alle GitHub Actions zijn gepind op commit-SHA (met versie-comment) voor de Scorecard "Pinned-Dependencies" check.
