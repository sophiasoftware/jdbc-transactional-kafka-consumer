package nl.sophiasoftware.sample

import io.github.oshai.kotlinlogging.KotlinLogging
import nl.sophiasoftware.jdbctransactionalkafkaconsumer.TransactionalKafkaOffsets
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

private val logger = KotlinLogging.logger {}

@Component
class SampleListener(
    private val jdbcTemplate: JdbcTemplate,
) {
    @TransactionalKafkaOffsets
    @KafkaListener(id = "batch-listener", topics = ["sample-batch-topic"], batch = "true")
    fun handleBatch(records: ConsumerRecords<String, String>) {
        records.forEach { record ->
            logger.info { "Batch: ${record.key()} -> ${record.value()}" }
            if ("throw" in record.value()) {
                throw IllegalArgumentException("Simulated failure for: ${record.value()}")
            }
        }
    }

    @TransactionalKafkaOffsets
    @KafkaListener(id = "single-listener", topics = ["sample-single-topic"])
    fun handleSingle(record: ConsumerRecord<String, String>) {
        logger.info { "Single: ${record.key()} -> ${record.value()}" }
        jdbcTemplate.update(
            "INSERT INTO processed_messages (key, value) VALUES (?, ?)",
            record.key(),
            record.value(),
        )
        if ("throw" in record.value()) {
            throw IllegalArgumentException("Simulated failure for: ${record.value()}")
        }
    }

    @TransactionalKafkaOffsets
    @KafkaListener(id = "ack-listener", topics = ["sample-ack-topic"])
    fun handleWithAcknowledgment(
        record: ConsumerRecord<String, String>,
        acknowledgment: Acknowledgment,
    ) {
        logger.info { "Ack: ${record.key()} -> ${record.value()}" }
        if ("throw" in record.value()) {
            throw IllegalArgumentException("Simulated failure for: ${record.value()}")
        }
    }
}
