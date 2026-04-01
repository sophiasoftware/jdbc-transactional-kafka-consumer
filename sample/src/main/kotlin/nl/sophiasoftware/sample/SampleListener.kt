package nl.sophiasoftware.sample

import nl.sophiasoftware.jdbctransactionalkafkaconsumer.TransactionalKafkaOffsets
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

private val logger = KotlinLogging.logger {}

@Component
class SampleListener {

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
        if ("throw" in record.value()) {
            throw IllegalArgumentException("Simulated failure for: ${record.value()}")
        }
    }
}
