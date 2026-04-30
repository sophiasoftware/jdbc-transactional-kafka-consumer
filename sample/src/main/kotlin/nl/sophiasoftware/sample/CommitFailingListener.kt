package nl.sophiasoftware.sample

import io.github.oshai.kotlinlogging.KotlinLogging
import nl.sophiasoftware.jdbctransactionalkafkaconsumer.TransactionalKafkaOffsets
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager

private val logger = KotlinLogging.logger {}

@Component
class CommitFailingListener {
    @TransactionalKafkaOffsets
    @KafkaListener(id = "commit-failing-listener", topics = ["sample-commit-failing-topic"])
    fun handle(record: ConsumerRecord<String, String>) {
        logger.info { "CommitFailing: ${record.key()} -> ${record.value()}" }
        failOnCommit(value = record.value())
    }

    private fun failOnCommit(value: String) {
        TransactionSynchronizationManager.registerSynchronization(
            object : TransactionSynchronization {
                override fun beforeCommit(readOnly: Boolean) {
                    logger.info { "Simulated failure after offset save for: $value" }
                    throw IllegalStateException("Simulated failure after offset save for: $value")
                }
            },
        )
    }
}
