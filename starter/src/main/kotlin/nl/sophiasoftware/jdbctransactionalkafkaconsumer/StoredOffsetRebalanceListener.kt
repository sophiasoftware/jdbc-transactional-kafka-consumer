package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener

private val logger = KotlinLogging.logger {}

class StoredOffsetRebalanceListener(
    private val repository: OffsetsRepository,
    private val groupId: String,
) : ConsumerAwareRebalanceListener {
    override fun onPartitionsAssigned(
        consumer: Consumer<*, *>,
        partitions: MutableCollection<TopicPartition>,
    ) {
        if (partitions.isEmpty()) {
            return
        }

        val storedOffsets =
            repository.findOffsets(
                groupId = groupId,
                topicPartitions = partitions,
            )

        if (storedOffsets.isEmpty()) {
            logger.warn {
                "No stored offsets found for group '$groupId' on partitions $partitions — falling back to auto.offset.reset"
            }
            return
        }

        storedOffsets.forEach { (topicPartition, offset) ->
            logger.info { "Seeking $topicPartition to stored offset $offset for group $groupId" }
            consumer.seek(topicPartition, offset)
        }
    }
}
