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
        val partitionsAsString = partitions.joinToString(", ") { "${it.topic()}:${it.partition()}" }
        logger.info { "Received partitions assigned to group $groupId: $partitionsAsString" }

        if (partitions.isEmpty()) {
            logger.warn { "No partitions assigned to group $groupId, not seeking offsets" }
            return
        }

        val storedOffsets = repository.findOffsets(groupId = groupId, topicPartitions = partitions)
        if (storedOffsets.isEmpty()) {
            logger.warn {
                "No stored offsets found for group '$groupId' on partitions $partitionsAsString, falling back to auto.offset.reset"
            }
            return
        }

        logger.info { "Found stored offsets for group $groupId" }
        storedOffsets.forEach { (topicPartition, offset) ->
            logger.info {
                "Seeking ${topicPartition.topic()}:${topicPartition.partition()} to stored offset $offset for group $groupId"
            }
            consumer.seek(topicPartition, offset)
        }
    }
}
