package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener

class CompositeRebalanceListener(
    private val delegates: List<ConsumerRebalanceListener>,
) : ConsumerAwareRebalanceListener {
    override fun onPartitionsRevokedBeforeCommit(
        consumer: Consumer<*, *>,
        partitions: MutableCollection<TopicPartition>,
    ) {
        delegates.forEach { delegate ->
            if (delegate is ConsumerAwareRebalanceListener) {
                delegate.onPartitionsRevokedBeforeCommit(consumer, partitions)
            } else {
                delegate.onPartitionsRevoked(partitions)
            }
        }
    }

    override fun onPartitionsRevokedAfterCommit(
        consumer: Consumer<*, *>,
        partitions: MutableCollection<TopicPartition>,
    ) {
        delegates.forEach { delegate ->
            if (delegate is ConsumerAwareRebalanceListener) {
                delegate.onPartitionsRevokedAfterCommit(consumer, partitions)
            }
        }
    }

    override fun onPartitionsLost(
        consumer: Consumer<*, *>,
        partitions: MutableCollection<TopicPartition>,
    ) {
        delegates.forEach { delegate ->
            if (delegate is ConsumerAwareRebalanceListener) {
                delegate.onPartitionsLost(consumer, partitions)
            } else {
                delegate.onPartitionsLost(partitions)
            }
        }
    }

    override fun onPartitionsAssigned(
        consumer: Consumer<*, *>,
        partitions: MutableCollection<TopicPartition>,
    ) {
        delegates.forEach { delegate ->
            if (delegate is ConsumerAwareRebalanceListener) {
                delegate.onPartitionsAssigned(consumer, partitions)
            } else {
                delegate.onPartitionsAssigned(partitions)
            }
        }
    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
        delegates.forEach { it.onPartitionsRevoked(partitions) }
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        delegates.forEach { it.onPartitionsAssigned(partitions) }
    }

    override fun onPartitionsLost(partitions: MutableCollection<TopicPartition>) {
        delegates.forEach { it.onPartitionsLost(partitions) }
    }
}
