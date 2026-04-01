package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import assertk.assertFailure
import assertk.assertions.isInstanceOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class StoredOffsetRebalanceListenerTest {

    private val defaultGroupId = "invoices-consumer-group"
    private val defaultTopicPartition = TopicPartition("invoices", 3)
    private val defaultStoredOffset = 41L

    private val consumer = mockk<Consumer<Any, Any>>(relaxed = true)
    private val repository = mockk<OffsetsRepository>()
    private lateinit var listener: StoredOffsetRebalanceListener

    @BeforeEach
    fun setup() {
        every {
            repository.findOffsets(groupId = defaultGroupId, topicPartitions = any())
        } returns mapOf(defaultTopicPartition to defaultStoredOffset)

        listener = StoredOffsetRebalanceListener(repository = repository, groupId = defaultGroupId)
    }

    @Test
    fun `seeks consumer to stored offset when partition is assigned`() {
        listener.onPartitionsAssigned(consumer = consumer, partitions = mutableListOf(defaultTopicPartition))

        verify { consumer.seek(defaultTopicPartition, defaultStoredOffset) }
    }

    @Test
    fun `seeks all partitions that have a stored offset`() {
        val anotherPartition = TopicPartition("invoices", 7)
        every {
            repository.findOffsets(groupId = defaultGroupId, topicPartitions = any())
        } returns mapOf(
            defaultTopicPartition to defaultStoredOffset,
            anotherPartition to 97L,
        )

        listener.onPartitionsAssigned(
            consumer = consumer,
            partitions = mutableListOf(defaultTopicPartition, anotherPartition),
        )

        verify { consumer.seek(defaultTopicPartition, defaultStoredOffset) }
        verify { consumer.seek(anotherPartition, 97L) }
    }

    @Test
    fun `does not contact repository when partitions list is empty`() {
        listener.onPartitionsAssigned(consumer = consumer, partitions = mutableListOf())

        verify(exactly = 0) { repository.findOffsets(any(), any()) }
    }

    @Test
    fun `does not seek when no stored offsets are found`() {
        every {
            repository.findOffsets(groupId = defaultGroupId, topicPartitions = any())
        } returns emptyMap()

        listener.onPartitionsAssigned(consumer = consumer, partitions = mutableListOf(defaultTopicPartition))

        verify(exactly = 0) { consumer.seek(any(), any<Long>()) }
    }
}
