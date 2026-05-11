package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener

class CompositeRebalanceListenerTest {
    private val defaultPartitions = mutableListOf(TopicPartition("invoices", 3))

    private val consumer = mockk<Consumer<Any, Any>>(relaxed = true)
    private val plainDelegate = mockk<ConsumerRebalanceListener>(relaxed = true)
    private val awareDelegate = mockk<ConsumerAwareRebalanceListener>(relaxed = true)
    private lateinit var listener: CompositeRebalanceListener

    @BeforeEach
    fun setup() {
        listener = CompositeRebalanceListener(delegates = listOf(plainDelegate, awareDelegate))
    }

    @Test
    fun `onPartitionsAssigned invokes delegates in order, ours last`() {
        listener.onPartitionsAssigned(consumer = consumer, partitions = defaultPartitions)

        verifyOrder {
            plainDelegate.onPartitionsAssigned(defaultPartitions)
            awareDelegate.onPartitionsAssigned(consumer, defaultPartitions)
        }
    }

    @Test
    fun `onPartitionsAssigned routes plain delegate to basic method, not aware method`() {
        listener.onPartitionsAssigned(consumer = consumer, partitions = defaultPartitions)

        verify { plainDelegate.onPartitionsAssigned(defaultPartitions) }
    }

    @Test
    fun `onPartitionsRevokedBeforeCommit routes plain to onPartitionsRevoked and aware to aware variant`() {
        listener.onPartitionsRevokedBeforeCommit(consumer = consumer, partitions = defaultPartitions)

        verify { plainDelegate.onPartitionsRevoked(defaultPartitions) }
        verify { awareDelegate.onPartitionsRevokedBeforeCommit(consumer, defaultPartitions) }
    }

    @Test
    fun `onPartitionsRevokedAfterCommit only invokes aware delegates`() {
        listener.onPartitionsRevokedAfterCommit(consumer = consumer, partitions = defaultPartitions)

        verify { awareDelegate.onPartitionsRevokedAfterCommit(consumer, defaultPartitions) }
        verify(exactly = 0) {
            plainDelegate.onPartitionsRevoked(any())
        }
    }

    @Test
    fun `onPartitionsLost routes plain delegate to basic method and aware delegate to aware method`() {
        listener.onPartitionsLost(consumer = consumer, partitions = defaultPartitions)

        verify { plainDelegate.onPartitionsLost(defaultPartitions) }
        verify { awareDelegate.onPartitionsLost(consumer, defaultPartitions) }
    }

    @Test
    fun `basic onPartitionsRevoked is fanned out to all delegates`() {
        listener.onPartitionsRevoked(defaultPartitions)

        verify { plainDelegate.onPartitionsRevoked(defaultPartitions) }
        verify { awareDelegate.onPartitionsRevoked(defaultPartitions) }
    }

    @Test
    fun `basic onPartitionsAssigned is fanned out to all delegates`() {
        listener.onPartitionsAssigned(defaultPartitions)

        verify { plainDelegate.onPartitionsAssigned(defaultPartitions) }
        verify { awareDelegate.onPartitionsAssigned(defaultPartitions) }
    }

    @Test
    fun `basic onPartitionsLost is fanned out to all delegates`() {
        listener.onPartitionsLost(defaultPartitions)

        verify { plainDelegate.onPartitionsLost(defaultPartitions) }
        verify { awareDelegate.onPartitionsLost(defaultPartitions) }
    }
}
