package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import assertk.assertFailure
import assertk.assertions.isInstanceOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.reflect.MethodSignature
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.Acknowledgment
import org.springframework.transaction.support.TransactionCallback
import org.springframework.transaction.support.TransactionTemplate

class AspectTest {
    private val defaultGroupId = "orders-consumer-group"
    private val defaultTopic = "orders"
    private val defaultPartition = 3
    private val defaultOffset = 11L
    private val defaultRecord = ConsumerRecord(defaultTopic, defaultPartition, defaultOffset, "key", "value")
    private val defaultMethod =
        AspectTestListener::class.java.getDeclaredMethod(
            "handle",
            ConsumerRecord::class.java,
        )

    private val transactionTemplate = mockk<TransactionTemplate>()
    private val repository = mockk<OffsetsRepository>(relaxed = true)
    private val containerCustomizer = mockk<ContainerCustomizer>()
    private val joinPoint = mockk<ProceedingJoinPoint>(relaxed = true)
    private val methodSignature = mockk<MethodSignature>()
    private lateinit var aspect: Aspect

    @BeforeEach
    fun setup() {
        every { joinPoint.args } returns arrayOf(defaultRecord)
        every { joinPoint.signature } returns methodSignature
        every { methodSignature.method } returns defaultMethod
        every { containerCustomizer.resolveGroupId(method = defaultMethod) } returns defaultGroupId
        every { transactionTemplate.execute(any<TransactionCallback<Any?>>()) } answers {
            firstArg<TransactionCallback<Any?>>().doInTransaction(mockk(relaxed = true))
        }

        aspect =
            Aspect(
                transactionTemplate = transactionTemplate,
                repository = repository,
                containerCustomizer = containerCustomizer,
            )
    }

    @Test
    fun `wraps single record processing in transaction and saves next offset`() {
        aspect.aroundTransactionalKafkaListener(joinPoint = joinPoint)

        val expectedOffsets = mapOf(TopicPartition(defaultTopic, defaultPartition) to defaultOffset + 1)
        verify { repository.saveAll(groupId = defaultGroupId, offsets = expectedOffsets) }
    }

    @Test
    fun `returns result of listener invocation`() {
        every { joinPoint.proceed() } returns "listener-result"

        val result = aspect.aroundTransactionalKafkaListener(joinPoint = joinPoint)

        verify { repository.saveAll(groupId = defaultGroupId, offsets = any()) }
    }

    @Test
    fun `wraps batch processing in transaction and saves last offset plus one per partition`() {
        val tp1 = TopicPartition(defaultTopic, defaultPartition)
        val tp2 = TopicPartition(defaultTopic, 7)
        val batchRecords =
            ConsumerRecords(
                mapOf(
                    tp1 to
                        listOf(
                            ConsumerRecord(defaultTopic, defaultPartition, 5L, "k1", "v1"),
                            ConsumerRecord(defaultTopic, defaultPartition, 11L, "k2", "v2"),
                        ),
                    tp2 to
                        listOf(
                            ConsumerRecord(defaultTopic, 7, 13L, "k3", "v3"),
                        ),
                ),
                emptyMap(),
            )
        every { joinPoint.args } returns arrayOf(batchRecords)

        aspect.aroundTransactionalKafkaListener(joinPoint = joinPoint)

        val expectedOffsets = mapOf(tp1 to 12L, tp2 to 14L)
        verify { repository.saveAll(groupId = defaultGroupId, offsets = expectedOffsets) }
    }

    @Test
    fun `proceeds without transaction when batch is empty`() {
        val emptyBatch = ConsumerRecords.empty<String, String>()
        every { joinPoint.args } returns arrayOf(emptyBatch)

        aspect.aroundTransactionalKafkaListener(joinPoint = joinPoint)

        verify(exactly = 0) { transactionTemplate.execute(any()) }
        verify { joinPoint.proceed() }
    }

    @Test
    fun `throws when no ConsumerRecord or ConsumerRecords argument is present`() {
        every { joinPoint.args } returns arrayOf("not-a-record")

        assertFailure { aspect.aroundTransactionalKafkaListener(joinPoint = joinPoint) }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `acknowledges Kafka offset after transaction when Acknowledgment parameter is present`() {
        val acknowledgment = mockk<Acknowledgment>(relaxed = true)
        val methodWithAck =
            AspectTestListener::class.java.getDeclaredMethod(
                "handleWithAcknowledgment",
                ConsumerRecord::class.java,
                Acknowledgment::class.java,
            )
        every { joinPoint.args } returns arrayOf(defaultRecord, acknowledgment)
        every { methodSignature.method } returns methodWithAck
        every { containerCustomizer.resolveGroupId(method = methodWithAck) } returns defaultGroupId

        aspect.aroundTransactionalKafkaListener(joinPoint = joinPoint)

        verify { acknowledgment.acknowledge() }
    }
}

private class AspectTestListener {
    fun handle(record: ConsumerRecord<String, String>) {}

    fun handleWithAcknowledgment(
        record: ConsumerRecord<String, String>,
        acknowledgment: Acknowledgment,
    ) {}
}
