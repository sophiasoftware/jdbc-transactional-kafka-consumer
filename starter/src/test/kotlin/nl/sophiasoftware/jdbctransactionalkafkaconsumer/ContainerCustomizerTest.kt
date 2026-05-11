package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isSameInstanceAs
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationContext
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.AbstractMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.MessageListenerContainer

class ContainerCustomizerTest {
    private val defaultGroupId = "payments-consumer-group"
    private val defaultContainerId = "valid-container-id"
    private val defaultMethod =
        ValidTestListener::class.java.getDeclaredMethod(
            "handle",
            ConsumerRecord::class.java,
        )

    private val applicationContext = mockk<ApplicationContext>()
    private val endpointRegistry = mockk<KafkaListenerEndpointRegistry>()
    private val repository = mockk<OffsetsRepository>(relaxed = true)
    private val defaultContainer = mockk<AbstractMessageListenerContainer<Any, Any>>(relaxed = true)
    private lateinit var customizer: ContainerCustomizer

    @BeforeEach
    fun setup() {
        every { applicationContext.beanDefinitionNames } returns arrayOf("validTestListener")
        every { applicationContext.getType("validTestListener") } returns ValidTestListener::class.java
        every { applicationContext.getBean("validTestListener") } returns ValidTestListener()
        every { endpointRegistry.getListenerContainer(defaultContainerId) } returns defaultContainer
        every { defaultContainer.groupId } returns defaultGroupId
        every { defaultContainer.containerProperties.consumerRebalanceListener } returns null

        customizer =
            ContainerCustomizer(
                applicationContext = applicationContext,
                endpointRegistry = endpointRegistry,
                repository = repository,
            )
    }

    @Test
    fun `resolveGroupId returns group id for configured method after context refresh`() {
        customizer.onContextRefreshed()

        val result = customizer.resolveGroupId(method = defaultMethod)

        assertThat(result).isEqualTo(defaultGroupId)
    }

    @Test
    fun `resolveGroupId throws for method that was not configured`() {
        assertFailure { customizer.resolveGroupId(method = defaultMethod) }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `onContextRefreshed sets MANUAL ack mode on container`() {
        customizer.onContextRefreshed()

        verify { defaultContainer.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL }
    }

    @Test
    fun `onContextRefreshed sets composite with only stored offset listener when no existing listener`() {
        customizer.onContextRefreshed()

        val captured = slot<CompositeRebalanceListener>()
        verify { defaultContainer.containerProperties.setConsumerRebalanceListener(capture(captured)) }
        val delegates = captured.captured.delegates()
        assertThat(delegates).hasSize(1)
        assertThat(delegates[0] as Any).isInstanceOf(StoredOffsetRebalanceListener::class)
    }

    @Test
    fun `onContextRefreshed wraps existing rebalance listener with composite, ours last`() {
        val existingListener = mockk<ConsumerRebalanceListener>(relaxed = true)
        every { defaultContainer.containerProperties.consumerRebalanceListener } returns existingListener

        customizer.onContextRefreshed()

        val captured = slot<CompositeRebalanceListener>()
        verify { defaultContainer.containerProperties.setConsumerRebalanceListener(capture(captured)) }
        val delegates = captured.captured.delegates()
        assertThat(delegates).hasSize(2)
        assertThat(delegates[0] as Any).isSameInstanceAs(existingListener)
        assertThat(delegates[1] as Any).isInstanceOf(StoredOffsetRebalanceListener::class)
    }

    private fun CompositeRebalanceListener.delegates(): List<*> =
        CompositeRebalanceListener::class.java
            .getDeclaredField("delegates")
            .apply { isAccessible = true }
            .get(this) as List<*>

    @Test
    fun `onContextRefreshed throws when method has @TransactionalKafkaOffsets but no @KafkaListener`() {
        every { applicationContext.beanDefinitionNames } returns arrayOf("listenerWithoutKafka")
        every { applicationContext.getType("listenerWithoutKafka") } returns ListenerWithoutKafkaAnnotation::class.java
        every { applicationContext.getBean("listenerWithoutKafka") } returns ListenerWithoutKafkaAnnotation()

        assertFailure { customizer.onContextRefreshed() }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `onContextRefreshed throws when method has no ConsumerRecord parameter`() {
        every { applicationContext.beanDefinitionNames } returns arrayOf("listenerWithoutRecordParam")
        every {
            applicationContext.getType("listenerWithoutRecordParam")
        } returns ListenerWithoutRecordParam::class.java
        every { applicationContext.getBean("listenerWithoutRecordParam") } returns ListenerWithoutRecordParam()

        assertFailure { customizer.onContextRefreshed() }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `onContextRefreshed throws when @KafkaListener id is blank`() {
        every { applicationContext.beanDefinitionNames } returns arrayOf("listenerWithBlankId")
        every {
            applicationContext.getType("listenerWithBlankId")
        } returns ListenerWithBlankKafkaListenerId::class.java
        every { applicationContext.getBean("listenerWithBlankId") } returns ListenerWithBlankKafkaListenerId()

        assertFailure { customizer.onContextRefreshed() }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `onContextRefreshed throws when no container found for listener id`() {
        every { endpointRegistry.getListenerContainer(defaultContainerId) } returns null

        assertFailure { customizer.onContextRefreshed() }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `onContextRefreshed throws when container is not an AbstractMessageListenerContainer`() {
        every { endpointRegistry.getListenerContainer(defaultContainerId) } returns mockk<MessageListenerContainer>()

        assertFailure { customizer.onContextRefreshed() }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `onContextRefreshed throws when container has no group id`() {
        every { defaultContainer.groupId } returns null

        assertFailure { customizer.onContextRefreshed() }
            .isInstanceOf(IllegalStateException::class)
    }

    @Test
    fun `onContextRefreshed skips beans with null type`() {
        every { applicationContext.beanDefinitionNames } returns arrayOf("validTestListener", "nullTypeBean")
        every { applicationContext.getType("nullTypeBean") } returns null

        customizer.onContextRefreshed()

        val result = customizer.resolveGroupId(method = defaultMethod)
        assertThat(result).isEqualTo(defaultGroupId)
    }
}

private class ValidTestListener {
    @TransactionalKafkaOffsets
    @org.springframework.kafka.annotation.KafkaListener(id = "valid-container-id", topics = ["valid-topic"])
    fun handle(record: ConsumerRecord<String, String>) {}
}

private class ListenerWithoutKafkaAnnotation {
    @TransactionalKafkaOffsets
    fun handle(record: ConsumerRecord<String, String>) {}
}

private class ListenerWithoutRecordParam {
    @TransactionalKafkaOffsets
    @org.springframework.kafka.annotation.KafkaListener(id = "no-record-param", topics = ["test-topic"])
    fun handle(message: String) {}
}

private class ListenerWithBlankKafkaListenerId {
    @TransactionalKafkaOffsets
    @org.springframework.kafka.annotation.KafkaListener(topics = ["test-topic"])
    fun handle(record: ConsumerRecord<String, String>) {}
}
