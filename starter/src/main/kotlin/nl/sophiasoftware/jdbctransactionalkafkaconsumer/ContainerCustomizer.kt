package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.springframework.aop.support.AopUtils
import org.springframework.context.ApplicationContext
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.AbstractMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import java.lang.reflect.Method

private val logger = KotlinLogging.logger {}

class ContainerCustomizer(
    private val applicationContext: ApplicationContext,
    private val endpointRegistry: KafkaListenerEndpointRegistry,
    private val repository: OffsetsRepository,
) {
    private val groupIdsByMethod = mutableMapOf<Method, String>()

    @EventListener(ContextRefreshedEvent::class)
    fun onContextRefreshed() {
        applicationContext.beanDefinitionNames
            .filter { beanName -> applicationContext.getType(beanName) != null }
            .forEach { beanName ->
                val targetClass = AopUtils.getTargetClass(applicationContext.getBean(beanName))

                targetClass.methods
                    .filter { it.isAnnotationPresent(TransactionalKafkaOffsets::class.java) }
                    .forEach { method -> configureForMethod(method = method) }
            }
    }

    fun resolveGroupId(method: Method): String =
        groupIdsByMethod[method]
            ?: throw IllegalStateException(
                "No group ID resolved for ${method.declaringClass.simpleName}#${method.name}. " +
                    "Ensure it is annotated with both @${TransactionalKafkaOffsets::class.simpleName} and @${KafkaListener::class.simpleName}.",
            )

    private fun configureForMethod(method: Method) {
        val location =
            "@${TransactionalKafkaOffsets::class.simpleName} on " +
                "${method.declaringClass.simpleName}#${method.name}"
        val kafkaListener = method.getAnnotation(KafkaListener::class.java)
        if (kafkaListener == null) {
            throw IllegalStateException(
                "$location requires @${KafkaListener::class.simpleName} to be present on the same method.",
            )
        }

        val hasConsumerRecordParam =
            method.parameterTypes.any {
                ConsumerRecords::class.java.isAssignableFrom(it) || ConsumerRecord::class.java.isAssignableFrom(it)
            }
        if (!hasConsumerRecordParam) {
            throw IllegalStateException(
                "$location requires a ${ConsumerRecords::class.simpleName}<*, *> or " +
                    "${ConsumerRecord::class.simpleName}<*, *> parameter.",
            )
        }

        val containerId =
            kafkaListener.id.ifBlank {
                throw IllegalStateException(
                    "$location requires @${KafkaListener::class.simpleName} to have an explicit 'id'.",
                )
            }

        val container =
            endpointRegistry.getListenerContainer(containerId)
                ?: throw IllegalStateException(
                    "No ${AbstractMessageListenerContainer::class.simpleName} found with id '$containerId' " +
                        "for ${method.declaringClass.simpleName}#${method.name}.",
                )

        if (container !is AbstractMessageListenerContainer<*, *>) {
            throw IllegalStateException(
                "Container '$containerId' is not an ${AbstractMessageListenerContainer::class.simpleName}.",
            )
        }

        val groupId =
            container.groupId
                ?: throw IllegalStateException(
                    "Container '$containerId' has no group ID configured. " +
                        "Set groupId on @${KafkaListener::class.simpleName} or configure spring.kafka.consumer.group-id.",
                )

        container.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL

        val storedOffsetListener =
            StoredOffsetRebalanceListener(
                repository = repository,
                groupId = groupId,
            )
        listOfNotNull(container.containerProperties.consumerRebalanceListener, storedOffsetListener)
            .let { CompositeRebalanceListener(delegates = it) }
            .also { container.containerProperties.setConsumerRebalanceListener(it) }

        groupIdsByMethod[method] = groupId

        logger.info { "Configured container '$containerId' (group: $groupId) for transactional offset management" }
    }
}
