package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.reflect.MethodSignature
import org.springframework.transaction.support.TransactionTemplate

private val logger = KotlinLogging.logger {}

@Aspect
class Aspect(
    private val transactionTemplate: TransactionTemplate,
    private val repository: OffsetsRepository,
    private val containerCustomizer: ContainerCustomizer,
) {

    @Around("@annotation(nl.sophiasoftware.jdbctransactionalkafkaconsumer.TransactionalKafkaOffsets)")
    fun aroundTransactionalKafkaListener(joinPoint: ProceedingJoinPoint): Any? {
        val nextOffsets = extractNextOffsets(joinPoint = joinPoint)

        if (nextOffsets.isEmpty()) {
            return joinPoint.proceed()
        }

        val method = (joinPoint.signature as MethodSignature).method
        val groupId = containerCustomizer.resolveGroupId(method = method)

        return transactionTemplate.execute {
            val result = joinPoint.proceed()

            repository.saveAll(groupId = groupId, offsets = nextOffsets)

            logger.debug { "Saved offsets for group $groupId: $nextOffsets" }

            result
        }
    }

    private fun extractNextOffsets(joinPoint: ProceedingJoinPoint): Map<TopicPartition, Long> {
        val batchRecords = joinPoint.args.filterIsInstance<ConsumerRecords<*, *>>().firstOrNull()
        if (batchRecords != null) {
            if (batchRecords.isEmpty) {
                return emptyMap()
            }
            return batchRecords.partitions().associateWith { topicPartition ->
                batchRecords.records(topicPartition).maxBy { it.offset() }.offset() + 1
            }
        }

        val singleRecord = joinPoint.args.filterIsInstance<ConsumerRecord<*, *>>().firstOrNull()
        if (singleRecord != null) {
            return mapOf(
                TopicPartition(singleRecord.topic(), singleRecord.partition()) to singleRecord.offset() + 1,
            )
        }

        throw IllegalStateException(
            "No ConsumerRecords or ConsumerRecord argument found in ${joinPoint.signature}"
        )
    }
}
