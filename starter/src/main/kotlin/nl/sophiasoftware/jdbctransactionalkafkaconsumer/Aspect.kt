package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.reflect.MethodSignature
import org.slf4j.MDC
import org.springframework.core.KotlinDetector
import org.springframework.kafka.support.Acknowledgment
import org.springframework.transaction.UnexpectedRollbackException
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
        val method = (joinPoint.signature as MethodSignature).method

        if (KotlinDetector.isSuspendingFunction(method)) {
            throw UnsupportedOperationException(
                "@TransactionalKafkaOffsets does not support suspend functions: ${method.name}",
            )
        }

        val acknowledgment = joinPoint.args.filterIsInstance<Acknowledgment>().firstOrNull()
        val groupId = containerCustomizer.resolveGroupId(method = method)

        MDC.put("jtkc-method", method.name)
        MDC.put("jtkc-group-id", groupId)
        logger.info {
            "Starting transactional kafka offsets logic for method ${method.name}, with acknowledgment: ${acknowledgment != null}, group id: $groupId"
        }

        val nextOffsets = extractNextOffsets(joinPoint = joinPoint)
        logger.debug { "Next offsets for group $groupId: $nextOffsets" }

        if (nextOffsets.isEmpty()) {
            logger.warn { "No next offsets found, skipping" }
            return joinPoint.proceed()
        }

        try {
            return transactionTemplate
                .execute {
                    logger.info { "Opening transaction" }
                    logger.debug { "Executing join point" }
                    val result =
                        try {
                            joinPoint.proceed()
                        } catch (exception: Throwable) {
                            logger.error(exception) {
                                "Join point ${method.name} threw an exception, marking transaction as rollback-only"
                            }
                            throw exception
                        }

                    logger.debug { "Executed join point, saving offsets for group $groupId: $nextOffsets" }
                    repository.saveAll(groupId = groupId, offsets = nextOffsets)

                    logger.info { "Saved offsets, closing transaction and returning result" }
                    result
                }.also {
                    acknowledgment
                        ?.also { logger.info { "Sending offset acknowledgment for group $groupId" } }
                        ?.acknowledge()
                }
        } catch (exception: UnexpectedRollbackException) {
            logger.error(exception) {
                "Transaction for method ${method.name} was rolled back because it was already marked " +
                    "rollback-only; see preceding log entries for the exception that originally caused this"
            }
            throw exception
        } finally {
            MDC.remove("jtkc-method")
            MDC.remove("jtkc-group-id")
        }
    }

    private fun extractNextOffsets(joinPoint: ProceedingJoinPoint): Map<TopicPartition, Long> {
        val batchRecords = joinPoint.args.filterIsInstance<ConsumerRecords<*, *>>().firstOrNull()
        if (batchRecords != null) {
            logger.debug {
                val topicsAndPartitions =
                    batchRecords
                        .partitions()
                        .joinToString(", ") { "${it.topic()}:${it.partition()}" }
                "Extracting offsets for batch records in partitions $topicsAndPartitions"
            }
            if (batchRecords.isEmpty) {
                return emptyMap()
            }
            return batchRecords.partitions().associateWith { topicPartition ->
                batchRecords.records(topicPartition).maxBy { it.offset() }.offset() + 1
            }
        }

        val singleRecord = joinPoint.args.filterIsInstance<ConsumerRecord<*, *>>().firstOrNull()
        if (singleRecord != null) {
            logger.debug {
                "Extracting offsets for single record in partition ${singleRecord.topic()}:${singleRecord.partition()}"
            }
            return mapOf(
                TopicPartition(singleRecord.topic(), singleRecord.partition()) to singleRecord.offset() + 1,
            )
        }

        throw IllegalStateException(
            "No ConsumerRecords or ConsumerRecord argument found in ${joinPoint.signature}",
        )
    }
}
