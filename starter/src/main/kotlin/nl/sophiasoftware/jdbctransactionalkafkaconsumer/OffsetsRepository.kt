package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import org.apache.kafka.common.TopicPartition
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional

open class OffsetsRepository(
    private val jdbcTemplate: JdbcTemplate,
) {

    open fun findOffsets(
        groupId: String,
        topicPartitions: Collection<TopicPartition>,
    ): Map<TopicPartition, Long> =
        topicPartitions.mapNotNull { tp ->
            jdbcTemplate.queryForList(SELECT_OFFSET_SQL, groupId, tp.topic(), tp.partition())
                .firstOrNull()
                ?.let { row -> tp to row["offset_id"] as Long }
        }.toMap()

    @Transactional(propagation = Propagation.MANDATORY)
    open fun saveAll(groupId: String, offsets: Map<TopicPartition, Long>) {
        offsets.forEach { (topicPartition, offset) ->
            jdbcTemplate.update(
                UPSERT_OFFSET_SQL,
                groupId,
                topicPartition.topic(),
                topicPartition.partition(),
                offset,
            )
        }
    }

    companion object {
        private val SELECT_OFFSET_SQL = """
            SELECT offset_id
            FROM kafka_consumer_offsets
            WHERE consumer_group = ? AND topic = ? AND partition_id = ?
        """.trimIndent()

        private val UPSERT_OFFSET_SQL = """
            INSERT INTO kafka_consumer_offsets (consumer_group, topic, partition_id, offset_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (consumer_group, topic, partition_id) DO UPDATE SET offset_id = EXCLUDED.offset_id
        """.trimIndent()
    }
}
