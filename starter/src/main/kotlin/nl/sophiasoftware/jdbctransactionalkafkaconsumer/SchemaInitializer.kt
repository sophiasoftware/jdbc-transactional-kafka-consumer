package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.InitializingBean
import org.springframework.jdbc.BadSqlGrammarException
import org.springframework.jdbc.core.JdbcTemplate

private val logger = KotlinLogging.logger {}

class SchemaInitializer(
    private val properties: Properties,
    private val jdbcTemplate: JdbcTemplate,
) : InitializingBean {

    override fun afterPropertiesSet() {
        if (tableExists()) {
            logger.info { "Table 'kafka_consumer_offsets' already exists, skipping schema initialization" }
            return
        }

        if (properties.schemaInitialization == Properties.SchemaInitialization.NONE) {
            logger.error { "Table 'kafka_consumer_offsets' does not exist and schema-initialization is set to 'none'" }
            throw IllegalStateException(
                "Table 'kafka_consumer_offsets' does not exist and schema-initialization is set to 'none'. " +
                    "Either create the table manually or set jdbc-transactional-kafka-consumer.schema-initialization=create.",
            )
        }

        logger.info { "Creating table 'kafka_consumer_offsets'" }
        jdbcTemplate.execute(CREATE_TABLE_SQL)
    }

    private fun tableExists(): Boolean = try {
        jdbcTemplate.execute("SELECT 1 FROM kafka_consumer_offsets WHERE 1=0")
        true
    } catch (_: BadSqlGrammarException) {
        false
    }

    companion object {
        private val CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS kafka_consumer_offsets (
                consumer_group VARCHAR(512) NOT NULL,
                topic          VARCHAR(512) NOT NULL,
                partition_id   INT          NOT NULL,
                offset_id      BIGINT       NOT NULL,
                PRIMARY KEY (consumer_group, topic, partition_id)
            )
        """.trimIndent()
    }
}
