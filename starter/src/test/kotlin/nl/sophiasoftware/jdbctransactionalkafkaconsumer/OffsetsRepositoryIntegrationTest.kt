package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.springframework.transaction.IllegalTransactionStateException
import org.springframework.transaction.annotation.EnableTransactionManagement
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

@ExtendWith(SpringExtension::class)
@ContextConfiguration(classes = [OffsetsRepositoryIntegrationTest.Config::class])
@Transactional
class OffsetsRepositoryIntegrationTest {

    @Configuration
    @EnableTransactionManagement
    class Config {
        private val postgres = PostgreSQLContainer("postgres:17").also { it.start() }

        @Bean
        fun dataSource(): DataSource = DriverManagerDataSource(
            postgres.jdbcUrl,
            postgres.username,
            postgres.password,
        )

        @Bean
        fun jdbcTemplate(dataSource: DataSource) = JdbcTemplate(dataSource)

        @Bean
        fun transactionManager(dataSource: DataSource) = DataSourceTransactionManager(dataSource)

        @Bean
        fun schemaInitializer(jdbcTemplate: JdbcTemplate) = SchemaInitializer(
            properties = Properties().apply { schemaInitialization = Properties.SchemaInitialization.CREATE },
            jdbcTemplate = jdbcTemplate,
        )

        @Bean
        fun offsetsRepository(jdbcTemplate: JdbcTemplate) = OffsetsRepository(jdbcTemplate = jdbcTemplate)
    }

    private val defaultGroupId = "payments-consumer-group"
    private val defaultTopic = "payments"
    private val defaultPartition = 3
    private val defaultTopicPartition = TopicPartition(defaultTopic, defaultPartition)
    private val defaultOffset = 41L

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var repository: OffsetsRepository

    @BeforeEach
    fun setup() {
        jdbcTemplate.update(
            "INSERT INTO kafka_consumer_offsets (consumer_group, topic, partition_id, offset_id) VALUES (?, ?, ?, ?)",
            defaultGroupId, defaultTopic, defaultPartition, defaultOffset,
        )
    }

    @AfterEach
    fun tearDown() {
        jdbcTemplate.execute("TRUNCATE TABLE kafka_consumer_offsets")
    }

    @Test
    fun `findOffsets returns stored offset for matching group and partition`() {
        val result = repository.findOffsets(
            groupId = defaultGroupId,
            topicPartitions = listOf(defaultTopicPartition),
        )

        assertThat(result).isEqualTo(mapOf(defaultTopicPartition to defaultOffset))
    }

    @Test
    fun `findOffsets returns empty for partition with no stored offset`() {
        val unknownPartition = TopicPartition(defaultTopic, 7)

        val result = repository.findOffsets(
            groupId = defaultGroupId,
            topicPartitions = listOf(unknownPartition),
        )

        assertThat(result).isEmpty()
    }

    @Test
    fun `findOffsets does not return offsets for different group`() {
        val result = repository.findOffsets(
            groupId = "other-consumer-group",
            topicPartitions = listOf(defaultTopicPartition),
        )

        assertThat(result).isEmpty()
    }

    @Test
    fun `findOffsets returns only offsets for requested partitions`() {
        val anotherPartition = TopicPartition(defaultTopic, 7)
        repository.saveAll(groupId = defaultGroupId, offsets = mapOf(anotherPartition to 97L))

        val result = repository.findOffsets(
            groupId = defaultGroupId,
            topicPartitions = listOf(defaultTopicPartition),
        )

        assertThat(result).isEqualTo(mapOf(defaultTopicPartition to defaultOffset))
    }

    @Test
    fun `saveAll updates existing offset`() {
        val updatedOffset = 83L
        repository.saveAll(
            groupId = defaultGroupId,
            offsets = mapOf(defaultTopicPartition to updatedOffset),
        )

        val result = repository.findOffsets(
            groupId = defaultGroupId,
            topicPartitions = listOf(defaultTopicPartition),
        )

        assertThat(result).isEqualTo(mapOf(defaultTopicPartition to updatedOffset))
    }

    @Test
    fun `saveAll persists offsets for multiple partitions`() {
        val anotherPartition = TopicPartition(defaultTopic, 7)
        repository.saveAll(groupId = defaultGroupId, offsets = mapOf(anotherPartition to 97L))

        val result = repository.findOffsets(
            groupId = defaultGroupId,
            topicPartitions = listOf(defaultTopicPartition, anotherPartition),
        )

        assertThat(result).isEqualTo(mapOf(defaultTopicPartition to defaultOffset, anotherPartition to 97L))
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun `saveAll throws when called without active transaction`() {
        assertFailure {
            repository.saveAll(
                groupId = defaultGroupId,
                offsets = mapOf(defaultTopicPartition to defaultOffset),
            )
        }.isInstanceOf(IllegalTransactionStateException::class)
    }
}
