package nl.sophiasoftware.sample

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isNotEmpty
import org.awaitility.kotlin.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.util.backoff.FixedBackOff
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Duration

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = ["sample-single-topic", "sample-batch-topic", "sample-ack-topic"],
)
@ExtendWith(OutputCaptureExtension::class)
class IntegrationTest {
    @TestConfiguration
    class Config {
        @Bean
        @ServiceConnection
        fun postgres() = PostgreSQLContainer("postgres:17")

        @Bean
        fun errorHandler() = DefaultErrorHandler(FixedBackOff(0L, 0L))
    }

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    lateinit var endpointRegistry: KafkaListenerEndpointRegistry

    @BeforeEach
    fun setup() {
        jdbcTemplate.execute(
            "CREATE TABLE IF NOT EXISTS processed_messages (key VARCHAR(512), value VARCHAR(512))",
        )
        jdbcTemplate.execute("TRUNCATE TABLE processed_messages")
        jdbcTemplate.execute("TRUNCATE TABLE kafka_consumer_offsets")
    }

    @Test
    fun `offsets are committed when processing succeeds`(output: CapturedOutput) {
        kafkaTemplate.send("sample-single-topic", "key-1", "hello").get()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThatFirstOffsetOfTopic("sample-single-topic").isGreaterThan(0L)
            assertThat(jdbcTemplate.queryForList("SELECT * FROM processed_messages")).isNotEmpty()
        }
    }

    @Test
    fun `offsets are not committed when processing fails`(output: CapturedOutput) {
        kafkaTemplate.send("sample-single-topic", "key-1", "throw").get()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(output.toString()).contains("Simulated failure for: throw")
        }

        assertNoOffsetsForTopic("sample-single-topic")
    }

    @Test
    fun `consumer resumes from stored offset after rebalance`(output: CapturedOutput) {
        kafkaTemplate.send("sample-batch-topic", "key-0", "msg-0").get()
        kafkaTemplate.send("sample-batch-topic", "key-1", "msg-1").get()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThatFirstOffsetOfTopic("sample-batch-topic").isEqualTo(2L)
        }

        jdbcTemplate.update(
            "UPDATE kafka_consumer_offsets SET offset_id = 1 WHERE topic = ?",
            "sample-batch-topic",
        )

        val container = endpointRegistry.getListenerContainer("batch-listener")!!
        container.stop()
        container.start()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(output.toString().lines().count { "Batch: key-1 -> msg-1" in it }).isEqualTo(2)
        }

        assertThat(output.toString().lines().count { "Batch: key-0 -> msg-0" in it }).isEqualTo(1)
        assertThatFirstOffsetOfTopic("sample-batch-topic").isEqualTo(2L)
    }

    @Test
    fun `database insert and offset are both rolled back when processing fails`(output: CapturedOutput) {
        kafkaTemplate.send("sample-single-topic", "key-1", "throw").get()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(output.toString()).contains("Simulated failure for: throw")
        }

        assertThat(jdbcTemplate.queryForList("SELECT * FROM processed_messages")).isEmpty()
        assertNoOffsetsForTopic("sample-single-topic")
    }

    private fun assertNoOffsetsForTopic(topic: String) =
        jdbcTemplate
            .queryForList(
                "SELECT * FROM kafka_consumer_offsets WHERE topic = ?",
                topic,
            ).isEmpty()

    private fun assertThatFirstOffsetOfTopic(topic: String) =
        jdbcTemplate
            .queryForList(
                "SELECT * FROM kafka_consumer_offsets WHERE topic = ?",
                topic,
            ).let { assertThat(it.first()["offset_id"] as Long) }
}
