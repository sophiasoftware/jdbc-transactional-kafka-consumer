package nl.sophiasoftware.sample

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isNotEmpty
import org.awaitility.kotlin.await
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
    topics = ["sample-single-topic", "sample-batch-topic"],
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

    @Test
    fun `offsets are committed when processing succeeds`(output: CapturedOutput) {
        kafkaTemplate.send("sample-single-topic", "key-1", "hello").get()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val offsets = queryOffsets(topic = "sample-single-topic")
            assertThat(offsets).isNotEmpty()
            assertThat(offsets.last()["committed_offset"] as Long).isGreaterThan(0L)
        }
    }

    @Test
    fun `offsets are not committed when processing fails`(output: CapturedOutput) {
        jdbcTemplate.update(
            "INSERT INTO kafka_consumer_offsets (group_id, topic, partition_id, committed_offset) VALUES (?, ?, ?, ?) ON CONFLICT (group_id, topic, partition_id) DO UPDATE SET committed_offset = EXCLUDED.committed_offset",
            "sample-group", "sample-single-topic", 0, 41L,
        )

        kafkaTemplate.send("sample-single-topic", "key-1", "throw").get()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(output.toString()).contains("Simulated failure for: throw")
        }

        assertThat(queryOffsets(topic = "sample-single-topic").first()["committed_offset"] as Long).isEqualTo(41L)
    }

    @Test
    fun `consumer resumes from stored offset after rebalance`(output: CapturedOutput) {
        kafkaTemplate.send("sample-batch-topic", "key-0", "msg-0").get()
        kafkaTemplate.send("sample-batch-topic", "key-1", "msg-1").get()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val offsets = queryOffsets(topic = "sample-batch-topic")
            assertThat(offsets).isNotEmpty()
            assertThat(offsets.first()["committed_offset"] as Long).isEqualTo(2L)
        }

        jdbcTemplate.update(
            "UPDATE kafka_consumer_offsets SET committed_offset = 1 WHERE topic = ?",
            "sample-batch-topic",
        )

        val container = endpointRegistry.getListenerContainer("batch-listener")!!
        container.stop()
        container.start()

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(output.toString().lines().count { "Batch: key-1 -> msg-1" in it }).isEqualTo(2)
        }

        assertThat(output.toString().lines().count { "Batch: key-0 -> msg-0" in it }).isEqualTo(1)
        assertThat(queryOffsets(topic = "sample-batch-topic").first()["committed_offset"] as Long).isEqualTo(2L)
    }

    private fun queryOffsets(topic: String) = jdbcTemplate.queryForList(
        "SELECT * FROM kafka_consumer_offsets WHERE topic = ?",
        topic,
    )
}
