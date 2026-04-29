package nl.sophiasoftware.sample

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotEmpty
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.util.backoff.FixedBackOff
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Duration

@SpringBootTest
@DirtiesContext
class AcknowledgmentIntegrationTest {
    companion object {
        @JvmStatic
        val kafka: ConfluentKafkaContainer = ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1").apply { start() }

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
        }
    }

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
    lateinit var kafkaAdmin: KafkaAdmin

    private lateinit var adminClient: AdminClient

    @BeforeEach
    fun setup() {
        adminClient = AdminClient.create(kafkaAdmin.configurationProperties)
    }

    @AfterEach
    fun teardown() {
        adminClient.close()
    }

    @Test
    fun `Kafka consumer group offset is committed to broker after acknowledgment`() {
        kafkaTemplate.send("sample-ack-topic", "key-1", "hello").get()

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            val offsets =
                adminClient
                    .listConsumerGroupOffsets("ack-listener")
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .filterKeys { it.topic() == "sample-ack-topic" }

            assertThat(offsets).isNotEmpty()
            assertThat(offsets.getValue(TopicPartition("sample-ack-topic", 0)).offset()).isEqualTo(1L)
        }
    }
}
