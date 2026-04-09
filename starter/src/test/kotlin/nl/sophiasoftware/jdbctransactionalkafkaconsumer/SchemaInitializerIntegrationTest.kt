package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.hasClass
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

@ExtendWith(SpringExtension::class)
@ContextConfiguration(classes = [SchemaInitializerIntegrationTest.Config::class])
class SchemaInitializerIntegrationTest {
    @Configuration
    class Config {
        private val postgres = PostgreSQLContainer("postgres:17").also { it.start() }

        @Bean
        fun dataSource(): DataSource =
            DriverManagerDataSource(
                postgres.jdbcUrl,
                postgres.username,
                postgres.password,
            )

        @Bean
        fun jdbcTemplate(dataSource: DataSource) = JdbcTemplate(dataSource)
    }

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @BeforeEach
    fun setup() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS kafka_consumer_offsets")
    }

    @Test
    fun `creates kafka_consumer_offsets table when schema initialization is CREATE`() {
        val schemaInitializer =
            SchemaInitializer(
                properties = Properties().apply { schemaInitialization = Properties.SchemaInitialization.CREATE },
                jdbcTemplate = jdbcTemplate,
            )

        schemaInitializer.afterPropertiesSet()

        val tableCount =
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'kafka_consumer_offsets'",
                Int::class.java,
            )
        assertThat(tableCount).isEqualTo(1)
    }

    @Test
    fun `does not create table when schema initialization is NONE and table does not exist`() {
        val schemaInitializer =
            SchemaInitializer(
                properties = Properties().apply { schemaInitialization = Properties.SchemaInitialization.NONE },
                jdbcTemplate = jdbcTemplate,
            )

        assertFailure { schemaInitializer.afterPropertiesSet() }
            .hasClass(IllegalStateException::class)
    }

    @Test
    fun `does not throw when schema initialization is NONE and table exists`() {
        SchemaInitializer(
            properties = Properties().apply { schemaInitialization = Properties.SchemaInitialization.CREATE },
            jdbcTemplate = jdbcTemplate,
        ).afterPropertiesSet()

        val schemaInitializer =
            SchemaInitializer(
                properties = Properties().apply { schemaInitialization = Properties.SchemaInitialization.NONE },
                jdbcTemplate = jdbcTemplate,
            )

        schemaInitializer.afterPropertiesSet()

        val tableCount =
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'kafka_consumer_offsets'",
                Int::class.java,
            )
        assertThat(tableCount).isEqualTo(1)
    }

    @Test
    fun `table creation is idempotent`() {
        val schemaInitializer =
            SchemaInitializer(
                properties = Properties().apply { schemaInitialization = Properties.SchemaInitialization.CREATE },
                jdbcTemplate = jdbcTemplate,
            )

        schemaInitializer.afterPropertiesSet()
        schemaInitializer.afterPropertiesSet()
    }
}
