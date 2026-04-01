package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import assertk.assertFailure
import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.hasClass
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.jdbc.BadSqlGrammarException
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.SQLException

class SchemaInitializerTest {

    private val properties = mockk<Properties>()
    private val jdbcTemplate = mockk<JdbcTemplate>(relaxed = true)
    private lateinit var schemaInitializer: SchemaInitializer

    @BeforeEach
    fun setup() {
        every { properties.schemaInitialization } returns Properties.SchemaInitialization.CREATE
        every { jdbcTemplate.execute(match { it.startsWith("SELECT") }) } throws BadSqlGrammarException("", "", SQLException())
        schemaInitializer = SchemaInitializer(properties = properties, jdbcTemplate = jdbcTemplate)
    }

    @Test
    fun `creates table when schema initialization is CREATE and table does not exist`() {
        schemaInitializer.afterPropertiesSet()

        verify(exactly = 1) { jdbcTemplate.execute(match { it.startsWith("CREATE") }) }
    }

    @Test
    fun `skips schema initialization when table already exists`() {
        every { jdbcTemplate.execute(match { it.startsWith("SELECT") }) } returns Unit

        schemaInitializer.afterPropertiesSet()

        verify(exactly = 0) { jdbcTemplate.execute(match { it.startsWith("CREATE") }) }
    }

    @Test
    fun `throws exception when schema initialization is NONE and table does not exist`() {
        every { properties.schemaInitialization } returns Properties.SchemaInitialization.NONE

        assertFailure { schemaInitializer.afterPropertiesSet() }
            .hasClass(IllegalStateException::class)
    }

    @Test
    fun `SQL contains IF NOT EXISTS to be idempotent`() {
        schemaInitializer.afterPropertiesSet()

        verify { jdbcTemplate.execute(match { it.uppercase().contains("IF NOT EXISTS") }) }
    }
}
