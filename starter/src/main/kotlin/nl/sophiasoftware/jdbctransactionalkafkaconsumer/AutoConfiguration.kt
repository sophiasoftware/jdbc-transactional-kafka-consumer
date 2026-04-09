package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.EnableAspectJAutoProxy
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.transaction.support.TransactionTemplate

@AutoConfiguration
@EnableConfigurationProperties(Properties::class)
@EnableAspectJAutoProxy
@ConditionalOnClass(
    KafkaTemplate::class,
    JdbcTemplate::class,
    KafkaListenerEndpointRegistry::class,
    TransactionTemplate::class,
)
class AutoConfiguration {
    @Bean
    fun transactionalKafkaOffsetsRepository(jdbcTemplate: JdbcTemplate) =
        OffsetsRepository(
            jdbcTemplate = jdbcTemplate,
        )

    @Bean
    fun transactionalKafkaSchemaInitializer(
        properties: Properties,
        jdbcTemplate: JdbcTemplate,
    ) = SchemaInitializer(
        properties = properties,
        jdbcTemplate = jdbcTemplate,
    )

    @Bean
    fun transactionalKafkaContainerCustomizer(
        applicationContext: ApplicationContext,
        endpointRegistry: KafkaListenerEndpointRegistry,
        repository: OffsetsRepository,
    ) = ContainerCustomizer(
        applicationContext = applicationContext,
        endpointRegistry = endpointRegistry,
        repository = repository,
    )

    @Bean
    fun transactionalKafkaAspect(
        transactionTemplate: TransactionTemplate,
        repository: OffsetsRepository,
        containerCustomizer: ContainerCustomizer,
    ) = Aspect(
        transactionTemplate = transactionTemplate,
        repository = repository,
        containerCustomizer = containerCustomizer,
    )
}
