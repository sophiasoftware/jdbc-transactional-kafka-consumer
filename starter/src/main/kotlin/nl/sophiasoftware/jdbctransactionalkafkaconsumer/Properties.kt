package nl.sophiasoftware.jdbctransactionalkafkaconsumer

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "jdbc-transactional-kafka-consumer")
class Properties {
    var schemaInitialization: SchemaInitialization = SchemaInitialization.NONE

    enum class SchemaInitialization {
        NONE,
        CREATE,
    }
}
