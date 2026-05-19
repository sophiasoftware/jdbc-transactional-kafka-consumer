plugins {
    id("com.vanniktech.maven.publish") version "0.36.0"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}

tasks.named<Jar>("jar") {
    enabled = true
}

dependencies {
    api("org.springframework.boot:spring-boot-starter")
    api("org.springframework.boot:spring-boot-starter-aspectj")
    api("org.springframework.boot:spring-boot-starter-jdbc")
    api("org.springframework.boot:spring-boot-starter-kafka")
    api("io.github.oshai:kotlin-logging-jvm:8.0.03")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("io.mockk:mockk:1.14.9")
    testImplementation("com.willowtreeapps.assertk:assertk-jvm:0.28.1")
    testRuntimeOnly("org.postgresql:postgresql")
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    coordinates(groupId = "nl.sophiasoftware", artifactId = "jdbc-transactional-kafka-consumer-spring-boot-starter")

    pom {
        name.set("JDBC Transactional Kafka Consumer Spring Boot Starter")
        description.set(
            "Spring Boot Starter that stores Kafka consumer offsets in a JDBC database within the same transaction as your business logic.",
        )
        url.set("https://github.com/sophiasoftware/jdbc-transactional-kafka-consumer")

        licenses {
            license {
                name.set("MIT License")
                url.set("https://opensource.org/licenses/MIT")
            }
        }

        developers {
            developer {
                id.set("Frank Koornstra")
            }
        }

        scm {
            connection.set("scm:git:git://github.com/sophiasoftware/jdbc-transactional-kafka-consumer.git")
            developerConnection.set("scm:git:ssh://git@github.com/sophiasoftware/jdbc-transactional-kafka-consumer.git")
            url.set("https://github.com/sophiasoftware/jdbc-transactional-kafka-consumer")
        }
    }
}
