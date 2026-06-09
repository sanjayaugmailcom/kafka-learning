plugins {
    kotlin("jvm") version "2.3.20"
    application
}

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass.set("com.example.ProducerKt")
}
